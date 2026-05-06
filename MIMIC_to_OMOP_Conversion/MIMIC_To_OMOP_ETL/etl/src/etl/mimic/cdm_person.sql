CREATE TABLE tmp_subject_ethnicity AS
SELECT DISTINCT src.subject_id AS          subject_id,
        nth_value(src.ethnicity, 1) over (
        partition BY src.subject_id
        ORDER BY src.admittime ASC)     AS ethnicity_first
FROM src_admissions src
;

-- -------------------------------------------------------------------
-- lk_pat_ethnicity_concept
-- -------------------------------------------------------------------

CREATE TABLE lk_pat_ethnicity_concept AS
SELECT DISTINCT src.ethnicity_first AS source_code,
                vc.concept_id       AS source_concept_id,
                vc.vocabulary_id    AS source_vocabulary_id,
                vc1.concept_id      AS target_concept_id,
                vc1.vocabulary_id   AS target_vocabulary_id -- look here to distinguish Race and Ethnicity
FROM tmp_subject_ethnicity src
         LEFT JOIN
     -- gcpt_ethnicity_to_concept -> mimiciv_per_ethnicity
         voc_concept vc
    --  ON UPPER(vc.concept_code) = UPPER(src.ethnicity_first) -- do the custom mapping

        ON UPPER(vc.concept_code) = UPPER(TRIM(REPLACE(src.ethnicity_first, ' OR ', '/'))) 
         AND vc.domain_id IN ('Race', 'Ethnicity')
         LEFT JOIN
     voc_concept_relationship cr1
     ON cr1.concept_id_1 = vc.concept_id
         AND cr1.relationship_id = 'Maps to'
         LEFT JOIN
     voc_concept vc1
     ON cr1.concept_id_2 = vc1.concept_id
         AND vc1.invalid_reason IS NULL
         AND vc1.standard_concept = 'S'
;

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm_person;

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_person
(
    person_id                   INTEGER     NOT NULL ,
    gender_concept_id           INTEGER     NOT NULL ,
    year_of_birth               INTEGER     NOT NULL ,
    month_of_birth              INTEGER              ,
    day_of_birth                INTEGER              ,
    birth_datetime              TIMESTAMP           ,
    race_concept_id             INTEGER     NOT NULL,
    ethnicity_concept_id        INTEGER     NOT NULL,
    location_id                 INTEGER              ,
    provider_id                 INTEGER              ,
    care_site_id                INTEGER              ,
    person_source_value         text             ,
    gender_source_value         text             ,
    gender_source_concept_id    INTEGER              ,
    race_source_value           text             ,
    race_source_concept_id      INTEGER              ,
    ethnicity_source_value      text             ,
    ethnicity_source_concept_id INTEGER              ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

INSERT INTO cdm_person
SELECT row_number() OVER (ORDER BY random())                 AS person_id,
       CASE
           WHEN p.gender = 'F' THEN 8532 -- FEMALE
           WHEN p.gender = 'M' THEN 8507 -- MALE
           ELSE 0
           END                      AS gender_concept_id,
       p.anchor_year                AS year_of_birth,
       CAST(NULL AS INTEGER)        AS month_of_birth,
       CAST(NULL AS INTEGER)        AS day_of_birth,
       CAST(NULL AS TIMESTAMP)      AS birth_datetime,
       COALESCE(
               CASE
                   WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                       THEN map_eth.target_concept_id
                   ELSE NULL
                   END, 0)          AS race_concept_id,
       COALESCE(
               CASE
                   WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                       THEN map_eth.target_concept_id
                   ELSE NULL
                   END, 0)          AS ethnicity_concept_id,
       CAST(NULL AS INTEGER)        AS location_id,
       CAST(NULL AS INTEGER)        AS provider_id,
       CAST(NULL AS INTEGER)        AS care_site_id,
       CAST(p.subject_id AS text) AS person_source_value,
       p.gender                     AS gender_source_value,
       0                            AS gender_source_concept_id,
       CASE
           WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
               THEN eth.ethnicity_first
           ELSE NULL
           END                      AS race_source_value,
           COALESCE(
        CASE
            -- אם מדובר במזהה חדש (מעל 2,000,000,000) ואינו שייך ל-"Ethnicity"
            -- WHEN map_eth.source_concept_id > 2000000000 AND map_eth.target_vocabulary_id <> 'Ethnicity'
            --     THEN map_eth.source_concept_id
            -- אם מדובר במיפוי רגיל שאינו אתניות
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.source_concept_id
            -- אם אף אחד מהתנאים לא מתקיים, החזר NULL
            ELSE NULL
        END, 0
        ) AS race_source_concept_id,

    --    COALESCE(
    --            CASE
    --                WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
    --                    THEN map_eth.source_concept_id
    --                ELSE NULL
    --                END, 0)          AS race_source_concept_id,
       CASE
           WHEN map_eth.target_vocabulary_id = 'Ethnicity'
               THEN eth.ethnicity_first
           ELSE NULL
           END                      AS ethnicity_source_value,
       COALESCE(
               CASE
                   WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                       THEN map_eth.source_concept_id
                   ELSE NULL
                   END, 0)          AS ethnicity_source_concept_id,
       --
       'person.patients'            AS unit_id,
       p.load_table_id              AS load_table_id,
       p.load_row_id                AS load_row_id,
       p.trace_id                   AS trace_id
FROM src_patients p
         LEFT JOIN
     tmp_subject_ethnicity eth
     ON p.subject_id = eth.subject_id
         LEFT JOIN
     lk_pat_ethnicity_concept map_eth
     ON eth.ethnicity_first = map_eth.source_code
;


-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

-- DROP TABLE if EXISTS tmp_subject_ethnicity;

