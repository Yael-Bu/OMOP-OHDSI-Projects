DROP TABLE IF EXISTS cdm_condition_occurrence;

CREATE TABLE cdm_condition_occurrence
(
    condition_occurrence_id       INTEGER     NOT NULL ,
    person_id                     INTEGER     NOT NULL ,
    condition_concept_id          INTEGER     NOT NULL ,
    condition_start_date          DATE      NOT NULL ,
    condition_start_datetime      TIMESTAMP           ,
    condition_end_date            DATE               ,
    condition_end_datetime        TIMESTAMP           ,
    condition_type_concept_id     INTEGER     NOT NULL ,
    stop_reason                   text             ,
    provider_id                   INTEGER              ,
    visit_occurrence_id           INTEGER              ,
    visit_detail_id               INTEGER              ,
    condition_source_value        text             ,
    condition_source_concept_id   INTEGER              ,
    condition_status_source_value text             ,
    condition_status_concept_id   INTEGER              ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

-- -------------------------------------------------------------------
-- Rule 1
-- diagnoses
-- -------------------------------------------------------------------

INSERT INTO cdm_condition_occurrence
SELECT row_number() OVER ()                       AS condition_occurrence_id,
       per.person_id                      AS person_id,
       COALESCE(src.target_concept_id, 0) AS condition_concept_id,
       CAST(src.start_datetime AS DATE)   AS condition_start_date,
       src.start_datetime                 AS condition_start_datetime,
       CAST(src.end_datetime AS DATE)     AS condition_end_date,
       src.end_datetime                   AS condition_end_datetime,
       src.type_concept_id                AS condition_type_concept_id,
       CAST(NULL AS text)               AS stop_reason,
       CAST(NULL AS INTEGER)              AS provider_id,
       vis.visit_occurrence_id            AS visit_occurrence_id,
       CAST(NULL AS INTEGER)              AS visit_detail_id,
       src.source_code                    AS condition_source_value,
       COALESCE(src.source_concept_id, 0) AS condition_source_concept_id,
       CAST(NULL AS text)               AS condition_status_source_value,
       CAST(NULL AS INTEGER)              AS condition_status_concept_id,
       --
       concat('condition.', src.unit_id)  AS unit_id,
       src.load_table_id                  AS load_table_id,
       src.load_row_id                    AS load_row_id,
       src.trace_id                       AS trace_id
FROM lk_diagnoses_icd_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Condition'
;

-- -------------------------------------------------------------------
-- rule 2
-- Chartevents.value
-- -------------------------------------------------------------------

INSERT INTO cdm_condition_occurrence
SELECT row_number() OVER ()                       AS condition_occurrence_id,
       per.person_id                      AS person_id,
       COALESCE(src.target_concept_id, 0) AS condition_concept_id,
       CAST(src.start_datetime AS DATE)   AS condition_start_date,
       src.start_datetime                 AS condition_start_datetime,
       CAST(src.start_datetime AS DATE)   AS condition_end_date,
       src.start_datetime                 AS condition_end_datetime,
       32817                              AS condition_type_concept_id, -- EHR  Type Concept    Type Concept
       CAST(NULL AS text)               AS stop_reason,
       CAST(NULL AS INTEGER)              AS provider_id,
       vis.visit_occurrence_id            AS visit_occurrence_id,
       CAST(NULL AS INTEGER)              AS visit_detail_id,
       src.source_code                    AS condition_source_value,
       COALESCE(src.source_concept_id, 0) AS condition_source_concept_id,
       CAST(NULL AS text)               AS condition_status_source_value,
       CAST(NULL AS INTEGER)              AS condition_status_concept_id,
       --
       concat('condition.', src.unit_id)  AS unit_id,
       src.load_table_id                  AS load_table_id,
       src.load_row_id                    AS load_row_id,
       src.trace_id                       AS trace_id
FROM lk_chartevents_condition_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Condition'
;



-- -------------------------------------------------------------------
-- rule 3
-- Chartevents
-- -------------------------------------------------------------------

INSERT INTO cdm_condition_occurrence
SELECT row_number() OVER ()                       AS condition_occurrence_id,
       per.person_id                      AS person_id,
       COALESCE(src.target_concept_id, 0) AS condition_concept_id,
       CAST(src.start_datetime AS DATE)   AS condition_start_date,
       src.start_datetime                 AS condition_start_datetime,
       CAST(src.start_datetime AS DATE)   AS condition_end_date,
       src.start_datetime                 AS condition_end_datetime,
       src.type_concept_id                AS condition_type_concept_id,
       CAST(NULL AS text)               AS stop_reason,
       CAST(NULL AS INTEGER)              AS provider_id,
       vis.visit_occurrence_id            AS visit_occurrence_id,
       CAST(NULL AS INTEGER)              AS visit_detail_id,
       src.source_code                    AS condition_source_value,
       COALESCE(src.source_concept_id, 0) AS condition_source_concept_id,
       CAST(NULL AS text)               AS condition_status_source_value,
       CAST(NULL AS INTEGER)              AS condition_status_concept_id,
       --
       concat('condition.', src.unit_id)  AS unit_id,
       src.load_table_id                  AS load_table_id,
       src.load_row_id                    AS load_row_id,
       src.trace_id                       AS trace_id
FROM lk_chartevents_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Condition'
;


