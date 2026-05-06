CREATE TABLE lk_death_adm_mapped AS
SELECT DISTINCT src.subject_id,
    nth_value(src.deathtime, 1)   over(
        partition BY src.subject_id
        ORDER BY src.admittime ASC
    )                                   AS deathtime,
    nth_value(src.dischtime, 1) over(
        partition BY src.subject_id
        ORDER BY src.admittime ASC
    )                                   AS dischtime,
    32817 AS type_concept_id, -- OMOP4976890 EHR
                --
                'admissions'      AS         unit_id,
                src.load_table_id AS         load_table_id,
    nth_value(src.load_row_id, 1) over(
        partition BY src.subject_id
        ORDER BY src.admittime ASC
    )                                   AS load_row_id,
    -- nth_value(src.trace_id, 1) over(
    nth_value(src.trace_id::text, 1) over(
        partition BY src.subject_id
        ORDER BY src.admittime ASC
    )                                   AS trace_id
FROM src_admissions src -- adm
WHERE src.deathtime IS NOT NULL
;

-- -------------------------------------------------------------------
-- cdm_death
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm_death;
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_death
(
    person_id               INTEGER     NOT NULL ,
    death_date              DATE      NOT NULL ,
    death_datetime          TIMESTAMP           ,
    death_type_concept_id   INTEGER     NOT NULL ,
    cause_concept_id        INTEGER              ,
    cause_source_value      text             ,
    cause_source_concept_id INTEGER              ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

INSERT INTO cdm_death
SELECT per.person_id                 AS person_id,
       CASE WHEN src.deathtime <= src.dischtime THEN src.deathtime::date ELSE src.dischtime::date END AS death_date,
       CASE WHEN src.deathtime <= src.dischtime THEN src.deathtime ELSE src.dischtime END             AS death_datetime,
       src.type_concept_id           AS death_type_concept_id,
       NULL                          AS cause_concept_id,
       CAST(NULL AS text)          AS cause_source_value,
       NULL                          AS cause_source_concept_id,
       --
       concat('death.', src.unit_id) AS unit_id,
       src.load_table_id             AS load_table_id,
       src.load_row_id               AS load_row_id,
       src.trace_id                  AS trace_id
FROM lk_death_adm_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
;