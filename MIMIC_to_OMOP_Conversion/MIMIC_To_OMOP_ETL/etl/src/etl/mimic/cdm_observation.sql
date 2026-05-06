DROP TABLE IF EXISTS cdm_observation;


CREATE TABLE cdm_observation
(
    observation_id                INTEGER     NOT NULL ,
    person_id                     INTEGER     NOT NULL ,
    observation_concept_id        INTEGER     NOT NULL ,
    observation_date              DATE      NOT NULL ,
    observation_datetime          TIMESTAMP           ,
    observation_type_concept_id   INTEGER     NOT NULL ,
    value_as_number               NUMERIC        ,
    value_as_text               text         ,
    value_as_concept_id           INTEGER          ,
    qualifier_concept_id          INTEGER          ,
    unit_concept_id               INTEGER          ,
    provider_id                   INTEGER          ,
    visit_occurrence_id           INTEGER          ,
    visit_detail_id               INTEGER          ,
    observation_source_value      text         ,
    observation_source_concept_id INTEGER          ,
    unit_source_value             text         ,
    qualifier_source_value        text         ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

-- -------------------------------------------------------------------
-- Rules 1-4
-- lk_observation_mapped (demographics and DRG codes)
-- -------------------------------------------------------------------

INSERT INTO cdm_observation
SELECT row_number() OVER ()                        AS observation_id,
       per.person_id                       AS person_id,
       src.target_concept_id               AS observation_concept_id,
       CAST(src.start_datetime AS DATE)    AS observation_date,
       src.start_datetime                  AS observation_datetime,
       src.type_concept_id                 AS observation_type_concept_id,
       CAST(NULL AS NUMERIC)               AS value_as_number,
       src.value_as_text                 AS value_as_text,
       CASE WHEN src.value_as_text IS NOT NULL THEN COALESCE(src.value_as_concept_id, 0) END  AS value_as_concept_id,
       CAST(NULL AS INTEGER)               AS qualifier_concept_id,
       CAST(NULL AS INTEGER)               AS unit_concept_id,
       CAST(NULL AS INTEGER)               AS provider_id,
       vis.visit_occurrence_id             AS visit_occurrence_id,
       CAST(NULL AS INTEGER)               AS visit_detail_id,
       src.source_code                     AS observation_source_value,
       src.source_concept_id               AS observation_source_concept_id,
       CAST(NULL AS text)                AS unit_source_value,
       CAST(NULL AS text)                AS qualifier_source_value,
       --
       concat('observation.', src.unit_id) AS unit_id,
       src.load_table_id                   AS load_table_id,
       src.load_row_id                     AS load_row_id,
       src.trace_id                        AS trace_id
FROM lk_observation_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 5
-- chartevents
-- -------------------------------------------------------------------

INSERT INTO cdm_observation
SELECT src.measurement_id                  AS observation_id, -- id is generated already
       per.person_id                       AS person_id,
       src.target_concept_id               AS observation_concept_id,
       CAST(src.start_datetime AS DATE)    AS observation_date,
       src.start_datetime                  AS observation_datetime,
       src.type_concept_id                 AS observation_type_concept_id,
       src.value_as_number                 AS value_as_number,
       src.value_source_value              AS value_as_text,
       CASE WHEN src.value_source_value IS NOT NULL THEN COALESCE(src.value_as_concept_id, 0) END AS value_as_concept_id,
       CAST(NULL AS INTEGER)               AS qualifier_concept_id,
       src.unit_concept_id                 AS unit_concept_id,
       CAST(NULL AS INTEGER)               AS provider_id,
       vis.visit_occurrence_id             AS visit_occurrence_id,
       CAST(NULL AS INTEGER)               AS visit_detail_id,
       src.source_code                     AS observation_source_value,
       src.source_concept_id               AS observation_source_concept_id,
       src.unit_source_value               AS unit_source_value,
       CAST(NULL AS text)                AS qualifier_source_value,
       --
       concat('observation.', src.unit_id) AS unit_id,
       src.load_table_id                   AS load_table_id,
       src.load_row_id                     AS load_row_id,
       src.trace_id                        AS trace_id
FROM lk_chartevents_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 6
-- lk_procedure_mapped
-- -------------------------------------------------------------------

INSERT INTO cdm_observation
SELECT row_number() OVER ()                        AS observation_id,
       per.person_id                       AS person_id,
       src.target_concept_id               AS observation_concept_id,
       CAST(src.start_datetime AS DATE)    AS observation_date,
       src.start_datetime                  AS observation_datetime,
       src.type_concept_id                 AS observation_type_concept_id,
       CAST(NULL AS NUMERIC)               AS value_as_number,
       CAST(NULL AS text)                AS value_as_text,
       CAST(NULL AS INTEGER)               AS value_as_concept_id,
       CAST(NULL AS INTEGER)               AS qualifier_concept_id,
       CAST(NULL AS INTEGER)               AS unit_concept_id,
       CAST(NULL AS INTEGER)               AS provider_id,
       vis.visit_occurrence_id             AS visit_occurrence_id,
       CAST(NULL AS INTEGER)               AS visit_detail_id,
       src.source_code                     AS observation_source_value,
       src.source_concept_id               AS observation_source_concept_id,
       CAST(NULL AS text)                AS unit_source_value,
       CAST(NULL AS text)                AS qualifier_source_value,
       --
       concat('observation.', src.unit_id) AS unit_id,
       src.load_table_id                   AS load_table_id,
       src.load_row_id                     AS load_row_id,
       src.trace_id                        AS trace_id
FROM lk_procedure_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 7
-- diagnoses
-- -------------------------------------------------------------------

INSERT INTO cdm_observation
SELECT row_number() OVER ()                        AS observation_id,
       per.person_id                       AS person_id,
       src.target_concept_id               AS observation_concept_id, -- to rename fields in *_mapped
       CAST(src.start_datetime AS DATE)    AS observation_date,
       src.start_datetime                  AS observation_datetime,
       src.type_concept_id                 AS observation_type_concept_id,
       CAST(NULL AS NUMERIC)               AS value_as_number,
       CAST(NULL AS text)                AS value_as_text,
       CAST(NULL AS INTEGER)               AS value_as_concept_id,
       CAST(NULL AS INTEGER)               AS qualifier_concept_id,
       CAST(NULL AS INTEGER)               AS unit_concept_id,
       CAST(NULL AS INTEGER)               AS provider_id,
       vis.visit_occurrence_id             AS visit_occurrence_id,
       CAST(NULL AS INTEGER)               AS visit_detail_id,
       src.source_code                     AS observation_source_value,
       src.source_concept_id               AS observation_source_concept_id,
       CAST(NULL AS text)                AS unit_source_value,
       CAST(NULL AS text)                AS qualifier_source_value,
       --
       concat('observation.', src.unit_id) AS unit_id,
       src.load_table_id                   AS load_table_id,
       src.load_row_id                     AS load_row_id,
       src.trace_id                        AS trace_id
FROM lk_diagnoses_icd_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Observation'
;

-- -------------------------------------------------------------------
-- Rule 8
-- lk_specimen_mapped
-- -------------------------------------------------------------------

INSERT INTO cdm_observation
SELECT row_number() OVER ()                        AS observation_id,
       per.person_id                       AS person_id,
       src.target_concept_id               AS observation_concept_id,
       CAST(src.start_datetime AS DATE)    AS observation_date,
       src.start_datetime                  AS observation_datetime,
       src.type_concept_id                 AS observation_type_concept_id,
       CAST(NULL AS NUMERIC)               AS value_as_number,
       CAST(NULL AS text)                AS value_as_text,
       CAST(NULL AS INTEGER)               AS value_as_concept_id,
       CAST(NULL AS INTEGER)               AS qualifier_concept_id,
       CAST(NULL AS INTEGER)               AS unit_concept_id,
       CAST(NULL AS INTEGER)               AS provider_id,
       vis.visit_occurrence_id             AS visit_occurrence_id,
       CAST(NULL AS INTEGER)               AS visit_detail_id,
       src.source_code                     AS observation_source_value,
       src.source_concept_id               AS observation_source_concept_id,
       CAST(NULL AS text)                AS unit_source_value,
       CAST(NULL AS text)                AS qualifier_source_value,
       --
       concat('observation.', src.unit_id) AS unit_id,
       src.load_table_id                   AS load_table_id,
       src.load_row_id                     AS load_row_id,
       src.trace_id                        AS trace_id
FROM lk_specimen_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|',
               COALESCE(CAST(src.hadm_id AS text), CAST(src.date_id AS text)))
WHERE src.target_domain_id = 'Observation'
;

