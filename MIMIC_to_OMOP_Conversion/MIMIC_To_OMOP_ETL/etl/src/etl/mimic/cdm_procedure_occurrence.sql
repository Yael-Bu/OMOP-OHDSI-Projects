DROP TABLE IF EXISTS cdm_procedure_occurrence;


CREATE TABLE cdm_procedure_occurrence
(
    procedure_occurrence_id     INTEGER     NOT NULL ,
    person_id                   INTEGER     NOT NULL ,
    procedure_concept_id        INTEGER     NOT NULL ,
    procedure_date              DATE      NOT NULL ,
    procedure_datetime          TIMESTAMP           ,
    procedure_type_concept_id   INTEGER     NOT NULL ,
    modifier_concept_id         INTEGER              ,
    quantity                    INTEGER              ,
    provider_id                 INTEGER              ,
    visit_occurrence_id         INTEGER              ,
    visit_detail_id             INTEGER              ,
    procedure_source_value      text             ,
    procedure_source_concept_id INTEGER              ,
    modifier_source_value      text              ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

-- -------------------------------------------------------------------
-- Rules 1-4
-- lk_procedure_mapped
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT row_number() OVER ()                      AS procedure_occurrence_id,
       per.person_id                     AS person_id,
       src.target_concept_id             AS procedure_concept_id,
       CAST(src.start_datetime AS DATE)  AS procedure_date,
       src.start_datetime                AS procedure_datetime,
       src.type_concept_id               AS procedure_type_concept_id,
       0                                 AS modifier_concept_id,
       CAST(src.quantity AS INTEGER)     AS quantity,
       CAST(NULL AS INTEGER)             AS provider_id,
       vis.visit_occurrence_id           AS visit_occurrence_id,
       CAST(NULL AS INTEGER)             AS visit_detail_id,
       src.source_code                   AS procedure_source_value,
       src.source_concept_id             AS procedure_source_concept_id,
       CAST(NULL AS text)              AS modifier_source_value,
       --
       concat('procedure.', src.unit_id) AS unit_id,
       src.load_table_id                 AS load_table_id,
       src.load_row_id                   AS load_row_id,
       src.trace_id                      AS trace_id
FROM lk_procedure_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Procedure'
;

-- -------------------------------------------------------------------
-- Rule 5
-- lk_observation_mapped, possible DRG codes
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT row_number() OVER ()                      AS procedure_occurrence_id,
       per.person_id                     AS person_id,
       src.target_concept_id             AS procedure_concept_id,
       CAST(src.start_datetime AS DATE)  AS procedure_date,
       src.start_datetime                AS procedure_datetime,
       src.type_concept_id               AS procedure_type_concept_id,
       0                                 AS modifier_concept_id,
       CAST(NULL AS INTEGER)             AS quantity,
       CAST(NULL AS INTEGER)             AS provider_id,
       vis.visit_occurrence_id           AS visit_occurrence_id,
       CAST(NULL AS INTEGER)             AS visit_detail_id,
       src.source_code                   AS procedure_source_value,
       src.source_concept_id             AS procedure_source_concept_id,
       CAST(NULL AS text)              AS modifier_source_value,
       --
       concat('procedure.', src.unit_id) AS unit_id,
       src.load_table_id                 AS load_table_id,
       src.load_row_id                   AS load_row_id,
       src.trace_id                      AS trace_id
FROM lk_observation_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Procedure'
;

-- -------------------------------------------------------------------
-- Rule 6
-- lk_specimen_mapped, small part of specimen is mapped to Procedure
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT row_number() OVER ()                      AS procedure_occurrence_id,
       per.person_id                     AS person_id,
       src.target_concept_id             AS procedure_concept_id,
       CAST(src.start_datetime AS DATE)  AS procedure_date,
       src.start_datetime                AS procedure_datetime,
       src.type_concept_id               AS procedure_type_concept_id,
       0                                 AS modifier_concept_id,
       CAST(NULL AS INTEGER)             AS quantity,
       CAST(NULL AS INTEGER)             AS provider_id,
       vis.visit_occurrence_id           AS visit_occurrence_id,
       CAST(NULL AS INTEGER)             AS visit_detail_id,
       src.source_code                   AS procedure_source_value,
       src.source_concept_id             AS procedure_source_concept_id,
       CAST(NULL AS text)              AS modifier_source_value,
       --
       concat('procedure.', src.unit_id) AS unit_id,
       src.load_table_id                 AS load_table_id,
       src.load_row_id                   AS load_row_id,
       src.trace_id                      AS trace_id
FROM lk_specimen_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|',
               COALESCE(CAST(src.hadm_id AS text), CAST(src.date_id AS text)))
WHERE src.target_domain_id = 'Procedure'
;


-- -------------------------------------------------------------------
-- Rule 7
-- lk_chartevents_mapped, a part of chartevents table is mapped to Procedure
-- -------------------------------------------------------------------

INSERT INTO cdm_procedure_occurrence
SELECT row_number() OVER ()                      AS procedure_occurrence_id,
       per.person_id                     AS person_id,
       src.target_concept_id             AS procedure_concept_id,
       CAST(src.start_datetime AS DATE)  AS procedure_date,
       src.start_datetime                AS procedure_datetime,
       src.type_concept_id               AS procedure_type_concept_id,
       0                                 AS modifier_concept_id,
       CAST(NULL AS INTEGER)             AS quantity,
       CAST(NULL AS INTEGER)             AS provider_id,
       vis.visit_occurrence_id           AS visit_occurrence_id,
       CAST(NULL AS INTEGER)             AS visit_detail_id,
       src.source_code                   AS procedure_source_value,
       src.source_concept_id             AS procedure_source_concept_id,
       CAST(NULL AS text)              AS modifier_source_value,
       --
       concat('procedure.', src.unit_id) AS unit_id,
       src.load_table_id                 AS load_table_id,
       src.load_row_id                   AS load_row_id,
       src.trace_id                      AS trace_id
FROM lk_chartevents_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Procedure'
;

