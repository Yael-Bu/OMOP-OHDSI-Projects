DROP TABLE IF EXISTS cdm_device_exposure;

CREATE TABLE cdm_device_exposure
(
    device_exposure_id              INTEGER       NOT NULL ,
    person_id                       INTEGER       NOT NULL ,
    device_concept_id               INTEGER       NOT NULL ,
    device_exposure_start_date      DATE        NOT NULL ,
    device_exposure_start_datetime  TIMESTAMP             ,
    device_exposure_end_date        DATE                 ,
    device_exposure_end_datetime    TIMESTAMP             ,
    device_type_concept_id          INTEGER       NOT NULL ,
    unique_device_id                text               ,
    quantity                        INTEGER                ,
    provider_id                     INTEGER                ,
    visit_occurrence_id             INTEGER                ,
    visit_detail_id                 INTEGER                ,
    device_source_value             text               ,
    device_source_concept_id        INTEGER                ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;


INSERT INTO cdm_device_exposure
SELECT row_number() OVER ()                     AS device_exposure_id,
       per.person_id                    AS person_id,
       src.target_concept_id            AS device_concept_id,
       CAST(src.start_datetime AS DATE) AS device_exposure_start_date,
       src.start_datetime               AS device_exposure_start_datetime,
       CAST(src.end_datetime AS DATE)   AS device_exposure_end_date,
       src.end_datetime                 AS device_exposure_end_datetime,
       src.type_concept_id              AS device_type_concept_id,
       CAST(NULL AS text)             AS unique_device_id,
       CAST((
               CASE WHEN round(src.quantity) = src.quantity THEN src.quantity END)
           AS INTEGER)                  AS quantity,
        --    CAST(
        --        CASE WHEN round(src.quantity) = src.quantity THEN src.quantity END)
        --    AS INTEGER)                  AS quantity,
       CAST(NULL AS INTEGER)            AS provider_id,
       vis.visit_occurrence_id          AS visit_occurrence_id,
       CAST(NULL AS INTEGER)            AS visit_detail_id,
       src.source_code                  AS device_source_value,
       src.source_concept_id            AS device_source_concept_id,
       --
       concat('device.', src.unit_id)   AS unit_id,
       src.load_table_id                AS load_table_id,
       src.load_row_id                  AS load_row_id,
       src.trace_id                     AS trace_id
FROM lk_drug_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Device'
;


INSERT INTO cdm_device_exposure
SELECT row_number() OVER ()                     AS device_exposure_id,
       per.person_id                    AS person_id,
       src.target_concept_id            AS device_concept_id,
       CAST(src.start_datetime AS DATE) AS device_exposure_start_date,
       src.start_datetime               AS device_exposure_start_datetime,
       CAST(src.start_datetime AS DATE) AS device_exposure_end_date,
       src.start_datetime               AS device_exposure_end_datetime,
       src.type_concept_id              AS device_type_concept_id,
       CAST(NULL AS text)             AS unique_device_id,
       CAST((
               CASE WHEN round(src.value_as_number) = src.value_as_number THEN src.value_as_number END)
           AS INTEGER)                  AS quantity,
        --    CAST(
        --        CASE WHEN round(src.value_as_number) = src.value_as_number THEN src.value_as_number END)
        --    AS INTEGER)                  AS quantity,
       CAST(NULL AS INTEGER)            AS provider_id,
       vis.visit_occurrence_id          AS visit_occurrence_id,
       CAST(NULL AS INTEGER)            AS visit_detail_id,
       src.source_code                  AS device_source_value,
       src.source_concept_id            AS device_source_concept_id,
       --
       concat('device.', src.unit_id)   AS unit_id,
       src.load_table_id                AS load_table_id,
       src.load_row_id                  AS load_row_id,
       src.trace_id                     AS trace_id
FROM lk_chartevents_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|', CAST(src.hadm_id AS text))
WHERE src.target_domain_id = 'Device'
;
