CREATE TABLE lk_meas_d_labitems_clean AS
SELECT dlab.itemid                                             AS itemid,       -- for <cdm>.<source_value>
       COALESCE(dlab.loinc_code,
                CAST(dlab.itemid AS text))                   AS source_code,  -- to join to vocabs
       dlab.loinc_code                                         AS loinc_code,   -- for the crosswalk table
       concat(dlab.label, '|', dlab.fluid, '|', dlab.category) AS source_label, -- for the crosswalk table
       CASE WHEN dlab.loinc_code IS NOT NULL THEN 'LOINC'
           ELSE 'mimiciv_meas_lab_loinc' END                   AS source_vocabulary_id
FROM src_d_labitems dlab
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_clean
-- source_code: itemid
-- filter: only valid itemid (100%)
-- -------------------------------------------------------------------

CREATE TABLE lk_meas_labevents_clean AS
SELECT row_number() OVER ()   AS measurement_id,
       src.subject_id AS subject_id,
       src.charttime  AS start_datetime, -- measurement_datetime,
       src.hadm_id    AS hadm_id,
       src.itemid     AS itemid,
       src.value AS VALUE, -- value_source_value
       CASE
        WHEN SUBSTRING(src.value FROM 1 FOR 1) IN ('<', '>')
             AND SUBSTRING(src.value FROM 2 FOR 1) = '='
            THEN SUBSTRING(src.value FROM 1 FOR 2)
        WHEN SUBSTRING(src.value FROM 1 FOR 1) IN ('=', '<', '>')
            THEN SUBSTRING(src.value FROM 1 FOR 1)
    END AS value_operator,
    SUBSTRING(src.value FROM '([-]?[\d]+[.]?[\d]*)') AS value_number, -- assume "-0.34 etc"
    -- CASE
    --     WHEN subtext(src.value,1, 1) IN ('<', '>')
    --              AND subtext(src.value,2, 1) = '='
    --         THEN subtext(src.value,1, 2)
    --     WHEN subtext(src.value,1, 1) IN ('=', '<', '>')
    --         THEN subtext(src.value,1, 1)
    -- END AS value_operator,
    -- REGEXP_EXTRACT(src.value, r'([-]?[\d]+[.]?[\d]*)')    AS value_number, -- assume "-0.34 etc"
    CASE WHEN TRIM(src.valueuom) <> '' THEN src.valueuom END    AS valueuom, -- unit_source_value,
    src.ref_range_lower                     AS ref_range_lower,
    src.ref_range_upper                     AS ref_range_upper,
    'labevents'                             AS unit_id,
    --
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    src_labevents src
    INNER JOIN
    src_d_labitems dlab
ON src.itemid = dlab.itemid
;

-- -------------------------------------------------------------------
-- lk_meas_d_labitems_concept
--  gcpt_lab_label_to_concept -> mimiciv_meas_lab_loinc
-- all dlab.itemid, all available concepts from LOINC and custom mapped dlab.label
-- -------------------------------------------------------------------
CREATE TABLE lk_meas_d_labitems_concept AS
SELECT dlab.itemid               AS itemid,
       dlab.source_code          AS source_code,
       dlab.loinc_code           AS loinc_code,
       dlab.source_label         AS source_label,
       dlab.source_vocabulary_id AS source_vocabulary_id,
       -- source concept
       vc.domain_id              AS source_domain_id,
       vc.concept_id             AS source_concept_id,
       vc.concept_name           AS source_concept_name,
       -- target concept
       vc2.vocabulary_id         AS target_vocabulary_id,
       vc2.domain_id             AS target_domain_id,
       vc2.concept_id            AS target_concept_id,
       vc2.concept_name          AS target_concept_name,
       vc2.standard_concept      AS target_standard_concept
FROM lk_meas_d_labitems_clean dlab
         LEFT JOIN
     voc_concept vc
     ON vc.concept_code = dlab.source_code -- join
         AND vc.vocabulary_id = dlab.source_vocabulary_id
         -- AND vc.domain_id = 'Measurement'
         LEFT JOIN
     voc_concept_relationship vcr
     ON vc.concept_id = vcr.concept_id_1
         AND vcr.relationship_id = 'Maps to'
         LEFT JOIN
     voc_concept vc2
     ON vc2.concept_id = vcr.concept_id_2
         AND vc2.standard_concept = 'S'
         AND vc2.invalid_reason IS NULL
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_hadm_id
-- pick additional hadm_id by event start_datetime
-- row_num is added to select the earliest if more than one hadm_ids are found
-- -------------------------------------------------------------------

CREATE TABLE lk_meas_labevents_hadm_id AS
SELECT src.trace_id AS event_trace_id,
       adm.hadm_id  AS hadm_id,
       row_number()    over (
        partition BY src.trace_id::text
        ORDER BY adm.start_datetime
    )                                   AS row_num
FROM lk_meas_labevents_clean src
         INNER JOIN
     lk_admissions_clean adm
     ON adm.subject_id = src.subject_id
         AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
WHERE src.hadm_id IS NULL
;

-- -------------------------------------------------------------------
-- lk_meas_labevents_mapped
-- Rule 1 (LABS from labevents)
-- measurement_source_value: itemid
-- -------------------------------------------------------------------

CREATE TABLE lk_meas_labevents_mapped AS
SELECT src.measurement_id                             AS measurement_id,
       src.subject_id                                 AS subject_id,
       COALESCE(src.hadm_id, hadm.hadm_id)            AS hadm_id,
       CAST(src.start_datetime AS DATE)               AS date_id,
       src.start_datetime                             AS start_datetime,
       src.itemid                                     AS itemid,
       CAST(src.itemid AS text)                     AS source_code, -- change working source code to the rerpresentation
       labc.source_vocabulary_id                      AS source_vocabulary_id,
       labc.source_concept_id                         AS source_concept_id,
       COALESCE(labc.target_domain_id, 'Measurement') AS target_domain_id,
       labc.target_concept_id                         AS target_concept_id,
       src.valueuom                                   AS unit_source_value,
       CASE WHEN src.valueuom IS NOT NULL THEN COALESCE(uc.target_concept_id, 0) END    AS unit_concept_id,
       src.value_operator                             AS operator_source_value,
       opc.target_concept_id                          AS operator_concept_id,
       src.value                                      AS value_source_value,
       src.value_number                               AS value_as_number,
       CAST(NULL AS INTEGER)                          AS value_as_concept_id,
       src.ref_range_lower                            AS range_low,
       src.ref_range_upper                            AS range_high,
       --
       concat('meas.', src.unit_id)                   AS unit_id,
       src.load_table_id                              AS load_table_id,
       src.load_row_id                                AS load_row_id,
       src.trace_id                                   AS trace_id
FROM lk_meas_labevents_clean src
         INNER JOIN
     lk_meas_d_labitems_concept labc
     ON labc.itemid = src.itemid
         LEFT JOIN
     lk_meas_operator_concept opc
     ON opc.source_code = src.value_operator
         LEFT JOIN
     lk_meas_unit_concept uc
     ON uc.source_code = src.valueuom
         LEFT JOIN
     lk_meas_labevents_hadm_id hadm
     ON hadm.event_trace_id::text = src.trace_id::text
         AND hadm.row_num = 1
;
