CREATE TABLE src_procedureevents AS
SELECT hadm_id    AS hadm_id,
       subject_id AS subject_id,
       stay_id    AS stay_id,
       itemid     AS itemid,
       starttime  AS starttime,
       value AS VALUE,
    CAST(0 AS INTEGER)                    AS cancelreason, -- MIMIC IV 2.0 change, the field is removed
    --
    'procedureevents'                   AS load_table_id,
    row_number() OVER()   AS load_row_id,
    json_object(
               ARRAY['subject_id','hadm_id','starttime'],
               ARRAY[subject_id::text,hadm_id::text, starttime::text]
           )          AS trace_id
FROM
    mimiciv_icu.procedureevents
;

-- -------------------------------------------------------------------
-- src_d_items
-- -------------------------------------------------------------------

CREATE TABLE src_d_items AS
SELECT itemid       AS itemid,
       label        AS label,
       linksto      AS linksto,
       -- abbreviation
       -- category
       -- unitname
       -- param_type
       -- lownormalvalue
       -- highnormalvalue
       --
       'd_items'    AS load_table_id,
       row_number() OVER() AS load_row_id,
       json_object(
               ARRAY['itemid','linksto'],
               ARRAY[itemid::text,linksto::text]
           )          AS trace_id
FROM mimiciv_icu.d_items
;

-- -------------------------------------------------------------------
-- src_datetimeevents
-- -------------------------------------------------------------------

CREATE TABLE src_datetimeevents AS
SELECT subject_id AS subject_id,
       hadm_id    AS hadm_id,
       stay_id    AS stay_id,
       itemid     AS itemid,
       charttime  AS charttime,
       value AS VALUE,
    --
    'datetimeevents'                    AS load_table_id,
    row_number() OVER()   AS load_row_id,
    json_object(
               ARRAY['subject_id','hadm_id', 'stay_id', 'charttime'],
               ARRAY[subject_id::text,hadm_id::text, stay_id::text,charttime::text]
           )          AS trace_id
FROM
    mimiciv_icu.datetimeevents
;


CREATE TABLE src_chartevents AS
SELECT subject_id AS subject_id,
       hadm_id    AS hadm_id,
       stay_id    AS stay_id,
       itemid     AS itemid,
       charttime  AS charttime,
       value AS VALUE,
    valuenum    AS valuenum,
    valueuom    AS valueuom,
    --
    'chartevents'                       AS load_table_id,
    row_number() OVER()   AS load_row_id,
    json_object(
               ARRAY['subject_id','hadm_id', 'stay_id', 'charttime'],
               ARRAY[subject_id::text,hadm_id::text, stay_id::text,charttime::text]
           )          AS trace_id
FROM
    mimiciv_icu.chartevents
;