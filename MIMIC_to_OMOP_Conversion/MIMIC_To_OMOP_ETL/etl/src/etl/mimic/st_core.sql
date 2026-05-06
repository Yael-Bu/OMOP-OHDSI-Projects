CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE src_patients AS
SELECT subject_id                        AS subject_id,
       anchor_year                       AS anchor_year,
       anchor_age                        AS anchor_age,
       anchor_year_group                 AS anchor_year_group,
       gender                            AS gender,
       --
       'patients'                        AS load_table_id,
       row_number() OVER() AS load_row_id,
       json_object(
               ARRAY['subject_id'],
               ARRAY[subject_id::text]
           )          AS trace_id
FROM mimiciv_hosp.patients
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------

CREATE TABLE src_admissions AS
SELECT hadm_id            AS hadm_id,   -- PK
       subject_id         AS subject_id,
       admittime          AS admittime,
       dischtime          AS dischtime,
       deathtime          AS deathtime,
       admission_type     AS admission_type,
       admission_location AS admission_location,
       discharge_location AS discharge_location,
       race               AS ethnicity, -- MIMIC IV 2.0 change, field race replaced field ethnicity
       edregtime          AS edregtime,
       insurance          AS insurance,
       marital_status     AS marital_status, LANGUAGE AS LANGUAGE,
       -- edouttime
       -- hospital_expire_flag
       --
    'admissions' AS load_table_id,
    row_number() OVER() AS load_row_id,
    json_object(
               ARRAY['subject_id','hadm_id'],
               ARRAY[subject_id::text,hadm_id::text]
           )          AS trace_id
FROM
    mimiciv_hosp.admissions
;

-- -------------------------------------------------------------------
-- src_transfers
-- -------------------------------------------------------------------

CREATE TABLE src_transfers AS
SELECT transfer_id                       AS transfer_id,
       hadm_id                           AS hadm_id,
       subject_id                        AS subject_id,
       careunit                          AS careunit,
       intime                            AS intime,
       outtime                           AS outtime,
       eventtype                         AS eventtype,
       --
       'transfers'                       AS load_table_id,
       row_number() OVER() AS load_row_id,
       json_object(
               ARRAY['subject_id','hadm_id', 'transfer_id'],
               ARRAY[subject_id::text,hadm_id::text, transfer_id::text]
           )          AS trace_id
FROM mimiciv_hosp.transfers
;
