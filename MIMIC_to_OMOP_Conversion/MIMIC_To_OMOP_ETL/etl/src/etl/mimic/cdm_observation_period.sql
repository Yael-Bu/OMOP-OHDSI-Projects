CREATE TABLE tmp_observation_period_clean AS
SELECT src.person_id             AS person_id,
       MIN(src.visit_start_date) AS start_date,
       MAX(src.visit_end_date)   AS end_date,
       src.unit_id               AS unit_id
FROM cdm_visit_occurrence src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id                 AS person_id,
       MIN(src.condition_start_date) AS start_date,
       MAX(src.condition_end_date)   AS end_date,
       src.unit_id                   AS unit_id
FROM cdm_condition_occurrence src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id           AS person_id,
       MIN(src.procedure_date) AS start_date,
       MAX(src.procedure_date) AS end_date,
       src.unit_id             AS unit_id
FROM cdm_procedure_occurrence src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id                     AS person_id,
       MIN(src.drug_exposure_start_date) AS start_date,
       MAX(src.drug_exposure_end_date)   AS end_date,
       src.unit_id                       AS unit_id
FROM cdm_drug_exposure src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id                       AS person_id,
       MIN(src.device_exposure_start_date) AS start_date,
       MAX(src.device_exposure_end_date)   AS end_date,
       src.unit_id                         AS unit_id
FROM cdm_device_exposure src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id             AS person_id,
       MIN(src.measurement_date) AS start_date,
       MAX(src.measurement_date) AS end_date,
       src.unit_id               AS unit_id
FROM cdm_measurement src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id          AS person_id,
       MIN(src.specimen_date) AS start_date,
       MAX(src.specimen_date) AS end_date,
       src.unit_id            AS unit_id
FROM cdm_specimen src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id             AS person_id,
       MIN(src.observation_date) AS start_date,
       MAX(src.observation_date) AS end_date,
       src.unit_id               AS unit_id
FROM cdm_observation src
GROUP BY src.person_id,
         src.unit_id
;

INSERT INTO tmp_observation_period_clean
SELECT src.person_id       AS person_id,
       MIN(src.death_date) AS start_date,
       MAX(src.death_date) AS end_date,
       src.unit_id         AS unit_id
FROM cdm_death src
GROUP BY src.person_id,
         src.unit_id
;


-- -------------------------------------------------------------------
-- tmp_observation_period
-- -------------------------------------------------------------------

CREATE TABLE tmp_observation_period AS
SELECT src.person_id       AS person_id,
       MIN(src.start_date) AS start_date,
       MAX(src.end_date)   AS end_date,
       src.unit_id         AS unit_id
FROM tmp_observation_period_clean src
GROUP BY src.person_id,
         src.unit_id
;

-- -------------------------------------------------------------------
-- cdm_observation_period
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm_observation_period;

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE cdm_observation_period
(
    observation_period_id             INTEGER   NOT NULL ,
    person_id                         INTEGER   NOT NULL ,
    observation_period_start_date     DATE    NOT NULL ,
    observation_period_end_date       DATE    NOT NULL ,
    period_type_concept_id            INTEGER   NOT NULL ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

INSERT INTO cdm_observation_period
SELECT row_number() OVER ()         AS observation_period_id,
       src.person_id        AS person_id,
       MIN(src.start_date)  AS observation_period_start_date,
       MAX(src.end_date)    AS observation_period_end_date,
       32828                AS period_type_concept_id, -- 32828    OMOP4976901 EHR episode record
       --
       'observation_period' AS unit_id,
       'event tables'       AS load_table_id,
       0                    AS load_row_id,
       CAST(NULL AS text) AS trace_id
FROM tmp_observation_period src
GROUP BY src.person_id
;

-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS tmp_observation_period_clean;
DROP TABLE IF EXISTS tmp_observation_period;
