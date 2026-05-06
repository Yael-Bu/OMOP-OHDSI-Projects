DROP TABLE IF EXISTS cdm_fact_relationship;

CREATE TABLE cdm_fact_relationship
(
    domain_concept_id_1     INTEGER     NOT NULL ,
    fact_id_1               INTEGER     NOT NULL ,
    domain_concept_id_2     INTEGER     NOT NULL ,
    fact_id_2               INTEGER     NOT NULL ,
    relationship_concept_id INTEGER     NOT NULL ,
    -- 
    unit_id                       text
)
;

-- -------------------------------------------------------------------
-- specimen to test-organism
-- -------------------------------------------------------------------

INSERT INTO cdm_fact_relationship
SELECT 36                 AS domain_concept_id_1,     -- Specimen
       spec.specimen_id   AS fact_id_1,
       21                 AS domain_concept_id_2,     -- Measurement
       org.measurement_id AS fact_id_2,
       32669              AS relationship_concept_id, -- Specimen to Measurement   Standard
       'fact.spec.test'   AS unit_id
FROM lk_specimen_mapped spec
         INNER JOIN
     lk_meas_organism_mapped org
     ON org.trace_id_spec = spec.trace_id
;

INSERT INTO cdm_fact_relationship
SELECT 21                 AS domain_concept_id_1,     -- Measurement
       org.measurement_id AS fact_id_1,
       36                 AS domain_concept_id_2,     -- Specimen
       spec.specimen_id   AS fact_id_2,
       32668              AS relationship_concept_id, -- Measurement to Specimen   Standard
       'fact.test.spec'   AS unit_id
FROM lk_specimen_mapped spec
         INNER JOIN
     lk_meas_organism_mapped org
     ON org.trace_id_spec = spec.trace_id
;

-- -------------------------------------------------------------------
-- test-organism to antibiotic
-- -------------------------------------------------------------------

INSERT INTO cdm_fact_relationship
SELECT 21                 AS domain_concept_id_1,     -- Measurement
       org.measurement_id AS fact_id_1,
       21                 AS domain_concept_id_2,     -- Measurement
       ab.measurement_id  AS fact_id_2,
       581436             AS relationship_concept_id, -- Parent to Child Measurement   Standard
       'fact.test.ab'     AS unit_id
FROM lk_meas_organism_mapped org
         INNER JOIN
     lk_meas_ab_mapped ab
     ON ab.trace_id_org::text = org.trace_id
;

INSERT INTO cdm_fact_relationship
SELECT 21                 AS domain_concept_id_1,     -- Measurement
       ab.measurement_id  AS fact_id_1,
       21                 AS domain_concept_id_2,     -- Measurement
       org.measurement_id AS fact_id_2,
       581437             AS relationship_concept_id, -- Child to Parent Measurement   Standard
       'fact.ab.test'     AS unit_id
FROM lk_meas_organism_mapped org
         INNER JOIN
     lk_meas_ab_mapped ab
     ON ab.trace_id_org::text = org.trace_id
;
