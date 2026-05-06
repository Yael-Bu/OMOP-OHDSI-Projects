CREATE TABLE tmp_person AS
SELECT per.*
FROM cdm_person per
         INNER JOIN
     cdm_observation_period op
     ON per.person_id = op.person_id
;

TRUNCATE TABLE cdm_person;

INSERT INTO cdm_person
SELECT per.*
FROM tmp_person per
;

DROP TABLE if EXISTS tmp_person;
