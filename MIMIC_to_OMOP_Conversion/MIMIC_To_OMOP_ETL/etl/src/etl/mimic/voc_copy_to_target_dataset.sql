DROP TABLE IF EXISTS voc_concept;


CREATE TABLE voc_concept AS
SELECT * FROM concept
;

DROP TABLE IF EXISTS voc_concept_relationship;


CREATE TABLE voc_concept_relationship AS
SELECT * FROM concept_relationship
;


DROP TABLE IF EXISTS voc_vocabulary;

CREATE TABLE voc_vocabulary AS
SELECT * FROM vocabulary
;

-- not affected by custom mapping
DROP TABLE IF EXISTS voc_domain;

CREATE TABLE voc_domain AS
SELECT * FROM domain
;

DROP TABLE IF EXISTS voc_concept_class;

CREATE TABLE voc_concept_class AS
SELECT * FROM concept_class
;



DROP TABLE IF EXISTS voc_relationship;

CREATE TABLE voc_relationship AS
SELECT * FROM relationship
;



DROP TABLE IF EXISTS voc_concept_synonym;

CREATE TABLE voc_concept_synonym AS
SELECT * FROM concept_synonym
;



DROP TABLE IF EXISTS voc_concept_ancestor;

CREATE TABLE voc_concept_ancestor AS
SELECT * FROM concept_ancestor
;


DROP TABLE IF EXISTS voc_drug_strength;

CREATE TABLE voc_drug_strength AS
SELECT * FROM drug_strength
;
