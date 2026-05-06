DROP SCHEMA IF EXISTS vocabulary CASCADE;
CREATE SCHEMA IF NOT EXISTS vocabulary AUTHORIZATION postgres;
SET search_path TO vocabulary;

CREATE TABLE concept (
  concept_id          INTEGER       NOT NULL ,
  concept_name        text      NOT NULL ,
  domain_id           text      NOT NULL ,
  vocabulary_id       text      NOT NULL ,
  concept_class_id    text      NOT NULL ,
  standard_concept    text               ,
  concept_code        text      NOT NULL ,
  valid_start_DATE    DATE        NOT NULL ,
  valid_end_DATE      DATE        NOT NULL ,
  invalid_reason      text
)
;


CREATE TABLE vocabulary (
  vocabulary_id         text      NOT NULL,
  vocabulary_name       text      NOT NULL,
  vocabulary_reference  text      NOT NULL,
  vocabulary_version    text              ,
  vocabulary_concept_id INTEGER       NOT NULL
)
;


CREATE TABLE domain (
  domain_id         text      NOT NULL,
  domain_name       text      NOT NULL,
  domain_concept_id INTEGER       NOT NULL
)
;


CREATE TABLE concept_class (
  concept_class_id          text      NOT NULL,
  concept_class_name        text      NOT NULL,
  concept_class_concept_id  INTEGER       NOT NULL
)
;


CREATE TABLE concept_relationship (
  concept_id_1      INTEGER     NOT NULL,
  concept_id_2      INTEGER     NOT NULL,
  relationship_id   text    NOT NULL,
  valid_start_DATE  DATE      NOT NULL,
  valid_end_DATE    DATE      NOT NULL,
  invalid_reason    text
  )
;


CREATE TABLE relationship (
  relationship_id         text      NOT NULL,
  relationship_name       text      NOT NULL,
  is_hierarchical         text      NOT NULL,
  defines_ancestry        text      NOT NULL,
  reverse_relationship_id text      NOT NULL,
  relationship_concept_id INTEGER       NOT NULL
)
;


CREATE TABLE concept_synonym (
  concept_id            INTEGER       NOT NULL,
  concept_synonym_name  text      NOT NULL,
  language_concept_id   INTEGER       NOT NULL
)
;


CREATE TABLE concept_ancestor (
  ancestor_concept_id       INTEGER   NOT NULL,
  descendant_concept_id     INTEGER   NOT NULL,
  min_levels_of_separation  INTEGER   NOT NULL,
  max_levels_of_separation  INTEGER   NOT NULL
)
;


-- CREATE TABLE source_to_concept_map (
--   source_code             text      NOT NULL,
--   source_concept_id       INTEGER       NOT NULL,
--   source_vocabulary_id    text      NOT NULL,
--   source_code_description text              ,
--   target_concept_id       INTEGER       NOT NULL,
--   target_vocabulary_id    text      NOT NULL,
--   valid_start_DATE        DATE        NOT NULL,
--   valid_end_DATE          DATE        NOT NULL,
--   invalid_reason          text
-- )
-- ;


CREATE TABLE drug_strength (
  drug_concept_id             INTEGER     NOT NULL,
  ingredient_concept_id       INTEGER     NOT NULL,
  amount_value                NUMERIC           ,
  amount_unit_concept_id      INTEGER             ,
  numerator_value             NUMERIC           ,
  numerator_unit_concept_id   INTEGER             ,
  denominator_value           NUMERIC           ,
  denominator_unit_concept_id INTEGER             ,
  box_size                    INTEGER             ,
  valid_start_DATE            DATE       NOT NULL,
  valid_end_DATE              DATE       NOT NULL,
  invalid_reason              text
)
;

