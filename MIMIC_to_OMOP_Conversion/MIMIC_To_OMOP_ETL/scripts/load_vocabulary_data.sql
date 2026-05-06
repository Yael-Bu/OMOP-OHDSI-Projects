


-- -- Set the search path to the vocabulary schema
-- SET search_path TO vocabulary;


-- ALTER TABLE concept  ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
-- ALTER TABLE vocabulary  ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);
-- ALTER TABLE domain  ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);
-- ALTER TABLE concept_class  ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
-- ALTER TABLE relationship  ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);

-- ALTER TABLE concept  ADD CONSTRAINT fpk_concept_domain_id FOREIGN KEY (domain_id) REFERENCES DOMAIN (DOMAIN_ID);
-- ALTER TABLE concept  ADD CONSTRAINT fpk_concept_vocabulary_id FOREIGN KEY (vocabulary_id) REFERENCES VOCABULARY (VOCABULARY_ID);
-- ALTER TABLE concept  ADD CONSTRAINT fpk_concept_concept_class_id FOREIGN KEY (concept_class_id) REFERENCES CONCEPT_CLASS (CONCEPT_CLASS_ID);
-- ALTER TABLE vocabulary  ADD CONSTRAINT fpk_vocabulary_vocabulary_concept_id FOREIGN KEY (vocabulary_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE domain  ADD CONSTRAINT fpk_domain_domain_concept_id FOREIGN KEY (domain_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_class  ADD CONSTRAINT fpk_concept_class_concept_class_concept_id FOREIGN KEY (concept_class_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_relationship  ADD CONSTRAINT fpk_concept_relationship_concept_id_1 FOREIGN KEY (concept_id_1) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_relationship  ADD CONSTRAINT fpk_concept_relationship_concept_id_2 FOREIGN KEY (concept_id_2) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_relationship  ADD CONSTRAINT fpk_concept_relationship_relationship_id FOREIGN KEY (relationship_id) REFERENCES RELATIONSHIP (RELATIONSHIP_ID);
-- ALTER TABLE relationship  ADD CONSTRAINT fpk_relationship_relationship_concept_id FOREIGN KEY (relationship_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_synonym  ADD CONSTRAINT fpk_concept_synonym_concept_id FOREIGN KEY (concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_synonym  ADD CONSTRAINT fpk_concept_synonym_language_concept_id FOREIGN KEY (language_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_ancestor  ADD CONSTRAINT fpk_concept_ancestor_ancestor_concept_id FOREIGN KEY (ancestor_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE concept_ancestor  ADD CONSTRAINT fpk_concept_ancestor_descendant_concept_id FOREIGN KEY (descendant_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_drug_concept_id FOREIGN KEY (drug_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_ingredient_concept_id FOREIGN KEY (ingredient_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_amount_unit_concept_id FOREIGN KEY (amount_unit_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_numerator_unit_concept_id FOREIGN KEY (numerator_unit_concept_id) REFERENCES CONCEPT (CONCEPT_ID);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_denominator_unit_concept_id FOREIGN KEY (denominator_unit_concept_id) REFERENCES CONCEPT (CONCEPT_ID);


-- CREATE INDEX idx_concept_concept_id  ON concept  (concept_id ASC);
-- CLUSTER concept  USING idx_concept_concept_id ;
-- CREATE INDEX idx_concept_code ON concept (concept_code ASC);
-- CREATE INDEX idx_concept_vocabluary_id ON concept (vocabulary_id ASC);
-- CREATE INDEX idx_concept_domain_id ON concept (domain_id ASC);
-- CREATE INDEX idx_concept_class_id ON concept (concept_class_id ASC);
-- CREATE INDEX idx_vocabulary_vocabulary_id  ON vocabulary  (vocabulary_id ASC);
-- CLUSTER vocabulary  USING idx_vocabulary_vocabulary_id ;
-- CREATE INDEX idx_domain_domain_id  ON domain  (domain_id ASC);
-- CLUSTER domain  USING idx_domain_domain_id ;
-- CREATE INDEX idx_concept_class_class_id  ON concept_class  (concept_class_id ASC);
-- CLUSTER concept_class  USING idx_concept_class_class_id ;
-- CREATE INDEX idx_concept_relationship_id_1  ON concept_relationship  (concept_id_1 ASC);
-- CLUSTER concept_relationship  USING idx_concept_relationship_id_1 ;
-- CREATE INDEX idx_concept_relationship_id_2 ON concept_relationship (concept_id_2 ASC);
-- CREATE INDEX idx_concept_relationship_id_3 ON concept_relationship (relationship_id ASC);
-- CREATE INDEX idx_relationship_rel_id  ON relationship  (relationship_id ASC);
-- CLUSTER relationship  USING idx_relationship_rel_id ;
-- CREATE INDEX idx_concept_synonym_id  ON concept_synonym  (concept_id ASC);
-- CLUSTER concept_synonym  USING idx_concept_synonym_id ;
-- CREATE INDEX idx_concept_ancestor_id_1  ON concept_ancestor  (ancestor_concept_id ASC);
-- CLUSTER concept_ancestor  USING idx_concept_ancestor_id_1 ;
-- CREATE INDEX idx_concept_ancestor_id_2 ON concept_ancestor (descendant_concept_id ASC);
-- CREATE INDEX idx_drug_strength_id_1  ON drug_strength  (drug_concept_id ASC);
-- CLUSTER drug_strength  USING idx_drug_strength_id_1 ;
-- CREATE INDEX idx_drug_strength_id_2 ON drug_strength (ingredient_concept_id ASC);


-- -- TRUNCATE TABLE concept;

-- COPY concept (
--   concept_id,
--   concept_name,
--   domain_id,
--   vocabulary_id,
--   concept_class_id,
--   standard_concept,
--   concept_code,
--   valid_start_date,
--   valid_end_date,
--   invalid_reason
-- )
-- FROM '/data/vocabulary/CONCEPT.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL '',
--   FORCE_NULL (standard_concept, invalid_reason)
-- );
-- -- Begin a transaction to ensure data integrity



-- SELECT COUNT(*) FROM concept;

-- -- TRUNCATE TABLE vocabulary;

-- -- Load data into the vocabulary table
-- COPY vocabulary (
--   vocabulary_id,
--   vocabulary_name,
--   vocabulary_reference,
--   vocabulary_version,
--   vocabulary_concept_id
-- )
-- FROM '/data/vocabulary/VOCABULARY.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL '',
--   FORCE_NULL (vocabulary_version)
-- );


-- TRUNCATE TABLE domain;
-- -- Load data into the domain table
-- COPY domain (
--   domain_id,
--   domain_name,
--   domain_concept_id
-- )
-- FROM '/data/vocabulary/DOMAIN.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL ''
-- );


-- -- TRUNCATE TABLE concept_class;
-- -- Load data into the concept_class table
-- COPY concept_class (
--   concept_class_id,
--   concept_class_name,
--   concept_class_concept_id
-- )
-- FROM '/data/vocabulary/CONCEPT_CLASS.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL ''
-- );


-- -- TRUNCATE TABLE concept_relationship;
-- -- Load data into the concept_relationship table
-- COPY concept_relationship (
--   concept_id_1,
--   concept_id_2,
--   relationship_id,
--   valid_start_date,
--   valid_end_date,
--   invalid_reason
-- )
-- FROM '/data/vocabulary/CONCEPT_RELATIONSHIP.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL '',
--   FORCE_NULL (invalid_reason)
-- );


-- -- TRUNCATE TABLE relationship;
-- -- Load data into the relationship table
-- COPY relationship (
--   relationship_id,
--   relationship_name,
--   is_hierarchical,
--   defines_ancestry,
--   reverse_relationship_id,
--   relationship_concept_id
-- )
-- FROM '/data/vocabulary/RELATIONSHIP.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL ''
-- );


-- -- TRUNCATE TABLE concept_synonym;
-- -- -- Load data into the concept_synonym table
-- COPY concept_synonym (
--   concept_id,
--   concept_synonym_name,
--   language_concept_id
-- )
-- FROM '/data/vocabulary/CONCEPT_SYNONYM.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL ''
-- );


-- -- TRUNCATE TABLE concept_ancestor;
-- -- Load data into the concept_ancestor table
-- COPY concept_ancestor (
--   ancestor_concept_id,
--   descendant_concept_id,
--   min_levels_of_separation,
--   max_levels_of_separation
-- )
-- FROM '/data/vocabulary/CONCEPT_ANCESTOR.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL ''
-- );


-- -- -- Load data into the drug_strength table
-- COPY drug_strength (
--   drug_concept_id,
--   ingredient_concept_id,
--   amount_value,
--   amount_unit_concept_id,
--   numerator_value,
--   numerator_unit_concept_id,
--   denominator_value,
--   denominator_unit_concept_id,
--   box_size,
--   valid_start_date,
--   valid_end_date,
--   invalid_reason
-- )
-- FROM '/data/vocabulary/DRUG_STRENGTH.csv'
-- WITH (
--   FORMAT CSV,
--   DELIMITER E'\t',
--   HEADER,
--   NULL '',
--   FORCE_NULL (
--     amount_value,
--     amount_unit_concept_id,
--     numerator_value,
--     numerator_unit_concept_id,
--     denominator_value,
--     denominator_unit_concept_id,
--     box_size,
--     invalid_reason
--   )
-- );

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";


-- Set the search path to the vocabulary schema
SET search_path TO vocabulary;

-- -- הוספת Constraints: Primary Keys
-- ALTER TABLE concept  ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
-- ALTER TABLE vocabulary  ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);
-- ALTER TABLE domain  ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);
-- ALTER TABLE concept_class  ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
-- ALTER TABLE relationship  ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);

-- -- הוספת Constraints: Foreign Keys
-- ALTER TABLE concept  ADD CONSTRAINT fpk_concept_domain_id FOREIGN KEY (domain_id) REFERENCES domain (domain_id);
-- ALTER TABLE concept  ADD CONSTRAINT fpk_concept_vocabulary_id FOREIGN KEY (vocabulary_id) REFERENCES vocabulary (vocabulary_id);
-- ALTER TABLE concept  ADD CONSTRAINT fpk_concept_concept_class_id FOREIGN KEY (concept_class_id) REFERENCES concept_class (concept_class_id);
-- ALTER TABLE vocabulary  ADD CONSTRAINT fpk_vocabulary_vocabulary_concept_id FOREIGN KEY (vocabulary_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE domain  ADD CONSTRAINT fpk_domain_domain_concept_id FOREIGN KEY (domain_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE concept_class  ADD CONSTRAINT fpk_concept_class_concept_class_concept_id FOREIGN KEY (concept_class_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE concept_relationship  ADD CONSTRAINT fpk_concept_relationship_concept_id_1 FOREIGN KEY (concept_id_1) REFERENCES concept (concept_id);
-- ALTER TABLE concept_relationship  ADD CONSTRAINT fpk_concept_relationship_concept_id_2 FOREIGN KEY (concept_id_2) REFERENCES concept (concept_id);
-- ALTER TABLE concept_relationship  ADD CONSTRAINT fpk_concept_relationship_relationship_id FOREIGN KEY (relationship_id) REFERENCES relationship (relationship_id);
-- ALTER TABLE relationship  ADD CONSTRAINT fpk_relationship_relationship_concept_id FOREIGN KEY (relationship_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE concept_synonym  ADD CONSTRAINT fpk_concept_synonym_concept_id FOREIGN KEY (concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE concept_synonym  ADD CONSTRAINT fpk_concept_synonym_language_concept_id FOREIGN KEY (language_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE concept_ancestor  ADD CONSTRAINT fpk_concept_ancestor_ancestor_concept_id FOREIGN KEY (ancestor_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE concept_ancestor  ADD CONSTRAINT fpk_concept_ancestor_descendant_concept_id FOREIGN KEY (descendant_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_drug_concept_id FOREIGN KEY (drug_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_ingredient_concept_id FOREIGN KEY (ingredient_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_amount_unit_concept_id FOREIGN KEY (amount_unit_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_numerator_unit_concept_id FOREIGN KEY (numerator_unit_concept_id) REFERENCES concept (concept_id);
-- ALTER TABLE drug_strength  ADD CONSTRAINT fpk_drug_strength_denominator_unit_concept_id FOREIGN KEY (denominator_unit_concept_id) REFERENCES concept (concept_id);

-- -- יצירת אינדקסים ו-Clustering
-- CREATE INDEX idx_concept_concept_id  ON concept  (concept_id ASC);
-- CLUSTER concept  USING idx_concept_concept_id ;
-- CREATE INDEX idx_concept_code ON concept (concept_code ASC);
-- CREATE INDEX idx_concept_vocabluary_id ON concept (vocabulary_id ASC);
-- CREATE INDEX idx_concept_domain_id ON concept (domain_id ASC);
-- CREATE INDEX idx_concept_class_id ON concept (concept_class_id ASC);
-- CREATE INDEX idx_vocabulary_vocabulary_id  ON vocabulary  (vocabulary_id ASC);
-- CLUSTER vocabulary  USING idx_vocabulary_vocabulary_id ;
-- CREATE INDEX idx_domain_domain_id  ON domain  (domain_id ASC);
-- CLUSTER domain  USING idx_domain_domain_id ;
-- CREATE INDEX idx_concept_class_class_id  ON concept_class  (concept_class_id ASC);
-- CLUSTER concept_class  USING idx_concept_class_class_id ;
-- CREATE INDEX idx_concept_relationship_id_1  ON concept_relationship  (concept_id_1 ASC);
-- CREATE INDEX idx_concept_relationship_id_2 ON concept_relationship (concept_id_2 ASC);
-- CREATE INDEX idx_concept_relationship_id_3 ON concept_relationship (relationship_id ASC);
-- CLUSTER concept_relationship  USING idx_concept_relationship_id_1 ;
-- CREATE INDEX idx_relationship_rel_id  ON relationship  (relationship_id ASC);
-- CLUSTER relationship  USING idx_relationship_rel_id ;
-- CREATE INDEX idx_concept_synonym_id  ON concept_synonym  (concept_id ASC);
-- CLUSTER concept_synonym  USING idx_concept_synonym_id ;
-- CREATE INDEX idx_concept_ancestor_id_1  ON concept_ancestor  (ancestor_concept_id ASC);
-- CLUSTER concept_ancestor  USING idx_concept_ancestor_id_1 ;
-- CREATE INDEX idx_concept_ancestor_id_2 ON concept_ancestor (descendant_concept_id ASC);
-- CREATE INDEX idx_drug_strength_id_1  ON drug_strength  (drug_concept_id ASC);
-- CLUSTER drug_strength  USING idx_drug_strength_id_1 ;
-- CREATE INDEX idx_drug_strength_id_2 ON drug_strength (ingredient_concept_id ASC);

-- -- טעינת הנתונים באמצעות COPY
-- -- חשוב לטעון את טבלת relationship לפני concept_relationship
TRUNCATE TABLE concept;
-- טעינת טבלת concept
COPY concept (
  concept_id,
  concept_name,
  domain_id,
  vocabulary_id,
  concept_class_id,
  standard_concept,
  concept_code,
  valid_start_date,
  valid_end_date,
  invalid_reason
)
FROM '/data/vocabulary/CONCEPT.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  QUOTE E'\x01',
  ESCAPE E'\x01',
  HEADER,
  NULL '',
  FORCE_NULL (standard_concept, invalid_reason)
);

COPY concept (
  concept_id,
  concept_name,
  domain_id,
  vocabulary_id,
  concept_class_id,
  standard_concept,
  concept_code,
  valid_start_date,
  valid_end_date,
  invalid_reason
)
FROM '/data/vocabulary/2b_concept.csv'
WITH (
  FORMAT CSV,
  DELIMITER E',',
  QUOTE '"',
  ESCAPE '"',
  HEADER,
  NULL '',
  FORCE_NULL (standard_concept, invalid_reason)
);

SELECT COUNT(*) FROM concept;

TRUNCATE TABLE vocabulary;

-- טעינת טבלת vocabulary
COPY vocabulary (
  vocabulary_id,
  vocabulary_name,
  vocabulary_reference,
  vocabulary_version,
  vocabulary_concept_id
)
FROM '/data/vocabulary/VOCABULARY.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL '',
  FORCE_NULL (vocabulary_version)
);

COPY vocabulary (
  vocabulary_id,
  vocabulary_name,
  vocabulary_reference,
  vocabulary_version,
  vocabulary_concept_id
)
FROM '/data/vocabulary/2b_vocabulary.csv'
WITH (
  FORMAT CSV,
  DELIMITER E',',
  HEADER,
  NULL '',
  FORCE_NULL (vocabulary_version)
);


-- טעינת טבלת domain
TRUNCATE TABLE domain;
COPY domain (
  domain_id,
  domain_name,
  domain_concept_id
)
FROM '/data/vocabulary/DOMAIN.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL ''
);

TRUNCATE TABLE concept_class;

-- טעינת טבלת concept_class
COPY concept_class (
  concept_class_id,
  concept_class_name,
  concept_class_concept_id
)
FROM '/data/vocabulary/CONCEPT_CLASS.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL ''
);

TRUNCATE TABLE relationship;

-- טעינת טבלת relationship לפני concept_relationship
COPY relationship (
  relationship_id,
  relationship_name,
  is_hierarchical,
  defines_ancestry,
  reverse_relationship_id,
  relationship_concept_id
)
FROM '/data/vocabulary/RELATIONSHIP.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL ''
);

TRUNCATE TABLE concept_relationship;

-- טעינת טבלת concept_relationship לאחר טעינת relationship
COPY concept_relationship (
  concept_id_1,
  concept_id_2,
  relationship_id,
  valid_start_date,
  valid_end_date,
  invalid_reason
)
FROM '/data/vocabulary/CONCEPT_RELATIONSHIP.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL '',
  FORCE_NULL (invalid_reason)
);

COPY concept_relationship (
  concept_id_1,
  concept_id_2,
  relationship_id,
  valid_start_date,
  valid_end_date,
  invalid_reason
)
FROM '/data/vocabulary/2b_concept_relationship.csv'
WITH (
  FORMAT CSV,
  DELIMITER E',',
  HEADER,
  NULL '',
  FORCE_NULL (invalid_reason)
);

TRUNCATE TABLE concept_synonym;

-- טעינת טבלת concept_synonym
COPY concept_synonym (
  concept_id,
  concept_synonym_name,
  language_concept_id
)
FROM '/data/vocabulary/CONCEPT_SYNONYM.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  QUOTE E'\x01',
  ESCAPE E'\x01',
  HEADER,
  NULL ''
);

TRUNCATE TABLE concept_ancestor;

-- טעינת טבלת concept_ancestor
COPY concept_ancestor (
  ancestor_concept_id,
  descendant_concept_id,
  min_levels_of_separation,
  max_levels_of_separation
)
FROM '/data/vocabulary/CONCEPT_ANCESTOR.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL ''
);

TRUNCATE TABLE drug_strength;

-- טעינת טבלת drug_strength
COPY drug_strength (
  drug_concept_id,
  ingredient_concept_id,
  amount_value,
  amount_unit_concept_id,
  numerator_value,
  numerator_unit_concept_id,
  denominator_value,
  denominator_unit_concept_id,
  box_size,
  valid_start_date,
  valid_end_date,
  invalid_reason
)
FROM '/data/vocabulary/DRUG_STRENGTH.csv'
WITH (
  FORMAT CSV,
  DELIMITER E'\t',
  HEADER,
  NULL '',
  FORCE_NULL (
    amount_value,
    amount_unit_concept_id,
    numerator_value,
    numerator_unit_concept_id,
    denominator_value,
    denominator_unit_concept_id,
    box_size,
    invalid_reason
  )
);

