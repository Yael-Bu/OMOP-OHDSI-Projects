/**
 ==============================================================================
 PROJECT: Clinical Cohort Definition & Phenotyping
 DATABASE STANDARD: OMOP Common Data Model (CDM) v5.x
 SUBJECT: Diabetes Mellitus Patient Stratification and Ancestry Validation
 AUTHOR: Yael Buchman
 ==============================================================================
 
 DESCRIPTION:
 This SQL script defines and validates the cohort selection process for patients
 diagnosed with Diabetes Mellitus. It utilizes the hierarchical structure of the
 OHDSI/OMOP vocabulary system via the `concept_ancestor` table to guarantee accurate
 descendant mapping.
 
 METHODOLOGICAL APPROACH:
 1. PHASE 1: Exploratory Data Analysis & Concept Profiling (Validating sub-type frequencies).
 2. PHASE 2: Coarse-Grained Cohort Validation (Broad inclusion mapping).
 3. PHASE 3: Hierarchical Multi-Tier Phenotyping (Final prioritized stratification).
 4. APPENDIX: Direct Relational Taxonomy (Navigating explicit single-level 'Is a' mappings).
 ==============================================================================
*/

-- ============================================================================
-- PHASE 1: EXPLORATORY DATA ANALYSIS (EDA) & CONCEPT PROFILING
-- Objective: Profile the frequency of individual descendant concept IDs under the
-- main umbrella concept 'Diabetes mellitus' (Concept ID: 201826).
-- ============================================================================

SELECT 
    c.condition_concept_id AS concept_id,
    vc.concept_name AS concept_name,
    COUNT(DISTINCT c.person_id) AS unique_person_count
FROM omop.condition_occurrence c
-- Hierarchical mapping via ancestor-descendant relationships
JOIN omop.concept_ancestor ca 
    ON c.condition_concept_id = ca.descendant_concept_id
-- Resolution of concept standard names using the vocabulary schema
JOIN vocabulary.concept vc 
    ON c.condition_concept_id = vc.concept_id
WHERE ca.ancestor_concept_id = 201826 -- Parent Concept: Diabetes mellitus
GROUP BY c.condition_concept_id, vc.concept_name
ORDER BY unique_person_count DESC;


-- ============================================================================
-- PHASE 2: COARSE-GRAINED COHORT VALIDATION (PRELIMINARY BENCHMARK)
-- Objective: Establish a baseline binary classification (Has Diabetes vs. No Diabetes)
-- based exclusively on the top-level parent concept (201826) to validate total denominator.
-- ============================================================================

WITH person_diabetes_status AS (
    SELECT 
        p.person_id,
        -- Binary flag evaluation
        CASE 
            WHEN ca.ancestor_concept_id = 201826 THEN 'Has Diabetes (Concept 201826)'
            ELSE 'No Diabetes'
        END AS diabetes_status
    FROM omop.person p
    LEFT JOIN omop.condition_occurrence c 
        ON p.person_id = c.person_id
    -- Restricting the hierarchy resolution only to the primary top-level ancestor
    LEFT JOIN omop.concept_ancestor ca 
        ON c.condition_concept_id = ca.descendant_concept_id
        AND ca.ancestor_concept_id = 201826
),
person_final_status AS (
    SELECT 
        person_id,
        -- Aggregation logic: If a patient has at least one matching row, they are classified as 'Has Diabetes'
        MAX(diabetes_status) AS final_status
    FROM person_diabetes_status
    GROUP BY person_id
)
SELECT 
    final_status AS status,
    COUNT(*) AS person_count
FROM person_final_status
GROUP BY final_status
ORDER BY person_count DESC;


-- ============================================================================
-- PHASE 3: FINAL HIERARCHICAL MULTI-TIER PHENOTYPING
-- Objective: Generate a mutually exclusive, comprehensive stratification of the 
-- cohort into 11 granular diabetes phenotypes, utilizing priority-score logic
-- to resolve overlapping medical coding.
-- ============================================================================

WITH person_diabetes_all_types AS (
    SELECT 
        p.person_id,
        /* 
          PRIORITY LOGIC RESOLUTION:
          Patients may hold multiple conflicting codes over time. Higher integer mapping 
          represents stricter phenotypic specificity (e.g., Type 2 takes precedence over General).
        */
        CASE 
            WHEN ca.ancestor_concept_id = 201254   THEN 12 -- Type 2 Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 201820   THEN 11 -- Type 1 Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 4052029  THEN 10 -- Gestational Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 4312138  THEN 9  -- Secondary Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 4192804  THEN 8  -- Drug or Chemical-Induced Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 4215673  THEN 7  -- Post-Procedural / Post-Surgical Diabetes
            WHEN ca.ancestor_concept_id = 4030043  THEN 6  -- Neonatal Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 4130026  THEN 5  -- Maturity-Onset Diabetes of the Young (MODY)
            WHEN ca.ancestor_concept_id = 4213162  THEN 4  -- Malnutrition-Related Diabetes Mellitus
            WHEN ca.ancestor_concept_id = 43530960 THEN 3  -- Latent Autoimmune Diabetes in Adults (LADA)
            WHEN ca.ancestor_concept_id = 436706   THEN 2  -- Pre-Diabetes / Impaired Glucose Tolerance
            -- Specific sub-types caught under the root tree but lacking isolated mapping
            WHEN ca.ancestor_concept_id = 201826 AND c.condition_concept_id <> 201826 THEN 1 
            -- Root un-specified umbrella code
            WHEN ca.ancestor_concept_id = 201826   THEN 1  -- Diabetes Mellitus (General / Unspecified)
            ELSE 0 -- Control Group / No Diabetes
        END AS diabetes_priority
    FROM omop.person p
    LEFT JOIN omop.condition_occurrence c 
        ON p.person_id = c.person_id
    -- Full dictionary mapping for the 11 targeted OHDSI Ancestor Concepts
    LEFT JOIN omop.concept_ancestor ca 
        ON c.condition_concept_id = ca.descendant_concept_id
        AND ca.ancestor_concept_id IN (201826, 201254, 201820, 4052029, 4312138, 4192804, 4215673, 4030043, 4130026, 4213162, 43530960, 436706)
),
person_final_category AS (
    SELECT 
        person_id,
        -- Collapse multiple patient records by selecting the highest phenotypic priority score
        MAX(diabetes_priority) AS final_priority
    FROM person_diabetes_all_types
    GROUP BY person_id
)
SELECT 
    CASE 
        WHEN final_priority = 12 THEN 'Type 2 Diabetes Mellitus'
        WHEN final_priority = 11 THEN 'Type 1 Diabetes Mellitus'
        WHEN final_priority = 10 THEN 'Gestational Diabetes Mellitus'
        WHEN final_priority = 9  THEN 'Secondary Diabetes Mellitus'
        WHEN final_priority = 8  THEN 'Drug or Chemical-Induced Diabetes Mellitus'
        WHEN final_priority = 7  THEN 'Post-Procedural / Post-Surgical Diabetes'
        WHEN final_priority = 6  THEN 'Neonatal Diabetes Mellitus'
        WHEN final_priority = 5  THEN 'Maturity-Onset Diabetes of the Young (MODY)'
        WHEN final_priority = 4  THEN 'Malnutrition-Related Diabetes Mellitus'
        WHEN final_priority = 3  THEN 'Latent Autoimmune Diabetes in Adults (LADA)'
        WHEN final_priority = 2  THEN 'Pre-Diabetes / Impaired Glucose Tolerance'
        WHEN final_priority = 1  THEN 'Diabetes Mellitus (General / Unspecified)'
        ELSE 'No Diabetes'
    END AS diabetes_type,
    COUNT(*) AS person_count
FROM person_final_category
GROUP BY final_priority
ORDER BY final_priority DESC;


-- ============================================================================
-- APPENDIX: DIRECT RELATIONAL TAXONOMY (SINGLE-LEVEL COHORT QUERY)
-- Objective: Isolate immediate, direct descendants (one level below) of a specific
-- target concept using explicit 'Is a' metadata relationships instead of full ancestry paths.
-- Current Configuration Target: Type 1 Diabetes Mellitus (Concept ID: 201820).
-- ============================================================================

WITH TargetConcepts AS (
    -- Single-value array wrapper to preserve script genericity and modular adjustments
    SELECT concept_id 
    FROM UNNEST(ARRAY[201820]) AS t(concept_id) 
),
IsA_Children AS (
    SELECT 
        c1.concept_id AS child_concept_id,
        c1.concept_name AS child_concept_name,
        cr.concept_id_2 AS parent_concept_id,
        c2.concept_name AS parent_concept_name
    FROM 
        omop.concept_relationship cr
    JOIN 
        omop.concept c1 ON cr.concept_id_1 = c1.concept_id
    JOIN 
        omop.concept c2 ON cr.concept_id_2 = c2.concept_id
    JOIN
        TargetConcepts tc ON cr.concept_id_2 = tc.concept_id 
    WHERE 
        cr.relationship_id = 'Is a'
        AND cr.invalid_reason IS NULL
)
SELECT 
    ic.parent_concept_name,
    ic.child_concept_id,
    ic.child_concept_name,
    COUNT(DISTINCT co.person_id) AS patient_count
FROM 
    IsA_Children ic
LEFT JOIN 
    omop.condition_occurrence co ON ic.child_concept_id = co.condition_concept_id
GROUP BY 
    ic.parent_concept_name,
    ic.child_concept_id,
    ic.child_concept_name
ORDER BY 
    ic.parent_concept_name, 
    patient_count DESC;