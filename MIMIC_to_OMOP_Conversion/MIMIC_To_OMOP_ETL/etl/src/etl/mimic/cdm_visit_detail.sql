DROP TABLE IF EXISTS cdm_visit_detail;


CREATE TABLE cdm_visit_detail
(
    visit_detail_id                    INTEGER     NOT NULL ,
    person_id                          INTEGER     NOT NULL ,
    visit_detail_concept_id            INTEGER     NOT NULL ,
    visit_detail_start_date            DATE      NOT NULL ,
    visit_detail_start_datetime        TIMESTAMP           ,
    visit_detail_end_date              DATE      NOT NULL ,
    visit_detail_end_datetime          TIMESTAMP           ,
    visit_detail_type_concept_id       INTEGER     NOT NULL , -- detail! -- this typo still exists in v.5.3.1(???)
    provider_id                        INTEGER              ,
    care_site_id                       INTEGER              ,
    admitting_source_concept_id        INTEGER              ,
    discharge_to_concept_id            INTEGER              ,
    preceding_visit_detail_id          INTEGER              ,
    visit_detail_source_value          text             ,
    visit_detail_source_concept_id     INTEGER              , -- detail! -- this typo still exists in v.5.3.1(???)
    admitting_source_value             text             ,
    discharge_to_source_value          text             ,
    visit_detail_parent_id             INTEGER              ,
    visit_occurrence_id                INTEGER     NOT NULL ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

-- -------------------------------------------------------------------
-- Rule 1. transfers
-- Rule 2. services
-- -------------------------------------------------------------------



INSERT INTO cdm_visit_detail
SELECT src.visit_detail_id                  AS visit_detail_id,
       per.person_id                        AS person_id,
       COALESCE(vdc.target_concept_id, 0)   AS visit_detail_concept_id,
       -- see source value in care_site.care_site_source_value
       CAST(src.start_datetime AS DATE)     AS visit_start_date,
       src.start_datetime                   AS visit_start_datetime,
       CAST(src.end_datetime AS DATE)       AS visit_end_date,
       src.end_datetime                     AS visit_end_datetime,
       32817                                AS visit_detail_type_concept_id, -- EHR   Type Concept    Standard
       CAST(NULL AS INTEGER)                AS provider_id,
       cs.care_site_id                      AS care_site_id,

       CASE WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0) END  AS admitting_source_concept_id,
       CASE WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0) END  AS discharge_to_concept_id,

       src.preceding_visit_detail_id        AS preceding_visit_detail_id,
       src.source_value                     AS visit_detail_source_value,
       COALESCE(vdc.source_concept_id, 0)   AS visit_detail_source_concept_id,
       src.admission_location               AS admitting_source_value,
       src.discharge_location               AS discharge_to_source_value,
       CAST(NULL AS INTEGER)                AS visit_detail_parent_id,
       vis.visit_occurrence_id              AS visit_occurrence_id,
       --
       concat('visit_detail.', src.unit_id) AS unit_id,
       src.load_table_id                    AS load_table_id,
       src.load_row_id                      AS load_row_id,
       src.trace_id                         AS trace_id
FROM lk_visit_detail_prev_next src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS text) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS text), '|',
               COALESCE(CAST(src.hadm_id AS text), CAST(src.date_id AS text)))
         LEFT JOIN
     cdm_care_site cs
     ON cs.care_site_source_value = src.current_location
         LEFT JOIN
     lk_visit_concept vdc
     ON vdc.source_code = src.current_location
         LEFT JOIN
     lk_visit_concept la
     ON la.source_code = src.admission_location
         LEFT JOIN
     lk_visit_concept ld
     ON ld.source_code = src.discharge_location
;
