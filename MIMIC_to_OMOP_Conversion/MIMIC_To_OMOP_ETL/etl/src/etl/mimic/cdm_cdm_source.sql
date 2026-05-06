DROP TABLE IF EXISTS cdm_cdm_source;
CREATE TABLE cdm_cdm_source
(
    cdm_source_name                 text        NOT NULL ,
    cdm_source_abbreviation         text             ,
    cdm_holder                      text             ,
    source_description              text             ,
    source_documentation_reference  text             ,
    cdm_etl_reference               text             ,
    source_release_date             DATE               ,
    cdm_release_date                DATE               ,
    cdm_version                     text             ,
    vocabulary_version              text             ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

INSERT INTO cdm_cdm_source
SELECT 'MIMIC-DEMO'                                                                       AS cdm_source_name,
       'MIMIC-DEMO'                                                                       AS cdm_source_abbreviation,
       'Tufts CTSI'                                                                       AS cdm_holder,
       concat('MIMIC-IV is a publicly available database of patients ',
              'admitted to the Beth Israel Deaconess Medical Center in Boston, MA, USA.') AS source_description,
       'https://mimic-iv.mit.edu/docs/'                                                   AS source_documentation_reference,
       'https://github.com/OHDSI/MIMIC/'                                                  AS cdm_etl_reference,
       CURRENT_DATE                                                                       AS source_release_date, -- to look up
       CURRENT_DATE                                                                       AS cdm_release_date,
       '5.3.1'                                                                            AS cdm_version,
       v.vocabulary_version                                                               AS vocabulary_version,
       --
       'cdm.source'                                                                       AS unit_id,
       'none'                                                                             AS load_table_id,
       1                                                                                  AS load_row_id,
       json_object(
               ARRAY['trace_id'],
               ARRAY['mimiciv']
           )          AS trace_id

FROM voc_vocabulary v
WHERE v.vocabulary_id = 'None'
;

