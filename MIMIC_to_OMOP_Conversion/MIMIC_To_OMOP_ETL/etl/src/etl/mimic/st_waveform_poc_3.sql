
-- CREATE TABLE src_waveform_header_3
-- (       
--     reference_id            text,
--     raw_files_path          text,
--     case_id                 text,
--     subject_id              INTEGER,
--     start_datetime          TIMESTAMP,
--     end_datetime            TIMESTAMP,
--     --
--     load_table_id           text,
--     load_row_id             INTEGER,
--     trace_id                text
-- );

-- -- parsed codes to be targeted to table cdm_measurement
-- DROP TABLE IF EXISTS src_waveform_mx_3;

-- CREATE TABLE src_waveform_mx_3
-- (
--     case_id                 text,  -- FK to the header
--     segment_name            text, -- two digits of case_id, 5 digits of internal sequence number
--     mx_datetime             TIMESTAMP, -- time of measurement
--     source_code             text,   -- type of measurement
--     value_as_number         NUMERIC,
--     unit_source_value       text, -- measurement unit "BPM", "MS", "UV" (microvolt) etc.
--                                     -- map these labels and populate unit_concept_id
--     --
--     Visit_Detail___Source               text,
--     Visit_Detail___Start_from_minutes   INTEGER,
--     Visit_Detail___Report_minutes       INTEGER,
--     Visit_Detail___Sumarize_minutes     INTEGER,
--     Visit_Detail___Method               text,
--     --
--     load_table_id           text,
--     load_row_id             INTEGER,
--     trace_id                text
-- );


-- -- parse xml from Manlik? -> src_waveform
-- -- src_waveform -> visit_detail (visit_detail_source_value = <reference ID>)

-- -- finding the visit 
-- -- create visit_detail
-- -- create measurement -> link visit_detail using visit_detail_source_value = meas_source_value 
-- -- (start with Manlik's proposal)


-- -- -------------------------------------------------------------------
-- -- insert sample data
-- -- -------------------------------------------------------------------


-- INSERT INTO src_waveform_header_3
-- SELECT subj.short_reference_id                         AS reference_id,
--        subj.long_reference_id                          AS raw_files_path,
--        subj.case_id                                    AS case_id,    -- text
--        CAST(REPLACE(subj.case_id, 'p', '') AS INTEGER) AS subject_id, -- int
--        CAST(sign.start AS TIMESTAMP)          AS start_datetime,
--        CAST(sign.end AS TIMESTAMP)            AS end_datetime,
--        --
--        'poc_3_header'                                  AS load_table_id,
--        0                                               AS load_row_id,
--        json_object(
--                ARRAY['case_id','reference_id'],
--                ARRAY[case_id::text,short_reference_id::text]
--            )          AS trace_id
-- FROM wf_header_mimic subj
--          INNER JOIN (
--                         SELECT case_id,
--                                MIN(date_time) AS start,
--                                MAX(date_time) AS end
--                         FROM ecgmx_041_mimic GROUP BY case_id
--                     ) sign ON subj.case_id = sign.case_id
-- ;

-- -- Chunk 1
-- -- 25-second interval, mass data

-- INSERT INTO src_waveform_mx_3
-- SELECT src.case_id                      AS case_id, -- FK to the header
--        src.segment_name                 AS segment_name,
--        --
--        CAST(src.date_time AS TIMESTAMP) AS mx_datetime,
--        src.src_name                     AS source_code,
--        CAST(src.value AS NUMERIC)       AS value_as_number,
--        src.unit_concept_name            AS unit_source_value,
--        'csv'                            AS visit_detail___source,
--        CAST(NULL AS INTEGER)            AS visit_detail___start_from_minutes,
--        CAST(NULL AS INTEGER)            AS visit_detail___report_minutes,
--        CAST(NULL AS INTEGER)            AS visit_detail___sumarize_minutes,
--        'NONE'                           AS visit_detail___method,
--        --
--        'poc_3_chunk_1'                  AS load_table_id,
--        row_number() OVER()                     AS load_row_id,
--        json_object(
--                ARRAY['case_id','date_time', 'src_name'],
--                ARRAY[case_id::text,date_time::text, src_name::text]
--            )          AS trace_id
-- FROM ecgmx_041_mimic src
--          INNER JOIN
--      mimiciv_hosp.patients pat
--      ON CAST(REPLACE(src.case_id, 'p', '') AS INTEGER) = pat.subject_id -- filter out mass data in demo dataset
-- ;



