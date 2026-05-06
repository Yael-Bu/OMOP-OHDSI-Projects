CREATE TABLE tmp_custom_mapping AS

SELECT *
FROM gcpt_cs_place_of_service_mimic

UNION

SELECT *
FROM gcpt_drug_ndc_mimic

UNION

SELECT *
FROM gcpt_drug_route_mimic

UNION

SELECT *
FROM gcpt_meas_chartevents_main_mod_mimic

UNION

SELECT *
FROM gcpt_meas_chartevents_value_mimic

UNION

SELECT *
FROM gcpt_meas_lab_loinc_mod_mimic

UNION

SELECT *
FROM gcpt_meas_unit_mimic

UNION

SELECT *
FROM gcpt_meas_waveforms_mimic

UNION

SELECT *
FROM gcpt_micro_antibiotic_mimic

UNION

SELECT *
FROM gcpt_micro_microtest_mimic

UNION

SELECT *
FROM gcpt_micro_organism_mimic

UNION

SELECT *
FROM gcpt_micro_resistance_mimic

UNION

SELECT *
FROM gcpt_micro_specimen_mimic

UNION

SELECT *
FROM gcpt_mimic_generated_mimic

UNION

SELECT *
FROM gcpt_obs_drgcodes_mimic

UNION

SELECT *
FROM gcpt_obs_insurance_mimic

UNION

SELECT *
FROM gcpt_obs_marital_mimic

UNION

SELECT *
FROM gcpt_per_ethnicity_mimic

UNION

SELECT *
FROM gcpt_proc_datetimeevents_mimic

UNION

SELECT *
FROM gcpt_proc_itemid_mimic

UNION

SELECT *
FROM gcpt_vis_admission_mimic;