import datetime
import itertools
import os
import typing

from .config import (
    ETL_DIR,
    ARES_DATA_ROOT,
    PRODUCTION_SCHEMA,
    TEMP_SCHEMA,
    VOCABULARY_SCHEMA,
)
from .common import (
    CDM_TABLES,
    DELIVERED_TABLES,
    VOCABULARY_TABLES,
    archive_and_rename_schema,
    create_or_replace_schema,
    execute_sql,
    # generate_flow_run_name,
    get_last_cdm_release_date,
    get_schemas_as_list,
    ingest_omop,
    load_and_execute_sql,
    load_sql,
    orchestrate_sql_w_dependencies,
    rename_schema,
    subprocess_run,
    view_tables,
)


def load_mimic_source(stage_schema: str):
    # Define the schemas and tables in MIMIC-IV
    mimic_schemas_tables = {
        'mimiciv_hosp': [
            'admissions',
            'patients',
            'diagnoses_icd',
            'd_icd_diagnoses',
            'procedures_icd',
            'd_icd_procedures',
            'labevents',
            'microbiologyevents',
            'prescriptions',
            'transfers',
            'd_hcpcs',
            'd_labitems',
            'drgcodes',
            'emar_detail',
            'emar',
            'hcpcsevents',
            'omr',
            'pharmacy',
            'poe_detail',
            'poe',
            'provider',
            'services',

        ],
        'mimiciv_icu': [
            'icustays',
            'chartevents',
            'd_items',
            'procedureevents',
            'inputevents',
            'outputevents',
            'datetimeevents',
            'caregiver',
            'ingredientevents',


        ],
    }

    # Loop over the schemas and create views for each table
    for source_schema, tables in mimic_schemas_tables.items():
        view_tables(
            source_schema=source_schema,
            source_tables=tables,
            schema=stage_schema,
        )



def mimic_etl():
    print(f"Started etl pipeline for transforming the MIMIC dataset.")
    t0 = datetime.datetime.now()
    target_schema = PRODUCTION_SCHEMA

    stage_schema = 'stage_' + target_schema
    print("Populating staging area...")
    create_or_replace_schema(
        stage_schema,
        "Staging area for MIMIC Transformation Process",
    )
    # load_and_execute_sql(
    #     'mimic/ddl_cdm_5_3_1.sql',
    #     schema=stage_schema
    # )

    # load_mimic_source(
    #     # This function should load mimic source tables
    # )
    load_mimic_source(stage_schema)

    view_tables(
        VOCABULARY_SCHEMA,
        VOCABULARY_TABLES,
        schema=stage_schema,
    )

    print("Building MIMIC...")
    base_etl_dir = os.path.join(ETL_DIR, 'mimic')

    orchestrate_sql_w_dependencies(base_etl_dir, stage_schema)

    schemas = get_schemas_as_list()

    # if PRODUCTION_SCHEMA in schemas:
    #     archive_suffix = get_last_cdm_release_date(PRODUCTION_SCHEMA)
    #     archive_and_rename_schema(TEMP_SCHEMA, PRODUCTION_SCHEMA, archive_suffix)
    # else:
    #     rename_schema(TEMP_SCHEMA, PRODUCTION_SCHEMA)
    
    
    
    # if PRODUCTION_SCHEMA in schemas:
    #     archive_suffix = get_last_cdm_release_date(PRODUCTION_SCHEMA)
    #     archive_and_rename_schema(stage_schema, PRODUCTION_SCHEMA, archive_suffix)
    # else:
    #     rename_schema(stage_schema, PRODUCTION_SCHEMA)




    # subprocess_run(
    #     ['Rscript', os.path.join(ETL_DIR, 'ares.R'), ARES_DATA_ROOT, mode, PRODUCTION_SCHEMA],
    #     cwd='/ares',
    #     check=True,
    # )

    t1 = datetime.datetime.now()
    minutes = round((t1 - t0).total_seconds() / 60)
    print(f"Successfully finished ETL pipeline for creating the MIMIC OMOP Instance in {minutes} minutes.")


