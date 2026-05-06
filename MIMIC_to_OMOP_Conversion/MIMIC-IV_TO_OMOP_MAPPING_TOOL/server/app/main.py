
# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware

# app = FastAPI()


# # הוספת Middleware עבור CORS
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#     "https://mimic-to-omop-mapping.onrender.com",  # צד הלקוח בפרודקשן
#     "http://localhost:3000"  # צד הלקוח בסביבת הפיתוח
# ],  # ניתן להחליף בכתובות ספציפיות לפי הצורך
#     allow_credentials=True,
#     allow_methods=["GET", "POST", "OPTIONS"],
#     allow_headers=["Content-Type", "Authorization", "X-Requested-With"],

# )

# # מילון הנתונים מהשאלה הקודמת
# mapping_list = {
#     "patients": {
#         "subject_id": ["person_id<br>Table: person"],
#         "anchor_age": ["year_of_birth<br>Table: person - together with the anchor_year field, The calculation is: the year_of_birth is the anchor_year minus the anchor_age"],
#         "anchor_year": ["year_of_birth<br>Table: person -  together with the anchor_age field, The calculation is: the year_of_birth is the anchor_year minus the anchor_age"],
#         "dod": ["death_datetime<br>Table: person"],
#         "explanation": "This table represents the patient's personal data, The patient table in OMOP is the <strong>PERSON</strong> table"
#     },
#     "admissions": {
#         "subject_id": ["person_id<br>Table: visit_occurrence"],
#         "hadm_id": ["visit_occurrence_id<br>Table: visit_occurrence"],
#         "admittime": ["visit_start_datetime<br>Table: visit_occurrence"],
#         "admission_type": ["visit_concept_id<br>Table: visit_occurrence"],
#         "explanation": "This table encodes the patient's hospitalization table, the hospitalization table in OMOP is the <strong>VISIT_OCCURENCE</strong> table"
#     },
#     "d_icd_diagnoses": {
#         "icd_code": ["to checkkkkk<br>Table: concept"],
#         "icd_version": ["vocabulary_id<br>Table: concept"],
#         "long_title": ["concept_name<br>Table: concept"],
#         "explanation": "This table represents the data on the icd codes, this table is represented in OMOP by the <strong>CONCEPT</strong> table."
#     },
#     "diagnoses_icd": {
#         "subject_id": ["person_id/nTable: condition_occurrence"],
#         "hadm_id": ["visit_occurrence_id<br>Table: condition_occurrence"],
#         "seq_num": ["condition_occurrence_rank<br>Table: condition_occurrence"],
#         "icd_code": ["condition_source_value<br>Table: condition_occurrence"],
#         "icd_version": ["condition_source_concept_id<br>Table: condition_occurrence"],
#         "explanation": "This table represents the patient diagnosis data, this table is represented in OMOP by the <strong>CONDITION_OCCURENCE</strong> table."
#     }
# }

# @app.get("/tables")
# def get_tables():
#     print("hello world3")
#     return {"tables": list(mapping_list.keys())}

# @app.get("/tables/{table_name}/explanation")
# def get_table_explanation(table_name: str):
#     if table_name in mapping_list:
#         return {"explanation": mapping_list[table_name].get("explanation")}
#     return {"error": "Table not found"}

# @app.get("/tables/{table_name}/columns")
# def get_table_columns(table_name: str):
#     if table_name in mapping_list:
#         columns = list(mapping_list[table_name].keys())
#         columns.remove("explanation")  # להסיר את ההסבר מהרשימה
#         return {"columns": columns}
#     return {"error": "Table not found"}

# @app.get("/tables/{table_name}/columns/{column_name}")
# def get_column_mapping(table_name: str, column_name: str):
#     if table_name in mapping_list and column_name in mapping_list[table_name]:
#         return {"mapping": mapping_list[table_name][column_name][0]}
#     return {"error": "Table or column not found"}

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os

app = FastAPI()

# הוספת Middleware עבור CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://mimic-to-omop-mapping.onrender.com",  # צד הלקוח בפרודקשן
        "http://localhost:3000",  # צד הלקוח בסביבת הפיתוח
        "http://127.0.0.1:3000" # backend server in docker container
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)

# קריאת המילון מקובץ JSON
base_dir = os.path.dirname(os.path.abspath(__file__))
json_path = os.path.join(base_dir, "mapping_list.json")

with open(json_path, "r") as file:
    mapping_list = json.load(file)

@app.get("/tables")
def get_tables():
    print("hello world3")
    return {"tables": list(mapping_list.keys())}

@app.get("/tables/{table_name}/explanation")
def get_table_explanation(table_name: str):
    if table_name in mapping_list:
        return {"explanation": mapping_list[table_name].get("explanation")}
    return {"error": "Table not found"}

@app.get("/tables/{table_name}/columns")
def get_table_columns(table_name: str):
    if table_name in mapping_list:
        columns = list(mapping_list[table_name].keys())
        columns.remove("explanation")  # להסיר את ההסבר מהרשימה
        return {"columns": columns}
    return {"error": "Table not found"}

@app.get("/tables/{table_name}/columns/{column_name}")
def get_column_mapping(table_name: str, column_name: str):
    if table_name in mapping_list and column_name in mapping_list[table_name]:
        return {"mapping": mapping_list[table_name][column_name][0]}
    return {"error": "Table or column not found"}
