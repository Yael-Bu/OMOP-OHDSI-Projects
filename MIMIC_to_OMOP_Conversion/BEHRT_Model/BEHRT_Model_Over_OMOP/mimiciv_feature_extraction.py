import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql

from sqlalchemy import create_engine
password = '<password>'

postgres_ip = '<ip>'
db = 'omop_med_db'
schema = 'omop'
port = 5432 # default
username = '<user_name>'


connection_string = f"postgresql://{username}:{password}@{postgres_ip}:{port}/{db}"
engine = create_engine(connection_string)


sql_query = f"""
WITH diagnoses AS (
  SELECT 
    co.person_id AS subject_id,
    co.visit_occurrence_id AS hadm_id,
    co.condition_source_value AS icd_code,
    '10' AS icd_version,  
    co.condition_occurrence_id AS seq_num,
    c.concept_name AS long_title
  FROM 
    {schema}.condition_occurrence co
    LEFT JOIN {schema}.concept c ON co.condition_source_concept_id = c.concept_id
  WHERE 
    c.vocabulary_id = 'ICD10CM'  
),
diagnoses_with_year AS (
  SELECT 
    EXTRACT(YEAR FROM vo.visit_start_date) AS event_year,
    d.hadm_id,
    d.subject_id,
    d.icd_code,
    d.icd_version,
    d.long_title,
    vo.visit_concept_id AS admission_type,
    d.seq_num
  FROM 
    diagnoses d
    INNER JOIN {schema}.visit_occurrence vo ON d.hadm_id = vo.visit_occurrence_id
),
admission_with_age AS (
  SELECT 
    vo.person_id AS subject_id,
    vo.visit_occurrence_id AS hadm_id,
    vo.visit_start_date AS admittime,
    vo.visit_concept_id AS admission_type,
    EXTRACT(YEAR FROM vo.visit_start_date) - p.year_of_birth AS age
  FROM 
    {schema}.visit_occurrence vo
    INNER JOIN {schema}.person p ON vo.person_id = p.person_id
),
final_feature_extraction AS (
  SELECT 
    a.subject_id,
    a.hadm_id,
    a.age,
    a.admittime,
    a.admission_type,
    d.event_year,
    d.icd_code,
    d.icd_version,
    d.long_title,
    d.seq_num
  FROM 
    diagnoses_with_year d
    INNER JOIN admission_with_age a ON d.hadm_id = a.hadm_id
)
SELECT 
  * 
FROM 
  final_feature_extraction
ORDER BY 
  admittime ASC
"""



diagnosis_df = psql.read_sql(sql_query, con=engine)

print(diagnosis_df['age'].max())

print(diagnosis_df['event_year'].max())

print(len(set(diagnosis_df['icd_code'])))

import pandas as pd
import requests



# הורדת הגרש משמות העמודות ומהערכים בטבלה
# Download the CSV file from the URL and save it to a local file
icd_aggegator_df = pd.read_csv('/home/viskymi/project/FederatedBEHRT/ccs_dx_icd10cm_2018_1.csv')
icd_aggegator_df.columns = icd_aggegator_df.columns.str.replace("'", "")
# icd_aggegator_df = icd_aggegator_df.applymap(lambda x: x.replace("'", ""))
icd_aggegator_df = icd_aggegator_df.replace("'", "", regex=True)



# Create a dictionary mapping ICD-10-CM codes to CCS categories
icd10_ccs_mapping = icd_aggegator_df.set_index('ICD-10-CM CODE')['CCS CATEGORY'].to_dict()
temp_icd_codes = set(diagnosis_df['icd_code']) 

# שם במקום האייסידי את הקוד הכללי יותר עבור כל קוד
def icd_codes_preprocessing(icd_code: str, with_icd10_codes_aggregation=False):
    icd_code = icd_code.strip()
    if with_icd10_codes_aggregation:
        icd_code = icd10_ccs_mapping[icd_code] if icd_code in icd10_ccs_mapping else icd_code
    return icd_code

diagnosis_df['icd_code'] = diagnosis_df['icd_code'].apply(lambda icd_code: icd_codes_preprocessing(icd_code, with_icd10_codes_aggregation=True))



counter = 0
missing_elements = []

for element in temp_icd_codes:
    element = element.strip()
    if element not in icd10_ccs_mapping:
        counter += 1
        missing_elements.append(element)
print(f'Number of elements in the set that are not in the dictionary: {counter}')

# הדפסת הקודים החסרים
print("Missing ICD codes that are not found in the dictionary:")
for code in missing_elements:
    print(code)

missing_elements[:10]



print(f'The number of unique patients is: {len(set(diagnosis_df.subject_id))}')

print(f'The number of unique diagnosis codes after aggregation is: {len(set(diagnosis_df.icd_code))}')




import os
if not os.path.exists('/home/viskymi/project/FederatedBEHRT/data/icd10'):
    os.makedirs('/home/viskymi/project/FederatedBEHRT/data/icd10')

diagnosis_df.to_csv(f'/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_icd10_features_with_aggregations.csv')



# בניית רצץ עבור אשפוז יחיד
def build_behrt_visit_codes(df: pd.DataFrame, hadm_id: int, visit_separator: str):
    visit_df = df[df['hadm_id'] == hadm_id]
    #visit_df = visit_df.sort_values(by=['seq_num'])
    concept_ids, ages, years =  list(visit_df['icd_code']), list(visit_df['age']), list(visit_df['event_year'])
    concept_ids.append(visit_separator)
    ages.append(ages[-1])
    years.append(years[-1])
    return concept_ids, ages, years



# Assume pr is the result from the function call
pr = build_behrt_visit_codes(diagnosis_df, hadm_id=tuple(diagnosis_df['hadm_id'].sample())[0], visit_separator='SEP')

# Define how many numbers you want per line
numbers_per_line = 10

print("The visit token: ")
# Print the numbers in chunks
for i in range(0, len(pr), numbers_per_line):
    print(pr[i:i + numbers_per_line])

# בניית רצפים עבור כל האשפוזים של מטופל.
def build_behrt_codes_for_person(person_id: int, visit_separator: str):
    person_codes = []
    person_ages = []
    person_years = []
    
    person_visit_hadm_ids = list(dict.fromkeys((diagnosis_df[diagnosis_df['subject_id'] == person_id]['hadm_id'])))
    for person_visit_id in person_visit_hadm_ids:
        visit_codes, visit_ages, visit_years = build_behrt_visit_codes(df=diagnosis_df, hadm_id=person_visit_id, visit_separator=visit_separator)
        person_codes.extend(visit_codes)
        person_ages.extend(visit_ages)
        person_years.extend(visit_years)
    return person_codes, person_ages, person_years


print(build_behrt_codes_for_person(person_id=tuple(diagnosis_df['subject_id'].sample())[0], visit_separator='SEP'))


from tqdm import tqdm

# בניית רצפים עבור כל האשפוזים של כל! המטופלים.
def build_behrt_codes_dataset(df: pd.DataFrame, visit_separator: str):
    person_ids = list(dict.fromkeys(df['subject_id']))
    ds_rows = []
    for person_id in tqdm(person_ids, desc = 'building ds from patients Progress Bar', disable=True):
        person_condition_codes, person_condition_ages, person_condition_years = build_behrt_codes_for_person(person_id, visit_separator)
        person_data = {'person_id': person_id, 'code': person_condition_codes, 'age': person_condition_ages, 'year': person_condition_years}
        ds_rows.append(person_data)
    return pd.DataFrame(ds_rows)


input_dataset = build_behrt_codes_dataset(diagnosis_df, visit_separator='SEP')

# עבור כל מטופל נשמר בקובץ כל הצפים של כל האבחונים שלו
input_dataset.to_csv('/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_behrt_with_aggregations_ds.csv')



# train-test split
from sklearn.model_selection import train_test_split

input_dataset_train_df, input_dataset_test_df = train_test_split(input_dataset, test_size=0.2)
input_dataset_train_df.to_csv("/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_behrt_with_aggregations_train_ds.csv", index=False)
input_dataset_test_df.to_csv("/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_behrt_with_aggregations_test_ds.csv", index=False)

TOKEN2INX_FILE_PATH = '/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_icd10_with_aggregations_token2idx.json'

from typing import List 

def get_all_codes(df: pd.DataFrame, codes_to_ignore: List[str]) -> List[str]:
    codes = []
    for df_list_codes in list(df['code']):
        codes.extend(df_list_codes)
    return list(set(codes) - set(codes_to_ignore))

print(f'number of unique codes: {len(get_all_codes(df=input_dataset, codes_to_ignore=[])) - 5}')


from typing import Dict
import json
from collections import OrderedDict

from typing import List 

def get_all_codes(df: pd.DataFrame, codes_to_ignore: List[str]) -> List[str]:
    codes = []
    for df_list_codes in list(df['code']):
        codes.extend(df_list_codes)
    return list(set(codes) - set(codes_to_ignore))

def get_bert_tokens() -> Dict[str, int]:
    return {
      "PAD": 0,
      "UNK": 1,
      "SEP": 2,
      "CLS": 3,
      "MASK": 4,
    }
    
def build_token2index_dict(df: pd.DataFrame) -> Dict[str, int]:
    token2inx_dict = get_bert_tokens()
    next_index = max(token2inx_dict.values()) + 1
    
    codes = get_all_codes(df= df, codes_to_ignore=token2inx_dict.keys())
    for code in codes:
        token2inx_dict[str(code)] = next_index
        next_index += 1
    return token2inx_dict

def create_token2index_file(df: pd.DataFrame, output_file_path: str):
    token2inx_dict = build_token2index_dict(df= df)
    with open(output_file_path, 'w') as f:
        json.dump(token2inx_dict, f)
        print(f'token2inx was created, path={output_file_path}')
      

create_token2index_file(df= input_dataset, output_file_path=TOKEN2INX_FILE_PATH)


import subprocess
subprocess.run(['python', '/home/viskymi/project/FederatedBEHRT/preprocess/bert_vocab_builder.py', TOKEN2INX_FILE_PATH, 'mimic_iv_icd10_with_aggregations_vocab.pkl'])


from typing import List, Dict
from random import randrange


def split_list(list_to_split: List, separator: str):
    list_after_split = []
    current_list = []
    for element in list_to_split:
        current_list.append(element)
        if element == separator:
            list_after_split.append(current_list)
            current_list = []
    return list_after_split

def split_visits(codes: List, ages: List, years: List, visit_index: int, separator: str):
    codes_after_split = split_list(list_to_split=codes, separator=separator)
    train_codes, test_codes = codes_after_split[:visit_index + 1], codes_after_split[visit_index + 1]
    train_codes = [item for sublist in train_codes for item in sublist]
    
    #train_codes_num = sum([len(visit_codes) for visit_codes in train_codes])
    train_codes_num = len(train_codes)
    test_codes_num = len(test_codes) # test_codes is not a nested list, because it contains only one visit details.

    train_ages, test_ages = ages[:train_codes_num], ages[train_codes_num:train_codes_num + test_codes_num]
    train_years, test_years = years[:train_codes_num], years[train_codes_num:train_codes_num + test_codes_num]################################
    return {
        'train_codes': train_codes,
        'test_codes': test_codes, 
        'train_ages': train_ages, 
        'test_ages': test_ages, 
        'train_years': train_years,
        'test_years': test_years
    } 

def build_next_visit_for_person(df: pd.DataFrame, min_visit_num: int, person_id: int, visit_separator: str) -> Dict[str, List]:
    person_df = df[df['person_id'] == person_id]
    codes, ages, years =  list(person_df['code'])[0], list(person_df['age'])[0], list(person_df['year'])[0]
    number_of_visits = codes.count(visit_separator)
    j = randrange(start=min_visit_num - 1, stop=number_of_visits - 1, step=1) # another -1 for stop criteria because we need to test on the next visit.
    return split_visits(codes, ages, years, visit_index=j, separator='SEP')

print("build_next_visit_for_person:")
print(build_next_visit_for_person(input_dataset, min_visit_num=1, person_id=84	, visit_separator='SEP'))

# בונה את הדאטה למודל, מכינה דאטה פריים שמכיל רשימת קודים לאימון וכן את הגילאים ואת מזהה המטופל וכן רשימת קודים לאימות
def build_next_visit_ds(input_dataset: pd.DataFrame, min_visit_num: int, visit_separator: str) -> pd.DataFrame:
    person_ids = list(dict.fromkeys(input_dataset['person_id']))
    ds_rows = []
    for person_id in tqdm(person_ids, desc = 'building next visit dataset Progress Bar'):
        person_df = input_dataset[input_dataset['person_id'] == person_id]
        codes =  list(person_df['code'])[0]
        num_of_visits = codes.count(visit_separator)
        if num_of_visits > min_visit_num:
            person_visits_dict = build_next_visit_for_person(df=input_dataset, min_visit_num=min_visit_num,
                                                       person_id=person_id, visit_separator=visit_separator)
            person_visits_dict['person_id'] = person_id
            ds_rows.append(person_visits_dict)
    next_visit_ds = pd.DataFrame(ds_rows)
    next_visit_ds = next_visit_ds[['train_codes', 'train_ages', 'person_id', 'test_codes']]
    next_visit_ds = next_visit_ds.rename(columns={'train_codes': 'code', 'train_ages': 'age', 'test_codes': 'label', 'person_id': 'patid'})
    return next_visit_ds


from sklearn.model_selection import train_test_split

input_dataset_train_df = pd.read_csv("/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_behrt_with_aggregations_train_ds.csv")
input_dataset_test_df = pd.read_csv("/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_behrt_with_aggregations_test_ds.csv")
for df in (input_dataset_train_df, input_dataset_test_df):
    for column_name in ('code', 'age', 'year'):
        df[column_name] = df[column_name].apply(lambda x: eval(x))


train_next_visit_df = build_next_visit_ds(input_dataset_train_df, min_visit_num=1, visit_separator='SEP')
test_next_visit_df = build_next_visit_ds(input_dataset_test_df, min_visit_num=1, visit_separator='SEP')


print(test_next_visit_df.sample())
print(test_next_visit_df.shape[0])


train_next_visit_df.to_csv('/home/viskymi/project/FederatedBEHRT/data/icd10/train_mimic_iv_behrt_with_aggregations_next_visit_ds.csv')
test_next_visit_df.to_csv('/home/viskymi/project/FederatedBEHRT/data/icd10/test_mimic_iv_behrt_with_aggregations_next_visit_ds.csv')

from tqdm import tqdm
import pandas as pd
import os
from sklearn.model_selection import train_test_split

def split_data_to_centers(df: pd.DataFrame, group_by_key: str, output_dir: str):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # no-need for train/test split again.
    #diagnosis_df_train, diagnosis_df_test = train_test_split(diagnosis_df, test_size=test_size, random_state=42)
    #diagnosis_df_test.to_csv(f'{output_dir}/test.csv')

    gb = df.groupby(group_by_key)   
    groups = dict(list(gb))
    for group_key, group_df in tqdm(groups.items(), desc="split data to centers"):
        group_key = group_key.replace('/', '-') # something we have / in the data, for example Hematology/Oncology.
        print(f'group_key={group_key}')
        df_output_path = f'{output_dir}/{group_key}.csv'
        group_df.to_csv(df_output_path)
    print('done to split the data to centers.')

transfers_sql_query = f"""
    SELECT
    vd.person_id AS subject_id,
    vd.visit_occurrence_id AS hadm_id,
    vd.visit_detail_id AS transfer_id,
    cs.care_site_source_value AS careunit,
    vd.visit_detail_start_datetime AS intime,
    vd.visit_detail_end_datetime AS outtime,
    EXTRACT(epoch FROM (vd.visit_detail_end_datetime - vd.visit_detail_start_datetime)) AS stay_time
FROM
    {schema}.visit_detail vd
    LEFT JOIN {schema}.care_site cs ON vd.care_site_id = cs.care_site_id;

"""

transfers_df = psql.read_sql(transfers_sql_query, con=engine).dropna(subset=['hadm_id'])
transfers_df['hadm_id'] = transfers_df['hadm_id'].astype(int)

def split_by_max_stay_time(transfers_df: pd.DataFrame, behrt_train_df: pd.DataFrame, group_by_key: str, output_dir: str):
    df = transfers_df.loc[transfers_df.groupby(by=['subject_id']).stay_time.idxmax()]
    transfers_with_behrt_df = df.merge(behrt_train_df, left_on='subject_id', right_on='person_id', how='inner', suffixes=('', '_drop'))
    ids = list(transfers_with_behrt_df['subject_id'])
    if len(ids) == len(set(ids)):
        print("There are no repeated elements in the list.")
    else:
        print("There are repeated elements in the list.")

    split_data_to_centers(df=transfers_with_behrt_df, group_by_key=group_by_key,
                          output_dir=output_dir)
    

split_by_max_stay_time(transfers_df, input_dataset_train_df, group_by_key='careunit', output_dir='data/icd10-multi-center/split_by_max_stay_time')


from tqdm import tqdm
import os 
import glob 

def centers_to_behrt_format(centers_data_dir_path: str, output_behrt_dir_data_path: str):
    if not os.path.exists(output_behrt_dir_data_path):
        os.makedirs(output_behrt_dir_data_path)

    for center_csv_path in tqdm(glob.iglob(f'{centers_data_dir_path}/*.csv'), desc="centers to behrt format"):
        center_df = pd.read_csv(center_csv_path)
        center_file_name = os.path.basename(center_csv_path)
        center_behrt_dataset = build_behrt_codes_dataset(center_df, visit_separator='SEP')
        output_ds_path = f"{output_behrt_dir_data_path}/{center_file_name}"
        center_behrt_dataset.to_csv(output_ds_path)



centers_to_behrt_format(centers_data_dir_path='/home/viskymi/project/FederatedBEHRT/data/icd10-multi-center/split_by_max_stay_time/',
               output_behrt_dir_data_path='/home/viskymi/project/FederatedBEHRT/data/icd10-multi-center/BEHRT_format/split_by_max_stay_time')


pd.read_csv('/home/viskymi/project/FederatedBEHRT/data/icd10/train_mimic_iv_behrt_with_aggregations_next_visit_ds.csv').columns


def split_behrt_next_visit_to_centers(behrt_next_visit_path: str, output_dir: str):
    behrt_next_visit_df = pd.read_csv(behrt_next_visit_path)
    # convert the lists to list, because after read_csv their type is string. 
    for column_name in ('code', 'age', 'label'):
        behrt_next_visit_df[column_name] = behrt_next_visit_df[column_name].apply(lambda x: eval(x))
    

    df = transfers_df.loc[transfers_df.groupby(by=['subject_id']).stay_time.idxmax()]
    transfers_with_diagnosis_df = df.merge(behrt_next_visit_df, left_on='subject_id', right_on='patid', how='inner', suffixes=('', '_drop'))
    split_data_to_centers(df=transfers_with_diagnosis_df, group_by_key='careunit', output_dir=output_dir)



split_behrt_next_visit_to_centers(behrt_next_visit_path='/home/viskymi/project/FederatedBEHRT/data/icd10/train_mimic_iv_behrt_with_aggregations_next_visit_ds.csv', 
                                  output_dir='/home/viskymi/project/FederatedBEHRT/data/icd10-multi-center/BEHRT_format/split_by_max_stay_time')

import glob
import os
import seaborn as sns 
import pandas as pd 
import matplotlib.pyplot as plt

def show_centers_statistics(dir_path: str, title_split_name: str):
    center_name_to_size_dict = {}
    
    for center_csv_path in glob.iglob(f'{dir_path}/*.csv'):
        df = pd.read_csv(center_csv_path)
        center_name_to_size_dict[os.path.basename(center_csv_path).replace('.csv', '')] = df.shape[0]
        
    print(f'The number of centers is={len(center_name_to_size_dict)}')
    print(f'The names of the centers: {center_name_to_size_dict.keys()}')
    d = pd.DataFrame(center_name_to_size_dict.items(), columns=['name', 'count'])
    d = d.sort_values(by=['count'], ascending=False).head(10).sort_values(by=['count'], ascending=True)
    
    sns.set(font_scale=1.2, rc={"figure.figsize":(7, 5)})
    #sns.barplot(data=d, x="count", y="name").set_title(f'multi center by {title_split_name} split. There are {len(center_name_to_size_dict)} centers')
    d.rename(columns={'count': 'number of patients'}, inplace=True)
    sns.barplot(data=d, x="number of patients", y="name").set_title(f'multi center by {title_split_name} split')
    plt.tight_layout()
    plt.savefig("multi-center-analysis.pdf", format='pdf')

    
def show_centers_statistics_logarithmic_scale(dir_path: str, title_split_name: str):
    center_name_to_size_dict = {}
    
    for center_csv_path in glob.iglob(f'{dir_path}/*.csv'):
        df = pd.read_csv(center_csv_path)
        center_name_to_size_dict[os.path.basename(center_csv_path).replace('.csv', '')] = df.shape[0]
        
    print(f'The number of centers is={len(center_name_to_size_dict)}')
    print(f'The names of the centers: {center_name_to_size_dict.keys()}')
    d = pd.DataFrame(center_name_to_size_dict.items(), columns=['name', 'count'])
    d = d.sort_values(by=['count'], ascending=False).sort_values(by=['count'], ascending=True)
    
    sns.set(font_scale=1.2, rc={"figure.figsize":(12, 8)})
    d.rename(columns={'count': 'number of patients'}, inplace=True)
    sns.barplot(data=d, x="number of patients", y="name", log=True)
    plt.tight_layout()
    plt.savefig("multi-center-log-scale-analysis.pdf", format='pdf')

show_centers_statistics(dir_path='/home/viskymi/project/FederatedBEHRT/data/icd10-multi-center/split_by_max_stay_time/', title_split_name='max stay time')

show_centers_statistics(dir_path='/home/viskymi/project/FederatedBEHRT/data/icd10-multi-center/BEHRT_format/next_visit/split_by_max_stay_time/', title_split_name='next-visit, max_stay_time')