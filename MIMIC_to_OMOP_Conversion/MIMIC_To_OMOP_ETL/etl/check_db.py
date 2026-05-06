# check_db.py
# from etl.common import postgresql_cursor
# import os

# PGHOST = os.environ.get('PGHOST')
# PGPORT = int(os.environ.get('PGPORT', '5432'))
# PGUSER = os.environ.get('PGUSER')
# PGPASSWORD = os.environ.get('PGPASSWORD')

# postgresql_cursor()

# start_etl.py
print("Loading environment variables...1")
import os
from etl.common import postgresql_cursor

# Load environment variables
print("Loading environment variables...")
PGHOST = os.environ.get('PGHOST')
PGPORT = int(os.environ.get('PGPORT', '5432'))
PGUSER = os.environ.get('PGUSER')
PGPASSWORD = os.environ.get('PGPASSWORD')
MODEPGDB = os.environ.get('MODEPGDB')

print("Environment variables loaded:")
print(f"PGHOST={PGHOST}, PGPORT={PGPORT}, PGUSER={PGUSER}, PGPASSWORD={'*****' if PGPASSWORD else None}, MODEPGDB={MODEPGDB}")

# Run the PostgreSQL cursor function and check the connection
try:
    print("Attempting to run postgresql_cursor()...")
    with postgresql_cursor() as cursor:
        # Check PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("Connected to PostgreSQL successfully. Version:", version[0])

        # List tables in each schema
        schemas = ['mimiciv_hosp', 'mimiciv_icu', 'vocabulary']
        for schema in schemas:
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';")
            tables = cursor.fetchall()
            print(f"Tables in schema '{schema}': {[table[0] for table in tables]}")

    print("postgresql_cursor() ran successfully.")
except Exception as e:
    print(f"Error while running postgresql_cursor: {e}")
