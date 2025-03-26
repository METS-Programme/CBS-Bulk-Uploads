import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# --- Configuration ---
CSV_FOLDER = ''
DB_URL = ''
TARGET_TABLE = 'staging_patient_encounters'
SUCCESS_LOG_TABLE = 'import_encounter_logs'
FAILURE_LOG_TABLE = 'import_encounter_failures'

# Create database engine
engine = create_engine(DB_URL, echo=False)

# List all CSV files
csv_files = [f for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')]
failed_files = []

# Ensure the tracking tables exist
with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SUCCESS_LOG_TABLE} (
            file_name TEXT PRIMARY KEY,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {FAILURE_LOG_TABLE} (
            file_name TEXT PRIMARY KEY,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )
    """))

for file_name in csv_files:
    file_path = os.path.join(CSV_FOLDER, file_name)
    print(f"Reading file: {file_path}")

    try:
        df = pd.read_csv(file_path, na_values=['\\N', 'NA', 'null', 'NULL', ''])
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

        # Format date columns
        if 'encounter_date' in df.columns:
            df['encounter_date'] = pd.to_datetime(df['encounter_date'], errors='coerce')
            df['encounter_date'] = df['encounter_date'].fillna(pd.Timestamp('1900-01-01'))
            df['encounter_date'] = df['encounter_date'].dt.strftime('%Y-%m-%d')

        if 'updated_date' in df.columns:
            df['updated_date'] = pd.to_datetime(df['updated_date'], errors='coerce')
            df['updated_date'] = df['updated_date'].dt.strftime('%Y-%m-%d %H:%M')

        with engine.begin() as conn:
            # Skip files already logged as successful
            result = conn.execute(text(f"SELECT 1 FROM {SUCCESS_LOG_TABLE} WHERE file_name = :fname"), {"fname": file_name})
            if result.scalar():
                print(f"⏩ Skipping already imported file: {file_name}")
                continue

            # Skip duplicate encounter_ids
            if 'encounter_id' in df.columns:
                existing_ids = pd.read_sql(f"SELECT encounter_id FROM {TARGET_TABLE}", conn)
                df = df[~df['encounter_id'].isin(existing_ids['encounter_id'])]

            if df.empty:
                print(f"⚠️ No new rows to insert for {file_name}. Skipping.")
                continue

            print(f"Inserting {len(df)} rows into table '{TARGET_TABLE}'...")
            df.to_sql(TARGET_TABLE, engine, if_exists='append', index=False)

            # Log successful insert
            conn.execute(text(f"INSERT INTO {SUCCESS_LOG_TABLE} (file_name) VALUES (:fname)"), {"fname": file_name})
            print(f"✅ Inserted and logged: {file_name}")

    except Exception as e:
        error_message = str(e).replace("'", "")[:1000]  # Truncate to avoid overly long errors
        with engine.begin() as conn:
            conn.execute(text(f"INSERT INTO {FAILURE_LOG_TABLE} (file_name, error_message) VALUES (:fname, :err) ON CONFLICT (file_name) DO UPDATE SET error_message = EXCLUDED.error_message, failed_at = CURRENT_TIMESTAMP"), {"fname": file_name, "err": error_message})
        print(f"❌ Error processing {file_name}: {e}")
        failed_files.append(file_name)

# Summary of failed files
if failed_files:
    print("\n🚨 The following files failed to import:")
    for f in failed_files:
        print(f" - {f}")
else:
    print("\n✅ All files imported successfully.")
