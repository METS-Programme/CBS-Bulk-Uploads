import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# --- Configuration ---
CSV_FOLDER = ''
DB_URL = ''
TARGET_TABLE = 'staging_patient'

# Create database engine
engine = create_engine(DB_URL, echo=False)

# Ensure the tracking table exists
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS imported_files_log (
            file_name TEXT PRIMARY KEY,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.commit()

# List all CSV files
csv_files = [f for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')]
failed_files = []

with engine.connect() as conn:
    for file_name in csv_files:
        # Check if the file has already been imported
        result = conn.execute(text("SELECT 1 FROM imported_files_log WHERE file_name = :fname"), {"fname": file_name})
        if result.scalar():
            print(f"⏩ Skipping already imported file: {file_name}")
            continue

        file_path = os.path.join(CSV_FOLDER, file_name)
        print(f"📥 Importing: {file_name}")

        try:
            df = pd.read_csv(file_path, na_values=['\\N', 'NA', 'null', 'NULL', ''])
            for col in df.select_dtypes(include='object').columns:
                df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

            # Format date columns
                # Format date columns
                if 'date_of_birth' in df.columns:
                    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
                    df['date_of_birth'] = df['date_of_birth'].fillna(pd.Timestamp('1900-01-01'))
                    df['date_of_birth'] = df['date_of_birth'].dt.strftime('%Y-%m-%d')

                if 'date_of_death' in df.columns:
                    df['date_of_death'] = pd.to_datetime(df['date_of_death'], errors='coerce')
                    df['date_of_death'] = df['date_of_death'].dt.strftime('%Y-%m-%d')

                if 'updated_date' in df.columns:
                    df['updated_date'] = pd.to_datetime(df['updated_date'], errors='coerce')
                    df['updated_date'] = df['updated_date'].dt.strftime('%Y-%m-%d %H:%M')

            # Remove rows with duplicate primary keys (already in DB)
            existing_ids = pd.read_sql(f"SELECT case_id FROM {TARGET_TABLE}", conn)
            df = df[~df['case_id'].isin(existing_ids['case_id'])]

            if df.empty:
                print(f"⚠️ No new rows to insert for {file_name}. Skipping.")
                continue

            df.to_sql(TARGET_TABLE, engine, if_exists='append', index=False)

            # Log the successful insert
            conn.execute(text("INSERT INTO imported_files_log (file_name) VALUES (:fname)"), {"fname": file_name})
            conn.commit()
            print(f"✅ Inserted: {file_name}")

        except IntegrityError as e:
            print(f"❌ Integrity error in {file_name}: {e.orig}")
            failed_files.append(file_name)

        except Exception as e:
            print(f"❌ Failed to import {file_name}: {e}")
            failed_files.append(file_name)

# Summary of failed files
if failed_files:
    print("\n🚨 The following files failed to import:")
    for f in failed_files:
        print(f" - {f}")
else:
    print("\n✅ All files imported successfully.")
