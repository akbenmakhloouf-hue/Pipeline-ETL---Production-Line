"""
ETL Pipeline for Production Line Data Integration - VERSION CORRIGÉE
"""
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta


# ============================================================================
# PHASE 1: EXTRACT
# ============================================================================

def extract_sensor_data(file_path, days_back=None):
    """
    Extract sensor data from CSV file.

    Args:
        file_path: Path to sensor CSV file
        days_back: Number of days to look back (None = all data)

    Returns:
        pandas DataFrame with sensor readings
    """
    try:
        # Read CSV file
        df = pd.read_csv('Dataset 1 — Capteurs.csv')
        print(f"✓ Successfully loaded sensor data: {len(df)} records")

        # Convert timestamp column to datetime
        timestamp_col = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
        if timestamp_col:
            df[timestamp_col[0]] = pd.to_datetime(df[timestamp_col[0]], errors='coerce')

            # CORRECTION: Afficher la plage de dates disponibles
            min_date = df[timestamp_col[0]].min()
            max_date = df[timestamp_col[0]].max()
            print(f"📅 Date range in data: {min_date} to {max_date}")

            # Filter for last N days SEULEMENT si days_back est spécifié
            if days_back is not None:
                cutoff_date = datetime.now() - timedelta(days=days_back)
                original_count = len(df)
                df = df[df[timestamp_col[0]] >= cutoff_date]
                print(f"✓ Filtered to last {days_back} days: {len(df)}/{original_count} records")

                # CORRECTION: Avertissement si aucune donnée dans la période
                if len(df) == 0:
                    print(f"⚠ WARNING: No data found in the last {days_back} days!")
                    print(f"⚠ Using ALL available data instead ({original_count} records)")
                    # Recharger toutes les données
                    df = pd.read_csv(file_path)
                    df[timestamp_col[0]] = pd.to_datetime(df[timestamp_col[0]], errors='coerce')
            else:
                print(f"✓ Using all available data: {len(df)} records")

        return df

    except FileNotFoundError:
        print(f"✗ ERROR: File not found - {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"✗ ERROR extracting sensor data: {str(e)}")
        return pd.DataFrame()


def extract_quality_data(file_path):
    """Extract quality inspection data with encoding handling."""
    encodings = ['utf-8', 'iso-8859-1', 'latin1']

    for encoding in encodings:
        try:
            df = pd.read_csv('Dataset 2 — Qualité.csv', encoding=encoding)
            print(f"✓ Successfully loaded quality data with {encoding}: {len(df)} records")

            # Convert timestamp if exists
            timestamp_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
            if timestamp_cols:
                df[timestamp_cols[0]] = pd.to_datetime(df[timestamp_cols[0]], errors='coerce')
                min_date = df[timestamp_cols[0]].min()
                max_date = df[timestamp_cols[0]].max()
                print(f"📅 Quality data range: {min_date} to {max_date}")

            # Filter for completed inspections if status column exists
            status_cols = [col for col in df.columns if 'status' in col.lower()]
            if status_cols:
                original_count = len(df)
                df = df[df[status_cols[0]].notna()]
                print(f"✓ Filtered completed inspections: {len(df)}/{original_count} records")

            return df

        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"✗ ERROR: File not found - {file_path}")
            return pd.DataFrame()
        except Exception as e:
            if encoding == encodings[-1]:  # Last encoding
                print(f"✗ ERROR: Could not read file with any encoding: {str(e)}")
            continue

    return pd.DataFrame()


# ============================================================================
# PHASE 2: TRANSFORM
# ============================================================================

def clean_sensor_data(df):
    """Clean sensor data by handling error codes and validating ranges."""
    if df.empty:
        return df

    df_clean = df.copy()

    # Identify sensor columns
    sensor_cols = {
        'temperature': [col for col in df.columns if 'temp' in col.lower()],
        'pressure': [col for col in df.columns if 'press' in col.lower()],
        'vibration': [col for col in df.columns if 'vibr' in col.lower()]
    }

    # Valid ranges
    ranges = {
        'temperature': (0, 150),
        'pressure': (0, 10),
        'vibration': (0, 100)
    }

    # Initialize data quality flag
    df_clean['data_quality'] = 'good'

    for sensor_type, cols in sensor_cols.items():
        for col in cols:
            if col not in df_clean.columns:
                continue

            # Replace error codes with NaN
            df_clean[col] = df_clean[col].replace([-999, -1, None], np.nan)

            # Validate ranges
            min_val, max_val = ranges.get(sensor_type, (None, None))
            if min_val is not None:
                invalid_mask = (df_clean[col] < min_val) | (df_clean[col] > max_val)
                invalid_indices = df_clean[invalid_mask].index
                df_clean.loc[invalid_mask, col] = np.nan
                df_clean.loc[invalid_indices, 'data_quality'] = 'invalid'

            # Forward fill missing values
            filled_mask = df_clean[col].isna()
            df_clean[col] = df_clean[col].ffill()

            # Mark estimated values
            estimated_mask = filled_mask & df_clean[col].notna()
            df_clean.loc[estimated_mask, 'data_quality'] = 'estimated'

            # Mark still NaN values as invalid
            still_nan_mask = df_clean[col].isna()
            df_clean.loc[still_nan_mask, 'data_quality'] = 'invalid'

    print(f"✓ Cleaned sensor data: {len(df_clean)} records")
    quality_dist = df_clean['data_quality'].value_counts().to_dict()
    print(f"  Quality distribution: {quality_dist}")

    return df_clean


def standardize_data(df):
    """Standardize data formats and naming conventions."""
    if df.empty:
        return df

    df_std = df.copy()

    # Remove spaces from column names and lowercase
    df_std.columns = df_std.columns.str.strip().str.lower().str.replace(' ', '_')

    # Convert timestamps to datetime
    for col in df_std.columns:
        if 'time' in col or 'date' in col:
            if df_std[col].dtype != 'datetime64[ns]':
                df_std[col] = pd.to_datetime(df_std[col], errors='coerce')

    # Lowercase machine/sensor names
    for col in df_std.columns:
        if 'machine' in col or 'line' in col or 'sensor' in col:
            if df_std[col].dtype == 'object':
                df_std[col] = df_std[col].str.lower().str.strip()

    # CORRECTION: Supprimer l'ancien record_id s'il existe, puis en créer un nouveau
    if 'record_id' in df_std.columns:
        df_std = df_std.drop(columns=['record_id'])

    # Create unique record_id
    df_std['record_id'] = [f"REC_{i:08d}" for i in range(len(df_std))]

    print(f"✓ Standardized data: {len(df_std)} records")

    return df_std


def join_sensor_quality_data(sensor_df, quality_df):
    """Join sensor readings with quality checks."""
    if sensor_df.empty:
        return sensor_df

    if quality_df.empty:
        sensor_df['quality_status'] = 'not_checked'
        print("⚠ No quality data available - marking all as 'not_checked'")
        return sensor_df

    # Find join columns
    sensor_time_cols = [col for col in sensor_df.columns if 'time' in col or 'date' in col]
    quality_time_cols = [col for col in quality_df.columns if 'time' in col or 'date' in col]

    sensor_machine_cols = [col for col in sensor_df.columns if 'machine' in col or 'line' in col]
    quality_machine_cols = [col for col in quality_df.columns if 'machine' in col or 'line' in col]

    if not sensor_time_cols or not quality_time_cols:
        print("⚠ No timestamp columns found for joining")
        sensor_df['quality_status'] = 'not_checked'
        return sensor_df

    sensor_time_col = sensor_time_cols[0]
    quality_time_col = quality_time_cols[0]
    sensor_machine_col = sensor_machine_cols[0] if sensor_machine_cols else None
    quality_machine_col = quality_machine_cols[0] if quality_machine_cols else None

    # Standardize quality data
    quality_df_std = quality_df.copy()
    quality_df_std.columns = quality_df_std.columns.str.strip().str.lower().str.replace(' ', '_')

    if quality_df_std[quality_time_col].dtype != 'datetime64[ns]':
        quality_df_std[quality_time_col] = pd.to_datetime(quality_df_std[quality_time_col], errors='coerce')

    # Perform LEFT JOIN
    if sensor_machine_col and quality_machine_col:
        joined_df = sensor_df.merge(
            quality_df_std,
            left_on=[sensor_time_col, sensor_machine_col],
            right_on=[quality_time_col, quality_machine_col],
            how='left',
            suffixes=('', '_quality')
        )
    else:
        joined_df = sensor_df.merge(
            quality_df_std,
            left_on=sensor_time_col,
            right_on=quality_time_col,
            how='left',
            suffixes=('', '_quality')
        )

    # Add quality_status column
    result_cols = [col for col in joined_df.columns if 'result' in col.lower()]
    if result_cols:
        joined_df['quality_status'] = joined_df[result_cols[0]].apply(
            lambda x: 'pass' if pd.notna(x) and 'pass' in str(x).lower()
            else 'fail' if pd.notna(x) and 'fail' in str(x).lower()
            else 'not_checked'
        )
    else:
        joined_df['quality_status'] = 'not_checked'

    print(f"✓ Joined sensor and quality data: {len(joined_df)} records")
    status_dist = joined_df['quality_status'].value_counts().to_dict()
    print(f"  Quality status: {status_dist}")

    return joined_df


def calculate_hourly_summaries(df):
    """Calculate hourly aggregates from joined data."""
    if df.empty:
        return pd.DataFrame()

    # Find timestamp column
    time_cols = [col for col in df.columns if 'time' in col and 'quality' not in col]
    if not time_cols:
        print("✗ No timestamp column found")
        return pd.DataFrame()

    time_col = time_cols[0]

    # Create hour column
    df['hour'] = df[time_col].dt.floor('h')  # CORRECTION: 'h' au lieu de 'H'

    # Identify grouping columns
    group_cols = ['hour']
    for col in df.columns:
        if ('line' in col.lower() or 'machine' in col.lower()) and 'quality' not in col:
            if col not in group_cols:
                group_cols.append(col)

    # Identify sensor columns
    sensor_mapping = {}
    for col in df.columns:
        if 'temp' in col.lower() and 'quality' not in col:
            sensor_mapping['temperature'] = col
        elif 'press' in col.lower() and 'quality' not in col:
            sensor_mapping['pressure'] = col
        elif 'vibr' in col.lower() and 'quality' not in col:
            sensor_mapping['vibration'] = col
        elif 'power' in col.lower() and 'quality' not in col:
            sensor_mapping['power'] = col

    # Build aggregation dictionary
    agg_dict = {}
    for sensor_name, col_name in sensor_mapping.items():
        agg_dict[col_name] = ['mean', 'min', 'max', 'std']

    # Perform aggregation
    summary = df.groupby(group_cols, as_index=False).agg(agg_dict)

    # Flatten column names
    new_columns = []
    for col in summary.columns:
        if isinstance(col, tuple):
            sensor_col, stat = col
            sensor_name = None
            for name, original_col in sensor_mapping.items():
                if original_col == sensor_col:
                    sensor_name = name
                    break
            if sensor_name:
                stat_map = {'mean': 'avg', 'min': 'min', 'max': 'max', 'std': 'std'}
                new_columns.append(f"{stat_map[stat]}_{sensor_name}")
            else:
                new_columns.append('_'.join(str(c) for c in col).strip('_'))
        else:
            new_columns.append(col)

    summary.columns = new_columns

    # Calculate quality metrics
    quality_stats = df.groupby(group_cols, as_index=False).agg({
        'quality_status': [
            ('total_checks', 'count'),
            ('defect_count', lambda x: (x == 'fail').sum())
        ]
    })

    quality_stats.columns = group_cols + ['total_checks', 'defect_count']
    quality_stats['defect_rate'] = (
            quality_stats['defect_count'] / quality_stats['total_checks'] * 100
    ).fillna(0)

    # Merge
    summary = summary.merge(quality_stats, on=group_cols, how='left')

    print(f"✓ Calculated hourly summaries: {len(summary)} records")

    return summary


# ============================================================================
# PHASE 3: LOAD
# ============================================================================

def create_database(db_path='production.db'):
    """Create SQLite database with required schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_readings (
        record_id TEXT PRIMARY KEY,
        timestamp DATETIME,
        line_id TEXT,
        machine_id TEXT,
        temperature REAL,
        pressure REAL,
        vibration REAL,
        power REAL,
        data_quality TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS quality_checks (
        check_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME,
        line_id TEXT,
        machine_id TEXT,
        result TEXT,
        defect_type TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hourly_summary (
        summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
        hour DATETIME,
        line_id TEXT,
        machine_id TEXT,
        avg_temperature REAL,
        min_temperature REAL,
        max_temperature REAL,
        avg_pressure REAL,
        avg_vibration REAL,
        total_checks INTEGER,
        defect_count INTEGER,
        defect_rate REAL
    )
    ''')

    conn.commit()
    print(f"✓ Database created: {db_path}")

    return conn


def load_to_database(sensor_df, quality_df, summary_df, conn):
    """Load transformed data into SQLite database."""
    try:
        # Load sensor readings
        if not sensor_df.empty:
            sensor_load = sensor_df.copy()

            # CORRECTION: D'abord, supprimer toutes les colonnes en double
            sensor_load = sensor_load.loc[:, ~sensor_load.columns.duplicated()]

            # Identifier les colonnes disponibles (sans créer de duplicates)
            col_map = {}
            for col in sensor_load.columns:
                col_lower = col.lower()
                if 'record_id' == col_lower and 'record_id' not in col_map.values():
                    col_map[col] = 'record_id'
                elif ('time' in col_lower or 'date' in col_lower) and 'timestamp' not in col_map.values():
                    col_map[col] = 'timestamp'
                elif 'line' in col_lower and 'quality' not in col_lower and 'line_id' not in col_map.values():
                    col_map[col] = 'line_id'
                elif 'machine' in col_lower and 'quality' not in col_lower and 'machine_id' not in col_map.values():
                    col_map[col] = 'machine_id'
                elif 'temp' in col_lower and 'quality' not in col_lower and 'temperature' not in col_map.values():
                    col_map[col] = 'temperature'
                elif 'press' in col_lower and 'quality' not in col_lower and 'pressure' not in col_map.values():
                    col_map[col] = 'pressure'
                elif 'vibr' in col_lower and 'quality' not in col_lower and 'vibration' not in col_map.values():
                    col_map[col] = 'vibration'
                elif 'power' in col_lower and 'quality' not in col_lower and 'power' not in col_map.values():
                    col_map[col] = 'power'
                elif 'data_quality' == col_lower and 'data_quality' not in col_map.values():
                    col_map[col] = 'data_quality'

            # Renommer et sélectionner uniquement les colonnes mappées
            sensor_load = sensor_load.rename(columns=col_map)

            # Ordre des colonnes pour la base de données
            target_cols = ['record_id', 'timestamp', 'line_id', 'machine_id',
                           'temperature', 'pressure', 'vibration', 'power', 'data_quality']
            available_cols = [c for c in target_cols if c in sensor_load.columns]

            # Sélectionner uniquement ces colonnes (élimine les doublons)
            sensor_load = sensor_load[available_cols].copy()

            # Vérification finale: pas de colonnes dupliquées
            if sensor_load.columns.duplicated().any():
                print(f"⚠ Removing duplicate columns: {sensor_load.columns[sensor_load.columns.duplicated()].tolist()}")
                sensor_load = sensor_load.loc[:, ~sensor_load.columns.duplicated()]

            if 'record_id' in sensor_load.columns:
                print(f"📋 Columns to load: {sensor_load.columns.tolist()}")
                sensor_load.to_sql('sensor_readings', conn, if_exists='replace', index=False)
                print(f"✓ Loaded {len(sensor_load)} sensor readings")
            else:
                print("⚠ Could not map sensor columns to database schema")

        if not quality_df.empty:
            quality_load = quality_df.copy()

            col_map = {}
            for col in quality_load.columns:
                if 'time' in col.lower() or 'date' in col.lower():
                    if 'timestamp' not in col_map.values():
                        col_map[col] = 'timestamp'
                elif 'line' in col.lower() and 'line_id' not in col_map.values():
                    col_map[col] = 'line_id'
                elif 'machine' in col.lower() and 'machine_id' not in col_map.values():
                    col_map[col] = 'machine_id'
                elif 'result' in col.lower() and 'result' not in col_map.values():
                    col_map[col] = 'result'
                elif 'defect' in col.lower() or 'fault' in col.lower():
                    if 'defect_type' not in col_map.values():
                        col_map[col] = 'defect_type'

            quality_load = quality_load.rename(columns=col_map)
            quality_load = quality_load[[c for c in col_map.values() if c in quality_load.columns]]

            if not quality_load.empty:
                quality_load.to_sql('quality_checks', conn, if_exists='replace', index=False)
                print(f"✓ Loaded {len(quality_load)} quality checks")

        if not summary_df.empty:
            summary_load = summary_df.copy()
            summary_load.columns = summary_load.columns.str.lower()
            summary_load.to_sql('hourly_summary', conn, if_exists='replace', index=False)
            print(f"✓ Loaded {len(summary_load)} hourly summaries")

        conn.commit()
        print("✓ All data successfully loaded to database")

    except Exception as e:
        print(f"✗ ERROR loading data to database: {str(e)}")
        import traceback
        traceback.print_exc()
        conn.rollback()


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_etl_pipeline(sensor_file, quality_file, db_path='production.db', days_back=None):
    """
    Execute the complete ETL pipeline.

    Args:
        sensor_file: Path to sensor CSV file
        quality_file: Path to quality CSV file
        db_path: Path to output database
        days_back: Number of days to look back (None = all data)
    """
    print("=" * 70)
    print("ETL PIPELINE - PRODUCTION LINE DATA INTEGRATION")
    print("=" * 70)

    # PHASE 1: EXTRACT
    print("\n[PHASE 1: EXTRACT]")
    sensor_raw = extract_sensor_data(sensor_file, days_back=days_back)
    quality_raw = extract_quality_data(quality_file)

    if sensor_raw.empty:
        print("✗ PIPELINE FAILED: No sensor data extracted")
        return

    # PHASE 2: TRANSFORM
    print("\n[PHASE 2: TRANSFORM]")
    print("Task 2.1: Cleaning sensor data...")
    sensor_clean = clean_sensor_data(sensor_raw)

    print("Task 2.2: Standardizing data...")
    sensor_std = standardize_data(sensor_clean)
    quality_std = standardize_data(quality_raw) if not quality_raw.empty else quality_raw

    print("Task 2.3: Joining sensor and quality data...")
    joined_data = join_sensor_quality_data(sensor_std, quality_std)

    print("Task 2.4: Calculating hourly summaries...")
    hourly_summary = calculate_hourly_summaries(joined_data)

    # PHASE 3: LOAD
    print("\n[PHASE 3: LOAD]")

    # DIAGNOSTIC: Afficher les colonnes avant chargement
    print("\n🔍 DIAGNOSTIC - Colonnes dans joined_data:")
    print(f"  Total colonnes: {len(joined_data.columns)}")
    print(f"  Colonnes: {joined_data.columns.tolist()[:20]}...")  # Afficher les 20 premières
    if joined_data.columns.duplicated().any():
        duplicates = joined_data.columns[joined_data.columns.duplicated()].tolist()
        print(f"  ⚠ Colonnes dupliquées détectées: {duplicates}")

    conn = create_database(db_path)
    load_to_database(joined_data, quality_std, hourly_summary, conn)

    conn.close()

    print("\n" + "=" * 70)
    print("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"\nDatabase saved to: {db_path}")

    # Vérification de la base de données
    verify_database(db_path)


def verify_database(db_path='production.db'):
    """Verify database contents and display statistics."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print("\n" + "=" * 70)
        print("📊 DATABASE VERIFICATION")
        print("=" * 70)

        # Check sensor_readings table
        cursor.execute("SELECT COUNT(*) FROM sensor_readings")
        sensor_count = cursor.fetchone()[0]
        print(f"✓ sensor_readings: {sensor_count:,} records")

        # Check quality_checks table
        cursor.execute("SELECT COUNT(*) FROM quality_checks")
        quality_count = cursor.fetchone()[0]
        print(f"✓ quality_checks: {quality_count:,} records")

        # Check hourly_summary table
        cursor.execute("SELECT COUNT(*) FROM hourly_summary")
        summary_count = cursor.fetchone()[0]
        print(f"✓ hourly_summary: {summary_count:,} records")

        print("\n📈 Sample queries you can run:")
        print("  SELECT * FROM sensor_readings LIMIT 10;")
        print("  SELECT * FROM hourly_summary ORDER BY defect_rate DESC LIMIT 10;")
        print("  SELECT AVG(defect_rate) FROM hourly_summary;")

        conn.close()

    except Exception as e:
        print(f"✗ Error verifying database: {str(e)}")


if __name__ == "__main__":
    # CORRECTION: Utiliser days_back=None pour prendre TOUTES les données
    SENSOR_FILE = "Dataset 1 — Capteurs.csv"
    QUALITY_FILE = "Dataset 2 — Qualité.csv"

    # Option 1: Prendre TOUTES les données (recommandé pour les anciennes données)
    run_etl_pipeline(SENSOR_FILE, QUALITY_FILE, days_back=None)

    # Option 2: Filtrer sur 30 jours (si vous avez des données récentes)
    # run_etl_pipeline(SENSOR_FILE, QUALITY_FILE, days_back=30)

    # Option 3: Filtrer sur 365 jours (1 an)
    # run_etl_pipeline(SENSOR_FILE, QUALITY_FILE, days_back=365)