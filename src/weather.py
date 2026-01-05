import duckdb
import pendulum
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient
from src.blob_storage import get_csv_as_df

WEATHER_BUCKET = "weather"

def _classify_fall_type(temperature: float) -> str:
    return 'snow' if temperature < 2.0 else 'rain'

def _classify_general_circumstances(temp: float, wind: float, humidity: float, precip: float) -> str:
    score = 0
    if 10 <= temp <= 25:
        score += 2
    elif 2 <= temp < 10:
        score += 1
    elif temp < 2 or temp > 35:
        score -= 1
    if wind < 5:
        score += 2
    elif wind < 10:
        score += 1
    elif wind > 15:
        score -= 1
    if humidity < 70:
        score += 1
    elif humidity > 90:
        score -= 1
    if precip == 0:
        score += 2
    elif precip > 5:
        score -= 1
    if score >= 6:
        return 'ludicrously-divine'
    elif score >= 4:
        return 'titanically-passable'
    elif score >= 2:
        return 'nobly-sufficient'
    elif score >= 0:
        return 'courageously-subpar'
    else:
        return 'opera-level-atrocious'

def _apply_weather_transformations(df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns to match the query's output
    df = df.rename(columns={
        'id_stacji': 'station_id',
        'data_pomiaru': 'measurement_date',
        'godzina_pomiaru': 'hour',
        'temperatura': 'temperature',
        'suma_opadu': 'precipitation_mm',
        'predkosc_wiatru': 'wind_speed_mps',
        'kierunek_wiatru': 'wind_direction_deg',
        'wilgotnosc_wzgledna': 'humidity_percent',
        'cisnienie': 'pressure_hpa'
    })

    # Create 'id' column as in the query
    df['id'] = (
            df['station_id'].astype(str) + '-' +
            df['measurement_date'].astype(str) + '-' +
            df['hour'].astype(int).astype(str).str.zfill(2)
    )

    # Cast columns to correct types
    df['temperature'] = df['temperature'].astype(float)
    df['precipitation_mm'] = df['precipitation_mm'].astype(float)
    df['wind_speed_mps'] = df['wind_speed_mps'].astype(float)
    df['wind_direction_deg'] = df['wind_direction_deg'].astype(int)
    df['humidity_percent'] = df['humidity_percent'].astype(float)
    df['pressure_hpa'] = df['pressure_hpa'].astype(float)

    # Filter out rows with missing temperature or wind speed
    df = df[df['temperature'].notnull() & df['wind_speed_mps'].notnull()]

    # Drop duplicates as in the original logic
    df = df.drop_duplicates(subset=['station_id', 'hour'])

    # Continue with business logic transformations
    print(f"✅ Merged {len(df):,} weather records")
    print("🔄 Applying business transformations...")

    df['fall_mm'] = df['precipitation_mm'].fillna(0).round().astype(int)
    df['fall_type'] = df['temperature'].apply(_classify_fall_type)
    df['wind_speed_mps'] = df['wind_speed_mps'].fillna(0).round().astype(int)
    df['pressure_hpa'] = df['pressure_hpa'].fillna(1013).round().astype(int)
    df['general_circumstances'] = df.apply(
        lambda row: _classify_general_circumstances(
            row['temperature'],
            row['wind_speed_mps'],
            row['humidity_percent'],
            row['fall_mm']
        ),
        axis=1
    )

    final_df = df[[
        'id',
        'temperature',
        'fall_mm',
        'fall_type',
        'wind_speed_mps',
        'wind_direction_deg',
        'humidity_percent',
        'pressure_hpa',
        'general_circumstances'
    ]]
    return final_df

def _get_weather_files_for_day(
    container_client: ContainerClient,
    as_of: pendulum.Date
) -> list[str]:
    prefix = f"{as_of.year}/{as_of.month:02d}/{as_of.day:02d}/"
    return [
        blob.name
        for blob in container_client.list_blobs(name_starts_with=prefix)
        if blob.name.endswith('.csv')
    ]

def _merge_weather_files(
    blob_service_client: BlobServiceClient,
    as_of: pendulum.Date
) -> pd.DataFrame:
    container_client = blob_service_client.get_container_client(WEATHER_BUCKET)
    files = _get_weather_files_for_day(container_client, as_of)
    dfs = [get_csv_as_df(container_client, file) for file in files]
    if not dfs:
        return pd.DataFrame()
    merged_df = pd.concat(dfs, ignore_index=True)
    if 'hour' in merged_df.columns:
        merged_df = merged_df.drop_duplicates(subset=['hour'])
    return merged_df

def load_weather_into_pandas(
    blob_service_client: BlobServiceClient,
    as_of: pendulum.Date,
) -> None:
    merged_df = _merge_weather_files(blob_service_client, as_of)
    merged_df = _apply_weather_transformations(merged_df)
