import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from google.cloud import bigquery
import pandas as pd

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.config import load_config, get_bigquery_config, get_paths_config
from utils.bigquery_utils import upload_to_bigquery
from src.data_processing import process_health_data, process_workout_data, process_final_data

def main():
    # Load configuration
    config = load_config()
    bq_config = get_bigquery_config(config)
    paths_config = get_paths_config(config)

    # Find the latest export file
    x = [element for element in os.listdir(paths_config['downloads']) 
         if element[:6] == "export" and element[-4:] == ".zip"]
    
    # Path to the zip file and XML file inside it
    zip_file = os.path.join(paths_config['downloads'], x[-1] if len(x) == 1 else x[-2])
    file_path = os.path.join("apple_health_export", "export.xml")

    # Open the zip file and parse the XML file inside it
    with zipfile.ZipFile(zip_file, 'r') as z:
        with z.open(file_path) as xml_file:
            tree = ET.parse(xml_file)
            root = tree.getroot()

    # Process data
    df = process_health_data(root)
    df2 = process_workout_data(root)
    df_final = process_final_data(df, df2)

    # Define schemas for BigQuery uploads
    health_record_schema = [
        bigquery.SchemaField("EndDate", "DATE"),
        bigquery.SchemaField("Weekday", "STRING"),
        bigquery.SchemaField("CoreSleepHours", "FLOAT64"),
        bigquery.SchemaField("DeepSleepHours", "FLOAT64"),
        bigquery.SchemaField("REMSleepHours", "FLOAT64"),
        bigquery.SchemaField("TotalSleepHours", "FLOAT64"),
        bigquery.SchemaField("CoreSleepHoursNextNight", "FLOAT64"),
        bigquery.SchemaField("DeepSleepHoursNextNight", "FLOAT64"),
        bigquery.SchemaField("REMSleepHoursNextNight", "FLOAT64"),
        bigquery.SchemaField("TotalSleepHoursNextNight", "FLOAT64"),
        bigquery.SchemaField("AvgRestingHeartRateBPM", "FLOAT64"),
        bigquery.SchemaField("ActiveCaloriesBurned", "FLOAT64"),
        bigquery.SchemaField("BasalCaloriesBurned", "FLOAT64"),
        bigquery.SchemaField("TotalCaloriesBurned", "FLOAT64"),
        bigquery.SchemaField("StrengthTrainingMinutes", "INT64"),
        bigquery.SchemaField("RunningMinutes", "INT64"),
        bigquery.SchemaField("RunningMiles", "FLOAT64"),
        bigquery.SchemaField("RunningMetersAscended", "FLOAT64"),
        bigquery.SchemaField("HIITMinutes", "INT64"),
        bigquery.SchemaField("CoreTrainingMinutes", "INT64"),
        bigquery.SchemaField("TotalWorkoutMinutes", "INT64"),
        bigquery.SchemaField("WeekEndingDate", "DATE"),
        bigquery.SchemaField("StrengthTrained", "INT64"),
        bigquery.SchemaField("Ran", "INT64"),
        bigquery.SchemaField("HIITTrained", "INT64"),
        bigquery.SchemaField("CoreTrained", "INT64"),
        bigquery.SchemaField("Exercised", "INT64")
    ]

    workouts_grouped_schema = [
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("workout_activity_type", "STRING"),
        bigquery.SchemaField("duration", "INT64")
    ]

    vo2max_schema = [
        bigquery.SchemaField("end_date", "DATE"),
        bigquery.SchemaField("value", "FLOAT64")
    ]

    sleep_boxplots_schema = [
        bigquery.SchemaField("WeekEndingDate", "DATE"),
        bigquery.SchemaField("Min", "FLOAT64"),
        bigquery.SchemaField("Q1", "FLOAT64"),
        bigquery.SchemaField("Median", "FLOAT64"),
        bigquery.SchemaField("Q3", "FLOAT64"),
        bigquery.SchemaField("Max", "FLOAT64")
    ]

    regimen_boxplots_schema = [
        bigquery.SchemaField("Min", "FLOAT64"),
        bigquery.SchemaField("Q1", "FLOAT64"),
        bigquery.SchemaField("Median", "FLOAT64"),
        bigquery.SchemaField("Q3", "FLOAT64"),
        bigquery.SchemaField("Max", "FLOAT64"),
        bigquery.SchemaField("Regimen", "STRING")
    ]

    # Upload data to BigQuery
    upload_to_bigquery(df_final, 'health_record', health_record_schema, 
                      bq_config['project_id'], bq_config['dataset_id'], bq_config['tables'])

    # Process and upload workouts grouped data
    raw_data_grouped = df2[['workout_activity_type', 'duration', 'end_date']].groupby(
        ['end_date', 'workout_activity_type']
    ).sum().reset_index().copy()
    raw_data_grouped.end_date = raw_data_grouped.end_date.dt.date
    raw_data_grouped['workout_activity_type'].replace(
        to_replace={"TraditionalStrengthTraining": "StrengthTraining"},
        inplace=True
    )
    upload_to_bigquery(raw_data_grouped, 'workouts_grouped', workouts_grouped_schema,
                      bq_config['project_id'], bq_config['dataset_id'], bq_config['tables'])

    # Process and upload VO2 max data
    vo2max = df[df.type == 'VO2Max'][['end_date', 'value']].copy().reset_index(drop=True)
    vo2max.end_date = vo2max.end_date.dt.date
    upload_to_bigquery(vo2max, 'vo2max', vo2max_schema,
                      bq_config['project_id'], bq_config['dataset_id'], bq_config['tables'])

    # Process and upload sleep boxplots data
    sleep_dist = df_final[['EndDate', 'TotalSleepHours']].copy().reset_index(drop=True)
    sleep_dist['WeekEndingDate'] = pd.to_datetime(sleep_dist['EndDate']) + pd.to_timedelta(
        6 - pd.to_datetime(sleep_dist['EndDate']).dt.weekday,
        unit='D'
    )
    sleep_dist.drop('EndDate', axis=1, inplace=True)
    sleep_dist = sleep_dist.groupby('WeekEndingDate')['TotalSleepHours'].agg(
        Min="min",
        Q1=lambda x: x.quantile(0.25),
        Median="median",
        Q3=lambda x: x.quantile(0.75),
        Max="max"
    ).reset_index()
    sleep_dist = sleep_dist[sleep_dist.WeekEndingDate > '2024-11-17']
    sleep_dist.WeekEndingDate = sleep_dist.WeekEndingDate.dt.date
    upload_to_bigquery(sleep_dist, 'sleep_boxplots', sleep_boxplots_schema,
                      bq_config['project_id'], bq_config['dataset_id'], bq_config['tables'])

    # Process and upload regimen boxplots data
    regimen_a_dist = df_final[['EndDate', 'StrengthTrainingMinutes']].copy().reset_index(drop=True)
    regimen_a_dist.EndDate = pd.to_datetime(regimen_a_dist.EndDate)
    regimen_a_dist = regimen_a_dist[
        (regimen_a_dist.EndDate < '12-31-2024') & 
        (regimen_a_dist.StrengthTrainingMinutes > 0)
    ]
    regimen_a_dist = regimen_a_dist['StrengthTrainingMinutes'].agg(
        Min="min",
        Q1=lambda x: x.quantile(0.25),
        Median="median",
        Q3=lambda x: x.quantile(0.75),
        Max="max"
    ).reset_index()
    regimen_a_dist = regimen_a_dist.set_index('index').transpose()
    regimen_a_dist.reset_index(inplace=True, drop=True)
    regimen_a_dist['Regimen'] = 'A'

    regimen_b_dist = df_final[['EndDate', 'StrengthTrainingMinutes']].copy().reset_index(drop=True)
    regimen_b_dist.EndDate = pd.to_datetime(regimen_b_dist.EndDate)
    regimen_b_dist = regimen_b_dist[
        (regimen_b_dist.EndDate >= '12-31-2024') & 
        (regimen_b_dist.StrengthTrainingMinutes > 0)
    ]
    regimen_b_dist = regimen_b_dist['StrengthTrainingMinutes'].agg(
        Min="min",
        Q1=lambda x: x.quantile(0.25),
        Median="median",
        Q3=lambda x: x.quantile(0.75),
        Max="max"
    ).reset_index()
    regimen_b_dist = regimen_b_dist.set_index('index').transpose()
    regimen_b_dist.reset_index(inplace=True, drop=True)
    regimen_b_dist['Regimen'] = 'B'

    regimen_dist = pd.concat([regimen_a_dist, regimen_b_dist])
    regimen_dist.reset_index(inplace=True, drop=True)
    upload_to_bigquery(regimen_dist, 'regimen_boxplots', regimen_boxplots_schema,
                      bq_config['project_id'], bq_config['dataset_id'], bq_config['tables'])

if __name__ == "__main__":
    main() 