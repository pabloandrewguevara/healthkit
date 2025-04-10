import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
import zipfile
import os
from utils.logging_config import logger

class DataProcessingError(Exception):
    """Custom exception for data processing operations."""
    pass

def process_health_data(xml_root):
    """Process health data from XML root.
    
    Args:
        xml_root: Root element of the XML tree
        
    Returns:
        DataFrame containing processed health data
        
    Raises:
        DataProcessingError: If there's an error during data processing
    """
    try:
        logger.info("Starting to process health data")
        
        # Extract relevant data
        health_records = []
        for record in xml_root.findall('Record'):
            record_data = {
                'type': record.get('type'),
                'source_name': record.get('sourceName'),
                'source_version': record.get('sourceVersion'),
                'unit': record.get('unit'),
                'value': record.get('value'),
                'start_date': record.get('startDate'),
                'end_date': record.get('endDate')
            }
            health_records.append(record_data)

        df = pd.DataFrame(health_records)
        logger.info(f"Successfully extracted {len(df)} health records")
        
        # Process sleep categories
        sleep_categories = [
            'HKCategoryValueSleepAnalysisInBed',
            'HKCategoryValueSleepAnalysisAsleepCore',
            'HKCategoryValueSleepAnalysisAsleepDeep',
            'HKCategoryValueSleepAnalysisAwake',
            'HKCategoryValueSleepAnalysisAsleepREM',
            'HKCategoryValueSleepAnalysisAsleepUnspecified'
        ]

        # Clean and transform data
        df.drop(['source_name', 'source_version'], axis=1, inplace=True)
        df.rename(columns={
            'start_date': 'start_datetime',
            'end_date': 'end_datetime'
        }, inplace=True)

        # Convert datetime columns
        datetime_columns = ['end_datetime', 'start_datetime']
        for col in datetime_columns:
            df[col] = pd.to_datetime(df[col])
            
        # Adjust sleep times
        df['end_datetime'] = df.apply(
            lambda row: row['end_datetime'] + pd.Timedelta(hours=4) 
            if row['value'] in sleep_categories else row['end_datetime'], 
            axis=1
        )
        df['start_datetime'] = df.apply(
            lambda row: row['start_datetime'] + pd.Timedelta(hours=4) 
            if row['value'] in sleep_categories else row['start_datetime'], 
            axis=1
        )

        # Calculate time elapsed
        df['time_elapsed'] = df['end_datetime'] - df['start_datetime']

        # Extract dates
        df['start_date'] = pd.to_datetime(df['start_datetime'].dt.date)
        df['end_date'] = pd.to_datetime(df['end_datetime'].dt.date)

        # Clean type column
        df['type'] = df['type'].apply(lambda x: x[len('HKCategoryTypeIdentifier'):])

        # Process values
        df['value2'] = df['value']
        value_replacements = {
            'HKCategoryValueSleepAnalysisInBed': np.nan,
            'HKCategoryValueNotApplicable': np.nan,
            'HKCategoryValueHeadphoneAudioExposureEventSevenDayLimit': np.nan,
            'HKCategoryValueEnvironmentalAudioExposureEventMomentaryLimit': np.nan,
            'HKCategoryValueSleepAnalysisAsleepCore': np.nan,
            'HKCategoryValueSleepAnalysisAsleepDeep': np.nan,
            'HKCategoryValueSleepAnalysisAwake': np.nan,
            'HKCategoryValueSleepAnalysisAsleepREM': np.nan,
            'HKCategoryValueSleepAnalysisAsleepUnspecified': np.nan,
            'HKCategoryValueAppleStandHourIdle': np.nan,
            'HKCategoryValueAppleStandHourStood': np.nan
        }
        df['value'].replace(value_replacements, inplace=True)
        df['value'] = df['value'].astype(float)

        # Calculate sleep duration
        df['value'] = df.apply(
            lambda row: row['time_elapsed'].seconds 
            if row['value2'] in sleep_categories else row['value'], 
            axis=1
        )
        df['unit'] = df.apply(
            lambda row: 's' if row['value2'] in sleep_categories else row['unit'], 
            axis=1
        )

        logger.info("Health data processing completed successfully")
        return df
        
    except Exception as e:
        logger.error(f"Error processing health data: {str(e)}")
        raise DataProcessingError(f"Error processing health data: {str(e)}")

def process_workout_data(xml_root):
    """Process workout data from XML root.
    
    Args:
        xml_root: Root element of the XML tree
        
    Returns:
        DataFrame containing processed workout data
        
    Raises:
        DataProcessingError: If there's an error during data processing
    """
    try:
        logger.info("Starting to process workout data")
        
        workout_records = []
        for record in xml_root.findall('Workout'):
            record_data = {
                'workout_activity_type': record.get('workoutActivityType'),
                'duration': record.get('duration'),
                'duration_unit': record.get('durationUnit'),
                'start_date': record.get('startDate'),
                'end_date': record.get('endDate')
            }

            # Extract statistics
            for stat in record.findall('WorkoutStatistics'):
                if stat.get('type') == 'HKQuantityTypeIdentifierDistanceWalkingRunning':
                    record_data['distance_walking_running'] = stat.get('sum')
                    record_data['distance_walking_running_unit'] = stat.get('unit')
                    
            # Extract elevation data
            for metadata in record.findall('MetadataEntry'):
                if metadata.get('key') == 'HKElevationAscended':
                    elevation_value = metadata.get('value')
                    if ' ' in elevation_value:
                        value, unit = elevation_value.split(' ', 1)
                        record_data['elevation_ascended'] = value
                        record_data['elevation_ascended_unit'] = unit
                    else:
                        record_data['elevation_ascended'] = elevation_value
                        record_data['elevation_ascended_unit'] = None

            workout_records.append(record_data)

        df = pd.DataFrame(workout_records)
        
        # Clean activity type
        df['workout_activity_type'] = df['workout_activity_type'].apply(
            lambda x: x[len('HKWorkoutActivityType'):]
        )

        # Process numeric columns
        numeric_columns = {
            'duration': {'round': 0, 'type': int},
            'distance_walking_running': {'round': 1, 'type': float},
            'elevation_ascended': {'round': 1, 'type': float}
        }
        
        for col, settings in numeric_columns.items():
            if col in df.columns:
                df[col] = np.round(df[col].astype(float), settings['round']).astype(settings['type'])

        # Handle missing values
        df['elevation_ascended'].fillna(0, inplace=True)
        df['elevation_ascended'] = np.round(df['elevation_ascended'].astype(float)/100, 1)
        df['elevation_ascended_unit'].replace({'cm': 'm'}, inplace=True)

        # Process datetime columns
        datetime_columns = ['start_date', 'end_date']
        for col in datetime_columns:
            df[col] = pd.to_datetime(df[col])
            
        df.rename(columns={
            'start_date': 'start_datetime',
            'end_date': 'end_datetime'
        }, inplace=True)

        # Extract dates
        df['start_date'] = pd.to_datetime(df['start_datetime'].dt.date)
        df['end_date'] = pd.to_datetime(df['end_datetime'].dt.date)

        # Ensure all required columns exist
        required_columns = [
            'workout_activity_type',
            'duration',
            'distance_walking_running',
            'elevation_ascended',
            'start_date',
            'end_date'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = 0 if col in ['duration', 'distance_walking_running', 'elevation_ascended'] else None

        logger.info("Workout data processing completed successfully")
        return df
        
    except Exception as e:
        logger.error(f"Error processing workout data: {str(e)}")
        raise DataProcessingError(f"Error processing workout data: {str(e)}")

def process_final_data(health_df, workout_df):
    """Process final combined data from health and workout DataFrames.
    
    Args:
        health_df: Health data DataFrame
        workout_df: Workout data DataFrame
        
    Returns:
        DataFrame containing processed final data
        
    Raises:
        DataProcessingError: If there's an error during data processing
    """
    try:
        logger.info("Starting to process final combined data")
        
        # Define weekday mapping
        weekday_map = {
            0: 'Monday',
            1: 'Tuesday',
            2: 'Wednesday',
            3: 'Thursday',
            4: 'Friday',
            5: 'Saturday',
            6: 'Sunday'
        }

        # Create base DataFrame
        df_final = pd.DataFrame()
        df_final['end_date'] = np.sort(np.unique(health_df.end_date))
        df_final['end_date'] = pd.to_datetime(df_final['end_date']).sort_values()
        df_final['end_date'] = pd.to_datetime(df_final['end_date'].dt.date)
        df_final['weekday'] = df_final['end_date'].apply(lambda x: x.weekday())
        df_final['weekday'].replace(weekday_map, inplace=True)

        # Process sleep data
        sleep_categories = [
            'HKCategoryValueSleepAnalysisInBed',
            'HKCategoryValueSleepAnalysisAsleepCore',
            'HKCategoryValueSleepAnalysisAsleepDeep',
            'HKCategoryValueSleepAnalysisAwake',
            'HKCategoryValueSleepAnalysisAsleepREM',
            'HKCategoryValueSleepAnalysisAsleepUnspecified'
        ]

        # Process sleep metrics
        sleep_metrics = {
            'CoreSleepHours': 'HKCategoryValueSleepAnalysisAsleepCore',
            'DeepSleepHours': 'HKCategoryValueSleepAnalysisAsleepDeep',
            'REMSleepHours': 'HKCategoryValueSleepAnalysisAsleepREM'
        }

        for metric_name, category in sleep_metrics.items():
            if category == 'HKCategoryValueSleepAnalysisAwake':
                continue
            df_final = df_final.merge(
                health_df[health_df.value2 == category][['value', 'end_date']]
                .groupby('end_date').sum().reset_index(),
                on='end_date',
                how='left'
            )
            df_final['value'] = np.round(df_final['value']/3600, 1)
            df_final.rename(columns={'value': metric_name}, inplace=True)

        # Calculate total sleep
        df_final['TotalSleepHours'] = (
            df_final['CoreSleepHours'] + 
            df_final['DeepSleepHours'] + 
            df_final['REMSleepHours']
        )

        # Calculate next night's sleep
        for metric in ['CoreSleepHours', 'DeepSleepHours', 'REMSleepHours', 'TotalSleepHours']:
            df_final[f'{metric}NextNight'] = df_final[metric].shift(-1)

        # Process health metrics
        health_metrics = {
            'AvgRestingHeartRateBPM': 'RestingHeartRate',
            'ActiveCaloriesBurned': 'ActiveEnergyBurned',
            'BasalCaloriesBurned': 'BasalEnergyBurned'
        }

        for metric_name, metric_type in health_metrics.items():
            df_final = df_final.merge(
                health_df[health_df.type == metric_type][['value', 'end_date']]
                .groupby('end_date').sum().reset_index(),
                on='end_date',
                how='left'
            )
            df_final.rename(columns={'value': metric_name}, inplace=True)
            if 'Calories' in metric_name:
                df_final[metric_name] = np.round(df_final[metric_name], 0)

        # Calculate total calories
        df_final['TotalCaloriesBurned'] = (
            df_final['ActiveCaloriesBurned'] + 
            df_final['BasalCaloriesBurned']
        )

        # Process workout data
        workout_types = {
            'StrengthTrainingMinutes': 'TraditionalStrengthTraining',
            'RunningMinutes': 'Running',
            'HIITMinutes': 'HighIntensityIntervalTraining',
            'CoreTrainingMinutes': 'CoreTraining'
        }

        for metric_name, workout_type in workout_types.items():
            df_final = df_final.merge(
                workout_df[['workout_activity_type', 'duration', 'end_date']]
                .groupby(['workout_activity_type', 'end_date'])
                .sum()
                .loc[workout_type]
                .reset_index(),
                on='end_date',
                how='left'
            )
            df_final.rename(columns={'duration': metric_name}, inplace=True)
            df_final[metric_name].fillna(0, inplace=True)
            df_final[metric_name] = df_final[metric_name].astype(int)

        # Process running metrics
        running_metrics = {
            'RunningMiles': 'distance_walking_running',
            'RunningMetersAscended': 'elevation_ascended'
        }

        for metric_name, col_name in running_metrics.items():
            df_final = df_final.merge(
                workout_df[['workout_activity_type', col_name, 'end_date']]
                .groupby(['workout_activity_type', 'end_date'])
                .sum()
                .loc['Running']
                .reset_index(),
                on='end_date',
                how='left'
            )
            df_final.rename(columns={col_name: metric_name}, inplace=True)
            df_final[metric_name].fillna(0, inplace=True)

        # Calculate total workout time
        workout_minutes = [
            'StrengthTrainingMinutes',
            'RunningMinutes',
            'CoreTrainingMinutes',
            'HIITMinutes'
        ]
        df_final['TotalWorkoutMinutes'] = sum(df_final[col] for col in workout_minutes)

        # Create binary indicators
        workout_indicators = {
            'StrengthTrained': 'StrengthTrainingMinutes',
            'Ran': 'RunningMinutes',
            'HIITTrained': 'HIITMinutes',
            'CoreTrained': 'CoreTrainingMinutes'
        }

        for indicator_name, minutes_col in workout_indicators.items():
            df_final[indicator_name] = df_final[minutes_col].apply(lambda x: 1 if x > 0 else 0)

        df_final['Exercised'] = df_final['TotalWorkoutMinutes'].apply(lambda x: 1 if x > 0 else 0)

        # Final column renaming
        df_final.rename(columns={'end_date': 'EndDate'}, inplace=True)

        # Calculate week ending date
        df_final['WeekEndingDate'] = df_final['EndDate'] + pd.to_timedelta(
            6 - df_final['EndDate'].dt.weekday,
            unit='D'
        )

        # Filter data
        df_final = df_final[df_final.EndDate >= '2024-01-01']

        # Convert dates
        df_final['EndDate'] = pd.to_datetime(df_final['EndDate']).dt.date
        df_final['WeekEndingDate'] = pd.to_datetime(df_final['WeekEndingDate']).dt.date

        logger.info("Final data processing completed successfully")
        return df_final
        
    except Exception as e:
        logger.error(f"Error processing final data: {str(e)}")
        raise DataProcessingError(f"Error processing final data: {str(e)}") 