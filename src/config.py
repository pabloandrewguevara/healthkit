import json
import os
import yaml
from utils.logging_config import logger

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

def load_config():
    """Load configuration from config.json file."""
    try:
        config_path = os.path.join('config', 'config.json')
        if not os.path.exists(config_path):
            raise ConfigError(f"Configuration file not found at {config_path}")
            
        with open(config_path, 'r') as file:
            config = json.load(file)
            
        # Validate required configuration sections
        required_sections = ['BQ_PROJECT', 'BQ_DATASET', 'BQ_TABLES', 'PATHS']
        missing_sections = [section for section in required_sections if section not in config]
        if missing_sections:
            raise ConfigError(f"Missing required configuration sections: {', '.join(missing_sections)}")
            
        logger.info("Configuration loaded successfully")
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing configuration file: {str(e)}")
        raise ConfigError(f"Invalid JSON in configuration file: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error loading configuration: {str(e)}")
        raise ConfigError(f"Failed to load configuration: {str(e)}")

def get_bigquery_config(config):
    """Extract BigQuery related configuration."""
    try:
        bq_config = {
            'project_id': config['BQ_PROJECT'],
            'dataset_id': config['BQ_DATASET'],
            'tables': config['BQ_TABLES']
        }
        logger.debug("BigQuery configuration extracted successfully")
        return bq_config
    except KeyError as e:
        logger.error(f"Missing BigQuery configuration key: {str(e)}")
        raise ConfigError(f"Missing BigQuery configuration key: {str(e)}")

def get_paths_config(config):
    """Extract paths related configuration."""
    try:
        paths = config['PATHS']
        if 'downloads' not in paths:
            raise ConfigError("Missing required 'downloads' path in configuration")
        logger.debug("Paths configuration extracted successfully")
        return paths
    except KeyError as e:
        logger.error(f"Missing paths configuration key: {str(e)}")
        raise ConfigError(f"Missing paths configuration key: {str(e)}") 