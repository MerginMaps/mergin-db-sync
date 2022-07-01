"""
Mergin Maps DB Sync - a tool for two-way synchronization between Mergin Maps and a PostGIS database

Copyright (C) 2022 Lutra Consulting

License: MIT
"""
import os
import pytest

from config import config, ConfigError, validate_config

SERVER_URL = os.environ.get('TEST_MERGIN_URL')
API_USER = os.environ.get('TEST_API_USERNAME')
USER_PWD = os.environ.get('TEST_API_PASSWORD')


def _reset_config():
    """ helper to reset config settings to ensure valid config """
    config.update({
        'MERGIN__USERNAME': API_USER,
        'MERGIN__PASSWORD': USER_PWD,
        'MERGIN__URL': SERVER_URL,
        'WORKING_DIR': "/tmp/working_project",
        'GEODIFF_EXE': "geodiff",
        'SCHEMAS': [{"driver": "postgres", "conn_info": "", "modified": "mergin_main", "base": "mergin_base", "mergin_project": "john/dbsync", "sync_file": "sync.gpkg"}]
    })


def test_config():
    # valid config
    _reset_config()
    validate_config(config)

    with pytest.raises(ConfigError, match="Config error: Incorrect mergin settings"):
        config.update({'MERGIN__USERNAME': None})
        validate_config(config)

    _reset_config()
    with pytest.raises(ConfigError, match="Config error: Working directory is not set"):
        config.update({'WORKING_DIR': None})
        validate_config(config)

    _reset_config()
    with pytest.raises(ConfigError, match="Config error: Path to geodiff executable is not set"):
        config.update({'GEODIFF_EXE': None})
        validate_config(config)

    _reset_config()
    with pytest.raises(ConfigError, match="Config error: Schemas list can not be empty"):
        config.update({'SCHEMAS': []})
        validate_config(config)

    _reset_config()
    with pytest.raises(ConfigError, match="Config error: Incorrect schema settings"):
        config.update({'SCHEMAS': [{"modified": "mergin_main"}]})
        validate_config(config)

    _reset_config()
    with pytest.raises(ConfigError, match="Config error: Only 'postgres' driver is currently supported."):
        config.update({'SCHEMAS': [{"driver": "oracle", "conn_info": "", "modified": "mergin_main", "base": "mergin_base", "mergin_project": "john/dbsync", "sync_file": "sync.gpkg"}]})
        validate_config(config)

