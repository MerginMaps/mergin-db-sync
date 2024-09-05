import os
import shutil
import tempfile
from typing import List

import psycopg2
import psycopg2.extensions
import pytest
from mergin import ClientError, MerginClient
from psycopg2 import sql

from config import config
from dbsync import dbsync_init

GEODIFF_EXE = os.environ.get("TEST_GEODIFF_EXE")
DB_CONNINFO = os.environ.get("TEST_DB_CONNINFO")
SERVER_URL = os.environ.get("TEST_MERGIN_URL")
API_USER = os.environ.get("TEST_API_USERNAME")
USER_PWD = os.environ.get("TEST_API_PASSWORD")
WORKSPACE = os.environ.get("TEST_API_WORKSPACE")
TMP_DIR = os.path.join(tempfile.gettempdir(), "dbsync_test")
TEST_DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "test_data",
)


def _reset_config(project_name: str = "mergin", init_from: str = "gpkg"):
    """helper to reset config settings to ensure valid config"""
    db_schema_main = name_db_schema_main(project_name)
    db_schema_base = name_db_schema_base(project_name)
    full_project_name = complete_project_name(project_name)

    config.update(
        {
            "MERGIN__USERNAME": API_USER,
            "MERGIN__PASSWORD": USER_PWD,
            "MERGIN__URL": SERVER_URL,
            "init_from": init_from,
            "CONNECTIONS": [
                {
                    "driver": "postgres",
                    "conn_info": DB_CONNINFO,
                    "modified": db_schema_main,
                    "base": db_schema_base,
                    "mergin_project": full_project_name,
                    "sync_file": filename_sync_gpkg(),
                }
            ],
        }
    )


def cleanup(
    mc: MerginClient,
    project,
    dirs,
):
    """cleanup leftovers from previous test if needed such as remote project and local directories"""
    try:
        print("Deleting project on Mergin Maps server: " + project)
        mc.delete_project_now(project)
    except ClientError as e:
        print("Deleting project error: " + str(e))
        pass
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def cleanup_db(
    conn,
    schema_base,
    schema_main,
):
    """Removes test schemas from previous tests"""
    cur = conn.cursor()
    cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_base)))
    cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_main)))
    cur.execute("COMMIT")


def init_sync_from_geopackage(mc, project_name, source_gpkg_path, ignored_tables=[], *extra_init_files):
    """
    Initialize sync from given GeoPackage file:
    - (re)create Mergin Maps project with the file
    - (re)create local project working directory and sync directory
    - configure DB sync and let it do the init (make copies to the database)
    """
    full_project_name = complete_project_name(project_name)
    project_dir = name_project_dir(project_name)  # working directory
    sync_project_dir = name_project_sync_dir(project_name)  # used by dbsync
    db_schema_main = name_db_schema_main(project_name)
    db_schema_base = name_db_schema_base(project_name)

    conn = psycopg2.connect(DB_CONNINFO)

    cleanup(
        mc,
        full_project_name,
        [
            project_dir,
            sync_project_dir,
        ],
    )
    cleanup_db(
        conn,
        db_schema_base,
        db_schema_main,
    )

    # prepare a new Mergin Maps project
    mc.create_project(full_project_name)

    mc.download_project(
        full_project_name,
        project_dir,
    )
    shutil.copy(
        source_gpkg_path,
        os.path.join(
            project_dir,
            filename_sync_gpkg(),
        ),
    )
    for extra_filepath in extra_init_files:
        extra_filename = os.path.basename(extra_filepath)
        target_extra_filepath = os.path.join(
            project_dir,
            extra_filename,
        )
        shutil.copy(
            extra_filepath,
            target_extra_filepath,
        )
    mc.push_project(project_dir)

    # prepare dbsync config
    # patch config to fit testing purposes
    connection = {
        "driver": "postgres",
        "conn_info": DB_CONNINFO,
        "modified": db_schema_main,
        "base": db_schema_base,
        "mergin_project": full_project_name,
        "sync_file": "test_sync.gpkg",
    }

    if ignored_tables:
        if isinstance(ignored_tables, str):
            connection["skip_tables"] = [ignored_tables]
        elif isinstance(ignored_tables, list):
            connection["skip_tables"] = ignored_tables

    config.update(
        {
            "GEODIFF_EXE": GEODIFF_EXE,
            "WORKING_DIR": sync_project_dir,
            "MERGIN__USERNAME": API_USER,
            "MERGIN__PASSWORD": USER_PWD,
            "MERGIN__URL": SERVER_URL,
            "CONNECTIONS": [connection],
            "init_from": "gpkg",
        }
    )

    dbsync_init(mc)


@pytest.fixture(scope="session")
def mc():
    assert SERVER_URL and API_USER and USER_PWD
    # assert SERVER_URL and SERVER_URL.rstrip('/') != 'https://app.merginmaps.com/' and API_USER and USER_PWD
    return MerginClient(
        SERVER_URL,
        login=API_USER,
        password=USER_PWD,
    )


@pytest.fixture(scope="function")
def db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(DB_CONNINFO)


def name_db_schema_main(project_name: str) -> str:
    return project_name + "_main"


def name_db_schema_base(project_name: str) -> str:
    return project_name + "_base"


def name_project_dir(project_name: str) -> str:
    return os.path.join(
        TMP_DIR,
        project_name + "_work",
    )


def name_project_sync_dir(project_name: str) -> str:
    return os.path.join(
        TMP_DIR,
        project_name + "_dbsync",
    )


def complete_project_name(project_name: str) -> str:
    return WORKSPACE + "/" + project_name


def path_test_data(filename: str) -> str:
    return os.path.join(
        TEST_DATA_DIR,
        filename,
    )


def filename_sync_gpkg() -> str:
    return "test_sync.gpkg"


def init_sync_from_db(mc: MerginClient, project_name: str, path_sql_file: str, ignored_tables: List[str] = None):
    """
    Initialize sync from given database file:
    - prepare schema with simple table
    - create MM project
    - configure DB sync and let it do the init
    """
    if ignored_tables is None:
        ignored_tables = []

    full_project_name = complete_project_name(project_name)
    project_dir = name_project_dir(project_name)  # working directory
    sync_project_dir = name_project_sync_dir(project_name)  # used by dbsync
    db_schema_main = "test_init_from_db_main"
    db_schema_base = "test_init_from_db_base"

    conn = psycopg2.connect(DB_CONNINFO)

    cleanup(
        mc,
        full_project_name,
        [
            project_dir,
            sync_project_dir,
        ],
    )
    cleanup_db(
        conn,
        db_schema_base,
        db_schema_main,
    )

    with open(
        path_sql_file,
        encoding="utf-8",
    ) as file:
        base_table_dump = file.read()

    cur = conn.cursor()
    cur.execute(base_table_dump)

    # prepare a new Mergin Maps project
    mc.create_project(full_project_name)

    # prepare dbsync config
    # patch config to fit testing purposes
    if ignored_tables:
        connection = {
            "driver": "postgres",
            "conn_info": DB_CONNINFO,
            "modified": db_schema_main,
            "base": db_schema_base,
            "mergin_project": full_project_name,
            "sync_file": filename_sync_gpkg(),
            "skip_tables": ignored_tables,
        }
    else:
        connection = {
            "driver": "postgres",
            "conn_info": DB_CONNINFO,
            "modified": db_schema_main,
            "base": db_schema_base,
            "mergin_project": full_project_name,
            "sync_file": filename_sync_gpkg(),
        }

    config.update(
        {
            "GEODIFF_EXE": GEODIFF_EXE,
            "WORKING_DIR": sync_project_dir,
            "MERGIN__USERNAME": API_USER,
            "MERGIN__PASSWORD": USER_PWD,
            "MERGIN__URL": SERVER_URL,
            "CONNECTIONS": [connection],
            "init_from": "db",
        }
    )

    dbsync_init(mc)


def _clean_workspace(mc: MerginClient, workspace: str) -> None:
    """Immediately delete all projects in the test workspace."""

    projects = mc.projects_list(only_namespace=workspace)

    for project in projects:
        mc.delete_project_now(f"{workspace}/{project['name']}")


@pytest.fixture(autouse=True, scope="session")
def clean_workspace(mc: MerginClient):
    """Delete all projects in the test workspace prior and after test session."""

    _clean_workspace(mc, WORKSPACE)
    yield
    _clean_workspace(mc, WORKSPACE)


def _remove_dir(dir_path: str) -> None:
    """Remove directory if it exists."""

    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)


@pytest.fixture(autouse=True, scope="session")
def clean_test_dir():
    """Remove and recreate temporary directory for test files. Remove it after test session."""
    _remove_dir(TMP_DIR)
    os.makedirs(TMP_DIR)
    yield
    _remove_dir(TMP_DIR)
