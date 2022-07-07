"""
Mergin Maps DB Sync - a tool for two-way synchronization between Mergin Maps and a PostGIS database

Copyright (C) 2020 Lutra Consulting

License: MIT
"""

import getpass
import json
import os
import shutil
import string
import subprocess
import sys
import tempfile
import random

import psycopg2
from psycopg2 import sql

from mergin import MerginClient, MerginProject, LoginError, ClientError
from version import __version__
from config import config, validate_config, ConfigError

# set high logging level for geodiff (used by geodiff executable)
# so we get as much information as possible
os.environ["GEODIFF_LOGGER_LEVEL"] = '4'   # 0 = nothing, 1 = errors, 2 = warning, 3 = info, 4 = debug


class DbSyncError(Exception):
    pass


def _check_has_working_dir(work_path):
    if not os.path.exists(work_path):
        raise DbSyncError("The project working directory does not exist: " + work_path)

    if not os.path.exists(os.path.join(work_path, '.mergin')):
        raise DbSyncError("The project working directory does not seem to contain Mergin Maps project: " + work_path)


def _check_has_sync_file(file_path):
    """ Checks whether the dbsync environment is initialized already (so that we can pull/push).
     Emits an exception if not initialized yet. """

    if not os.path.exists(file_path):
        raise DbSyncError("The output GPKG file does not exist: " + file_path)


def _check_schema_exists(conn, schema_name):
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)", (schema_name,))
    return cur.fetchone()[0]


def _check_has_password():
    """ Checks whether we have password for Mergin Maps user - if not, we will ask for it """
    if config.mergin.password is None:
        config.mergin.password = getpass.getpass(prompt="Mergin Maps password for '{}': ".format(config.mergin.username))


def _run_geodiff(cmd):
    """ will run a command (with geodiff) and report what got to stderr and raise exception
    if the command returns non-zero exit code """
    res = subprocess.run(cmd, stderr=subprocess.PIPE)
    geodiff_stderr = res.stderr.decode()
    if geodiff_stderr:
        print("GEODIFF: " + geodiff_stderr)
    if res.returncode != 0:
        raise DbSyncError("geodiff failed!\n" + str(cmd))


def _geodiff_create_changeset(driver, conn_info, base, modified, changeset):
    _run_geodiff([config.geodiff_exe, "diff", "--driver", driver, conn_info, base, modified, changeset])


def _geodiff_apply_changeset(driver, conn_info, base, changeset):
    _run_geodiff([config.geodiff_exe, "apply", "--driver", driver, conn_info, base, changeset])


def _geodiff_rebase(driver, conn_info, base, our, base2their, conflicts):
    _run_geodiff([config.geodiff_exe, "rebase-db", "--driver", driver, conn_info, base, our, base2their, conflicts])


def _geodiff_list_changes_details(changeset):
    """ Returns a list with changeset details:
     [ { 'table': 'foo', 'type': 'update', 'changes': [ ... old/new column values ... ] }, ... ]
    """
    tmp_dir = tempfile.gettempdir()
    tmp_output = os.path.join(tmp_dir, 'dbsync-changeset-details')
    if os.path.exists(tmp_output):
        os.remove(tmp_output)
    _run_geodiff([config.geodiff_exe, "as-json", changeset, tmp_output])
    with open(tmp_output) as f:
        out = json.load(f)
    os.remove(tmp_output)
    return out["geodiff"]


def _geodiff_list_changes_summary(changeset):
    """ Returns a list with changeset summary:
     [ { 'table': 'foo', 'insert': 1, 'update': 2, 'delete': 3 }, ... ]
    """
    tmp_dir = tempfile.gettempdir()
    tmp_output = os.path.join(tmp_dir, 'dbsync-changeset-summary')
    if os.path.exists(tmp_output):
        os.remove(tmp_output)
    _run_geodiff([config.geodiff_exe, "as-summary", changeset, tmp_output])
    with open(tmp_output) as f:
        out = json.load(f)
    os.remove(tmp_output)
    return out["geodiff_summary"]


def _geodiff_make_copy(src_driver, src_conn_info, src, dst_driver, dst_conn_info, dst):
    _run_geodiff([config.geodiff_exe, "copy", "--driver-1", src_driver, src_conn_info, "--driver-2", dst_driver, dst_conn_info, src, dst])


def _geodiff_create_changeset_dr(src_driver, src_conn_info, src, dst_driver, dst_conn_info, dst, changeset):
    _run_geodiff([config.geodiff_exe, "diff", "--driver-1", src_driver, src_conn_info, "--driver-2", dst_driver, dst_conn_info, src, dst, changeset])


def _compare_datasets(src_driver, src_conn_info, src, dst_driver, dst_conn_info, dst, summary_only=True):
    """ Compare content of two datasets (from various drivers) and return geodiff JSON summary of changes """
    tmp_dir = tempfile.gettempdir()
    tmp_changeset = os.path.join(tmp_dir, ''.join(random.choices(string.ascii_letters, k=8)))

    _geodiff_create_changeset_dr(src_driver, src_conn_info, src, dst_driver, dst_conn_info, dst, tmp_changeset)
    if summary_only:
        return _geodiff_list_changes_summary(tmp_changeset)
    else:
        return _geodiff_list_changes_details(tmp_changeset)


def _print_changes_summary(summary, label=None):
    """ Takes a geodiff JSON summary of changes and prints them """
    print("Changes:" if label is None else label)
    for item in summary:
        print("{:20} {:4} {:4} {:4}".format(item['table'], item['insert'], item['update'], item['delete']))


def _print_mergin_changes(diff_dict):
    """ Takes a dictionary with format { 'added': [...], 'removed': [...], 'updated': [...] }
    where each item is another dictionary with file details, e.g.:
      { 'path': 'myfile.gpkg', size: 123456, ... }
    and prints it in a way that's easy to parse for a human :-)
    """
    for item in diff_dict['added']:
        print("  added:   " + item['path'])
    for item in diff_dict['updated']:
        print("  updated: " + item['path'])
    for item in diff_dict['removed']:
        print("  removed: " + item['path'])


def _get_project_version(work_path):
    """ Returns the current version of the project """
    mp = MerginProject(work_path)
    return mp.metadata["version"]


def _set_db_project_comment(conn, schema, project_name, version, error=None):
    """ Set postgres COMMENT on SCHEMA with Mergin Maps project name and version
        or eventually error message if initialisation failed
    """
    comment = {
        "name": project_name,
        "version": version,
    }
    if error:
        comment["error"] = error
    cur = conn.cursor()
    query = sql.SQL("COMMENT ON SCHEMA {} IS %s").format(sql.Identifier(schema))
    cur.execute(query.as_string(conn), (json.dumps(comment), ))
    conn.commit()


def _get_db_project_comment(conn, schema):
    """ Get Mergin Maps project name and its current version in db schema"""
    cur = conn.cursor()
    cur.execute("SELECT obj_description(%s::regnamespace, 'pg_namespace')", (schema, ))
    res = cur.fetchone()[0]
    try:
        comment = json.loads(res) if res else None
    except (TypeError, json.decoder.JSONDecodeError):
        return
    return comment


def create_mergin_client():
    """ Create instance of MerginClient"""
    _check_has_password()
    try:
        return MerginClient(config.mergin.url, login=config.mergin.username, password=config.mergin.password, plugin_version=f"DB-sync/{__version__}")
    except LoginError as e:
        # this could be auth failure, but could be also server problem (e.g. worker crash)
        raise DbSyncError(f"Unable to log in to Mergin Maps: {str(e)} \n\n" +
                          "Have you specified correct credentials in configuration file?")
    except ClientError as e:
        # this could be e.g. DNS error
        raise DbSyncError("Mergin Maps client error: " + str(e))


def pull(conn_cfg, mc):
    """ Downloads any changes from Mergin Maps and applies them to the database """

    print(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")

    project_name = conn_cfg.mergin_project.split("/")[1]
    work_dir = os.path.join(config.working_dir, project_name)
    gpkg_full_path = os.path.join(work_dir, conn_cfg.sync_file)

    _check_has_working_dir(work_dir)
    _check_has_sync_file(gpkg_full_path)

    mp = MerginProject(work_dir)
    if mp.geodiff is None:
        raise DbSyncError("Mergin Maps client installation problem: geodiff not available")
    project_path = mp.metadata["name"]
    local_version = mp.metadata["version"]

    try:
        projects = mc.get_projects_by_names([project_path])
        server_version = projects[project_path]["version"]
    except ClientError as e:
        # this could be e.g. DNS error
        raise DbSyncError("Mergin Maps client error: " + str(e))

    status_push = mp.get_push_changes()
    if status_push['added'] or status_push['updated'] or status_push['removed']:
        raise DbSyncError("There are pending changes in the local directory - that should never happen! " + str(status_push))

    if server_version == local_version:
        print("No changes on Mergin Maps.")
        return

    gpkg_basefile = os.path.join(work_dir, '.mergin', conn_cfg.sync_file)
    gpkg_basefile_old = gpkg_basefile + "-old"

    # make a copy of the basefile in the current version (base) - because after pull it will be set to "their"
    shutil.copy(gpkg_basefile, gpkg_basefile_old)

    tmp_dir = tempfile.gettempdir()
    tmp_base2our = os.path.join(tmp_dir, f'{project_name}-dbsync-pull-base2our')
    tmp_base2their = os.path.join(tmp_dir, f'{project_name}-dbsync-pull-base2their')

    # find out our local changes in the database (base2our)
    _geodiff_create_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, conn_cfg.modified, tmp_base2our)

    needs_rebase = False
    if os.path.getsize(tmp_base2our) != 0:
        needs_rebase = True
        summary = _geodiff_list_changes_summary(tmp_base2our)
        _print_changes_summary(summary, "DB Changes:")

    try:
        mc.pull_project(work_dir)  # will do rebase as needed
    except ClientError as e:
        # TODO: do we need some cleanup here?
        raise DbSyncError("Mergin Maps client error on pull: " + str(e))

    print("Pulled new version from Mergin Maps: " + _get_project_version(work_dir))

    # simple case when there are no pending local changes - just apply whatever changes are coming
    _geodiff_create_changeset("sqlite", "", gpkg_basefile_old, gpkg_basefile, tmp_base2their)

    # summarize changes
    summary = _geodiff_list_changes_summary(tmp_base2their)
    _print_changes_summary(summary, "Mergin Maps Changes:")

    if not needs_rebase:
        print("Applying new version [no rebase]")
        _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, tmp_base2their)
        _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified, tmp_base2their)
    else:
        print("Applying new version [WITH rebase]")
        tmp_conflicts = os.path.join(tmp_dir, f'{project_name}-dbsync-pull-conflicts')
        _geodiff_rebase(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base,
                        conn_cfg.modified, tmp_base2their, tmp_conflicts)
        _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, tmp_base2their)

    os.remove(gpkg_basefile_old)
    conn = psycopg2.connect(conn_cfg.conn_info)
    version = _get_project_version(work_dir)
    _set_db_project_comment(conn, conn_cfg.base, conn_cfg.mergin_project, version)


def status(conn_cfg, mc):
    """ Figure out if there are any pending changes in the database or in Mergin Maps"""

    print(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")

    project_name = conn_cfg.mergin_project.split("/")[1]

    work_dir = os.path.join(config.working_dir, project_name)
    gpkg_full_path = os.path.join(work_dir, conn_cfg.sync_file)

    _check_has_working_dir(work_dir)
    _check_has_sync_file(gpkg_full_path)

    # get basic information
    mp = MerginProject(work_dir)
    if mp.geodiff is None:
        raise DbSyncError("Mergin Maps client installation problem: geodiff not available")
    status_push = mp.get_push_changes()
    if status_push['added'] or status_push['updated'] or status_push['removed']:
        raise DbSyncError("Pending changes in the local directory - that should never happen! " + str(status_push))

    project_path = mp.metadata["name"]
    local_version = mp.metadata["version"]
    print("Working directory " + work_dir)
    print("Mergin Maps project " + project_path + " at local version " + local_version)
    print("")
    print("Checking status...")

    # check if there are any pending changes on server
    try:
        server_info = mc.project_info(project_path, since=local_version)
    except ClientError as e:
        raise DbSyncError("Mergin Maps client error: " + str(e))

    print("Server is at version " + server_info["version"])

    status_pull = mp.get_pull_changes(server_info["files"])
    if status_pull['added'] or status_pull['updated'] or status_pull['removed']:
        print("There are pending changes on server:")
        _print_mergin_changes(status_pull)
    else:
        print("No pending changes on server.")

    print("")
    conn = psycopg2.connect(conn_cfg.conn_info)

    if not _check_schema_exists(conn, conn_cfg.base):
        raise DbSyncError("The base schema does not exist: " + conn_cfg.base)
    if not _check_schema_exists(conn, conn_cfg.modified):
        raise DbSyncError("The 'modified' schema does not exist: " + conn_cfg.modified)

    # get changes in the DB
    tmp_dir = tempfile.gettempdir()
    tmp_changeset_file = os.path.join(tmp_dir, f'{project_name}-dbsync-status-base2our')
    if os.path.exists(tmp_changeset_file):
        os.remove(tmp_changeset_file)
    _geodiff_create_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, conn_cfg.modified, tmp_changeset_file)

    if os.path.getsize(tmp_changeset_file) == 0:
        print("No changes in the database.")
    else:
        print("There are changes in DB")
        # summarize changes
        summary = _geodiff_list_changes_summary(tmp_changeset_file)
        _print_changes_summary(summary)


def push(conn_cfg, mc):
    """ Take changes in the 'modified' schema in the database and push them to Mergin Maps"""

    print(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")

    project_name = conn_cfg.mergin_project.split("/")[1]

    tmp_dir = tempfile.gettempdir()
    tmp_changeset_file = os.path.join(tmp_dir, f'{project_name}-dbsync-push-base2our')
    if os.path.exists(tmp_changeset_file):
        os.remove(tmp_changeset_file)

    work_dir = os.path.join(config.working_dir, project_name)
    gpkg_full_path = os.path.join(work_dir, conn_cfg.sync_file)
    _check_has_working_dir(work_dir)
    _check_has_sync_file(gpkg_full_path)

    mp = MerginProject(work_dir)
    if mp.geodiff is None:
        raise DbSyncError("Mergin Maps client installation problem: geodiff not available")
    project_path = mp.metadata["name"]
    local_version = mp.metadata["version"]

    try:
        projects = mc.get_projects_by_names([project_path])
        server_version = projects[project_path]["version"]
    except ClientError as e:
        # this could be e.g. DNS error
        raise DbSyncError("Mergin Maps client error: " + str(e))

    status_push = mp.get_push_changes()
    if status_push['added'] or status_push['updated'] or status_push['removed']:
        raise DbSyncError(
            "There are pending changes in the local directory - that should never happen! " + str(status_push))

    # check there are no pending changes on server
    if server_version != local_version:
        raise DbSyncError("There are pending changes on server - need to pull them first.")

    conn = psycopg2.connect(conn_cfg.conn_info)

    if not _check_schema_exists(conn, conn_cfg.base):
        raise DbSyncError("The base schema does not exist: " + conn_cfg.base)
    if not _check_schema_exists(conn, conn_cfg.modified):
        raise DbSyncError("The 'modified' schema does not exist: " + conn_cfg.modified)

    # get changes in the DB
    _geodiff_create_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, conn_cfg.modified, tmp_changeset_file)

    if os.path.getsize(tmp_changeset_file) == 0:
        print("No changes in the database.")
        return

    # summarize changes
    summary = _geodiff_list_changes_summary(tmp_changeset_file)
    _print_changes_summary(summary)

    # write changes to the local geopackage
    print("Writing DB changes to working dir...")
    _geodiff_apply_changeset("sqlite", "", gpkg_full_path, tmp_changeset_file)

    # write to the server
    try:
        mc.push_project(work_dir)
    except ClientError as e:
        # TODO: should we do some cleanup here? (undo changes in the local geopackage?)
        raise DbSyncError("Mergin Maps client error on push: " + str(e))

    version = _get_project_version(work_dir)
    print("Pushed new version to Mergin Maps: " + version)

    # update base schema in the DB
    print("Updating DB base schema...")
    _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, tmp_changeset_file)
    _set_db_project_comment(conn, conn_cfg.base, conn_cfg.mergin_project, version)


def init(conn_cfg, mc, from_gpkg=True):
    """ Initialize the dbsync so that it is possible to do two-way sync between Mergin Maps and a database """

    print(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")
    project_name = conn_cfg.mergin_project.split("/")[1]

    # let's start with various environment checks to make sure
    # the environment is set up correctly before doing any work
    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(conn_cfg.conn_info)
    except psycopg2.Error as e:
        raise DbSyncError("Unable to connect to the database: " + str(e))

    base_schema_exists = _check_schema_exists(conn, conn_cfg.base)
    modified_schema_exists = _check_schema_exists(conn, conn_cfg.modified)

    work_dir = os.path.join(config.working_dir, project_name)
    gpkg_full_path = os.path.join(work_dir, conn_cfg.sync_file)
    if modified_schema_exists and base_schema_exists:
        print("Modified and base schemas already exist")
        # this is not a first run of db-sync init
        db_proj_info = _get_db_project_comment(conn, conn_cfg.base)
        if not db_proj_info:
            raise DbSyncError("Base schema exists but missing which project it belongs to")
        if "error" in db_proj_info:
            changes_gpkg_base = _compare_datasets("sqlite", "", gpkg_full_path, conn_cfg.driver,
                                                  conn_cfg.conn_info, conn_cfg.base,
                                                  summary_only=False)
            changes = json.dumps(changes_gpkg_base, indent=2)
            print(f"Changeset from failed init:\n {changes}")
            raise DbSyncError(db_proj_info["error"])

        # make sure working directory contains the same version of project
        if not os.path.exists(work_dir):
            print(f"Downloading version {db_proj_info['version']} of Mergin Maps project {conn_cfg.mergin_project} "
                  f"to {work_dir}")
            mc.download_project(conn_cfg.mergin_project, work_dir, db_proj_info["version"])
        else:
            local_version = _get_project_version(work_dir)
            print(f"Working directory {work_dir} already exists, with project version {local_version}")
            if local_version != db_proj_info["version"]:
                print(f"Removing local working directory {work_dir}")
                shutil.rmtree(work_dir)
                print(f"Downloading version {db_proj_info['version']} of Mergin Maps project {conn_cfg.mergin_project} "
                      f"to {work_dir}")
                mc.download_project(conn_cfg.mergin_project, work_dir, db_proj_info["version"])
    else:
        if not os.path.exists(work_dir):
            print("Downloading latest Mergin Maps project " + conn_cfg.mergin_project + " to " + work_dir)
            mc.download_project(conn_cfg.mergin_project, work_dir)
        else:
            local_version = _get_project_version(work_dir)
            print(f"Working directory {work_dir} already exists, with project version {local_version}")

    # make sure we have working directory now
    _check_has_working_dir(work_dir)
    local_version = _get_project_version(work_dir)

    # check there are no pending changes on server (or locally - which should never happen)
    status_pull, status_push, _ = mc.project_status(work_dir)
    if status_pull['added'] or status_pull['updated'] or status_pull['removed']:
        print("There are pending changes on server, please run pull command after init")
    if status_push['added'] or status_push['updated'] or status_push['removed']:
        raise DbSyncError("There are pending changes in the local directory - that should never happen! " + str(status_push))

    if from_gpkg:
        if not os.path.exists(gpkg_full_path):
            raise DbSyncError("The input GPKG file does not exist: " + gpkg_full_path)

        if modified_schema_exists and base_schema_exists:
            # if db schema already exists make sure it is already synchronized with source gpkg or fail
            summary_modified = _compare_datasets("sqlite", "", gpkg_full_path, conn_cfg.driver,
                                                 conn_cfg.conn_info, conn_cfg.modified)
            summary_base = _compare_datasets("sqlite", "", gpkg_full_path, conn_cfg.driver,
                                             conn_cfg.conn_info, conn_cfg.base)
            if len(summary_base):
                # seems someone modified base schema manually - this should never happen!
                print(f"Local project version at {local_version} and base schema at {db_proj_info['version']}")
                _print_changes_summary(summary_base, "Base schema changes:")
                raise DbSyncError("The db schemas already exist but 'base' schema is not synchronized with source GPKG")
            elif len(summary_modified):
                print("Modified schema is not synchronised with source GPKG, please run pull/push commands to fix it")
                _print_changes_summary(summary_modified, "Pending Changes:")
                return
            else:
                print("The GPKG file, base and modified schemas are already initialized and in sync")
                return  # nothing to do
        elif modified_schema_exists:
            raise DbSyncError(f"The 'modified' schema exists but the base schema is missing: {conn_cfg.base}")
        elif base_schema_exists:
            raise DbSyncError(f"The base schema exists but the modified schema is missing: {conn_cfg.modified}")

        # initialize: we have an existing GeoPackage in our Mergin Maps project and we want to initialize database
        print("The base and modified schemas do not exist yet, going to initialize them ...")
        try:
            # COPY: gpkg -> modified
            _geodiff_make_copy("sqlite", "", gpkg_full_path,
                               conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified)

            # COPY: modified -> base
            _geodiff_make_copy(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified,
                               conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base)

            # sanity check to verify that right after initialization we do not have any changes
            # between the 'base' schema and the geopackage in Mergin Maps project, to make sure that
            # copying data back and forth will keep data intact
            changes_gpkg_base = _compare_datasets("sqlite", "", gpkg_full_path, conn_cfg.driver,
                                                  conn_cfg.conn_info, conn_cfg.base,
                                                  summary_only=False)
            # mark project version into db schema
            if len(changes_gpkg_base):
                changes = json.dumps(changes_gpkg_base, indent=2)
                print(f"Changeset after internal copy (should be empty):\n {changes}")
                raise DbSyncError('Initialization of db-sync failed due to a bug in geodiff.\n '
                                  'Please report this problem to mergin-db-sync developers')
        except DbSyncError:
            # add comment to base schema before throwing exception
            _set_db_project_comment(conn, conn_cfg.base, conn_cfg.mergin_project, local_version,
                                    error='Initialization of db-sync failed due to a bug in geodiff')
            raise

        _set_db_project_comment(conn, conn_cfg.base, conn_cfg.mergin_project, local_version)
    else:
        if not modified_schema_exists:
            raise DbSyncError("The 'modified' schema does not exist: " + conn_cfg.modified)

        if os.path.exists(gpkg_full_path) and base_schema_exists:
            # make sure output gpkg is in sync with db or fail
            summary_modified = _compare_datasets(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified,
                                                "sqlite", "", gpkg_full_path)
            summary_base = _compare_datasets(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base,
                                            "sqlite", "", gpkg_full_path)
            if len(summary_base):
                print(f"Local project version at {_get_project_version(work_dir)} and base schema at {db_proj_info['version']}")
                _print_changes_summary(summary_base, "Base schema changes:")
                raise DbSyncError("The output GPKG file exists already but is not synchronized with db 'base' schema")
            elif len(summary_modified):
                print("The output GPKG file exists already but it is not synchronised with modified schema, "
                      "please run pull/push commands to fix it")
                _print_changes_summary(summary_modified, "Pending Changes:")
                return
            else:
                print("The GPKG file, base and modified schemas are already initialized and in sync")
                return  # nothing to do
        elif os.path.exists(gpkg_full_path):
            raise DbSyncError(f"The output GPKG exists but the base schema is missing: {conn_cfg.base}")
        elif base_schema_exists:
            raise DbSyncError(f"The base schema exists but the output GPKG exists is missing: {gpkg_full_path}")

        # initialize: we have an existing schema in database with tables and we want to initialize geopackage
        # within our Mergin Maps project
        print("The base schema and the output GPKG do not exist yet, going to initialize them ...")
        try:
            # COPY: modified -> base
            _geodiff_make_copy(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified,
                               conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base)

            # COPY: modified -> gpkg
            _geodiff_make_copy(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified,
                               "sqlite", "", gpkg_full_path)

            # sanity check to verify that right after initialization we do not have any changes
            # between the 'base' schema and the geopackage in Mergin Maps project, to make sure that
            # copying data back and forth will keep data intact
            changes_gpkg_base = _compare_datasets("sqlite", "", gpkg_full_path, conn_cfg.driver,
                                                  conn_cfg.conn_info, conn_cfg.base, summary_only=False)
            if len(changes_gpkg_base):
                changes = json.dumps(changes_gpkg_base, indent=2)
                print(f"Changeset after internal copy (should be empty):\n {changes}")
                raise DbSyncError('Initialization of db-sync failed due to a bug in geodiff.\n '
                                  'Please report this problem to mergin-db-sync developers')
        except DbSyncError:
            _set_db_project_comment(conn, conn_cfg.base, conn_cfg.mergin_project, local_version,
                                    error='Initialization of db-sync failed due to a bug in geodiff')
            raise

        # upload gpkg to Mergin Maps (client takes care of storing metadata)
        mc.push_project(work_dir)

        # mark project version into db schema
        version = _get_project_version(work_dir)
        _set_db_project_comment(conn, conn_cfg.base, conn_cfg.mergin_project, version)


def dbsync_init(mc, from_gpkg=True):
    for conn in config.connections:
        init(conn, mc, from_gpkg=True)

    print("Init done!")


def dbsync_pull(mc):
    for conn in config.connections:
        pull(conn, mc)

    print("Pull done!")


def dbsync_push(mc):
    for conn in config.connections:
        push(conn, mc)

    print("Push done!")


def dbsync_status(mc):
    for conn in config.connections:
        status(conn, mc)


def show_usage():
    print("dbsync")
    print("")
    print("    dbsync init-from-db   = will create base schema in DB + create gpkg file in working copy")
    print("    dbsync init-from-gpkg = will create base and main schema in DB from gpkg file in working copy")
    print("    dbsync status      = will check whether there is anything to pull or push")
    print("    dbsync push        = will push changes from DB to Mergin Maps")
    print("    dbsync pull        = will pull changes from Mergin Maps to DB")


def main():
    if len(sys.argv) < 2:
        show_usage()
        return

    print(f"== Starting Mergin Maps DB Sync version {__version__} ==")

    try:
        validate_config(config)
    except ConfigError as e:
        print("Error: " + str(e))
        return

    try:
        print("Logging in to Mergin Maps...")
        mc = create_mergin_client()

        if sys.argv[1] == 'init-from-gpkg':
            print("Initializing from an existing GeoPackage...")
            dbsync_init(mc, True)
        elif sys.argv[1] == 'init-from-db':
            print("Initializing from an existing DB schema...")
            dbsync_init(mc, False)
        elif sys.argv[1] == 'status':
            dbsync_status(mc)
        elif sys.argv[1] == 'push':
            print("Pushing...")
            dbsync_push(mc)
        elif sys.argv[1] == 'pull':
            print("Pulling...")
            dbsync_pull(mc)
        else:
            show_usage()
    except DbSyncError as e:
        print("Error: " + str(e))


if __name__ == '__main__':
    main()
