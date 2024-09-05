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
import tempfile
import random
import uuid
import re
import pathlib
import logging
import typing

import psycopg2
import psycopg2.extensions
from psycopg2 import (
    sql,
)
from itertools import (
    chain,
)

from mergin import (
    MerginClient,
    MerginProject,
    LoginError,
    ClientError,
    InvalidProject,
)
from version import (
    __version__,
)
from config import (
    config,
    validate_config,
    get_ignored_tables,
    ConfigError,
)

# set high logging level for geodiff (used by geodiff executable)
# so we get as much information as possible
os.environ["GEODIFF_LOGGER_LEVEL"] = "4"  # 0 = nothing, 1 = errors, 2 = warning, 3 = info, 4 = debug

FORCE_INIT_MESSAGE = "Running `dbsync_deamon.py` with `--force-init` should fix the issue."


class DbSyncError(Exception):
    default_print_password = "password='*****'"

    def __init__(
        self,
        message,
    ):
        # escaped password
        message = re.sub(
            r"password=[\"\'].+[\"\'](?=\s)",
            self.default_print_password,
            message,
        )
        # not escaped password
        message = re.sub(
            r"password=\S+",
            self.default_print_password,
            message,
        )
        super().__init__(message)


def _add_quotes_to_schema_name(
    schema: str,
) -> str:
    matches = re.findall(
        r"[^a-z0-9_]",
        schema,
    )
    if len(matches) != 0:
        schema = schema.replace(
            '"',
            '""',
        )
        schema = f'"{schema}"'
    return schema


def _tables_list_to_string(
    tables,
):
    return ";".join(tables)


def _check_has_working_dir(
    work_path,
):
    if not os.path.exists(work_path):
        raise DbSyncError("The project working directory does not exist: " + work_path)

    if not os.path.exists(
        os.path.join(
            work_path,
            ".mergin",
        )
    ):
        raise DbSyncError("The project working directory does not seem to contain Mergin Maps project: " + work_path)


def _check_has_sync_file(
    file_path,
):
    """Checks whether the dbsync environment is initialized already (so that we can pull/push).
    Emits an exception if not initialized yet."""

    if not os.path.exists(file_path):
        raise DbSyncError("The output GPKG file does not exist: " + file_path)


def _drop_schema(
    conn,
    schema_name: str,
) -> None:
    cur = conn.cursor()
    cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name)))
    conn.commit()


def _check_schema_exists(
    conn,
    schema_name,
):
    cur = conn.cursor()
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)",
        (schema_name,),
    )
    return cur.fetchone()[0]


def _check_postgis_available(
    conn: psycopg2.extensions.connection,
) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT extname FROM pg_extension;")
    try:
        result = cur.fetchall()
        for row in result:
            if row[0].lower() == "postgis":
                return True
        return False
    except psycopg2.ProgrammingError:
        return False


def _try_install_postgis(
    conn: psycopg2.extensions.connection,
) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("CREATE EXTENSION postgis;")
        return True
    except psycopg2.ProgrammingError:
        return False


def _check_has_password():
    """Checks whether we have password for Mergin Maps user - if not, we will ask for it"""
    if config.mergin.password is None:
        config.mergin.password = getpass.getpass(
            prompt="Mergin Maps password for '{}': ".format(config.mergin.username)
        )


def _run_geodiff(
    cmd,
):
    """will run a command (with geodiff) and report what got to stderr and raise exception
    if the command returns non-zero exit code"""
    res = subprocess.run(
        cmd,
        stderr=subprocess.PIPE,
    )
    geodiff_stderr = res.stderr.decode()
    if geodiff_stderr:
        logging.error("GEODIFF: " + geodiff_stderr)
    if res.returncode != 0:
        raise DbSyncError("geodiff failed!\n" + str(cmd))


def _geodiff_create_changeset(
    driver,
    conn_info,
    base,
    modified,
    changeset,
    ignored_tables,
):
    if ignored_tables:
        _run_geodiff(
            [
                config.geodiff_exe,
                "diff",
                "--driver",
                driver,
                conn_info,
                "--skip-tables",
                _tables_list_to_string(ignored_tables),
                base,
                modified,
                changeset,
            ]
        )
    else:
        _run_geodiff(
            [
                config.geodiff_exe,
                "diff",
                "--driver",
                driver,
                conn_info,
                base,
                modified,
                changeset,
            ]
        )


def _geodiff_apply_changeset(
    driver,
    conn_info,
    base,
    changeset,
    ignored_tables,
):
    if ignored_tables:
        _run_geodiff(
            [
                config.geodiff_exe,
                "apply",
                "--driver",
                driver,
                conn_info,
                "--skip-tables",
                _tables_list_to_string(ignored_tables),
                base,
                changeset,
            ]
        )
    else:
        _run_geodiff(
            [
                config.geodiff_exe,
                "apply",
                "--driver",
                driver,
                conn_info,
                base,
                changeset,
            ]
        )


def _geodiff_rebase(
    driver,
    conn_info,
    base,
    our,
    base2their,
    conflicts,
    ignored_tables,
):
    if ignored_tables:
        _run_geodiff(
            [
                config.geodiff_exe,
                "rebase-db",
                "--driver",
                driver,
                conn_info,
                "--skip-tables",
                _tables_list_to_string(ignored_tables),
                base,
                our,
                base2their,
                conflicts,
            ]
        )
    else:
        _run_geodiff(
            [
                config.geodiff_exe,
                "rebase-db",
                "--driver",
                driver,
                conn_info,
                base,
                our,
                base2their,
                conflicts,
            ]
        )


def _geodiff_list_changes_details(
    changeset,
):
    """Returns a list with changeset details:
    [ { 'table': 'foo', 'type': 'update', 'changes': [ ... old/new column values ... ] }, ... ]
    """
    tmp_dir = tempfile.gettempdir()
    tmp_output = os.path.join(
        tmp_dir,
        "dbsync-changeset-details",
    )
    if os.path.exists(tmp_output):
        os.remove(tmp_output)
    _run_geodiff(
        [
            config.geodiff_exe,
            "as-json",
            changeset,
            tmp_output,
        ]
    )
    with open(tmp_output) as f:
        out = json.load(f)
    os.remove(tmp_output)
    return out["geodiff"]


def _geodiff_list_changes_summary(
    changeset,
):
    """Returns a list with changeset summary:
    [ { 'table': 'foo', 'insert': 1, 'update': 2, 'delete': 3 }, ... ]
    """
    tmp_dir = tempfile.gettempdir()
    tmp_output = os.path.join(
        tmp_dir,
        "dbsync-changeset-summary",
    )
    if os.path.exists(tmp_output):
        os.remove(tmp_output)
    _run_geodiff(
        [
            config.geodiff_exe,
            "as-summary",
            changeset,
            tmp_output,
        ]
    )
    with open(tmp_output) as f:
        out = json.load(f)
    os.remove(tmp_output)
    return out["geodiff_summary"]


def _geodiff_make_copy(
    src_driver,
    src_conn_info,
    src,
    dst_driver,
    dst_conn_info,
    dst,
    ignored_tables,
):
    if ignored_tables:
        _run_geodiff(
            [
                config.geodiff_exe,
                "copy",
                "--driver-1",
                src_driver,
                src_conn_info,
                "--driver-2",
                dst_driver,
                dst_conn_info,
                "--skip-tables",
                _tables_list_to_string(ignored_tables),
                src,
                dst,
            ]
        )
    else:
        _run_geodiff(
            [
                config.geodiff_exe,
                "copy",
                "--driver-1",
                src_driver,
                src_conn_info,
                "--driver-2",
                dst_driver,
                dst_conn_info,
                src,
                dst,
            ]
        )


def _geodiff_create_changeset_dr(
    src_driver,
    src_conn_info,
    src,
    dst_driver,
    dst_conn_info,
    dst,
    changeset,
    ignored_tables,
):
    if ignored_tables:
        _run_geodiff(
            [
                config.geodiff_exe,
                "diff",
                "--driver-1",
                src_driver,
                src_conn_info,
                "--driver-2",
                dst_driver,
                dst_conn_info,
                "--skip-tables",
                _tables_list_to_string(ignored_tables),
                src,
                dst,
                changeset,
            ]
        )
    else:
        _run_geodiff(
            [
                config.geodiff_exe,
                "diff",
                "--driver-1",
                src_driver,
                src_conn_info,
                "--driver-2",
                dst_driver,
                dst_conn_info,
                src,
                dst,
                changeset,
            ]
        )


def _compare_datasets(
    src_driver,
    src_conn_info,
    src,
    dst_driver,
    dst_conn_info,
    dst,
    ignored_tables,
    summary_only=True,
):
    """Compare content of two datasets (from various drivers) and return geodiff JSON summary of changes"""
    tmp_dir = tempfile.gettempdir()
    tmp_changeset = os.path.join(
        tmp_dir,
        "".join(
            random.choices(
                string.ascii_letters,
                k=8,
            )
        ),
    )

    _geodiff_create_changeset_dr(
        src_driver,
        src_conn_info,
        src,
        dst_driver,
        dst_conn_info,
        dst,
        tmp_changeset,
        ignored_tables,
    )
    if summary_only:
        return _geodiff_list_changes_summary(tmp_changeset)
    else:
        return _geodiff_list_changes_details(tmp_changeset)


def _print_changes_summary(
    summary,
    label=None,
):
    """Takes a geodiff JSON summary of changes and prints them"""
    print("Changes:" if label is None else label)
    for item in summary:
        print(
            "{:20} {:4} {:4} {:4}".format(
                item["table"],
                item["insert"],
                item["update"],
                item["delete"],
            )
        )


def _print_mergin_changes(
    diff_dict,
):
    """Takes a dictionary with format { 'added': [...], 'removed': [...], 'updated': [...] }
    where each item is another dictionary with file details, e.g.:
      { 'path': 'myfile.gpkg', size: 123456, ... }
    and prints it in a way that's easy to parse for a human :-)
    """
    for item in diff_dict["added"]:
        logging.debug("  added:   " + item["path"])
    for item in diff_dict["updated"]:
        logging.debug("  updated: " + item["path"])
    for item in diff_dict["removed"]:
        logging.debug("  removed: " + item["path"])


# Dictionary used by _get_mergin_project() function below.
# key = path to a local dir with Mergin project, value = cached MerginProject object
cached_mergin_project_objects = {}


def _get_mergin_project(work_path) -> MerginProject:
    """
    Returns a cached MerginProject object or creates one if it does not exist yet.
    This is to avoid creating many of these objects (e.g. every pull/push) because it does
    initialization of geodiff as well, so things should be 1. a bit faster, and 2. safer.
    (Safer because we are having a cycle of refs between GeoDiff and MerginProject objects
    related to logging - and untangling those would need some extra calls when we are done
    with MerginProject. But since we use the object all the time, it's better to cache it anyway.)
    """
    if work_path not in cached_mergin_project_objects:
        cached_mergin_project_objects[work_path] = MerginProject(work_path)
    cached_mergin_project_objects[work_path]._read_metadata()
    return cached_mergin_project_objects[work_path]


def _get_project_version(work_path) -> str:
    """Returns the current version of the project"""
    mp = _get_mergin_project(work_path)
    return mp.version()


def _get_project_id(mp: typing.Union[MerginProject, str]):
    """Returns the project ID"""
    if isinstance(mp, str):
        mp = _get_mergin_project(mp)
    try:
        project_id = uuid.UUID(mp.project_id())
    except (
        KeyError,
        ValueError,
    ):
        project_id = None
    return project_id


def _set_db_project_comment(
    conn,
    schema,
    project_name,
    version,
    project_id=None,
    error=None,
):
    """Set postgres COMMENT on SCHEMA with Mergin Maps project name and version
    or eventually error message if initialisation failed
    """
    comment = {
        "name": project_name,
        "version": version,
    }
    if project_id:
        comment["project_id"] = str(project_id)
    if error:
        comment["error"] = error
    cur = conn.cursor()
    query = sql.SQL("COMMENT ON SCHEMA {} IS %s").format(sql.Identifier(schema))
    cur.execute(
        query.as_string(conn),
        (json.dumps(comment),),
    )
    conn.commit()


def _get_db_project_comment(conn, schema):
    """Get Mergin Maps project name and its current version in db schema"""
    cur = conn.cursor()
    schema = _add_quotes_to_schema_name(schema)
    cur.execute(
        "SELECT obj_description(%s::regnamespace, 'pg_namespace')",
        (schema,),
    )
    res = cur.fetchone()[0]
    try:
        comment = json.loads(res) if res else None
    except (
        TypeError,
        json.decoder.JSONDecodeError,
    ):
        return
    return comment


def _redownload_project(conn_cfg, mc, work_dir, db_proj_info):
    logging.debug(f"Removing local working directory {work_dir}")
    shutil.rmtree(work_dir)
    logging.debug(
        f"Downloading version {db_proj_info['version']} of Mergin Maps project {conn_cfg.mergin_project} "
        f"to {work_dir}"
    )
    try:
        mc.download_project(
            conn_cfg.mergin_project,
            work_dir,
            db_proj_info["version"],
        )
    except ClientError as e:
        raise DbSyncError("Mergin Maps client error: " + str(e))


def _validate_local_project_id(
    mp,
    mc,
    server_info=None,
):
    """Compare local project ID with remote version on the server."""
    local_project_id = _get_project_id(mp)
    if local_project_id is None:
        return
    if server_info is None:
        try:
            server_info = mc.project_info(mp.project_full_name())
        except ClientError as e:
            raise DbSyncError("Mergin Maps client error: " + str(e))

    remote_project_id = uuid.UUID(server_info["id"])
    if local_project_id != remote_project_id:
        raise DbSyncError(
            f"The local project ID ({local_project_id}) does not match the server project ID ({remote_project_id})"
        )


def create_mergin_client():
    """Create instance of MerginClient"""
    _check_has_password()
    try:
        return MerginClient(
            config.mergin.url,
            login=config.mergin.username,
            password=config.mergin.password,
            plugin_version=f"DB-sync/{__version__}",
        )
    except LoginError as e:
        # this could be auth failure, but could be also server problem (e.g. worker crash)
        raise DbSyncError(
            f"Unable to log in to Mergin Maps: {str(e)} \n\n"
            + "Have you specified correct credentials in configuration file?"
        )
    except ClientError as e:
        # this could be e.g. DNS error
        raise DbSyncError("Mergin Maps client error: " + str(e))


def revert_local_changes(
    mc,
    mp,
    local_changes=None,
):
    """Revert local changes from the existing project."""
    if local_changes is None:
        local_changes = mp.get_push_changes()
    if not any(local_changes.values()):
        return local_changes
    logging.debug("Reverting local changes: " + str(local_changes))
    for add_change in local_changes["added"]:
        added_file = add_change["path"]
        added_filepath = os.path.join(
            mp.dir,
            added_file,
        )
        os.remove(added_filepath)
    for update_delete_change in chain(
        local_changes["updated"],
        local_changes["removed"],
    ):
        update_delete_file = update_delete_change["path"]
        update_delete_filepath = os.path.join(
            mp.dir,
            update_delete_file,
        )
        delete_file = os.path.isfile(update_delete_filepath)
        if update_delete_file.lower().endswith(".gpkg"):
            update_delete_filepath_base = os.path.join(
                mp.meta_dir,
                update_delete_file,
            )
            if delete_file:
                os.remove(update_delete_filepath)
            shutil.copy(
                update_delete_filepath_base,
                update_delete_filepath,
            )
        else:
            if delete_file:
                os.remove(update_delete_filepath)
            try:
                mc.download_file(
                    mp.dir,
                    update_delete_file,
                    update_delete_filepath,
                    mp.version(),
                )
            except ClientError as e:
                raise DbSyncError("Mergin Maps client error: " + str(e))
    leftovers = mp.get_push_changes()
    logging.debug("LEFTOVERS: " + str(leftovers))
    return leftovers


def pull(conn_cfg, mc):
    """Downloads any changes from Mergin Maps and applies them to the database"""

    logging.debug(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")
    ignored_tables = get_ignored_tables(conn_cfg)

    project_name = conn_cfg.mergin_project.split("/")[1]
    work_dir = os.path.join(
        config.working_dir,
        project_name,
    )
    gpkg_full_path = os.path.join(
        work_dir,
        conn_cfg.sync_file,
    )

    _check_has_working_dir(work_dir)
    _check_has_sync_file(gpkg_full_path)

    mp = _get_mergin_project(work_dir)
    mp.set_tables_to_skip(ignored_tables)
    if mp.geodiff is None:
        raise DbSyncError("Mergin Maps client installation problem: geodiff not available")

    # Make sure that local project ID (if available) is the same as on  the server
    _validate_local_project_id(mp, mc)

    local_version = mp.version()

    try:
        projects = mc.get_projects_by_names([mp.project_full_name()])
        server_version = projects[mp.project_full_name()]["version"]
    except ClientError as e:
        # this could be e.g. DNS error
        raise DbSyncError("Mergin Maps client error: " + str(e))

    local_changes = mp.get_push_changes()
    if any(local_changes.values()):
        local_changes = revert_local_changes(
            mc,
            mp,
            local_changes,
        )
        if any(local_changes.values()):
            raise DbSyncError(
                "There are pending changes in the local directory - that should never happen! " + str(local_changes)
            )
    if server_version == local_version:
        logging.debug("No changes on Mergin Maps.")
        return

    gpkg_basefile = os.path.join(
        work_dir,
        ".mergin",
        conn_cfg.sync_file,
    )
    gpkg_basefile_old = gpkg_basefile + "-old"

    # make a copy of the basefile in the current version (base) - because after pull it will be set to "their"
    shutil.copy(
        gpkg_basefile,
        gpkg_basefile_old,
    )

    tmp_dir = tempfile.gettempdir()
    tmp_base2our = os.path.join(
        tmp_dir,
        f"{project_name}-dbsync-pull-base2our",
    )
    tmp_base2their = os.path.join(
        tmp_dir,
        f"{project_name}-dbsync-pull-base2their",
    )

    # find out our local changes in the database (base2our)
    _geodiff_create_changeset(
        conn_cfg.driver,
        conn_cfg.conn_info,
        conn_cfg.base,
        conn_cfg.modified,
        tmp_base2our,
        ignored_tables,
    )

    needs_rebase = False
    if os.path.getsize(tmp_base2our) != 0:
        needs_rebase = True
        summary = _geodiff_list_changes_summary(tmp_base2our)
        _print_changes_summary(
            summary,
            "DB Changes:",
        )

    try:
        mc.pull_project(work_dir)  # will do rebase as needed
    except ClientError as e:
        # TODO: do we need some cleanup here?
        raise DbSyncError("Mergin Maps client error on pull: " + str(e))

    logging.debug("Pulled new version from Mergin Maps: " + _get_project_version(work_dir))

    # simple case when there are no pending local changes - just apply whatever changes are coming
    _geodiff_create_changeset(
        "sqlite",
        "",
        gpkg_basefile_old,
        gpkg_basefile,
        tmp_base2their,
        ignored_tables,
    )

    # summarize changes
    summary = _geodiff_list_changes_summary(tmp_base2their)
    _print_changes_summary(
        summary,
        "Mergin Maps Changes:",
    )

    if not needs_rebase:
        logging.debug("Applying new version [no rebase]")
        _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, tmp_base2their, ignored_tables)
        _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.modified, tmp_base2their, ignored_tables)
    else:
        logging.debug("Applying new version [WITH rebase]")
        tmp_conflicts = os.path.join(tmp_dir, f"{project_name}-dbsync-pull-conflicts")
        _geodiff_rebase(
            conn_cfg.driver,
            conn_cfg.conn_info,
            conn_cfg.base,
            conn_cfg.modified,
            tmp_base2their,
            tmp_conflicts,
            ignored_tables,
        )
        _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, tmp_base2their, ignored_tables)

    os.remove(gpkg_basefile_old)
    conn = psycopg2.connect(conn_cfg.conn_info)
    _set_db_project_comment(
        conn,
        conn_cfg.base,
        conn_cfg.mergin_project,
        version=_get_project_version(work_dir),
        project_id=_get_project_id(work_dir),
    )


def status(conn_cfg, mc):
    """Figure out if there are any pending changes in the database or in Mergin Maps"""

    logging.debug(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")
    ignored_tables = get_ignored_tables(conn_cfg)

    project_name = conn_cfg.mergin_project.split("/")[1]

    work_dir = os.path.join(
        config.working_dir,
        project_name,
    )
    gpkg_full_path = os.path.join(
        work_dir,
        conn_cfg.sync_file,
    )

    _check_has_working_dir(work_dir)
    _check_has_sync_file(gpkg_full_path)

    # get basic information
    mp = _get_mergin_project(work_dir)
    mp.set_tables_to_skip(ignored_tables)
    if mp.geodiff is None:
        raise DbSyncError("Mergin Maps client installation problem: geodiff not available")
    project_path = mp.project_full_name()
    local_version = mp.version()
    logging.debug("Checking status...")
    try:
        server_info = mc.project_info(
            project_path,
            since=local_version,
        )
    except ClientError as e:
        raise DbSyncError("Mergin Maps client error: " + str(e))

    # Make sure that local project ID (if available) is the same as on  the server
    _validate_local_project_id(
        mp,
        mc,
        server_info,
    )

    status_push = mp.get_push_changes()
    if status_push["added"] or status_push["updated"] or status_push["removed"]:
        raise DbSyncError("Pending changes in the local directory - that should never happen! " + str(status_push))

    logging.debug("Working directory " + work_dir)
    logging.debug("Mergin Maps project " + project_path + " at local version " + local_version)
    logging.debug("")

    logging.debug("Server is at version " + server_info["version"])
    status_pull = mp.get_pull_changes(server_info["files"])
    if status_pull["added"] or status_pull["updated"] or status_pull["removed"]:
        logging.debug("There are pending changes on server:")
        _print_mergin_changes(status_pull)
    else:
        logging.debug("No pending changes on server.")

    logging.debug("")
    conn = psycopg2.connect(conn_cfg.conn_info)

    if not _check_schema_exists(
        conn,
        conn_cfg.base,
    ):
        raise DbSyncError("The base schema does not exist: " + conn_cfg.base)
    if not _check_schema_exists(
        conn,
        conn_cfg.modified,
    ):
        raise DbSyncError("The 'modified' schema does not exist: " + conn_cfg.modified)

    # get changes in the DB
    tmp_dir = tempfile.gettempdir()
    tmp_changeset_file = os.path.join(
        tmp_dir,
        f"{project_name}-dbsync-status-base2our",
    )
    if os.path.exists(tmp_changeset_file):
        os.remove(tmp_changeset_file)
    _geodiff_create_changeset(
        conn_cfg.driver,
        conn_cfg.conn_info,
        conn_cfg.base,
        conn_cfg.modified,
        tmp_changeset_file,
        ignored_tables,
    )

    if os.path.getsize(tmp_changeset_file) == 0:
        logging.debug("No changes in the database.")
    else:
        logging.debug("There are changes in DB")
        # summarize changes
        summary = _geodiff_list_changes_summary(tmp_changeset_file)
        _print_changes_summary(summary)


def push(conn_cfg, mc):
    """Take changes in the 'modified' schema in the database and push them to Mergin Maps"""

    logging.debug(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")
    ignored_tables = get_ignored_tables(conn_cfg)

    project_name = conn_cfg.mergin_project.split("/")[1]

    tmp_dir = tempfile.gettempdir()
    tmp_changeset_file = os.path.join(
        tmp_dir,
        f"{project_name}-dbsync-push-base2our",
    )
    if os.path.exists(tmp_changeset_file):
        os.remove(tmp_changeset_file)

    work_dir = os.path.join(
        config.working_dir,
        project_name,
    )
    gpkg_full_path = os.path.join(
        work_dir,
        conn_cfg.sync_file,
    )
    _check_has_working_dir(work_dir)
    _check_has_sync_file(gpkg_full_path)

    mp = _get_mergin_project(work_dir)
    mp.set_tables_to_skip(ignored_tables)
    if mp.geodiff is None:
        raise DbSyncError("Mergin Maps client installation problem: geodiff not available")

    # Make sure that local project ID (if available) is the same as on  the server
    _validate_local_project_id(mp, mc)

    local_version = mp.version()

    try:
        projects = mc.get_projects_by_names([mp.project_full_name()])
        server_version = projects[mp.project_full_name()]["version"]
    except ClientError as e:
        # this could be e.g. DNS error
        raise DbSyncError("Mergin Maps client error: " + str(e))

    status_push = mp.get_push_changes()
    if status_push["added"] or status_push["updated"] or status_push["removed"]:
        raise DbSyncError(
            "There are pending changes in the local directory - that should never happen! " + str(status_push)
        )

    # check there are no pending changes on server
    if server_version != local_version:
        raise DbSyncError("There are pending changes on server - need to pull them first.")

    conn = psycopg2.connect(conn_cfg.conn_info)

    if not _check_schema_exists(
        conn,
        conn_cfg.base,
    ):
        raise DbSyncError("The base schema does not exist: " + conn_cfg.base)
    if not _check_schema_exists(
        conn,
        conn_cfg.modified,
    ):
        raise DbSyncError("The 'modified' schema does not exist: " + conn_cfg.modified)

    # get changes in the DB
    _geodiff_create_changeset(
        conn_cfg.driver,
        conn_cfg.conn_info,
        conn_cfg.base,
        conn_cfg.modified,
        tmp_changeset_file,
        ignored_tables,
    )

    if os.path.getsize(tmp_changeset_file) == 0:
        logging.debug("No changes in the database.")
        return

    # summarize changes
    summary = _geodiff_list_changes_summary(tmp_changeset_file)
    _print_changes_summary(summary)

    # write changes to the local geopackage
    logging.debug("Writing DB changes to working dir...")
    _geodiff_apply_changeset("sqlite", "", gpkg_full_path, tmp_changeset_file, ignored_tables)

    # write to the server
    try:
        mc.push_project(work_dir)
    except ClientError as e:
        # TODO: should we do some cleanup here? (undo changes in the local geopackage?)
        raise DbSyncError("Mergin Maps client error on push: " + str(e))

    version = _get_project_version(work_dir)
    logging.debug("Pushed new version to Mergin Maps: " + version)

    # update base schema in the DB
    logging.debug("Updating DB base schema...")
    _geodiff_apply_changeset(conn_cfg.driver, conn_cfg.conn_info, conn_cfg.base, tmp_changeset_file, ignored_tables)
    _set_db_project_comment(
        conn,
        conn_cfg.base,
        conn_cfg.mergin_project,
        version,
        project_id=_get_project_id(work_dir),
    )


def init(
    conn_cfg,
    mc,
    from_gpkg=True,
):
    """Initialize the dbsync so that it is possible to do two-way sync between Mergin Maps and a database"""

    logging.debug(f"Processing Mergin Maps project '{conn_cfg.mergin_project}'")
    ignored_tables = get_ignored_tables(conn_cfg)

    project_name = conn_cfg.mergin_project.split("/")[1]

    # let's start with various environment checks to make sure
    # the environment is set up correctly before doing any work
    logging.debug("Connecting to the database...")
    try:
        conn = psycopg2.connect(conn_cfg.conn_info)
    except psycopg2.Error as e:
        raise DbSyncError("Unable to connect to the database: " + str(e))

    if conn_cfg.driver.lower() == "postgres":
        if not _check_postgis_available(conn):
            if not _try_install_postgis(conn):
                raise DbSyncError("Cannot find or activate `postgis` extension. You may need to install it.")

    base_schema_exists = _check_schema_exists(
        conn,
        conn_cfg.base,
    )
    modified_schema_exists = _check_schema_exists(
        conn,
        conn_cfg.modified,
    )

    work_dir = os.path.join(
        config.working_dir,
        project_name,
    )
    gpkg_full_path = os.path.join(
        work_dir,
        conn_cfg.sync_file,
    )
    if modified_schema_exists and base_schema_exists:
        logging.debug("Modified and base schemas already exist")
        # this is not a first run of db-sync init
        db_proj_info = _get_db_project_comment(
            conn,
            conn_cfg.base,
        )
        if not db_proj_info:
            raise DbSyncError(
                "Base schema exists but missing which project it belongs to. "
                "This may be a result of a previously failed attempt to initialize DB sync. "
                f"{FORCE_INIT_MESSAGE}"
            )
        if "error" in db_proj_info:
            changes_gpkg_base = _compare_datasets(
                "sqlite",
                "",
                gpkg_full_path,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                ignored_tables,
                summary_only=False,
            )
            changes = json.dumps(changes_gpkg_base, indent=2)
            logging.debug(f"Changeset from failed init:\n {changes}")
            raise DbSyncError(db_proj_info["error"])

        # make sure working directory contains the same version of project
        if not os.path.exists(work_dir):
            logging.debug(
                f"Downloading version {db_proj_info['version']} of Mergin Maps project {conn_cfg.mergin_project} "
                f"to {work_dir}"
            )
            project_info = mc.project_info(conn_cfg.mergin_project)
            if db_proj_info["project_id"] != project_info["id"]:
                raise DbSyncError(
                    "Mergin Maps project ID doesn't match Mergin Maps project ID stored in the database. "
                    "Did you change configuration from one Mergin Maps project to another? "
                    f"You either need to remove schema `{conn_cfg.base}` from Database or use `--force-init` option. "
                    f"{FORCE_INIT_MESSAGE}"
                )
            mc.download_project(conn_cfg.mergin_project, work_dir, db_proj_info["version"])
        else:
            # Get project ID from DB if available
            try:
                local_version = _get_project_version(work_dir)
                logging.debug(f"Working directory {work_dir} already exists, with project version {local_version}")
                # Compare local and database project version
                db_project_id_str = getattr(
                    db_proj_info,
                    "project_id",
                    None,
                )
                db_project_id = uuid.UUID(db_project_id_str) if db_project_id_str else None
                mp = _get_mergin_project(work_dir)
                local_project_id = _get_project_id(mp)
                if (db_project_id and local_project_id) and (db_project_id != local_project_id):
                    raise DbSyncError("Database project ID doesn't match local project ID. " f"{FORCE_INIT_MESSAGE}")
                if local_version != db_proj_info["version"]:
                    _redownload_project(
                        conn_cfg,
                        mc,
                        work_dir,
                        db_proj_info,
                    )
            except InvalidProject as e:
                logging.debug(f"Error: {e}")
                _redownload_project(conn_cfg, mc, work_dir, db_proj_info)
    else:
        if not os.path.exists(work_dir):
            logging.debug("Downloading latest Mergin Maps project " + conn_cfg.mergin_project + " to " + work_dir)
            mc.download_project(conn_cfg.mergin_project, work_dir)
        else:
            local_version = _get_project_version(work_dir)
            logging.debug(f"Working directory {work_dir} already exists, with project version {local_version}")

    # make sure we have working directory now
    _check_has_working_dir(work_dir)
    local_version = _get_project_version(work_dir)
    mp = _get_mergin_project(work_dir)
    # Make sure that local project ID (if available) is the same as on  the server
    _validate_local_project_id(mp, mc)

    # check there are no pending changes on server (or locally - which should never happen)
    status_pull, status_push, _ = mc.project_status(work_dir)
    if status_pull["added"] or status_pull["updated"] or status_pull["removed"]:
        logging.debug("There are pending changes on server, please run pull command after init")
    if status_push["added"] or status_push["updated"] or status_push["removed"]:
        raise DbSyncError(
            "There are pending changes in the local directory - that should never happen! "
            + str(status_push)
            + " "
            + f"{FORCE_INIT_MESSAGE}"
        )

    if from_gpkg:
        if not os.path.exists(gpkg_full_path):
            raise DbSyncError("The input GPKG file does not exist: " + gpkg_full_path)

        if modified_schema_exists and base_schema_exists:
            # if db schema already exists make sure it is already synchronized with source gpkg or fail
            logging.debug("Checking 'modified' schema content...")
            summary_modified = _compare_datasets(
                "sqlite",
                "",
                gpkg_full_path,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.modified,
                ignored_tables,
            )
            logging.debug("Checking 'base' schema content...")
            summary_base = _compare_datasets(
                "sqlite",
                "",
                gpkg_full_path,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                ignored_tables,
            )
            if len(summary_base):
                # seems someone modified base schema manually - this should never happen!
                logging.debug(f"Local project version at {local_version} and base schema at {db_proj_info['version']}")
                _print_changes_summary(summary_base, "Base schema changes:")
                raise DbSyncError(
                    "The db schemas already exist but 'base' schema is not synchronized with source GPKG. "
                    f"{FORCE_INIT_MESSAGE}"
                )
            elif len(summary_modified):
                logging.debug(
                    "Modified schema is not synchronised with source GPKG, please run pull/push commands to fix it"
                )
                _print_changes_summary(summary_modified, "Pending Changes:")
                return
            else:
                logging.debug("The GPKG file, base and modified schemas are already initialized and in sync")
                return  # nothing to do
        elif modified_schema_exists:
            raise DbSyncError(
                f"The 'modified' schema exists but the base schema is missing: {conn_cfg.base}. "
                "This may be a result of a previously failed attempt to initialize DB sync. "
                f"{FORCE_INIT_MESSAGE}"
            )
        elif base_schema_exists:
            raise DbSyncError(
                f"The base schema exists but the modified schema is missing: {conn_cfg.modified}. "
                "This may be a result of a previously failed attempt to initialize DB sync. "
                f"{FORCE_INIT_MESSAGE}"
            )

        # initialize: we have an existing GeoPackage in our Mergin Maps project and we want to initialize database
        logging.debug("The base and modified schemas do not exist yet, going to initialize them ...")
        try:
            # COPY: gpkg -> modified
            _geodiff_make_copy(
                "sqlite",
                "",
                gpkg_full_path,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.modified,
                ignored_tables,
            )

            # COPY: modified -> base
            _geodiff_make_copy(
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.modified,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                ignored_tables,
            )

            # sanity check to verify that right after initialization we do not have any changes
            # between the 'base' schema and the geopackage in Mergin Maps project, to make sure that
            # copying data back and forth will keep data intact
            changes_gpkg_base = _compare_datasets(
                "sqlite",
                "",
                gpkg_full_path,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                ignored_tables,
                summary_only=False,
            )
            # mark project version into db schema
            if len(changes_gpkg_base):
                changes = json.dumps(changes_gpkg_base, indent=2)
                logging.debug(f"Changeset after internal copy (should be empty):\n {changes}")
                raise DbSyncError(
                    "Initialization of db-sync failed due to a bug in geodiff.\n "
                    "Please report this problem to mergin-db-sync developers"
                )
        except DbSyncError:
            logging.debug(
                f"Cleaning up after a failed DB sync init - dropping schemas {conn_cfg.base} and {conn_cfg.modified}."
            )
            _drop_schema(conn, conn_cfg.base)
            _drop_schema(conn, conn_cfg.modified)
            raise

        _set_db_project_comment(
            conn,
            conn_cfg.base,
            conn_cfg.mergin_project,
            local_version,
            project_id=_get_project_id(work_dir),
        )
    else:
        if not modified_schema_exists:
            raise DbSyncError(
                f"The 'modified' schema does not exist: {conn_cfg.modified}. "
                "This schema is necessary if initialization should be done from database (parameter `init-from-db`)."
            )

        if os.path.exists(gpkg_full_path) and base_schema_exists:
            # make sure output gpkg is in sync with db or fail
            logging.debug("Checking GeoPackage content...")
            summary_modified = _compare_datasets(
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.modified,
                "sqlite",
                "",
                gpkg_full_path,
                ignored_tables,
            )
            logging.debug("Checking 'base' schema content...")
            summary_base = _compare_datasets(
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                "sqlite",
                "",
                gpkg_full_path,
                ignored_tables,
            )
            if len(summary_base):
                logging.debug(
                    f"Local project version at {_get_project_version(work_dir)} and base schema at {db_proj_info['version']}"
                )
                _print_changes_summary(summary_base, "Base schema changes:")
                raise DbSyncError(
                    "The output GPKG file exists already but is not synchronized with db 'base' schema."
                    f"{FORCE_INIT_MESSAGE}"
                )
            elif len(summary_modified):
                logging.debug(
                    "The output GPKG file exists already but it is not synchronised with modified schema, "
                    "please run pull/push commands to fix it"
                )
                _print_changes_summary(summary_modified, "Pending Changes:")
                return
            else:
                logging.debug("The GPKG file, base and modified schemas are already initialized and in sync")
                return  # nothing to do
        elif os.path.exists(gpkg_full_path):
            raise DbSyncError(
                f"The output GPKG exists but the base schema is missing: {conn_cfg.base}. " f"{FORCE_INIT_MESSAGE}"
            )
        elif base_schema_exists:
            raise DbSyncError(
                f"The base schema exists but the output GPKG exists is missing: {gpkg_full_path}. "
                f"{FORCE_INIT_MESSAGE}"
            )

        # initialize: we have an existing schema in database with tables and we want to initialize geopackage
        # within our Mergin Maps project
        logging.debug("The base schema and the output GPKG do not exist yet, going to initialize them ...")
        try:
            # COPY: modified -> base
            _geodiff_make_copy(
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.modified,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                ignored_tables,
            )

            # COPY: modified -> gpkg
            _geodiff_make_copy(
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.modified,
                "sqlite",
                "",
                gpkg_full_path,
                ignored_tables,
            )

            # sanity check to verify that right after initialization we do not have any changes
            # between the 'base' schema and the geopackage in Mergin Maps project, to make sure that
            # copying data back and forth will keep data intact
            changes_gpkg_base = _compare_datasets(
                "sqlite",
                "",
                gpkg_full_path,
                conn_cfg.driver,
                conn_cfg.conn_info,
                conn_cfg.base,
                ignored_tables,
                summary_only=False,
            )
            if len(changes_gpkg_base):
                changes = json.dumps(changes_gpkg_base, indent=2)
                logging.debug(f"Changeset after internal copy (should be empty):\n {changes}")
                raise DbSyncError(
                    "Initialization of db-sync failed due to a bug in geodiff.\n "
                    "Please report this problem to mergin-db-sync developers"
                )
        except DbSyncError:
            logging.debug(f"Cleaning up after a failed DB sync init - dropping schema {conn_cfg.base}.")
            _drop_schema(conn, conn_cfg.base)
            raise

        # upload gpkg to Mergin Maps (client takes care of storing metadata)
        mc.push_project(work_dir)

        # mark project version into db schema
        _set_db_project_comment(
            conn,
            conn_cfg.base,
            conn_cfg.mergin_project,
            version=_get_project_version(work_dir),
            project_id=_get_project_id(work_dir),
        )


def dbsync_init(mc):
    from_gpkg = config.init_from.lower() == "gpkg"
    for conn in config.connections:
        init(
            conn,
            mc,
            from_gpkg=from_gpkg,
        )

    logging.debug("Init done!")


def dbsync_pull(mc):
    for conn in config.connections:
        pull(conn, mc)

    logging.debug("Pull done!")


def dbsync_push(mc):
    for conn in config.connections:
        push(conn, mc)

    logging.debug("Push done!")


def dbsync_status(
    mc,
):
    for conn in config.connections:
        status(conn, mc)


def clean(conn_cfg, mc):
    from_db = config.init_from.lower() == "db"

    if pathlib.Path(config.working_dir).exists():
        try:
            shutil.rmtree(config.working_dir)
        except FileNotFoundError as e:
            raise DbSyncError("Unable to remove working directory: " + str(e))

    if from_db:
        temp_folder = pathlib.Path(config.working_dir).parent / "project_to_delete_sync_file"
        try:
            # to remove sync file, download project to created directory, drop file and push changes back
            file = temp_folder / conn_cfg.sync_file
            mc.download_project(
                conn_cfg.mergin_project,
                str(temp_folder),
            )
            if file.exists():
                file.unlink()
            mc.push_project(str(temp_folder))
        except Exception as e:
            raise DbSyncError("Error removing sync file from MM project:" + str(e))
        finally:
            # close mergin project file logger to avoid issues
            close_mergin_project_file_logger(temp_folder)
            # delete the temp_folder no matter what if it exist
            if temp_folder.exists():
                shutil.rmtree(temp_folder)

    try:
        conn_db = psycopg2.connect(conn_cfg.conn_info)
    except psycopg2.Error as e:
        raise DbSyncError("Unable to connect to the database: " + str(e))

    try:
        _drop_schema(
            conn_db,
            conn_cfg.base,
        )

        if not from_db:
            _drop_schema(
                conn_db,
                conn_cfg.modified,
            )

    except psycopg2.Error as e:
        raise DbSyncError("Unable to drop schema from database: " + str(e))


def dbsync_clean(
    mc,
):
    for conn in config.connections:
        clean(conn, mc)

    logging.debug("Cleaning done!")


def close_mergin_project_file_logger(project_folder: pathlib.Path) -> None:
    log = logging.getLogger("mergin.project." + str(project_folder))

    for handler in log.handlers:
        if isinstance(handler, logging.FileHandler):
            log.removeHandler(handler)
            handler.flush()
            handler.close()
