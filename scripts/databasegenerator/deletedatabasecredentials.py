#!/usr/bin/env python3

# Janis - Python framework for portable workflow generation
# Copyright (C) 2020  Michael Franklin
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
This script should be called with one argument, a <directory> for which to delete database credentials.

We consider the following requirements:

DATABASE:
    - Delete all credentials associated with the directory, we can find this by looking up the janis.TENDER tables
    - Even though we store the user details in the janis.TENDER_USR table, we'll look these up through:
        * 'SELECT user FROM mysql.db WHERE Db IN ({DBSTODELETE}) AND user LIKE 'janis_%;'

OTHER:
    - Remove the cache file in the <output-dir>

SETUP
    - You must ensure the credentials are correct within this script
    - The database in the variable TENDER_DB (default: "janis") is created
    - Install the requirements in 'requirements.txt' (primarily, 'pymsql')
"""

import logging
import os
import sys
from typing import Set, Optional, Collection

import pymysql

## ADD MYSQL CREDENTIALS HERE

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = None
MYSQL_PORT = 3306

## CONFIGURATION OPTIONS BELOW

TENDER_DB = "janis"  # Which database to store the TENDER details in
HASH_LENGTH = (
    12  # How long should the directory hash be, longer => less chance of collision
)
MYSQL_DB_PREFIX = (
    "janis_"  # Ensure databases start with this prefix, easier to keep track of
)

# Auth on '%' unless the following is True
#   This is because "localhost" is treated separately by mysql, if we want to use 'localhost', you'd
#   likely want to set "GIVE_CREDENTIALS_TO_LOCALHOST" to True, otherwise we'll use the '%' host.
GIVE_CREDENTIALS_TO_LOCALHOST = False

# setup environment
CREDENTIALS_HOST = "localhost" if GIVE_CREDENTIALS_TO_LOCALHOST else "%"

logging.basicConfig(level=logging.DEBUG)

if not MYSQL_HOST:
    raise Exception("Internal error: MYSQL_HOST requires a value")
if not MYSQL_USER:
    raise Exception("Internal error: MYSQL_USER requires a value")


def excepthook(exception_type, exception, traceback):
    prefix = ""
    if exception_type.__name__ != "Exception":
        prefix = f"{exception_type.__name__}: "
    print(f"{prefix}{exception}", file=sys.stderr)
    exit(1)


sys.excepthook = excepthook


def delete_database_credentials(directory: str, usernames: Optional[str]):
    logging.info(f"Deleting database for '{directory}'")

    directory = prepare_directory_for_hash(directory)

    ensure_has_access_to_(directory)
    remove_dbcache_in_directory(directory)

    # database things
    con = get_connection()
    cursor = con.cursor()

    dbs = get_dbs_for_directory(cursor, directory)
    if len(dbs) > 0:
        logging.info(f"Directory maps to dbs: '{', '.join(dbs)}'")

        usernames_to_delete = (
            [usernames] if usernames else get_users_to_remove(cursor, dbs)
        )
        logging.info(
            f"Dropping databases '{', '.join(dbs)}' and users: {', '.join(usernames_to_delete)}"
        )
        drop_tables(cursor, dbs)
        drop_users(cursor, usernames_to_delete)
    else:
        logging.info("No DBs to delete")
    con.commit()
    cursor.close()


def prepare_directory_for_hash(directory: str) -> str:
    """
    Expand any placeholders (~) and ensure trailing slashes "/" to
    ensure the same directory appears exactly the same between hashes

    >>> prepare_directory_for_hash("/test/trailing/")
    "/test/trailing/"

    >>> prepare_directory_for_hash("/test/trailing")
    "/test/tailing/"

    # no tests for user vars because they're OS dependent
    """
    original_dir = directory

    directory = os.path.abspath(os.path.expanduser(os.path.expandvars(directory)))

    # mfranklin: this is probably a terrible way to do this
    if directory[-1] != "/":
        directory += "/"

    if original_dir != directory:
        logging.info(f"Adjusted '{original_dir}' -> '{directory}'")
    else:
        logging.debug("Directory was unchanged in preparation for hashing")
    return directory


def ensure_has_access_to_(directory: str):
    from pathlib import Path

    file = os.path.join(directory, "janis_db_testfile")
    try:
        p = Path(file)
        p.touch()
        os.remove(file)
    except Exception as e:
        raise Exception(
            f"DB creation helper couldn't access a file in your directory '{file}', and hence can't generate DB "
            f"credentials. Please ensure you have write access to this directory. {repr(e)}"
        )


def remove_dbcache_in_directory(directory: str):
    dbname_path = os.path.join(directory, "generated_mysqldb.txt")
    if os.path.exists(dbname_path):
        logging.info(f"Removing db cache file '{dbname_path}'")
        try:
            os.remove(dbname_path)
        except Exception as e:
            logging.critical("Couldn't remove db cache file, " + repr(e))
    else:
        logging.info(
            f"Skipping removal of db cache file as didn't exist '{dbname_path}'"
        )


def get_dbs_for_directory(cursor: pymysql.cursors.Cursor, directory: str):
    SQL = "SELECT `databasename` FROM `TENDER` WHERE `directory` = %s"
    cursor.execute(SQL, directory)
    dbs = set(r["databasename"] for r in cursor.fetchall())
    return dbs


def generate_hash_from_string(value, length):
    import hashlib

    logging.debug(f"Generating hash of length {length} for '{value}'")
    hsh = hashlib.sha3_224(str(value).encode("UTF-8")).hexdigest()
    return hsh[: min(len(hsh), length) - 1]


def get_connection() -> pymysql.Connection:
    try:
        sqlcon = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            database=TENDER_DB,
        )
    except pymysql.err.OperationalError as e:
        if "Unknown database" in repr(e):
            logging.critical(
                f"You must create 'janis' database in mysql host '{MYSQL_HOST}' before running {sys.argv[0]}"
            )
        raise

    return sqlcon


def get_users_to_remove(cursor, dbs: Set[str]):
    placeholders = ", ".join(["%s"] * len(dbs))
    SQL = f"SELECT user FROM mysql.db WHERE Db IN ({placeholders}) AND user LIKE '{MYSQL_DB_PREFIX}%%';"
    cursor.execute(SQL, tuple(dbs))

    rows = [r["user"].lower() for r in cursor.fetchall()]
    return set(rows)


def drop_tables(cursor, dbs: Collection[str]):
    dbs_str = ", ".join(dbs)
    logging.debug(f"dropping databases {dbs_str} for host '{MYSQL_HOST}'")

    for db in dbs:
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db};")
        except Exception as e:
            logging.critical("Couldn't drop database: " + repr(e))


def drop_users(cursor, usernames: Collection[str]) -> bool:
    if len(usernames) == 0:
        logging.info("Skipping dropping users, as no users were found")
        return True

    logging.debug(f"Dropping users: " + ", ".join(usernames))

    SQL = f"DROP USER {', '.join(usernames)};"

    try:
        cursor.execute(SQL)
    except Exception as e:
        logging.critical(f"Couldn't drop users with '{SQL}': {repr(e)}")
        raise

    return True


if __name__ == "__main__":
    argv = sys.argv
    if len(argv) != 2:
        USAGE = f"""
Deletes database and users that have access to that database that begin with the DB_PREFIX '{MYSQL_DB_PREFIX}'. 
        
Usage:
    deletedatabasecredentials.py DIRECTORY [USERNAME]
        """
        raise Exception(
            f"Incorrect number of arguments, expected 1-2, received {len(argv)-1}"
        )

    directory = sys.argv[1]
    username = sys.argv[2] if len(sys.argv) >= 3 else None

    delete_database_credentials(directory, username)
