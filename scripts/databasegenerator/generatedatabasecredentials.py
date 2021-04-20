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
This script should be called with one argument, a <directory> for new database credentials to be stored against.

We consider the following requirements:

DATABASE:
    - Each directory should get it's own database
    - Running on the same directory should use the same "database",
        * UNLESS, the directory has been deleted, then create a new database

CREDENTIALS:
    - Each run should produce a new set of credentials, regardless if it's the same directory.

OTHER:
    - We should store generated usernames, databases, directories in a "TENDER" and "TENDER_USR" tables
        against the user who created them.
    - A paired script 'deletedatabasecredentials.py' should be called with a DIRECTORY to
        DROP the table, and delete the credentials

SETUP
    - You must ensure the credentials are correct within this script
    - The database in the variable TENDER_DB (default: "janis") is created
    - Install the requirements in 'requirements.txt' (primarily, 'pymsql')
"""

import os, sys, json, logging
from typing import Set
from getpass import getuser
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

# call 'FLUSH privileges;' if True.
FLUSH_PRIVILEGES = False

# Auth on '%' unless the following is True
#   This is because "localhost" is treated separately by mysql, if we want to use 'localhost', you'd
#   likely want to set "GIVE_CREDENTIALS_TO_LOCALHOST" to True, otherwise we'll use the '%' host.
GIVE_CREDENTIALS_TO_LOCALHOST = False


# setup environment

CREDENTIALS_HOST = "localhost" if GIVE_CREDENTIALS_TO_LOCALHOST else "%"
logging.basicConfig(level=logging.DEBUG)

# Sanity check provided values
if not MYSQL_HOST:
    raise Exception("Internal error: MYSQL_HOST requires a value")
if not MYSQL_USER:
    raise Exception("Internal error: MYSQL_USER requires a value")


def excepthook(exception_type, exception, traceback):
    prefix = ""
    if exception_type.__name__ != "Exception":
        prefix = f"{exception_type.__name__}: "
    print(f"{prefix}{exception}", file=sys.stderr)
    sys.exit(1)


sys.excepthook = excepthook


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


def create_tender_dbs(cursor: pymysql.cursors.Cursor):

    SQL_TENDER = f"""\
CREATE TABLE IF NOT EXISTS TENDER(
    databasename VARCHAR(64) PRIMARY KEY,
    timestamp DATETIME,
    directory VARCHAR(255),
    user_creator VARCHAR(64)
);"""
    SQL_TENDER_USR = f"""\
CREATE TABLE IF NOT EXISTS TENDER_USR(
    username VARCHAR(64) PRIMARY KEY,
    databasename VARCHAR(64),
    timestamp DATETIME,
    user_creator VARCHAR(64)
);"""
    try:
        logging.debug("Creating table TENDER")
        cursor.execute(SQL_TENDER)
        logging.debug("Creating table TENDER_USR")
        cursor.execute(SQL_TENDER_USR)
        logging.debug("Created TENDER tables")
    except Exception as e:
        logging.critical("Couldn't create tender tables: " + repr(e))
        sys.exit(1)


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

    directory = os.path.abspath(os.path.expanduser(os.path.expandvars(directory)))

    # mfranklin: this is probably a terrible way to do this
    if directory[-1] != "/":
        directory += "/"

    return directory


def get_dbname_for_directory(cursor: pymysql.cursors.Cursor, directory: str):
    """
    We'll check to see if the databasename is in the folder, else we'll generate a new one
    The theory being, if the directory is deleted, and a new task is run, we'll
    """
    dbname_path = os.path.join(directory, "generated_mysqldb.txt")
    if os.path.exists(dbname_path):
        # read from here
        with open(dbname_path) as f:
            dbname = f.readline()
            # if dbname is valid
            if len(dbname) < 31:
                return dbname

    # generate new dbname
    _SQL = "SELECT databasename FROM TENDER;"
    cursor.execute(_SQL)
    rows = cursor.fetchall()

    forbiddenids = set(r["databasename"] for r in rows)
    iter = None
    pref = MYSQL_DB_PREFIX + generate_hash_from_string(directory, length=HASH_LENGTH)
    new_id = pref
    while new_id in forbiddenids:
        iter = (iter + 1) if iter else 1
        new_id = pref + "_" + str(iter)

    # write the file back
    with open(dbname_path, "w+") as f:
        f.write(new_id)

    return new_id


def generate_database_credentials(directory: str):
    logging.info(f"Creating database credentials for directory '{directory}'")
    ensure_has_access_to_(directory)
    directory = prepare_directory_for_hash(directory)

    # database things
    con = get_connection()
    cursor: pymysql.cursors.Cursor = con.cursor()

    create_tender_dbs(cursor)

    # full, complete path (consistent trailing slash)
    databasename = get_dbname_for_directory(cursor, directory)

    username = generate_username(get_existing_users(cursor))
    password = generate_password()

    create_table(cursor, databasename)
    create_user(cursor, databasename, username=username, password=password)
    grant_privileges_to_table(cursor, databasename, username)

    logging.debug("Writing credentials to common tender")
    add_to_tender(cursor, directory, databasename, username)
    con.commit()
    cursor.close()

    logging.info(f"Succesfully created credentials for {username}@{databasename}")
    return databasename, username, password


def add_to_tender(
    cursor: pymysql.cursors.Cursor, directory: str, databasename: str, username: str
):
    from datetime import datetime

    adding_user = getuser()

    logging.info("Writing to credentials to tender")

    cursor.execute(
        """\
INSERT IGNORE INTO `TENDER` 
    (`databasename`, `timestamp`, `directory`, `user_creator`) 
VALUES 
    (%s, %s, %s, %s);
    """,
        (databasename, datetime.now(), directory, adding_user),
    )

    cursor.execute(
        """\
INSERT IGNORE INTO `TENDER_USR`
    (`username`, `databasename`, `timestamp`, `user_creator`)
VALUES 
    (%s, %s, %s, %s)
""",
        (username, databasename, datetime.now(), adding_user),
    )


def generate_password(length=12):
    import string, random

    password_characters = string.ascii_letters + string.digits + "-!@#$&*"
    return "".join(random.choice(password_characters) for _ in range(length))


def get_existing_users(cursor):
    SQL = "SELECT user FROM mysql.user;"
    cursor.execute(SQL)

    rows = [r["user"].lower() for r in cursor.fetchall()]
    return set(rows)


def generate_username(forbidden_usernames: Set[str], length=31):
    import string, random

    def generate_str_of_length(l):
        password_characters = string.ascii_lowercase
        return "".join(random.choice(password_characters) for _ in range(l))

    userpref = MYSQL_DB_PREFIX + getuser()
    if len(userpref) > (length - 5):
        userpref = userpref[: (5 - length)]

    random_length = min(length - len(userpref), 8)

    username = userpref + generate_str_of_length(random_length)
    iters = 0
    while username in forbidden_usernames:
        username = userpref + generate_str_of_length(random_length)
        if iters > 100:
            raise Exception("Couldn't find a valid username after 100 attempts")
        iters += 1

    return username


def ensure_has_access_to_(directory: str):
    from pathlib import Path

    try:
        file = os.path.join(directory, "janis_db_testfile")
        p = Path(file)
        p.touch()
        os.remove(file)
    except Exception as e:
        raise Exception(
            f"DB creation helper couldn't access a file in your directory '{file}', and hence can't generate DB credentials. "
            f"Please ensure you have write access to this directory. {repr(e)}"
        )


def generate_hash_from_string(value, length):
    import hashlib

    logging.debug(f"Generating hash of length {length} for '{value}'")
    hsh = hashlib.sha3_224(str(value).encode("UTF-8")).hexdigest()
    return hsh[: min(len(hsh), length) - 1]


def create_table(cursor, databasename):
    logging.debug(f"Creating database {databasename} for host '{MYSQL_HOST}'")
    SQL = f"CREATE DATABASE IF NOT EXISTS {databasename};"

    try:
        cursor.execute(SQL)
    except Exception as e:
        logging.critical("Couldn't create database: " + repr(e))
        raise


def create_user(cursor, databasename, username, password) -> bool:

    logging.debug(
        f"Creating user '{databasename}'@'{CREDENTIALS_HOST}' with a generated password"
    )

    SQL = f"CREATE USER '{username}'@'{CREDENTIALS_HOST}' IDENTIFIED BY '{password}';"

    try:
        cursor.execute(SQL)
    except Exception as e:
        logging.critical("Couldn't create user: " + repr(e))
        raise

    return True


def grant_privileges_to_table(cursor, databasename, username):

    logging.debug(f"Granting {username} privileges on {databasename}")
    SQL = f"GRANT CREATE, DROP, DELETE, INSERT, SELECT, UPDATE, ALTER, INDEX ON {databasename}.* TO '{username}'@'{CREDENTIALS_HOST}';"

    try:
        cursor.execute(SQL)
        if FLUSH_PRIVILEGES:
            cursor.execute("FLUSH privileges;")
    except Exception as e:
        logging.critical(
            f"Couldn't grant privileges for '{username}' to '{databasename}': "
            + repr(e)
        )
        raise


if __name__ == "__main__":
    argv = sys.argv
    if len(argv) != 2:
        raise Exception(
            f"Incorrect number of arguments, expected 1 (<directory>), received {len(argv)-1}"
        )

    databasename, username, password = generate_database_credentials(sys.argv[1])
    logging.debug("Writing credentials as JSON to stdout")

    print(
        json.dumps(
            {
                "username": username,
                "password": password,
                "database": databasename,
                "host": MYSQL_HOST + ":" + str(MYSQL_PORT),
            }
        )
    )
