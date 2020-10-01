# Database generation

These scripts allow Janis to generate _one-time_ database credentials for a MySQL database.


## Overview 


## Usage

It's usage is automatic through configuring Janis, but you should copy these scripts to your environment, change the relevant options and credentials and then configure Janis to interact with these scripts.

These scripts are called like the following (so the shebang is important):

```
./generatedatabasecredentials.py <output-dir>
./deletedatabasecredentials.py <output-dir>
```


### Configuring Janis

_This section will be updated in Janis `0.11.x`_.

## Overview

### Generate credentials

The generate credentials script should return credentials in JSON format to STDOUT with keys: 
`username`, `password`, `database`, `host`. This should be the only contents of STDOUT. Janis will
pipe the stderr to it's own stderr.

Please see the [requirements](#requirements) for more information.

 


## Requirements

- Database
    - Each directory should get it's own database
    - Running on the same directory should use the same "database",
        * UNLESS, the directory has been deleted, then create a new database
    - Delete all credentials associated with the directory, we can find this by looking up the janis.TENDER tables
    - Even though we store the user details in the janis.TENDER_USR table, we'll look these up through:
        * 'SELECT user FROM mysql.db WHERE Db IN ({DBSTODELETE}) AND user LIKE 'janis_%;'
        
- Credentials
    - Each run should produce a new set of credentials, regardless if it's the same directory.

- Other
    - We should store generated usernames, databases, directories in a "TENDER" and "TENDER_USR" tables
        against the user who created them.
    - A paired script 'deletedatabasecredentials.py' should be called with a DIRECTORY to
        DROP the table, and delete the credentials
    - Remove the cache file in the <output-dir> when deleting

- Setup:
    - You must ensure the credentials are correct within this script
    - The database in the variable TENDER_DB (default: "janis") is created
    - Install the requirements in 'requirements.txt' (primarily, 'pymsql')