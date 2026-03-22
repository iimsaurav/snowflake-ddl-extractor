# snowflake-ddl-extractor

A Python CLI tool that connects to a Snowflake database, extracts DDL scripts for all objects using `GET_DDL()`, and saves them into a structured local directory tree mirroring the database hierarchy.

## What It Does

- Connects to any Snowflake database with four authentication methods
- Extracts DDL for tables, views, procedures, functions, tasks, streams, stages, file formats, sequences, and pipes
- Writes each DDL to a `.sql` file organized by schema and object type
- Schema-qualifies all object names in the DDL (e.g., `CREATE TABLE SCHEMA.MY_TABLE`)
- Strips the database name from all references inside scripts so DDLs are portable
- Handles overloaded procedures/functions (same name, different signatures) without data loss
- Runs idempotent refreshes: overwrites existing files, removes stale ones, prunes empty directories
- Extracts concurrently using a thread pool for speed

## Installation

```bash
pip install snowflake-ddl-extractor
```

Or install from source:

```bash
git clone https://github.com/imsaurav/snowflake-ddl-extractor.git
cd snowflake-ddl-extractor
pip install -e .
```

## Quick Start

### Password Authentication

```bash
export SNOWFLAKE_PASSWORD='my-secret'
snow-extract --account myaccount --user myuser --db MY_DATABASE --out-dir ./ddl_output
```

### SSO (Browser) Authentication

```bash
snow-extract --account myaccount --user myuser --db MY_DATABASE --auth sso --out-dir ./ddl_output
```

### Key-Pair Authentication

```bash
export SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/snowflake_rsa_key.p8
snow-extract --account myaccount --user myuser --db MY_DATABASE --auth keypair --out-dir ./ddl_output
```

### OAuth Authentication

```bash
export SNOWFLAKE_OAUTH_TOKEN='eyJhbGci...'
snow-extract --account myaccount --user myuser --db MY_DATABASE --auth oauth --out-dir ./ddl_output
```

You can also run it as a Python module:

```bash
python -m snow_ddl_extractor --account myaccount --user myuser --db MY_DATABASE --auth sso
```

## CLI Reference

```
snow-extract [OPTIONS]

Options:
  -a, --account TEXT              Snowflake account identifier  [required]
  -u, --user TEXT                 Snowflake login user  [required]
      --database, --db TEXT       Target database  [required]
  -o, --out-dir DIRECTORY         Root output directory  [default: .]
      --auth [password|keypair|sso|oauth]
                                  Authentication method  [default: password]
      --password TEXT             Snowflake password
      --private-key-path TEXT     Path to PEM private key file
      --private-key-passphrase TEXT
                                  Passphrase for the private key
      --oauth-token TEXT          OAuth access token
      --role TEXT                 Snowflake role
      --warehouse TEXT            Snowflake warehouse
      --include-schemas TEXT      Comma-separated schemas to include
      --exclude-schemas TEXT      Comma-separated schemas to exclude
      --include-types TEXT        Comma-separated object types to include
      --exclude-types TEXT        Comma-separated object types to exclude
  -w, --workers INTEGER           Concurrent threads  [default: 10]
      --dry-run                   Preview what would be extracted, no files written
  -v, --verbose                   Enable debug logging
      --version                   Show version and exit
  -h, --help                      Show this message and exit
```

## Output Structure

```
{out_dir}/{DATABASE}/
├── {SCHEMA_A}/
│   ├── TABLES/
│   │   ├── CUSTOMERS.sql
│   │   └── ORDERS.sql
│   ├── VIEWS/
│   │   └── V_DAILY_SALES.sql
│   ├── PROCEDURES/
│   │   ├── LOAD_DATA.sql
│   │   └── LOAD_DATA(VARCHAR,NUMBER).sql    <- overloaded variant
│   ├── FUNCTIONS/
│   ├── TASKS/
│   ├── STREAMS/
│   ├── STAGES/
│   ├── FILE_FORMATS/
│   ├── SEQUENCES/
│   └── PIPES/
└── {SCHEMA_B}/
    └── ...
```

All folder names are uppercase. Filenames match the Snowflake object name. Overloaded procedures/functions include the argument signature in the filename to avoid collisions.

## DDL Processing

Each extracted DDL goes through two transformations before being written:

1. **Database prefix removal** -- All occurrences of the database name are stripped from the script. For example, `DEV_DB.SCHEMA.MY_TABLE` becomes `SCHEMA.MY_TABLE`. This makes the DDLs portable across environments (dev/uat/prod) without find-and-replace.

2. **Schema qualification** -- The object name in the `CREATE` statement is prefixed with its schema name. For example, `CREATE TABLE MY_TABLE` becomes `CREATE TABLE SCHEMA_A.MY_TABLE`. This ensures every script is self-contained and can be executed without setting a schema context first.

## Filtering

### By Schema

Extract only specific schemas:

```bash
snow-extract --account myacc --user myuser --db MY_DB --auth sso \
  --include-schemas ANALYTICS,STAGING
```

Exclude schemas:

```bash
snow-extract --account myacc --user myuser --db MY_DB --auth sso \
  --exclude-schemas PUBLIC
```

`INFORMATION_SCHEMA` is always excluded automatically.

### By Object Type

Extract only tables and views:

```bash
snow-extract --account myacc --user myuser --db MY_DB --auth sso \
  --include-types TABLES,VIEWS
```

Extract everything except stages and pipes:

```bash
snow-extract --account myacc --user myuser --db MY_DB --auth sso \
  --exclude-types STAGES,PIPES
```

Valid type names: `TABLES`, `VIEWS`, `PROCEDURES`, `FUNCTIONS`, `TASKS`, `STREAMS`, `STAGES`, `FILE_FORMATS`, `SEQUENCES`, `PIPES` (case-insensitive).

## Dry Run

Preview what would be extracted without writing any files:

```bash
snow-extract --account myacc --user myuser --db MY_DB --auth sso --dry-run
```

This discovers all objects, prints a summary table, and exits.

## Progress Bar and Summary Table

During extraction, a progress bar shows real-time status:

```
Extracting DDLs  [################----]  80%
```

After extraction, a summary table breaks down counts by schema and object type:

```
  Schema              TABLES  VIEWS  PROCEDURES  total
  ----------------------------------------------------
  ANALYTICS               10      5           3     18
  STAGING                  8      2           1     11
  ----------------------------------------------------
  TOTAL                   18      7           4     29
```

## Idempotent Refresh

On subsequent runs the tool:

1. **Overwrites** existing `.sql` files with the latest DDL
2. **Removes** `.sql` files for objects that no longer exist in Snowflake
3. **Prunes** empty directories automatically

This keeps the local directory tree an exact mirror of the database.

## Environment Variables

All connection options can be set via environment variables to avoid passing secrets on the command line:

| Variable | Corresponding Option |
|---|---|
| `SNOWFLAKE_ACCOUNT` | `--account` |
| `SNOWFLAKE_USER` | `--user` |
| `SNOWFLAKE_DATABASE` | `--database` |
| `SNOWFLAKE_PASSWORD` | `--password` |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | `--private-key-path` |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | `--private-key-passphrase` |
| `SNOWFLAKE_OAUTH_TOKEN` | `--oauth-token` |
| `SNOWFLAKE_ROLE` | `--role` |
| `SNOWFLAKE_WAREHOUSE` | `--warehouse` |

## Supported Object Types

| Type | Directory |
|---|---|
| Table | `TABLES/` |
| View | `VIEWS/` |
| Procedure | `PROCEDURES/` |
| Function | `FUNCTIONS/` |
| Task | `TASKS/` |
| Stream | `STREAMS/` |
| Stage | `STAGES/` |
| File Format | `FILE_FORMATS/` |
| Sequence | `SEQUENCES/` |
| Pipe | `PIPES/` |

## Dependencies

- `snowflake-connector-python` >= 3.6.0
- `click` >= 8.0
- `cryptography` >= 41.0

## License

MIT
