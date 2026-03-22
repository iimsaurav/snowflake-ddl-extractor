# snow-ddl-extractor — Project Requirements

## Context & Role

Expert Python Developer and Data Engineer project. A complete, production-ready Python package published on PyPI.

## Objective

A Python package (`snow-ddl-extractor`) that connects to a specific database within a Snowflake instance, extracts the DDL (Data Definition Language) scripts for all objects, and saves them locally into a highly structured, hierarchical file directory.

## Supported Object Types

The following Snowflake object types are extracted via `GET_DDL()`:

| Object Type  | SHOW Command        | Directory Name   |
|--------------|---------------------|------------------|
| TABLE        | SHOW TABLES         | `tables/`        |
| VIEW         | SHOW VIEWS          | `views/`         |
| PROCEDURE    | SHOW PROCEDURES     | `procedures/`    |
| FUNCTION     | SHOW USER FUNCTIONS | `functions/`     |
| TASK         | SHOW TASKS          | `tasks/`         |
| STREAM       | SHOW STREAMS        | `streams/`       |
| STAGE        | SHOW STAGES         | `stages/`        |
| FILE FORMAT  | SHOW FILE FORMATS   | `file_formats/`  |
| SEQUENCE     | SHOW SEQUENCES      | `sequences/`     |
| PIPE         | SHOW PIPES          | `pipes/`         |

## Core Functionality

### Connection

Securely authenticate and connect to a Snowflake instance targeting a specific database. Four authentication methods are supported:

- **Password** — standard user/password (`--auth password`)
- **SSO** — browser-based Single Sign-On via `externalbrowser` authenticator (`--auth sso`). Uses a single shared connection to avoid multiple browser popups across worker threads.
- **Key-Pair** — PEM private key authentication for CI/CD pipelines (`--auth keypair`). Supports optional passphrase for encrypted keys.
- **OAuth** — OAuth access token (`--auth oauth`)

All credentials can be read from environment variables (e.g., `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, etc.) to avoid hardcoding.

### Extraction

- Iterates through all schemas in the target database using `SHOW SCHEMAS IN DATABASE`.
- For each schema, discovers objects via `SHOW <type> IN SCHEMA`.
- Uses Snowflake's `GET_DDL()` function to extract creation scripts for each object.
- **Overloaded procedures/functions**: Detects duplicate names (same name, different argument signatures) and uses the full signature (e.g., `MY_PROC(VARCHAR, NUMBER)`) as the filename to avoid silent data loss from filename collisions.

### Directory Generation

Builds a local directory structure mirroring the database hierarchy:

```
{out_dir}/
└── {DATABASE_NAME}/
    ├── {SCHEMA_1}/
    │   ├── tables/
    │   │   ├── TABLE_A.sql
    │   │   └── TABLE_B.sql
    │   ├── views/
    │   │   └── VIEW_A.sql
    │   ├── procedures/
    │   │   ├── MY_PROC.sql
    │   │   └── MY_PROC(VARCHAR).sql   ← overloaded variant
    │   └── functions/
    │       └── MY_FUNC.sql
    └── {SCHEMA_2}/
        ├── tables/
        └── views/
```

- Object names are preserved as-is from Snowflake (uppercase).
- Filenames are cross-platform sanitized (illegal characters replaced, Windows reserved names handled, control characters stripped).

### Idempotent Refresh (Overwrite Logic)

On subsequent runs, the tool refreshes the local repository to exactly match the current state of the database:

1. **Overwrite** existing `.sql` files with the latest DDL.
2. **Remove** stale `.sql` files for objects that have been dropped in Snowflake.
3. **Prune** empty directories after stale file removal.

## Implemented Features

### CLI (Click)

Full CLI via the `click` library, invokable as:

```bash
snow-extract --account myacc --user myuser --db DEMO_DB --out-dir ./dwh_code
# or
python -m snow_ddl_extractor --account myacc --user myuser --db DEMO_DB
```

**Options:**

| Flag                        | Env Var                          | Description                                              |
|-----------------------------|----------------------------------|----------------------------------------------------------|
| `-a, --account`             | `SNOWFLAKE_ACCOUNT`              | Snowflake account identifier (required)                  |
| `-u, --user`                | `SNOWFLAKE_USER`                 | Snowflake login user (required)                          |
| `--database, --db`          | `SNOWFLAKE_DATABASE`             | Target database (required)                               |
| `-o, --out-dir`             |                                  | Root output directory (default: `.`)                     |
| `--auth`                    |                                  | Auth method: `password`, `keypair`, `sso`, `oauth`       |
| `--password`                | `SNOWFLAKE_PASSWORD`             | Snowflake password                                       |
| `--private-key-path`        | `SNOWFLAKE_PRIVATE_KEY_PATH`     | Path to PEM private key file                             |
| `--private-key-passphrase`  | `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Passphrase for encrypted private key                   |
| `--oauth-token`             | `SNOWFLAKE_OAUTH_TOKEN`          | OAuth access token                                       |
| `--role`                    | `SNOWFLAKE_ROLE`                 | Snowflake role                                           |
| `--warehouse`               | `SNOWFLAKE_WAREHOUSE`            | Snowflake warehouse                                      |
| `--include-schemas`         |                                  | Comma-separated allow-list of schemas                    |
| `--exclude-schemas`         |                                  | Comma-separated deny-list of schemas                     |
| `--include-types`           |                                  | Comma-separated object types to include (e.g. `tables,views`) |
| `--exclude-types`           |                                  | Comma-separated object types to exclude (e.g. `stages,pipes`) |
| `-w, --workers`             |                                  | Thread-pool size (default: 10)                           |
| `--dry-run`                 |                                  | Preview discovery without writing files                  |
| `-v, --verbose`             |                                  | Enable debug logging                                     |
| `--version`                 |                                  | Show version and exit                                    |

### Concurrency

Uses `concurrent.futures.ThreadPoolExecutor` (default 10 workers) to fetch multiple DDLs simultaneously. All workers share a single authenticated connection, each creating its own cursor, which avoids multiple SSO browser popups and is safe for read-only queries.

### Schema Filtering

- `--include-schemas`: Only extract from the listed schemas.
- `--exclude-schemas`: Skip the listed schemas.
- `INFORMATION_SCHEMA` is always excluded automatically.

### Object Type Filtering

- `--include-types`: Only extract the listed object types (uses directory names: `tables`, `views`, `procedures`, `functions`, `tasks`, `streams`, `stages`, `file_formats`, `sequences`, `pipes`).
- `--exclude-types`: Skip the listed object types.

### Progress Bar

Real-time progress bar via `click.progressbar` during DDL extraction. Discovery phase runs first to determine total count, then extraction runs with accurate progress tracking.

### Summary Table

After extraction, a breakdown table is printed showing object counts by schema and type, with column and row totals:

```
  Schema              tables  views  procedures  total
  ----------------------------------------------------
  SCHEMA_A                10      5           3     18
  SCHEMA_B                 8      2           1     11
  ----------------------------------------------------
  TOTAL                   18      7           4     29
```

### Dry-Run Mode

`--dry-run` discovers all objects and prints the summary table without writing any files. Useful for previewing what would be extracted.

### Robust Logging

Python's standard `logging` module outputs progress, warnings, and errors to stderr. `--verbose` enables debug-level logging.

### Filename Sanitization

- Illegal characters (`<>:"/\|?*`) replaced with underscores.
- Leading/trailing dots and spaces stripped.
- Windows reserved names (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`) get an underscore suffix.
- Control characters (0x00–0x1F) replaced.
- Empty names default to `_unnamed`.
- Parentheses are preserved (needed for overloaded procedure/function signatures).

## Package Structure

```
snowflake-ddl-extractor/
├── pyproject.toml                          # Build config, dependencies, CLI entry point
├── requirements.txt                        # Pinned dependencies
├── README.md                               # Installation, usage, auth examples
├── project-requirement.md                  # This file
├── src/
│   └── snow_ddl_extractor/
│       ├── __init__.py                     # Package version (__version__)
│       ├── __main__.py                     # python -m support
│       ├── cli.py                          # Click CLI, progress bar, summary table, dry-run
│       ├── connector.py                    # Multi-auth connection factory
│       ├── extractor.py                    # Schema/object discovery, concurrent DDL extraction
│       └── writer.py                       # File I/O, sanitization, idempotent cleanup
├── tests/
│   └── test_sanitize.py                    # Unit tests for filename sanitization
└── ddl_output/                             # Default extraction output (gitignored)
```

## Dependencies

| Package                      | Version   | Purpose                              |
|------------------------------|-----------|--------------------------------------|
| `snowflake-connector-python` | >= 3.6.0  | Snowflake connectivity               |
| `click`                      | >= 8.0    | CLI framework                        |
| `cryptography`               | >= 41.0   | PEM private key loading (key-pair)   |

## Code Quality

- PEP 8 compliant.
- Full type hints (`->`, `Optional`, `List`, `Dict`, `Set`, `Tuple`, `Callable`) on all functions.
- Docstrings on all classes and major functions.
- `try/except` error handling around network calls, DDL extraction, and file operations.
- `dataclass` used for `ExtractedObject` data transfer.
- 10 unit tests for filename sanitization covering edge cases (illegal chars, reserved names, control chars, empty strings, parentheses for overloads).
