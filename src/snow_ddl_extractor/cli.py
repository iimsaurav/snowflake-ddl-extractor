"""Command-line interface for snow-ddl-extractor."""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import click

from snow_ddl_extractor import __version__
from snow_ddl_extractor.connector import create_connection
from snow_ddl_extractor.extractor import (
    OBJECT_TYPES,
    TYPE_DIR_MAP,
    ExtractedObject,
    discover_objects,
    discover_schemas,
    extract_all,
)
from snow_ddl_extractor.writer import cleanup_stale_files, write_ddl_files

logger = logging.getLogger("snow_ddl_extractor")

# Valid type names users can pass to --include-types / --exclude-types.
VALID_TYPE_NAMES: Set[str] = {v for v in TYPE_DIR_MAP.values()}


def _configure_logging(verbose: bool) -> None:
    """Set up the root ``snow_ddl_extractor`` logger."""
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    )
    logger.setLevel(level)
    logger.addHandler(handler)


def _print_summary(results: List[ExtractedObject]) -> None:
    """Print a breakdown table of extracted objects by schema and type."""
    # schema -> type -> count
    grid: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for obj in results:
        type_dir = TYPE_DIR_MAP.get(obj.object_type, obj.object_type.lower())
        grid[obj.schema_name][type_dir] += 1

    if not grid:
        return

    # Collect all type columns that have at least one entry.
    all_types = sorted({t for counts in grid.values() for t in counts})
    schemas = sorted(grid.keys())

    # Column widths.
    schema_w = max(len("Schema"), *(len(s) for s in schemas))
    col_w = max(7, *(len(t) for t in all_types))

    header = f"  {'Schema':<{schema_w}}"
    for t in all_types:
        header += f"  {t:>{col_w}}"
    header += f"  {'total':>{col_w}}"
    click.echo()
    click.echo(header)
    click.echo("  " + "-" * (len(header) - 2))

    grand_total = 0
    for schema in schemas:
        row = f"  {schema:<{schema_w}}"
        row_total = 0
        for t in all_types:
            c = grid[schema].get(t, 0)
            row_total += c
            row += f"  {c:>{col_w}}" if c else f"  {'':>{col_w}}"
        row += f"  {row_total:>{col_w}}"
        grand_total += row_total
        click.echo(row)

    click.echo("  " + "-" * (len(header) - 2))
    totals_row = f"  {'TOTAL':<{schema_w}}"
    for t in all_types:
        col_sum = sum(grid[s].get(t, 0) for s in schemas)
        totals_row += f"  {col_sum:>{col_w}}"
    totals_row += f"  {grand_total:>{col_w}}"
    click.echo(totals_row)
    click.echo()


def _print_dry_run_summary(work_items: List[Tuple[str, str, str, str]]) -> None:
    """Print a breakdown of what *would* be extracted in dry-run mode."""
    # schema -> type -> count
    grid: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for ddl_type, _obj_name, _fq_id, schema in work_items:
        type_dir = TYPE_DIR_MAP.get(ddl_type, ddl_type.lower())
        grid[schema][type_dir] += 1

    if not grid:
        return

    all_types = sorted({t for counts in grid.values() for t in counts})
    schemas = sorted(grid.keys())
    schema_w = max(len("Schema"), *(len(s) for s in schemas))
    col_w = max(7, *(len(t) for t in all_types))

    header = f"  {'Schema':<{schema_w}}"
    for t in all_types:
        header += f"  {t:>{col_w}}"
    header += f"  {'total':>{col_w}}"
    click.echo()
    click.echo(header)
    click.echo("  " + "-" * (len(header) - 2))

    grand_total = 0
    for schema in schemas:
        row = f"  {schema:<{schema_w}}"
        row_total = 0
        for t in all_types:
            c = grid[schema].get(t, 0)
            row_total += c
            row += f"  {c:>{col_w}}" if c else f"  {'':>{col_w}}"
        row += f"  {row_total:>{col_w}}"
        grand_total += row_total
        click.echo(row)

    click.echo("  " + "-" * (len(header) - 2))
    totals_row = f"  {'TOTAL':<{schema_w}}"
    for t in all_types:
        col_sum = sum(grid[s].get(t, 0) for s in schemas)
        totals_row += f"  {col_sum:>{col_w}}"
    totals_row += f"  {grand_total:>{col_w}}"
    click.echo(totals_row)
    click.echo()


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--account", "-a",
    envvar="SNOWFLAKE_ACCOUNT",
    required=True,
    help="Snowflake account identifier (or SNOWFLAKE_ACCOUNT env var).",
)
@click.option(
    "--user", "-u",
    envvar="SNOWFLAKE_USER",
    required=True,
    help="Snowflake login user (or SNOWFLAKE_USER env var).",
)
@click.option(
    "--database", "--db",
    envvar="SNOWFLAKE_DATABASE",
    required=True,
    help="Target database to extract DDLs from.",
)
@click.option(
    "--out-dir", "-o",
    default=".",
    show_default=True,
    type=click.Path(file_okay=False),
    help="Root directory where the DDL tree will be written.",
)
@click.option(
    "--auth",
    type=click.Choice(["password", "keypair", "sso", "oauth"], case_sensitive=False),
    default="password",
    show_default=True,
    help="Authentication method.",
)
@click.option("--password", envvar="SNOWFLAKE_PASSWORD", default=None, help="Snowflake password.")
@click.option(
    "--private-key-path",
    envvar="SNOWFLAKE_PRIVATE_KEY_PATH",
    default=None,
    help="Path to PEM private key file (key-pair auth).",
)
@click.option(
    "--private-key-passphrase",
    envvar="SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    default=None,
    help="Passphrase for the private key.",
)
@click.option(
    "--oauth-token",
    envvar="SNOWFLAKE_OAUTH_TOKEN",
    default=None,
    help="OAuth access token.",
)
@click.option("--role", envvar="SNOWFLAKE_ROLE", default=None, help="Snowflake role to use.")
@click.option("--warehouse", envvar="SNOWFLAKE_WAREHOUSE", default=None, help="Snowflake warehouse.")
@click.option(
    "--include-schemas",
    default=None,
    help="Comma-separated list of schemas to include (others are skipped).",
)
@click.option(
    "--exclude-schemas",
    default=None,
    help="Comma-separated list of schemas to exclude.",
)
@click.option(
    "--include-types",
    default=None,
    help="Comma-separated object types to include (e.g. tables,views,procedures).",
)
@click.option(
    "--exclude-types",
    default=None,
    help="Comma-separated object types to exclude (e.g. stages,pipes).",
)
@click.option(
    "--workers", "-w",
    default=10,
    show_default=True,
    type=int,
    help="Number of concurrent threads for DDL extraction.",
)
@click.option("--dry-run", is_flag=True, help="Preview what would be extracted without writing files.")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.version_option(version=__version__, prog_name="snow-extract")
def main(
    account: str,
    user: str,
    database: str,
    out_dir: str,
    auth: str,
    password: Optional[str],
    private_key_path: Optional[str],
    private_key_passphrase: Optional[str],
    oauth_token: Optional[str],
    role: Optional[str],
    warehouse: Optional[str],
    include_schemas: Optional[str],
    exclude_schemas: Optional[str],
    include_types: Optional[str],
    exclude_types: Optional[str],
    workers: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Extract Snowflake DDLs into a structured local directory."""
    _configure_logging(verbose)

    include = (
        {s.strip() for s in include_schemas.split(",")} if include_schemas else None
    )
    exclude = (
        {s.strip() for s in exclude_schemas.split(",")} if exclude_schemas else None
    )

    # Parse type filters — map directory names back to GET_DDL type keywords.
    dir_to_type = {v.upper(): k for k, v in TYPE_DIR_MAP.items()}
    type_include: Optional[Set[str]] = None
    type_exclude: Optional[Set[str]] = None
    if include_types:
        type_include = set()
        for t in include_types.split(","):
            t = t.strip().upper()
            if t not in dir_to_type:
                raise click.BadParameter(
                    f"Unknown type {t!r}. Valid: {sorted(v.lower() for v in VALID_TYPE_NAMES)}",
                    param_hint="--include-types",
                )
            type_include.add(dir_to_type[t])
    if exclude_types:
        type_exclude = set()
        for t in exclude_types.split(","):
            t = t.strip().upper()
            if t not in dir_to_type:
                raise click.BadParameter(
                    f"Unknown type {t!r}. Valid: {sorted(v.lower() for v in VALID_TYPE_NAMES)}",
                    param_hint="--exclude-types",
                )
            type_exclude.add(dir_to_type[t])

    try:
        conn = create_connection(
            account=account,
            user=user,
            database=database,
            auth=auth,
            password=password,
            private_key_path=private_key_path,
            private_key_passphrase=private_key_passphrase,
            oauth_token=oauth_token,
            role=role,
            warehouse=warehouse,
        )
    except Exception as exc:
        logger.error("Connection failed: %s", exc)
        raise SystemExit(1) from exc

    try:
        # Phase 1 — discover objects so we know the total for the progress bar.
        click.echo(f"Discovering objects in {database} …")
        schemas = discover_schemas(conn, database, include, exclude)
        work_items: list = []
        for schema in schemas:
            objects = discover_objects(conn, database, schema)
            for ddl_type, obj_name, fq_id in objects:
                if type_include and ddl_type not in type_include:
                    continue
                if type_exclude and ddl_type in type_exclude:
                    continue
                work_items.append((ddl_type, obj_name, fq_id, schema))

        total = len(work_items)
        click.echo(f"Found {total} object(s) across {len(schemas)} schema(s).")

        if dry_run:
            _print_dry_run_summary(work_items)
            click.echo(f"Dry run — {total} DDL(s) would be extracted. No files changed.")
            return

        # Phase 2 — extract DDLs with a progress bar.
        with click.progressbar(length=total, label="Extracting DDLs") as bar:
            results = extract_all(
                conn=conn,
                database=database,
                max_workers=workers,
                progress_callback=lambda n: bar.update(n),
                _work_items=work_items,
            )

        _print_summary(results)

        written = write_ddl_files(results, out_dir, database)
        stale = cleanup_stale_files(out_dir, database, written)
        click.echo(
            f"Done — extracted {len(results)} DDL(s), "
            f"wrote {len(written)} file(s), removed {stale} stale file(s)."
        )
    finally:
        conn.close()
