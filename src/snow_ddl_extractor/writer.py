"""File I/O: directory creation, DDL writing, filename sanitization, and stale-file cleanup."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import List, Set

from snow_ddl_extractor.extractor import ExtractedObject, TYPE_DIR_MAP

logger = logging.getLogger(__name__)

# Characters illegal in file names on Windows (and generally problematic).
_ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
# Reserved device names on Windows.
_RESERVED_NAMES = frozenset(
    {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
)

# Matches the CREATE [OR REPLACE] <type-keyword(s)> <object-name> portion of a DDL.
# The object name may be bare (TABLE FOO) or double-quoted (PROCEDURE "FOO").
# We capture everything up to and including the type keyword(s) as group 1,
# and the object name (with optional quotes) as group 2.
_CREATE_RE = re.compile(
    r"(?i)"
    r"(create\s+(?:or\s+replace\s+)?(?:(?:temporary|transient|secure|recursive|materialized)\s+)*"
    r"(?:table|view|procedure|function|task|stream|stage|file\s+format|sequence|pipe)\s+)"
    r"(\"[^\"]+\"|[^\s(]+)",
)


def sanitize_filename(name: str) -> str:
    """Return a cross-platform–safe filename derived from *name*.

    Replaces illegal characters with underscores, handles Windows-reserved
    device names, and ensures the result is never empty.

    Args:
        name: Raw object name from Snowflake.

    Returns:
        Sanitized string safe for use as a filename (without extension).
    """
    sanitized = _ILLEGAL_CHARS.sub("_", name)
    # Strip leading/trailing spaces and dots (problematic on Windows).
    sanitized = sanitized.strip(" .")
    if not sanitized:
        sanitized = "_unnamed_"
    if sanitized.upper() in _RESERVED_NAMES:
        sanitized = sanitized + "_"
    return sanitized


def _qualify_ddl_with_schema(ddl: str, schema_name: str) -> str:
    """Prefix the object name in a DDL statement with *schema_name*.

    Transforms, for example::

        create or replace TABLE MBL_ANALYSIS_ACTIVITY_PROFILE (
    into::

        create or replace TABLE AZ_ENGINE_MBL.MBL_ANALYSIS_ACTIVITY_PROFILE (

    If the object name is already schema-qualified (contains a dot) or the
    CREATE pattern is not found, the DDL is returned unchanged.
    """
    m = _CREATE_RE.search(ddl)
    if not m:
        return ddl

    obj_name = m.group(2)
    # If already qualified (e.g. SCHEMA.TABLE or "SCHEMA"."TABLE"), skip.
    bare = obj_name.strip('"')
    if "." in bare or '"."' in obj_name:
        return ddl

    qualified = f"{schema_name.upper()}.{obj_name}"
    return ddl[: m.start(2)] + qualified + ddl[m.end(2) :]


def _strip_database_prefix(ddl: str, database: str) -> str:
    """Remove the database name prefix from all references in the DDL.

    Handles both quoted and unquoted forms::

        DEV_DB.SCHEMA.OBJ        →  SCHEMA.OBJ
        "DEV_DB"."SCHEMA"."OBJ"  →  "SCHEMA"."OBJ"
        "DEV_DB".SCHEMA.OBJ      →  SCHEMA.OBJ
    """
    db_upper = database.upper()
    # Quoted form first:  "DATABASE".  →  (empty)
    ddl = re.sub(
        rf'"{re.escape(db_upper)}"\s*\.\s*',
        "",
        ddl,
        flags=re.IGNORECASE,
    )
    # Unquoted form:  DATABASE.  →  (empty)
    ddl = re.sub(
        rf'(?<!["\w]){re.escape(db_upper)}\s*\.\s*',
        "",
        ddl,
        flags=re.IGNORECASE,
    )
    return ddl


def write_ddl_files(
    results: List[ExtractedObject],
    out_dir: str,
    database: str,
) -> Set[Path]:
    """Write DDL files to disk following the database/schema/type hierarchy.

    Directory layout::

        {out_dir}/{DATABASE}/{SCHEMA}/tables/OBJECT_NAME.sql

    Args:
        results: Extracted DDL objects.
        out_dir: Root output directory.
        database: Database name (used as top-level folder).

    Returns:
        Set of absolute :class:`Path` objects for every file written.
    """
    written: Set[Path] = set()
    base = Path(out_dir) / database.upper()

    for obj in results:
        type_dir = TYPE_DIR_MAP.get(obj.object_type, obj.object_type.upper() + "S")
        dir_path = base / obj.schema_name.upper() / type_dir
        dir_path.mkdir(parents=True, exist_ok=True)

        filename = sanitize_filename(obj.object_name).upper() + ".sql"
        file_path = dir_path / filename

        try:
            ddl = _strip_database_prefix(obj.ddl, database)
            ddl = _qualify_ddl_with_schema(ddl, obj.schema_name)
            file_path.write_text(ddl, encoding="utf-8")
            written.add(file_path.resolve())
            logger.debug("Wrote %s", file_path)
        except OSError as exc:
            logger.error("Failed to write %s: %s", file_path, exc)

    logger.info("Wrote %d DDL file(s) under %s.", len(written), base)
    return written


def cleanup_stale_files(
    out_dir: str,
    database: str,
    written_files: Set[Path],
) -> int:
    """Delete ``.sql`` files that no longer correspond to Snowflake objects.

    Walks the output tree, removes any ``.sql`` file whose resolved path is
    not in *written_files*, and prunes empty directories afterward.

    Args:
        out_dir: Root output directory.
        database: Database name.
        written_files: Set of paths that were just written (from
            :func:`write_ddl_files`).

    Returns:
        Number of stale files removed.
    """
    base = Path(out_dir) / database.upper()
    if not base.exists():
        return 0

    removed = 0
    for sql_file in base.rglob("*.sql"):
        if sql_file.resolve() not in written_files:
            try:
                sql_file.unlink()
                removed += 1
                logger.info("Removed stale file: %s", sql_file)
            except OSError as exc:
                logger.error("Could not remove %s: %s", sql_file, exc)

    # Remove empty directories bottom-up.
    for dirpath, dirnames, filenames in os.walk(str(base), topdown=False):
        if not dirnames and not filenames:
            try:
                Path(dirpath).rmdir()
                logger.debug("Removed empty directory: %s", dirpath)
            except OSError:
                pass

    if removed:
        logger.info("Cleaned up %d stale file(s).", removed)
    return removed
