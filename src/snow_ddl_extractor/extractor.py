"""Core extraction logic: discover schemas/objects and fetch DDLs concurrently."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set, Tuple

import snowflake.connector

logger = logging.getLogger(__name__)

# Object types we extract and their SHOW command / GET_DDL type keyword.
# Tuple: (show_command_plural, get_ddl_type)
OBJECT_TYPES: List[Tuple[str, str]] = [
    ("TABLES", "TABLE"),
    ("VIEWS", "VIEW"),
    ("PROCEDURES", "PROCEDURE"),
    ("USER FUNCTIONS", "FUNCTION"),
    ("TASKS", "TASK"),
    ("STREAMS", "STREAM"),
    ("STAGES", "STAGE"),
    ("FILE FORMATS", "FILE FORMAT"),
    ("SEQUENCES", "SEQUENCE"),
    ("PIPES", "PIPE"),
]

# Map GET_DDL type → directory name used on disk.
TYPE_DIR_MAP: Dict[str, str] = {
    "TABLE": "TABLES",
    "VIEW": "VIEWS",
    "PROCEDURE": "PROCEDURES",
    "FUNCTION": "FUNCTIONS",
    "TASK": "TASKS",
    "STREAM": "STREAMS",
    "STAGE": "STAGES",
    "FILE FORMAT": "FILE_FORMATS",
    "SEQUENCE": "SEQUENCES",
    "PIPE": "PIPES",
}


@dataclass
class ExtractedObject:
    """Represents a single extracted DDL."""

    schema_name: str
    object_type: str  # GET_DDL type keyword, e.g. "TABLE"
    object_name: str  # Simple name used for the file
    ddl: str


def _parse_callable_signature(arguments: str, name: str) -> str:
    """Build the ``name(type, …)`` signature required by GET_DDL for procedures/functions.

    The ``arguments`` column from ``SHOW PROCEDURES`` / ``SHOW USER FUNCTIONS``
    looks like ``MY_PROC(VARCHAR, NUMBER) RETURN VARCHAR`` or
    ``MY_FUNC(NUMBER, NUMBER) RETURN NUMBER``.  We need just
    ``MY_PROC(VARCHAR, NUMBER)`` — everything before `` RETURN``.

    Args:
        arguments: Raw *arguments* value from the SHOW result.
        name: Object name (fallback).

    Returns:
        Signature string suitable for GET_DDL, e.g. ``"MY_PROC(VARCHAR, NUMBER)"``.
    """
    # Strip everything from " RETURN" onward (case-insensitive).
    sig = re.split(r"\s+RETURN\s+", arguments, maxsplit=1, flags=re.IGNORECASE)[0]
    return sig.strip() if sig.strip() else name + "()"


def discover_schemas(
    conn: snowflake.connector.SnowflakeConnection,
    database: str,
    include: Optional[Set[str]] = None,
    exclude: Optional[Set[str]] = None,
) -> List[str]:
    """Return the list of schema names in *database* after applying filters.

    ``INFORMATION_SCHEMA`` is always excluded.

    Args:
        conn: Open Snowflake connection.
        database: Database to inspect.
        include: If provided, only these schema names are kept (case-insensitive).
        exclude: Schema names to skip (case-insensitive).

    Returns:
        Sorted list of schema names.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'SHOW SCHEMAS IN DATABASE "{database}"')
        rows = cur.fetchall()
    finally:
        cur.close()

    # "name" is typically the second column (index 1).
    schemas: List[str] = []
    for row in rows:
        name: str = row[1]
        upper = name.upper()
        if upper == "INFORMATION_SCHEMA":
            continue
        if include and upper not in {s.upper() for s in include}:
            continue
        if exclude and upper in {s.upper() for s in exclude}:
            continue
        schemas.append(name)

    schemas.sort()
    logger.info("Found %d schema(s) in %s: %s", len(schemas), database, schemas)
    return schemas


def discover_objects(
    conn: snowflake.connector.SnowflakeConnection,
    database: str,
    schema: str,
) -> List[Tuple[str, str, str]]:
    """Discover all extractable objects in a schema.

    Returns:
        List of tuples ``(get_ddl_type, simple_name, get_ddl_identifier)``
        where *get_ddl_identifier* is the fully-qualified string to pass to
        ``GET_DDL()``.  For procedures/functions this includes the arg-type
        signature.
    """
    objects: List[Tuple[str, str, str]] = []
    cur = conn.cursor()

    for show_plural, ddl_type in OBJECT_TYPES:
        try:
            cur.execute(f'SHOW {show_plural} IN SCHEMA "{database}"."{schema}"')
            rows = cur.fetchall()
        except snowflake.connector.errors.ProgrammingError as exc:
            logger.warning(
                "SHOW %s in %s.%s failed: %s", show_plural, database, schema, exc
            )
            continue

        col_names = [desc[0].upper() for desc in cur.description]
        name_idx = col_names.index("NAME") if "NAME" in col_names else 1

        if ddl_type in ("PROCEDURE", "FUNCTION"):
            # Collect all entries first, then disambiguate overloaded names.
            arg_idx = (
                col_names.index("ARGUMENTS")
                if "ARGUMENTS" in col_names
                else None
            )
            entries: List[Tuple[str, str]] = []  # (obj_name, signature)
            for row in rows:
                obj_name_raw: str = row[name_idx]
                if arg_idx is not None and row[arg_idx]:
                    sig = _parse_callable_signature(row[arg_idx], obj_name_raw)
                else:
                    sig = obj_name_raw + "()"
                entries.append((obj_name_raw, sig))

            # Detect names that appear more than once (overloads).
            name_counts: Dict[str, int] = {}
            for obj_name_raw, _ in entries:
                name_counts[obj_name_raw] = name_counts.get(obj_name_raw, 0) + 1

            for obj_name_raw, sig in entries:
                fq_id = f'"{database}"."{schema}".{sig}'
                # Use the full signature as the file name only when there
                # are overloads; otherwise keep the plain name.
                if name_counts[obj_name_raw] > 1:
                    display_name = sig
                else:
                    display_name = obj_name_raw
                objects.append((ddl_type, display_name, fq_id))
        else:
            for row in rows:
                obj_name_str: str = row[name_idx]
                fq_base = f'"{database}"."{schema}"."{obj_name_str}"'
                objects.append((ddl_type, obj_name_str, fq_base))

    cur.close()
    logger.info(
        "Schema %s.%s: discovered %d object(s).", database, schema, len(objects)
    )
    return objects


def _extract_single_ddl(
    conn: snowflake.connector.SnowflakeConnection,
    ddl_type: str,
    fq_id: str,
    schema_name: str,
    obj_name: str,
) -> Optional[ExtractedObject]:
    """Fetch one DDL using a cursor on the shared connection.

    Args:
        conn: Shared Snowflake connection (each call creates its own cursor).
        ddl_type: GET_DDL object type keyword.
        fq_id: Fully-qualified identifier for the object.
        schema_name: Schema the object belongs to.
        obj_name: Simple object name (used for the filename).

    Returns:
        An :class:`ExtractedObject` or ``None`` on failure.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT GET_DDL('{ddl_type}', '{fq_id}')")
        row = cur.fetchone()
        if row and row[0]:
            return ExtractedObject(
                schema_name=schema_name,
                object_type=ddl_type,
                object_name=obj_name,
                ddl=row[0],
            )
        logger.warning("Empty DDL for %s %s", ddl_type, fq_id)
        return None
    except snowflake.connector.errors.ProgrammingError as exc:
        logger.warning("GET_DDL failed for %s %s: %s", ddl_type, fq_id, exc)
        return None
    finally:
        cur.close()


def extract_all(
    conn: snowflake.connector.SnowflakeConnection,
    database: str,
    include_schemas: Optional[Set[str]] = None,
    exclude_schemas: Optional[Set[str]] = None,
    include_types: Optional[Set[str]] = None,
    exclude_types: Optional[Set[str]] = None,
    max_workers: int = 10,
    progress_callback: Optional[Callable[[int], None]] = None,
    _work_items: Optional[List[Tuple[str, str, str, str]]] = None,
) -> List[ExtractedObject]:
    """Discover and extract DDLs for every object in *database*.

    Uses a :class:`~concurrent.futures.ThreadPoolExecutor` to fetch DDLs in
    parallel.  All workers share the single authenticated *conn*, each
    creating its own cursor.

    Args:
        conn: Open connection used for discovery and DDL extraction.
        database: Target database.
        include_schemas: Optional allow-list of schemas.
        exclude_schemas: Optional deny-list of schemas.
        include_types: Optional allow-list of GET_DDL type keywords (e.g. {"TABLE", "VIEW"}).
        exclude_types: Optional deny-list of GET_DDL type keywords.
        max_workers: Thread-pool size (default 10).
        progress_callback: Called with increment (1) after each DDL is processed.
        _work_items: Pre-discovered work items to skip redundant discovery.
            Each tuple is ``(ddl_type, obj_name, fq_id, schema)``.

    Returns:
        List of successfully extracted :class:`ExtractedObject` instances.
    """
    if _work_items is not None:
        work_items = _work_items
    else:
        schemas = discover_schemas(conn, database, include_schemas, exclude_schemas)
        work_items = []
        for schema in schemas:
            objects = discover_objects(conn, database, schema)
            for ddl_type, obj_name, fq_id in objects:
                if include_types and ddl_type not in include_types:
                    continue
                if exclude_types and ddl_type in exclude_types:
                    continue
                work_items.append((ddl_type, obj_name, fq_id, schema))

    logger.info("Extracting DDLs for %d object(s) with %d workers …", len(work_items), max_workers)

    results: List[ExtractedObject] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _extract_single_ddl, conn, ddl_type, fq_id, schema, obj_name
            ): (ddl_type, obj_name)
            for ddl_type, obj_name, fq_id, schema in work_items
        }
        for future in as_completed(futures):
            ddl_type, obj_name = futures[future]
            try:
                obj = future.result()
                if obj:
                    results.append(obj)
                    logger.debug("Extracted %s %s", ddl_type, obj_name)
            except Exception:
                logger.exception(
                    "Unexpected error extracting %s %s", ddl_type, obj_name
                )
            if progress_callback:
                progress_callback(1)

    logger.info("Successfully extracted %d / %d DDL(s).", len(results), len(work_items))
    return results
