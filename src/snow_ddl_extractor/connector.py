"""Snowflake connection factory supporting multiple authentication methods."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger(__name__)


def _load_private_key(path: str, passphrase: Optional[str] = None) -> bytes:
    """Load a PEM-encoded private key and return DER bytes for Snowflake.

    Args:
        path: Filesystem path to the PEM private key file.
        passphrase: Optional passphrase protecting the key.

    Returns:
        DER-encoded private key bytes.
    """
    key_path = Path(path).expanduser()
    with open(key_path, "rb") as fh:
        private_key = serialization.load_pem_private_key(
            fh.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def create_connection(
    account: str,
    user: str,
    database: str,
    auth: str = "password",
    password: Optional[str] = None,
    private_key_path: Optional[str] = None,
    private_key_passphrase: Optional[str] = None,
    oauth_token: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
) -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection.

    Args:
        account: Snowflake account identifier.
        user: Login user name.
        database: Target database to extract DDLs from.
        auth: Authentication method — one of ``password``, ``keypair``,
              ``sso``, or ``oauth``.
        password: Password (used when *auth* is ``password``).
        private_key_path: Path to PEM private key file (``keypair`` auth).
        private_key_passphrase: Optional passphrase for the private key.
        oauth_token: OAuth access token (``oauth`` auth).
        role: Optional Snowflake role.
        warehouse: Optional Snowflake warehouse.

    Returns:
        An open :class:`snowflake.connector.SnowflakeConnection`.

    Raises:
        ValueError: If required credentials are missing for the chosen *auth* method.
    """
    params: Dict[str, Any] = {
        "account": account,
        "user": user,
        "database": database,
    }
    if role:
        params["role"] = role
    if warehouse:
        params["warehouse"] = warehouse

    if auth == "password":
        pwd = password or os.environ.get("SNOWFLAKE_PASSWORD")
        if not pwd:
            raise ValueError(
                "Password auth requires --password or SNOWFLAKE_PASSWORD env var."
            )
        params["password"] = pwd

    elif auth == "keypair":
        key_path = private_key_path or os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
        if not key_path:
            raise ValueError(
                "Key-pair auth requires --private-key-path or "
                "SNOWFLAKE_PRIVATE_KEY_PATH env var."
            )
        passphrase = private_key_passphrase or os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
        )
        params["private_key"] = _load_private_key(key_path, passphrase)

    elif auth == "sso":
        params["authenticator"] = "externalbrowser"

    elif auth == "oauth":
        token = oauth_token or os.environ.get("SNOWFLAKE_OAUTH_TOKEN")
        if not token:
            raise ValueError(
                "OAuth auth requires --oauth-token or SNOWFLAKE_OAUTH_TOKEN env var."
            )
        params["authenticator"] = "oauth"
        params["token"] = token

    else:
        raise ValueError(f"Unknown auth method: {auth!r}")

    logger.info("Connecting to Snowflake account=%s, database=%s …", account, database)
    return snowflake.connector.connect(**params)
