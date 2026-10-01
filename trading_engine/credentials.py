"""Load private broker credentials independently of the engine checkout."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values, load_dotenv


CREDENTIAL_KEYS = frozenset({
    "TRADIER_ACCESS_TOKEN", "TRADIER_LIVE_DATA_TOKEN",
    "TRADIER_ACCOUNT_ID", "UW_API_KEY",
})


def load_engine_environment(repo_dir: Path | None = None) -> Path | None:
    """Precedence: process environment, private file, checkout .env.

    The default private file lives in the user's home directory so replacing
    or updating the repository cannot overwrite it. An explicit path must
    exist; silently falling back could start with the wrong broker account.
    """
    repo_dir = Path(repo_dir) if repo_dir is not None else Path(__file__).resolve().parent.parent
    configured = os.environ.get("ENGINE_CREDENTIALS_FILE", "").strip()
    private = (Path(configured).expanduser() if configured else
               Path.home() / ".trading_engine" / "credentials.env")
    if configured and not private.is_file():
        raise FileNotFoundError("ENGINE_CREDENTIALS_FILE does not point to a readable file")
    if private.is_file():
        for key, value in dotenv_values(private).items():
            if key in CREDENTIAL_KEYS and value is not None:
                os.environ.setdefault(key, value)
    load_dotenv(repo_dir / ".env", override=False)
    return private if private.is_file() else None
