from __future__ import annotations

import os
from pathlib import Path


def is_databricks() -> bool:
    """Best-effort detection for Databricks runtime."""
    return (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DB_IS_DRIVER" in os.environ
        or "DATABRICKS_CLUSTER_ID" in os.environ
    )


def project_root() -> Path:
    # paths.py -> src/utils -> src -> repo_root
    return Path(__file__).resolve().parents[2]


def resolve_repo_path(*parts: str) -> str:
    return str(project_root().joinpath(*parts))


def dbfs_to_local(path: str) -> str:
    """Convert dbfs:/... to /dbfs/... (driver-local) for pandas/joblib.

    Notes:
    - Spark APIs usually accept dbfs:/..., but many python libs (pandas/joblib)
      need /dbfs/... in Databricks.
    """
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/") :]
    return path


def normalize_path(path: str) -> str:
    return dbfs_to_local(path)


def env_or_repo_path(env_var: str, *default_parts: str) -> str:
    value = os.getenv(env_var)
    if value:
        return normalize_path(value)
    return resolve_repo_path(*default_parts)


def ensure_parent_dir(path: str) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
