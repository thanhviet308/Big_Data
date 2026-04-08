# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Stage data to DBFS
# MAGIC 
# MAGIC Mục tiêu: đưa dữ liệu trong repo (git) lên DBFS để dùng ổn định cho Jobs.
# MAGIC 
# MAGIC - Input: `data/raw/...csv`, `data/blacklist/blacklist_accounts.csv` trong repo
# MAGIC - Output: `/dbfs/FileStore/fraud/...`

# COMMAND ----------

import os
from pathlib import Path


def find_repo_root(start: Path) -> Path:
	p = start
	while p != p.parent:
		if (p / "src").exists() and (p / "data").exists():
			return p
		p = p.parent
	return start


# When running this notebook, CWD is often .../databricks/notebooks
repo_root = find_repo_root(Path(os.getcwd()))
print("CWD:", Path(os.getcwd()))
print("REPO_ROOT:", repo_root)

raw_src = repo_root / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"
blacklist_src = repo_root / "data" / "blacklist" / "blacklist_accounts.csv"

DBFS_BASE = os.getenv("FRAUD_DBFS_BASE", "dbfs:/tmp/fraud_detection")
raw_dbfs = f"{DBFS_BASE}/raw.csv"
blacklist_dbfs = f"{DBFS_BASE}/blacklist_accounts.csv"

print("Copy RAW:", raw_src, "->", raw_dbfs)
print("Copy BLACKLIST:", blacklist_src, "->", blacklist_dbfs)

# COMMAND ----------

# Use dbutils for copy (works with DBFS and workspace paths)
# Workspace path for repo file is accessible via local filesystem in Repos as well,
# but dbutils is clearer here.

dbutils.fs.mkdirs(DBFS_BASE)
dbutils.fs.cp(f"file:{raw_src}", raw_dbfs, True)
dbutils.fs.cp(f"file:{blacklist_src}", blacklist_dbfs, True)

print("✅ Staged to DBFS")
