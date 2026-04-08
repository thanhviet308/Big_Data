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

# Repo root (Databricks Repos)
repo_root = Path(os.getcwd())
print("CWD:", repo_root)

raw_src = repo_root / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"
blacklist_src = repo_root / "data" / "blacklist" / "blacklist_accounts.csv"

raw_dst = Path("/dbfs/FileStore/fraud/raw.csv")
blacklist_dst = Path("/dbfs/FileStore/fraud/blacklist_accounts.csv")

raw_dst.parent.mkdir(parents=True, exist_ok=True)

print("Copy RAW:", raw_src, "->", raw_dst)
print("Copy BLACKLIST:", blacklist_src, "->", blacklist_dst)

# COMMAND ----------

# Use dbutils for copy (works with DBFS and workspace paths)
# Workspace path for repo file is accessible via local filesystem in Repos as well,
# but dbutils is clearer here.

dbutils.fs.cp(f"file:{raw_src}", "dbfs:/FileStore/fraud/raw.csv", True)
dbutils.fs.cp(f"file:{blacklist_src}", "dbfs:/FileStore/fraud/blacklist_accounts.csv", True)

print("✅ Staged to DBFS")
