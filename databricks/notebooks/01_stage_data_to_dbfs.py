# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Stage data (DBFS-free)
# MAGIC 
# MAGIC Mục tiêu: đưa dữ liệu trong repo (git) sang một thư mục dùng chung cho notebooks.
# MAGIC 
# MAGIC Lưu ý: nhiều workspace/Serverless **chặn Public DBFS root** (`dbfs:/...`), nên notebook này
# MAGIC mặc định stage vào filesystem local của driver (`/tmp/...`). Nếu bạn có Unity Catalog Volumes,
# MAGIC hãy set `FRAUD_STORAGE_BASE=/Volumes/<catalog>/<schema>/<volume>/fraud_detection` để có nơi lưu bền vững.
# MAGIC 
# MAGIC - Input: `data/raw/...csv`, `data/blacklist/blacklist_accounts.csv` trong repo
# MAGIC - Output: `${FRAUD_STORAGE_BASE}/raw.csv`, `${FRAUD_STORAGE_BASE}/blacklist_accounts.csv`

# COMMAND ----------

import os
import shutil
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

default_raw_src = repo_root / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"
default_blacklist_src = repo_root / "data" / "blacklist" / "blacklist_accounts.csv"

# IMPORTANT: `data/raw/` is gitignored in this repo, so the big raw CSV is
# typically NOT present in Databricks Repos clone.
# Upload the raw CSV to a Unity Catalog Volume (recommended) or another accessible
# filesystem location, then set FRAUD_RAW_SOURCE_PATH to that path.
raw_src = Path(os.getenv("FRAUD_RAW_SOURCE_PATH", str(default_raw_src)))
blacklist_src = Path(os.getenv("FRAUD_BLACKLIST_SOURCE_PATH", str(default_blacklist_src)))

storage_base = Path(os.getenv("FRAUD_STORAGE_BASE", "/tmp/fraud_detection"))
raw_out = storage_base / "raw.csv"
blacklist_out = storage_base / "blacklist_accounts.csv"

print("STORAGE_BASE:", storage_base)
print("RAW_SOURCE:", raw_src)
print("BLACKLIST_SOURCE:", blacklist_src)
print("Copy RAW:", raw_src, "->", raw_out)
print("Copy BLACKLIST:", blacklist_src, "->", blacklist_out)

# COMMAND ----------

storage_base.mkdir(parents=True, exist_ok=True)

if not raw_src.exists():
	raise FileNotFoundError(
		"Raw CSV not found. In this repo, `data/raw/` is gitignored so the file may not exist in Databricks Repos. "
		"Upload the raw CSV to a location accessible by the cluster (recommended: Unity Catalog Volume), then set: "
		"FRAUD_RAW_SOURCE_PATH=/Volumes/<catalog>/<schema>/<volume>/PS_..._log.csv "
		f"(tried: {raw_src})"
	)
if not blacklist_src.exists():
	raise FileNotFoundError(
		"Blacklist CSV not found. Ensure the repo was pulled correctly, or set: "
		"FRAUD_BLACKLIST_SOURCE_PATH=/Volumes/<catalog>/<schema>/<volume>/blacklist_accounts.csv "
		f"(tried: {blacklist_src})"
	)

shutil.copy2(raw_src, raw_out)
shutil.copy2(blacklist_src, blacklist_out)

print("✅ Staged to:", storage_base)
