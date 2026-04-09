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

raw_src = repo_root / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"
blacklist_src = repo_root / "data" / "blacklist" / "blacklist_accounts.csv"

storage_base = Path(os.getenv("FRAUD_STORAGE_BASE", "/tmp/fraud_detection"))
raw_out = storage_base / "raw.csv"
blacklist_out = storage_base / "blacklist_accounts.csv"

print("STORAGE_BASE:", storage_base)
print("Copy RAW:", raw_src, "->", raw_out)
print("Copy BLACKLIST:", blacklist_src, "->", blacklist_out)

# COMMAND ----------

storage_base.mkdir(parents=True, exist_ok=True)

if not raw_src.exists():
	raise FileNotFoundError(f"Raw CSV not found: {raw_src}")
if not blacklist_src.exists():
	raise FileNotFoundError(f"Blacklist CSV not found: {blacklist_src}")

shutil.copy2(raw_src, raw_out)
shutil.copy2(blacklist_src, blacklist_out)

print("✅ Staged to:", storage_base)
