# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Install dependencies
# MAGIC 
# MAGIC Chạy notebook này 1 lần để cài lib cho pipeline (pandas/sklearn/joblib).

# COMMAND ----------

# MAGIC %pip install -U pandas scikit-learn joblib

# COMMAND ----------

# MAGIC %md
# MAGIC (Tuỳ chọn) Nếu bạn chạy producer Kafka bằng Python.

# COMMAND ----------

# MAGIC %pip install -U kafka-python

# COMMAND ----------

dbutils.library.restartPython()
