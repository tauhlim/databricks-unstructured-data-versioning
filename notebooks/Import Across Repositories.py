# Databricks notebook source
import sys
import os
#  you can add different repos by traversing the directories using relative paths
sys.path.append(os.path.abspath('../../databricks-sg-datasets')) 

# COMMAND ----------

from power import n_to_mth
n_to_mth(3, 4)

# COMMAND ----------

# MAGIC %run ../../databricks-sg-datasets/power

# COMMAND ----------

n_to_mth(3, 2)

# COMMAND ----------


