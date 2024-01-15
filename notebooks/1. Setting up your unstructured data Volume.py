# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Unity Catalog Volumes
# MAGIC
# MAGIC Unity Catalog gives us a way to interact with and manage access to unstructured data, with a feature called `Volumes`.
# MAGIC
# MAGIC The [documentation](https://docs.databricks.com/en/data-governance/unity-catalog/create-volumes.html) gives a pretty good overview of what Volumes is about, how to use it, and some examples of how to get set up and running.
# MAGIC
# MAGIC The gist of it is - Volumes gives users a way to reference files stored on cloud storage, without worrying about _where_ the files are physically stored, and _what permissions_ they may need on the underlying storage container.
# MAGIC
# MAGIC So it's a prime candidate for us to store our unstructured data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a volume first
# MAGIC
# MAGIC -- Managed Volume
# MAGIC CREATE VOLUME IF NOT EXISTS field_demos.tauherng.tau_managed_volume;
# MAGIC
# MAGIC -- External Volume
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS field_demos.tauherng.tau_external_volume
# MAGIC LOCATION 's3://databricks-tau-demo-bucket/tau_test_volume'; -- note that this needs to an external location

# COMMAND ----------

# Let's use the external volume

VOLUME_PATH = "/Volumes/field_demos/tauherng/tau_external_volume"

# COMMAND ----------

# Let's put some data from the flowers dataset into the Volume

# only take the directories, remove the trailing slash
categories = [obj.name.replace("/", "") for obj in dbutils.fs.ls("/databricks-datasets/flower_photos/") if "." not in obj.name] 
FILES_PER_CATEGORY = 10

# This may take a while
for cat in categories:
  files_to_move = dbutils.fs.ls(f"/databricks-datasets/flower_photos/{cat}")[:FILES_PER_CATEGORY]
  for f in files_to_move:
    dbutils.fs.cp(f.path, f"{VOLUME_PATH}/{cat}/{f.name}")


# COMMAND ----------

# Now let's use Delta Lake to create some metadata for the unstructured data

dbutils.fs.ls("/Volumes/field_demos/tauherng/tau_external_volume/daisy")

# COMMAND ----------



# COMMAND ----------

# List all files and get their metadata
# Note that we only want files, not directories. So some recursive walking is required

FILES = []
DIRS = [VOLUME_PATH]

while len(DIRS) > 0:
  x = DIRS.pop()
  if not isinstance(x, str):
    x = x.path
  list_of_objects = dbutils.fs.ls(x)
  for obj in list_of_objects:
    if obj.isFile():
      FILES += [obj]
    elif obj.isDir():
      DIRS += [obj]

# Save as a delta lake table

# COMMAND ----------

FILES

# COMMAND ----------



# COMMAND ----------


