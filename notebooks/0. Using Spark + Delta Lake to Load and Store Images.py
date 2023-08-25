# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC There are 2 main ways to ingest image data using Spark, and store them as delta tables. 
# MAGIC
# MAGIC Both of these methods essentially ingest the images, read some different metadata, and keep the image binary data as a column. 

# COMMAND ----------

# DBTITLE 1,We'll use the flowers dataset as an example
dbutils.fs.ls("/databricks-datasets/flower_photos")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Method 1 - using `format("image")`
# MAGIC
# MAGIC More information here: https://docs.databricks.com/en/external-data/image.html
# MAGIC
# MAGIC * Spark reads the files specified by the filepath
# MAGIC * Stores some image-specific metadata, including height, width, nchannels, mode
# MAGIC * Stores the images as binary data (`OpenCV` compatible, no less)
# MAGIC * **Note**: Once table is saved, you no longer need the original files
# MAGIC * **Note**: The `image` field actually contains the image data _and_ metadata, and `display(df)` needs all that info in a single Struct field to display it properly.
# MAGIC    * Note that this is different from `binaryFile`, where just the `content` column is enough to display the image
# MAGIC
# MAGIC
# MAGIC Importantly, there are a bunch of caveats with this approach:
# MAGIC > The image data source decodes the image files during the creation of the Spark DataFrame, increases the data size, and introduces limitations in the following scenarios:
# MAGIC > * Persisting the DataFrame: If you want to persist the DataFrame into a Delta table for easier access, you should persist the raw bytes instead of the decoded data to save disk space.
# MAGIC > * Shuffling the partitions: Shuffling the decoded image data takes more disk space and network bandwidth, which results in slower shuffling. You should delay decoding the image as much as possible.
# MAGIC >  * Choosing other decoding method: The image data source uses the Image IO library of javax to decode the image, which prevents you from choosing other image decoding libraries for better performance or implementing customized decoding logic.
# MAGIC >
# MAGIC > Those limitations can be avoided by using the binary file data source to load image data and decoding only as needed.

# COMMAND ----------

image_df = spark.read.format("image").load("/databricks-datasets/flower_photos/*/*.jpg")

# COMMAND ----------

image_df.printSchema()

# COMMAND ----------

display(image_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Method 2 - using `format("binaryFile")`
# MAGIC
# MAGIC Note that this is the recommended approach, outlined here: https://docs.databricks.com/en/machine-learning/reference-solutions/images-etl-inference.html
# MAGIC
# MAGIC * Spark reads the files specified by the filepath
# MAGIC * Stores some basic file metadata, including path, mod time and length
# MAGIC * Stores the images as binary data
# MAGIC * **Note**: You can use `display(df)` and it will nicely show you a little thumbnail of the image
# MAGIC * **Note**: Once table is saved, you no longer need the original files

# COMMAND ----------

binary_df = spark.read.format("binaryFile").load("/databricks-datasets/flower_photos/*/*.jpg")

# COMMAND ----------

binary_df.printSchema()

# COMMAND ----------

display(binary_df.limit(10))

# COMMAND ----------

# Now, since binaryFile is preferred, we can save the binary_df as a Delta Table for downstream use
# Model training, image preprocessing, etc

binary_df.write.saveAsTable("field_demos.tauherng.flowers")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Actually, Method 2 above also has some limitations, specified in the [Reference Solution page](https://docs.databricks.com/en/machine-learning/reference-solutions/images-etl-inference.html) as well.
# MAGIC
# MAGIC >**Limitations: Image file sizes**
# MAGIC >For large image files (average image size greater than 100 MB), Databricks recommends using the Delta table only to manage the metadata (list of file names) and loading the images from >the object store using their paths when needed.
# MAGIC
# MAGIC So let's see how we can apply a general approach to handle this issue

# COMMAND ----------


