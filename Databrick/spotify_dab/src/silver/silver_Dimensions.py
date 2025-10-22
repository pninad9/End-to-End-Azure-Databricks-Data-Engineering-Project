# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

# COMMAND ----------

project_path = os.path.join(os.getcwd(),"..","..")
sys.path.append(project_path)



# COMMAND ----------

from utils.transformations import reuseable

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimUser**

# COMMAND ----------

df_user = spark.readStream .format("cloudFiles")\
        .option("cloudFiles.format", "parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimUser/checkpoint")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load("abfss://bronze@ninadspotifyproject.dfs.core.windows.net/DimUser")\
        




# COMMAND ----------

display(df_user)

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))

display(df_user)

# COMMAND ----------

df_user_obj = reuseable()

df_user = df_user_obj.dropcolumn(df_user, ['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user) 

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .option("schemaEvolutionMode","rescue")\
    .load("abfss://bronze@ninadspotifyproject.dfs.core.windows.net/DimArtist")


# COMMAND ----------

display(df_artist)

# COMMAND ----------

df_artist_obj = reuseable()

df_artist = df_artist_obj.dropcolumn(df_artist, ['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])
display(df_artist)

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimDate**
# MAGIC

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimDate/checkpoint")\
    .option("cloudFiles.schemaEvolationMode","rescue")\
    .option("cloudFiles.validateOptions", "false")\
    .load("abfss://bronze@ninadspotifyproject.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date_obj = reuseable()

df_date = df_date_obj.dropcolumn(df_date, ['_rescued_data'])

display(df_date)

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .option("cloudFiles.schemaEvolationMode","rescue")\
    .option("cloudFiles.validateOptions", "false")\
    .load("abfss://bronze@ninadspotifyproject.dfs.core.windows.net/DimTrack")

# COMMAND ----------

display(df_track)


# COMMAND ----------

df_track = df_track.withColumn("duration_flag",when(col('duration_sec') < 150, "low")\
                                            .when(col('duration_sec') < 300, "medium")\
                                            .otherwise("high"))
df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),'-', ' '))

df_track_obj = reuseable()
df_track = df_track_obj.dropcolumn(df_track,['_rescued_data'])

display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@ninadspotifyproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStrean**
# MAGIC

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/FactStream/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode","rescue")\
    .load("abfss://bronze@ninadspotifyproject.dfs.core.windows.net/FactStream")


# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact = reuseable().dropcolumn(df_fact,['_rescued_data'])

display(df_fact)

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","abfss://silver@ninadspotifyproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@ninadspotifyproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC WHERE __END_AT IS NOT NUll
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC WHERE track_id IN (46, 5)
# MAGIC