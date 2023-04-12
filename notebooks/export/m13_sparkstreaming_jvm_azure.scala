// Databricks notebook source
// MAGIC %md
// MAGIC ## 1. Data ingestion config

// COMMAND ----------

// input data params (substitute with your values)
spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "")
 
// access key for results storage 
spark.conf.set("fs.azure.account.key.stsparkstrmwesteurope.dfs.core.windows.net", "")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Create Spark Structured Streaming application with Auto Loader to incrementally processes hotel/weather data as it arrives in provisioned Azure ADLS gen2 storage

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType

val hotel_weather_data = spark.read.parquet("abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather")
val schema = hotel_weather_data.schema

// Read data from source ADLS Gen2 storage account with auto load and delay
val sourceData: DataFrame = spark.readStream
  .format("cloudFiles")
  .schema(schema)
  .option("cloudFiles.format", "parquet")
  .option("path", "abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather")
  .load()

// Write data to ADLS Gen2 storage
val query = sourceData.writeStream
  .format("delta")
  .option("header", "true")
  .option("checkpointLocation", "abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/checkpoint")
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("1 seconds")) // Set the trigger interval as needed
  .start("abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/output")

  //query.awaitTermination()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Calculate stats

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.1 Create delta table based on source data

// COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS m13_sparkstreaming_jvm_azure_db")
spark.sql("USE m13_sparkstreaming_jvm_azure_db")

val hotel_weather_ddl_query = """CREATE TABLE IF NOT EXISTS m13_sparkstreaming_jvm_azure_db.hotel_weather 
                   USING DELTA
                   LOCATION 'abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/output'
                   """
spark.sql(hotel_weather_ddl_query)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.2 Calculate number of distinct hotels in the city

// COMMAND ----------

var query = """
  SELECT city, COUNT(DISTINCT id) as num_distinct_hotels
  FROM hotel_weather
  GROUP BY city
  order by num_distinct_hotels DESC
  LIMIT 10
"""

display(spark.sql(query));

// COMMAND ----------

// MAGIC %md
// MAGIC Execution plan:
// MAGIC 
// MAGIC == Physical Plan ==
// MAGIC AdaptiveSparkPlan isFinalPlan=false
// MAGIC +- TakeOrderedAndProject(limit=10, orderBy=[num_distinct_hotels#3796L DESC NULLS LAST], output=[city#3819,num_distinct_hotels#3796L])
// MAGIC    +- HashAggregate(keys=[city#3819], functions=[finalmerge_count(distinct merge count#3833L) AS count(id#3822)#3830L])
// MAGIC       +- Exchange hashpartitioning(city#3819, 200), ENSURE_REQUIREMENTS, [plan_id=5078]
// MAGIC          +- HashAggregate(keys=[city#3819], functions=[partial_count(distinct id#3822) AS count#3833L])
// MAGIC             +- HashAggregate(keys=[city#3819, id#3822], functions=[])
// MAGIC                +- Exchange hashpartitioning(city#3819, id#3822, 200), ENSURE_REQUIREMENTS, [plan_id=5074]
// MAGIC                   +- HashAggregate(keys=[city#3819, id#3822], functions=[])
// MAGIC                      +- FileScan parquet spark_catalog.m13_sparkstreaming_jvm_azure_db.hotel_weather[city#3819,id#3822] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/result], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<city:string,id:string>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.3 Calculate average/max/min temperature in the city

// COMMAND ----------

var query = """
 SELECT city, wthr_date, AVG(avg_tmpr_c) AS avg_temperature, MAX(avg_tmpr_c) AS max_temperature, MIN(avg_tmpr_c) AS min_temperature
FROM hotel_weather
GROUP BY city, wthr_date
ORDER BY wthr_date DESC
"""

display(spark.sql(query));

// COMMAND ----------

// MAGIC %md
// MAGIC Execution plan:
// MAGIC 
// MAGIC == Physical Plan ==
// MAGIC AdaptiveSparkPlan isFinalPlan=false
// MAGIC +- Sort [wthr_date#3747 DESC NULLS LAST], true, 0
// MAGIC    +- Exchange rangepartitioning(wthr_date#3747 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=5027]
// MAGIC       +- HashAggregate(keys=[city#3740, wthr_date#3747], functions=[finalmerge_avg(merge sum#3757, count#3758L) AS avg(avg_tmpr_c#3738)#3751, finalmerge_max(merge max#3760) AS max(avg_tmpr_c#3738)#3752, finalmerge_min(merge min#3762) AS min(avg_tmpr_c#3738)#3753])
// MAGIC          +- Exchange hashpartitioning(city#3740, wthr_date#3747, 200), ENSURE_REQUIREMENTS, [plan_id=5024]
// MAGIC             +- HashAggregate(keys=[city#3740, wthr_date#3747], functions=[partial_avg(avg_tmpr_c#3738) AS (sum#3757, count#3758L), partial_max(avg_tmpr_c#3738) AS max#3760, partial_min(avg_tmpr_c#3738) AS min#3762])
// MAGIC                +- FileScan parquet spark_catalog.m13_sparkstreaming_jvm_azure_db.hotel_weather[avg_tmpr_c#3738,city#3740,wthr_date#3747] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/result], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<avg_tmpr_c:double,city:string,wthr_date:string>

// COMMAND ----------

// MAGIC %md
// MAGIC ## 4. Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.1 Save intermediate aggregated data

// COMMAND ----------

// aggregate cities data
var agg_and_grouped_cities_query = """
  SELECT wthr_date AS date,
        city,
        COUNT(DISTINCT name) AS hotel_count,
        MAX(avg_tmpr_c) AS max_tmpr,
        MIN(avg_tmpr_c) AS min_tmpr,
        ROUND(AVG(avg_tmpr_c), 2) AS avg_tmpr
  FROM hotel_weather
  GROUP BY wthr_date, city
"""

spark.sql(agg_and_grouped_cities_query)
  .write
  .format("delta")
  .mode(SaveMode.Overwrite)
  .save("abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/agg-cities")

// create delta table 
val agg_cities_ddl_query =
"""CREATE TABLE IF NOT EXISTS m13_sparkstreaming_jvm_azure_db.agg_cities
                  USING DELTA
                  LOCATION 'abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/agg-cities'
                  """
spark.sql(agg_cities_ddl_query)


// COMMAND ----------

// MAGIC %md
// MAGIC Execution plan:
// MAGIC 
// MAGIC == Physical Plan ==
// MAGIC AdaptiveSparkPlan isFinalPlan=false
// MAGIC +- HashAggregate(keys=[wthr_date#3644, city#3637], functions=[finalmerge_max(merge max#3656) AS max(avg_tmpr_c#3635)#3649, finalmerge_min(merge min#3658) AS min(avg_tmpr_c#3635)#3650, finalmerge_avg(merge sum#3661, count#3662L) AS avg(avg_tmpr_c#3635)#3651, finalmerge_count(distinct merge count#3654L) AS count(name#3643)#3648L])
// MAGIC    +- Exchange hashpartitioning(wthr_date#3644, city#3637, 200), ENSURE_REQUIREMENTS, [plan_id=4979]
// MAGIC       +- HashAggregate(keys=[wthr_date#3644, city#3637], functions=[merge_max(merge max#3656) AS max#3656, merge_min(merge min#3658) AS min#3658, merge_avg(merge sum#3661, count#3662L) AS (sum#3661, count#3662L), partial_count(distinct name#3643) AS count#3654L])
// MAGIC          +- HashAggregate(keys=[wthr_date#3644, city#3637, name#3643], functions=[merge_max(merge max#3656) AS max#3656, merge_min(merge min#3658) AS min#3658, merge_avg(merge sum#3661, count#3662L) AS (sum#3661, count#3662L)])
// MAGIC             +- Exchange hashpartitioning(wthr_date#3644, city#3637, name#3643, 200), ENSURE_REQUIREMENTS, [plan_id=4975]
// MAGIC                +- HashAggregate(keys=[wthr_date#3644, city#3637, name#3643], functions=[partial_max(avg_tmpr_c#3635) AS max#3656, partial_min(avg_tmpr_c#3635) AS min#3658, partial_avg(avg_tmpr_c#3635) AS (sum#3661, count#3662L)])
// MAGIC                   +- FileScan parquet spark_catalog.m13_sparkstreaming_jvm_azure_db.hotel_weather[avg_tmpr_c#3635,city#3637,name#3643,wthr_date#3644] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/result], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<avg_tmpr_c:double,city:string,name:string,wthr_date:string>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.2 Select and visualize data for each city from top-10
// MAGIC 
// MAGIC - X-axis: date (date of observation).
// MAGIC - Y-axis: number of distinct hotels, average/max/min temperature.

// COMMAND ----------

// MAGIC %md
// MAGIC #### 1. Paris

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Paris'

// COMMAND ----------

// MAGIC %md
// MAGIC Execution Plan (more or less the same for other cities):
// MAGIC 
// MAGIC == Physical Plan ==
// MAGIC *(1) Filter (isnotnull(city#3583) AND (city#3583 = Paris))
// MAGIC +- *(1) ColumnarToRow
// MAGIC    +- FileScan parquet spark_catalog.m13_sparkstreaming_jvm_azure_db.agg_cities[date#3582,city#3583,hotel_count#3584L,max_tmpr#3585,min_tmpr#3586,avg_tmpr#3587] Batched: true, DataFilters: [isnotnull(city#3583), (city#3583 = Paris)], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsparkstrmwesteurope.dfs.core.windows.net/agg-cities], PartitionFilters: [], PushedFilters: [IsNotNull(city), EqualTo(city,Paris)], ReadSchema: struct<date:string,city:string,hotel_count:bigint,max_tmpr:double,min_tmpr:double,avg_tmpr:double>

// COMMAND ----------

// MAGIC %md
// MAGIC #### 2. London

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'London'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3. Barcelona

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Barcelona'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 4. Milan

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Milan'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5. Amsterdam

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Amsterdam'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 6. Paddington

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Paddington'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 7. Houston

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Houston'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 8. New York

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'New York'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 9. Springfield

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'Springfield'

// COMMAND ----------

// MAGIC %md
// MAGIC #### 10. San Diego

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM agg_cities WHERE city = 'San Diego'
