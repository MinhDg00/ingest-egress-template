# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest and Egress Data in Databricks

# COMMAND ----------

# DBTITLE 1,Create Date Widget
### create date parameters 
dbutils.widgets.removeAll()

dbutils.widgets.text("year", "","") 
dbutils.widgets.text("month", "","")
dbutils.widgets.text("day", "","") 

# COMMAND ----------

# Get date, time from widget
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
day = dbutils.widgets.get("day")
date = year+month+day

# COMMAND ----------

print('day: ' + str(day))
print('month: ' + str(month))
print('year: ' + str(year))
print('date: ' + str(date))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Data

# COMMAND ----------

# DBTITLE 1,1.1 Mount Azure Data Lake to Databricks File Systems
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<your-service-client-id>",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "<scope-name>", key = "<key-name-for-service-credential>"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<your-directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<your-data-lake-blob>@<your-data-lake-account-name>.dfs.core.windows.net/<your-directory-name>",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

# Check files and their paths mounted from storage
%fs ls /mnt/<mount-name>

# Alternative 
dbutils.fs.ls("/mnt/<mount-name>")

# COMMAND ----------

# Other useful Databricks Ultilities commands 

# File System Ultilities
dbutils.fs.help()
dbutils.fs.mkdirs("<file-path>")
dbutils.fs.put("<file-path>", <new_name>)
dbutils.fs.rm("/<file-path>/file.csv")

# Library Ultilities 
dbutils.library.help()
dbutils.library.installPyPI("<pypipackage>", version="<version>", repo="<repo>")
dbutils.library.list() 
 

# COMMAND ----------

# DBTITLE 1,1.2. Unmount Azure Data Lake, if necessary
dbutils.fs.unmount("/mnt/<mount-name>")

# COMMAND ----------

# DBTITLE 1,1.3.a. Load datasets as Pyspark DataFrame
# General way to read file type from any format
df1 = spark.read.format('<file_type>').option('<specify-your-option>').load('dbfs:/mnt/<mount-name>/ingest/<file_1>')
df2 = spark.read.format('<file_type>').option('<specify-your-option>').load('dbfs:/mnt/<mount-name>/ingest/<file_2>')

# Example for csv file
df1 = spark.read.format('csv').option('header','true').load('dbfs:/mnt/<mount-name>/ingest/file_1.csv')
df2 = spark.read.format('csv').option('header','true').load('dbfs:/mnt/<mount-name>/ingest/file_1.csv')


# COMMAND ----------

# Alternative way to load csv file as Pyspark DataFrame
df1 = spark.read.csv("dbfs:/mnt/<mount-name>/ingest/<file_1>", header = True)
df2 = spark.read.csv("dbfs:/mnt/<mount-name>/ingest/<file_2>", header = True)


# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import col

colsSelect = ['col1','col2','col3','col4','col5','col6'] 
df_1 = df1.select(*colsSelect)\
         .withColumn('column_1', col('col1').cast(StringType()))\
         .withColumn('column_2', col('col2').cast(StringType()))\
         .withColumn('column_3', col('col3').cast(DoubleType()))\
         .withColumn('column_4', col('col4').cast(FloatType()))\
         .withColumn('column_5', col('col5').cast(IntegerType()))\
         .withColumn('column_6', col('col6').cast(IntegerType()))

display(df_1)


# COMMAND ----------

# If data in is your DB FileStore, we can load in the following way
df1 = spark.read.csv("/FileStore/Tables/<file_1>", header = True)
df2 = spark.read.csv("/FileStore/Tables/<file_2>", header = True)


# COMMAND ----------

# DBTITLE 1,1.3.b. Load Datasets as Pandas dataframe
import pandas as pd

types1 = {'col1':'str', 'col2':'str', 'col3': 'int', 
          'col4': 'float', 'col5': 'int', 'col6': 'int'} 

df1_pd = pd.read_csv("/dbfs/mnt/<mount-name>/<file_1>", header = "infer", dtype = types1)
df2_pd = pd.read_csv("/dbfs/mnt/<mount-name/<file_2>", header = "infer")

display(df1_pd)


# COMMAND ----------

# MAGIC %md 
# MAGIC Note : When calling pandas read_csv, we need to provide absolute path with ('/dbfs') unlike calling spark read.csv which uses root path ('dbfs:/')

# COMMAND ----------

# DBTITLE 1,1.4. Convert Spark DF to and from pandas DF
## Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

## Create a Spark DataFrame from a pandas DataFrame using Arrow
df1 = spark.createDataFrame(df1_pd)
df2 = spark.createDataFrame(df2_pd)

## Covert the Spark DataFrame back to a pandas DataFrame using Arrow
df1_pd = df1.select("*").toPandas()
df2_pd = df2.select("*").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Egress Data

# COMMAND ----------

# DBTITLE 1,2.1. Write output to ADLS Gen 2
# MAGIC %md
# MAGIC 2.1.a. Write Pyspark DataFrame to ADLS Gen 2

# COMMAND ----------

## df: final output
df\
.write\
.format("com.databricks.spark.csv")\
.option("header","true")\
.mode('overwrite')\
.save('/mnt/<mount-name>/<output-file>')


# COMMAND ----------

# MAGIC %md 
# MAGIC 2.2.b. Write Pandas dataframe to ADLS Gen 2

# COMMAND ----------

path = '/dbfs/mnt/<mount-name>/<output-name>'
df2.to_csv('/dbfs/mnt/vbigndemo/sad.csv', header = True) 

# COMMAND ----------

# DBTITLE 1,2.2. Write output to Databricks File Systems 
# MAGIC %md
# MAGIC 2.2.a. Write Pyspark DataFrame to DBFS

# COMMAND ----------

## df: final output
df\
.write\
.format("com.databricks.spark.csv")\
.option("header","true")\
.mode('overwrite')\
.save('/FileStore/tables/<output-name>')

# COMMAND ----------

# MAGIC %md 
# MAGIC 2.2.b. Write pandas dataframe to DBFS

# COMMAND ----------

df.to_csv('/dbfs/FileStore/tables/<output-name')

# COMMAND ----------

## Check output file in FileStore
%fs ls /FileStore/tables 


# COMMAND ----------

# DBTITLE 1,2.3. Write output as a Spark Table
# MAGIC %md 
# MAGIC Note: this option is only applicable to Spark DataFrame

# COMMAND ----------

## df: final ouput
df\
.format('csv')  #you can use other file type 
.write\
.saveAsTable("<output-name>", mode = "overwrite")


# COMMAND ----------

df.createOrReplaceTempView("<output-name>")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from <ouput-name> 

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table <output-name>

# COMMAND ----------

# DBTITLE 1,Remove Widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Secrets

# COMMAND ----------

# use Spark data frame to apply scala
df.createOrReplaceTempView("<output-name>")

# COMMAND ----------

# MAGIC %scala
# MAGIC // read Secrets
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC val connectionProperties = new java.util.Properties()
# MAGIC connectionProperties.setProperty("Driver", driverClass)
# MAGIC 
# MAGIC val dbUser = dbutils.secrets.get(scope = "sql", key = "dbuser")
# MAGIC val dbPasswd = dbutils.secrets.get(scope = "jdbc", key = "dbpasswd")
# MAGIC connectionProperties.put("user", s"${dbUser}")
# MAGIC connectionProperties.put("password", s"${dbPassword}")

# COMMAND ----------

# MAGIC %scala
# MAGIC // get Secrets
# MAGIC val dbUser = dbutils.secrets.get(scope = "sql", key = "dbuser")
# MAGIC val dbPasswd = dbutils.secrets.get(scope = "sql", key = "dbpasswd")
# MAGIC val dbHost = dbutils.secrets.get(scope = "sql", key = "dbhost")
# MAGIC val dbName = dbutils.secrets.get(scope = "sql", key = "dbname")

# COMMAND ----------

# MAGIC %scala
# MAGIC // egress output to SQL DB using Secrets
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val insertDate = date
# MAGIC 
# MAGIC val query = s"""|delete from <sql-table-name> where Date = '$insertDate';""".stripMargin
# MAGIC 
# MAGIC val config1 = Config(Map(
# MAGIC   "url"          -> dbHost,
# MAGIC   "databaseName" -> dbName,
# MAGIC   "user"         -> dbUser,
# MAGIC   "password"     -> dbPasswd,
# MAGIC   "queryCustom"  -> query
# MAGIC ))
# MAGIC 
# MAGIC //spark.azurePushdownQuery(config1)
# MAGIC sqlContext.sqlDBQuery(config1)
# MAGIC 
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.query._
# MAGIC 
# MAGIC val config = Config(Map(
# MAGIC   "url"          -> dbHost,
# MAGIC   "databaseName" -> dbName,
# MAGIC   "dbTable"      -> "<sql-table-name>",
# MAGIC   "user"         -> dbUser,
# MAGIC   "password"     -> dbPasswd
# MAGIC ))
# MAGIC 
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC 
# MAGIC <output-name>.write.mode(SaveMode.Append).sqlDB(config)
