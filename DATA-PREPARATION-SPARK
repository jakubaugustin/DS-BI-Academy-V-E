
# DATA PREPARATION 

**This assigment requires to already have completed DATA IMPORT.**

**This assigment focuses mainly on using spark-shell with python (Pyspark)**

## Pyspark Commands

> Open PySpark

```
pyspark
```

## PAGE-VIEWS TABLE

**Firstly we will start with table page_views and do some minor adjustments.**

> Load Data from HDFS

```
pageViews = spark.read.format("CSV").option("header","true").load("outbrain/page_views/page_views_sample.csv")
```

**Now is possible to start playing with the file.**

>Counts the number of rows in pageViews table

```
pageViews.count()
```

> Displays the first 5 rows

```
pageViews.head(5)
pageViews.show(5)
```

**Before starting working with the file we need to load all the prepared functions we want to use.**


> Load packages from the local repositories

```
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime
```

**Now we will split the column "geo_location" into "country", "state" and "DMA".**

> Transform Column Geo_Location to Country, State and DMA
```
pageViewsUpdate = pageViews.withColumn("country",split(col("geo_location"),">")[0])
			   .withColumn("state",split(col("geo_location"),">")[1])
                           .withColumn("DMA",split(col("geo_location"),">")[2]).drop("geo_location")
```

**Lets see the results.**

```
pageViewsUpdate.show(10)
```

**"State" and "DMA" have many missing values, we will delete these variables.**
```
pageViewsUpdate = pageViewsUpdate.drop("state").drop("DMA")
```

**The other variable that needs to be adjusted is timestamp.**

> Transform TimeStamp to Year, Month, Day, Hour, Minute, Second

```
pageViewsUpdate = pageViewsUpdate.withColumn("fullTime", from_unixtime((col("timestamp")+1465876799998)/1000))
               .withColumn("year", split(col("fullTime"),"-")[0])
               .withColumn("month", split(col("fullTime"),"-")[1])
               .withColumn("day",split( split(col("fullTime"),"-")[2]," ")[0])
               .withColumn("hour",split(split(split(col("fullTime"),"-")[2]," ")[1],":")[0])
               .withColumn("minute",split(split(split(col("fullTime"),"-")[2]," ")[1],":")[1])
               .withColumn("second",split(split(split(col("fullTime"),"-")[2]," ")[1],":")[2])
```

**Just to check if everything if okay.**

```
pageViewsUpdate.show(10)
```

**Now to remove unnecessary timestamp.**

```
pageViewsUpdate = pageViewsUpdate.drop("timestamp").drop("fullTime")
```

**To make sure we must replace all null values in the dataset with 0.**

```
pageViewsUpdate = pageViewsUpdate.na.fill("0")
```

**Final check of transformed table.**

```
pageViewsUpdate.show()
```

> Save Result File
```
pageViewsUpdate.repartition(1).write.format('csv').mode('overwrite').save("/user/cloudera/outbrain/page_views_update")
```

## HIVE Commands

**Lastly we need to save the final result to new Hive table.**

> Create New Table with Updated Fields

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.page_views_update (
  uuid        		STRING,
  document_id          	INT,
  platform      	TINYINT,
  traffic_source       	TINYINT,
  country		STRING,
  year			SMALLINT,
  month			TINYINT,
  day			TINYINT,
  hour			TINYINT,
  minute		TINYINT,
  second		TINYINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/page_views_update";
```

## EVENTS Table

**We will use the same approach. We will start in PySpark.**

> Load Text File From HDFS

```
events = spark.read.format("CSV").option("header","true").load("outbrain/events/events.csv")
```

> Test File

```
events.count()
events.show(5)
```

> Load Needed Packages

```
from pyspark.sql.functions import split
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime
```

> Transform Geo_Location to Country, State and DMA

```
eventsUpdate = events.withColumn("country",split(col("geo_location"),">")[0])
			   .withColumn("state",split(col("geo_location"),">")[1])
                           .withColumn("DMA",split(col("geo_location"),">")[2]).drop("geo_location")
```

> Transform TimeStamp to Year, Month, Day, Hour, Minute, Second
```
eventsUpdate = eventsUpdate.withColumn("fullTime", from_unixtime((col("timestamp")+1465876799998)/1000))
               .withColumn("year", split(col("fullTime"),"-")[0])
               .withColumn("month", split(col("fullTime"),"-")[1])
               .withColumn("day",split( split(col("fullTime"),"-")[2]," ")[0])
               .withColumn("hour",split(split(split(col("fullTime"),"-")[2]," ")[1],":")[0])
               .withColumn("minute",split(split(split(col("fullTime"),"-")[2]," ")[1],":")[1])
               .withColumn("second",split(split(split(col("fullTime"),"-")[2]," ")[1],":")[2])
```

> Drop Redundant Columns

```
eventsUpdate = eventsUpdate.drop("timestamp").drop("fullTime").drop("state").drop("DMA")
```

> Replace Null with 0

```
eventsUpdate = eventsUpdate.na.fill("0")
```

> Test File 

```
eventsUpdate.show(5)
```

> Save Transformed File to HDFS

```
eventsUpdate.repartition(1).write.format('csv').mode('overwrite').save("/user/cloudera/outbrain/events_update")
```

## HIVE

> Create New Table with Updated Fields

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.events_update (
  display_id		INT,
  uuid        		STRING,
  document_id          	INT,
  platform      	TINYINT,
  country		STRING,
  year			SMALLINT,
  month			TINYINT,
  day			TINYINT,
  hour			TINYINT,
  minute		TINYINT,
  second		TINYINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/events_update";
```

**Congratulations, you have completed this assigment. It's time to move to the next step.**
