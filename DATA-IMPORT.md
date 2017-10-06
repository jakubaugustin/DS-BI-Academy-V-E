# DATA IMPORT

**Before starting with command line it is important to download data files 
from the following site** https://www.kaggle.com/c/outbrain-click-prediction. 

**These files are to be copied to your directory on the server and unziped. Then from the same directory open Terminal and use command line for the following exercises.**

## COMMAND LINE

**Before importing data into the HDFS, we must firstly create directory structure, where the data will be stored on HDFS.**

> Create Directory Structure on HDFS

```
$ hdfs dfs -mkdir outbrain
$ hdfs dfs -mkdir outbrain/page_views
$ hdfs dfs -mkdir outbrain/promoted_content
$ hdfs dfs -mkdir outbrain/submission
$ hdfs dfs -mkdir outbrain/events
$ hdfs dfs -mkdir outbrain/documents_topics
$ hdfs dfs -mkdir outbrain/documents_meta
$ hdfs dfs -mkdir outbrain/documents_entities
$ hdfs dfs -mkdir outbrain/documents_categories
$ hdfs dfs -mkdir outbrain/clicks_train
$ hdfs dfs -mkdir outbrain/clicks_test
```

**Now we can use the following commands to check if the directory structure exist. We can use the following command.**

```
$ hdfs dfs -ls outbrain
```

**Next step will be to load data into the prepared directories. Loading data is done by using the command put. These commands will move data from local directory on server to hdfs. **

> Copy Data From Local Repository to HDFS

```
$ hdfs dfs -put promoted_content.csv outbrain/promoted_content
$ hdfs dfs -put page_views_sample.csv outbrain/page_views
$ hdfs dfs -put sample_submission.csv outbrain
$ hdfs dfs -put events.csv outbrain/events
$ hdfs dfs -put documents_topics.csv outbrain/documents_topics
$ hdfs dfs -put documents_meta.csv outbrain/documents_meta
$ hdfs dfs -put documents_entities.csv outbrain/documents_entities
$ hdfs dfs -put documents_categories.csv outbrain/documents_categories
$ hdfs dfs -put clicks_train.csv outbrain/clicks_train
$ hdfs dfs -put clicks_test.csv outbrain/clicks_test
```

**Now to check if the data are in the hdfs. We can use the following commands.**

> Look inside the file in HDFS

```
$ hdfs dfs -cat outbrain/promoted_content/promoted_content.csv | head
```

**When the data are properly loaded into hdfs, we can use Hive to create "database" on them and be able to use SQL to access and work with the imported data.**

## HIVE

**We can access Hive with HUE (http://quickstart.cloudera:8888/hue/editor/?type=hive) or hive shell ($ hive) in command line. Here we are showing approach using command line.**

**Before working with the data we need to create database and tables from the files in hdfs directory. We will name the tables same way the directories are named.**

> Create Database outbrain

```
CREATE DATABASE IF NOT EXISTS outbrain;
```

> Create data Tables 

**CREATE EXTERNAL TABLE means that the table will not delete the data if the table is droped. This will keep the data in hdfs for further usage.**

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.page_views (
  uuid        		STRING,
  document_id          	INT,
  timestamp           	BIGINT,
  platform      	TINYINT,
  geo_location         	STRING,
  traffic_source       	TINYINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/page_views"
tblproperties("skip.header.line.count"="1");
```

**After declaring table columns and their variable types we then say how the file is formatted and where it is stored. Last line declares if the source file should skip the first line (header).**

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.promoted_content (
  ad_id		INT,
  document_id	INT,
  campaign_id	INT,
  advertiser_id	INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/promoted_content"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.events (
  display_id		INT,
  uuid			STRING,
  document_id		INT,
  timestamp		BIGINT,
  platform		TINYINT,
  geo_location		STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/events"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.documents_topics (
  document_id		INT,
  topic_id		INT,
  confidence_level	DECIMAL(17,17))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/documents_topics"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.documents_meta (
  document_id		INT,
  source_id		INT,
  publisher_id		INT,
  publish_time		STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/documents_meta"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.documents_entities (
  document_id		INT,
  entity_id		STRING,
  confidence_level	DECIMAL(17,17))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/documents_entities"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.documents_categories (
  document_id		INT,
  category_id		INT,
  confidence_level	DECIMAL(17,17))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/documents_categories"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.clicks_train (
  display_id		INT,
  ad_id			INT,
  clicked		TINYINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/clicks_train"
tblproperties("skip.header.line.count"="1");
```

```
CREATE EXTERNAL TABLE IF NOT EXISTS outbrain.clicks_test (
  display_id		INT,
  ad_id			INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION "/user/cloudera/outbrain/clicks_test"
tblproperties("skip.header.line.count"="1");
```

**When all tables are loaded into hive, we can test the tables if the data have imported properly. We need to get 5 rows from each of the created tables.**


> Test Table Contents

```
SELECT * FROM outbrain.clicks_test LIMIT 5;
SELECT * FROM outbrain.clicks_train LIMIT 5;
SELECT * FROM outbrain.documents_categories LIMIT 5;
SELECT * FROM outbrain.documents_meta LIMIT 5;
SELECT * FROM outbrain.documents_entities LIMIT 5;
SELECT * FROM outbrain.documents_topics LIMIT 5;
SELECT * FROM outbrain.events LIMIT 5;
SELECT * FROM outbrain.page_views LIMIT 5;
SELECT * FROM outbrain.promoted_content LIMIT 5;
```

**Congratulations you have succesfully imported all data files and you are ready for next assigment.**
