# DATA PREPARATION

** Before starting with command line it is important to download data files **
from the following site https://www.kaggle.com/c/outbrain-click-prediction. 

** These files are to be copied to your directory on the server. **

## COMMAND LINE

** In your directory open terminal window. **
** Then run the following commands **

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

## HIVE

** Open Hive with HUE (http://quickstart.cloudera:8888/hue/editor/?type=hive) or hive shell ($ hive)

> Create Database outbrain
CREATE DATABASE IF NOT EXISTS outbrain;


> Create Table as External (Hive will read from selected folder data as table, but the data will stay if table deleted)

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

** Congratulations you have succesfully imported all data files and you are ready for next assigment. **
