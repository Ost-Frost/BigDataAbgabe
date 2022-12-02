CREATE EXTERNAL TABLE IF NOT EXISTS foreign_cards(
	name STRING,
    text STRING,
    type STRING,
    flavor STRING,
    imageUrl STRING,
    language STRING,
    multiverseid STRING,
    cardid STRING
) PARTITIONED BY(partition_year INT, partition_month INT, partition_day INT) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/mtg/raw/foreignCards';