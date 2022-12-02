CREATE EXTERNAL TABLE IF NOT EXISTS cards_reduced (
    name STRING,
    multiverseid STRING,
    imageUrl STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/cards';