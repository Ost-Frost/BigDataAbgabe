ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
INSERT OVERWRITE TABLE cards_reduced
SELECT
    c.name,
    c.multiverseid,
    f.imageUrl
FROM
    cards c
    JOIN foreign_cards f ON (c.id = f.cardid)
WHERE
    f.language = "German"
    AND c.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and c.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and c.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND f.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and f.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and f.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}};