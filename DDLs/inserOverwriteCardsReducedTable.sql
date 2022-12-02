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
    f.language = "German";