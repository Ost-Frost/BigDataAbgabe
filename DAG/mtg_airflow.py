# -*- coding: utf-8 -*-

##
## IMPORTS
##
from datetime import datetime
import requests
import json
from pymongo import MongoClient
from pyhive import hive
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow'
}

##
## SQL Querys
##

hiveSQL_create_cards_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS cards(
    id STRING,
	name STRING,
    manaCost STRING,
	cmc FLOAT,
    colors ARRAY<STRING>,
	colorIdentity ARRAY<STRING>,
    type STRING,
    types ARRAY<STRING>,
    subtypes ARRAY<STRING>,
    rarity STRING,
    setName STRING,
	text STRING,
	flavor STRING,
	artist STRING,
    power STRING,
	toughness STRING,
	layout STRING,
    multiverseid STRING,
    imageUrl STRING,
    variations ARRAY<STRING>,
	printings ARRAY<STRING>,
	originalText STRING,
	originalType STRING,
	legalities ARRAY<STRUCT<format:STRING, legality:STRING>>,
	names ARRAY<STRING>
) PARTITIONED BY(partition_year INT, partition_month INT, partition_day INT) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/mtg/raw/cards';
'''

hiveSQL_create_foreign_cards_table='''
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
'''

hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

hiveSQL_add_partition_cards='''
ALTER TABLE cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

hiveSQL_add_partition_foreign_cards='''
ALTER TABLE foreign_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/foreignCards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

hiveSQL_create_cards_reduced='''
CREATE EXTERNAL TABLE IF NOT EXISTS cards_reduced (
    name STRING,
    multiverseid STRING,
    imageUrl STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/cards';
'''

hiveSQL_insertoverwrite_cards_reduced='''
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
'''

##
## Python Functions
##

# Starts a http get request to given url and returns the response object 
def getUrl(url):
    print("Page " + url[-3] + url[-2] + url[-1])
    return requests.get(url)


# format JSON output for hdfs
def toJSON(cards):
    for i in range(len(cards)):
        cards[i] = json.dumps(cards[i])
    cardsJson = ",\n".join(cards)
    return cardsJson

# Querys the mtg api to get all cards
def getMTGCards():

    # query the first page of cards to get the total amount of pages
    startPage = 1
    pageSize = 100
    firstPageResponse = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=" + str(pageSize) + str("&page=") + str(startPage))
    totalCount = firstPageResponse.headers["Total-Count"]
    totalPageCount = int((int(totalCount) / 100))
    cards = firstPageResponse.json()["cards"]
    foreignCards = get_foreign_cards(cards)

    # query the remaining pages. use thread pool for performance
    getUrls = []
    for i in range(2, totalPageCount):
        getUrls.append("https://api.magicthegathering.io/v1/cards?pageSize=100&page=" + str(i))

    responseList = []
    with ThreadPoolExecutor(max_workers=30) as pool:
        responseList = list(pool.map(getUrl,getUrls))
	
    # format response objects
    for response in responseList:
        responseCards = response.json()["cards"]
        foreignCards = foreignCards + get_foreign_cards(responseCards)
        cards = cards + response.json()["cards"]

    for i in range(len(cards)):
        if "foreignNames" in cards[i]:
            del cards[i]["foreignNames"]

    # write files
    print("writing to files...")
    cardsJson = toJSON(cards)
    text_file = open("/home/airflow/mtg/mtg_cards.json", "w")
    text_file.write(cardsJson)

    foreignCardsJson = toJSON(foreignCards)
    text_file = open("/home/airflow/mtg/mtg_foreign_cards.json", "w")
    text_file.write(foreignCardsJson)
    return

# get nested table foreign cards of a card object
def get_foreign_cards(cards): 
    foreign_cards = []
    for card in cards:
        if "foreignNames" in card:
            for foreign_card in card["foreignNames"]:
                foreign_card["cardid"] = card["id"]
                foreign_cards.append(foreign_card)
    return foreign_cards

# transfer reduced hive table data to mongodb
def transferDataFromHiveToMongoDB():
    hiveConnection = hive.Connection(
        host="hadoop", 
        port=10000,
        username="hadoop"
    )
    cursor = hiveConnection.cursor()
    query = "SELECT * FROM cards_reduced"
    cursor.execute(query)
    hiveData = cursor.fetchall()
    toMongoDB(hiveData)

# transfers hiveData to mongodb
def toMongoDB(hiveData):
    myclient = MongoClient("mongodb://mongodb-container:27017/")
    db = myclient["MTGCards"]
    Collection = db["mtgCards"]
    formattedData = []
    for card in hiveData:

        # format data and remove errors
        name = card[0]
        if name is None:
            name = "-",
        try:
            name = str(name)
        except:
            name = "-"

        id = card[1]
        if id is None:
            id = "-",
        try:
            id = int(id)
        except:
            id = "-"


        imageUrl = card[2]
        if imageUrl is None:
            imageUrl = ""
        try:
            imageUrl = str(imageUrl)
        except:
            # card background url
            imageUrl = "https://static.wikia.nocookie.net/mtgsalvation_gamepedia/images/f/f8/Magic_card_back.jpg/revision/latest/scale-to-width-down/250?cb=20140813141013"
        
        formattedCard = {
            "name": name,
            "id": id,
            "imageUrl": imageUrl
        }

        formattedData.append(formattedCard)
    Collection.drop()
    Collection.insert_many(formattedData) 

##
## DAG Operators
##
dag = DAG('MTG-API', default_args=args, description='MTG-API Import',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_directory',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_directory',
    directory='/home/airflow/mtg',
    pattern='*',
    dag=dag,
)

download_mtg_cards = PythonOperator(
    task_id='download_mtg_cards',
    python_callable = getMTGCards,
    op_kwargs = {},
    dag=dag
)

create_cards_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_cards_dir',
    directory='/user/hadoop/mtg/raw/cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_foreign_cards_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_foreign_cards_dir',
    directory='/user/hadoop/mtg/raw/foreignCards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_cards = HdfsPutFileOperator(
    task_id='upload_cards_to_hdfs',
    local_file='/home/airflow/mtg/mtg_cards.json',
    remote_file='/user/hadoop/mtg/raw/cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/mtg_cards.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_foreign_cards = HdfsPutFileOperator(
    task_id='upload_foreign_cards_to_hdfs',
    local_file='/home/airflow/mtg/mtg_foreign_cards.json',
    remote_file='/user/hadoop/mtg/raw/foreignCards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/mtg_foreign_cards.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

addPartition_HiveTable_cards = HiveOperator(
    task_id='add_partition_cards_table',
    hql=hiveSQL_add_partition_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

addPartition_HiveTable_foreign_cards = HiveOperator(
    task_id='add_partition_foreign_cards_table',
    hql=hiveSQL_add_partition_foreign_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

add_JAR_dependencies = HiveOperator(
    task_id='add_jar_dependencies',
    hql=hiveSQL_add_Jar_dependency,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_cards = HiveOperator(
    task_id='create_cards_table',
    hql=hiveSQL_create_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_foreign_cards = HiveOperator(
    task_id='create_foreign_cards_table',
    hql=hiveSQL_create_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

create_HiveTable_cards_reduced = HiveOperator(
    task_id='create_cards_reduced',
    hql=hiveSQL_create_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

hive_insert_overwrite_cards_reduced = HiveOperator(
    task_id='hive_write_cards_reduced_table',
    hql=hiveSQL_insertoverwrite_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

hive_to_mongodb = PythonOperator (
    task_id='hive_to_mongodb',
    python_callable = transferDataFromHiveToMongoDB,
    op_kwargs = {},
    dag=dag
)


##
## DAG Order
##
create_local_import_dir >> clear_local_import_dir >> download_mtg_cards
download_mtg_cards >> create_cards_dir >> hdfs_put_cards >> create_HiveTable_cards >> addPartition_HiveTable_cards >> dummy_op
download_mtg_cards >> create_foreign_cards_dir >> hdfs_put_foreign_cards >> create_HiveTable_foreign_cards >> addPartition_HiveTable_foreign_cards >>dummy_op
dummy_op >> create_HiveTable_cards_reduced >> add_JAR_dependencies >> hive_insert_overwrite_cards_reduced >> hive_to_mongodb