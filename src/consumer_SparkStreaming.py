import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import json
from kafka import KafkaConsumer
from collections import Counter
import pymongo
from pymongo import MongoClient

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

# Thêm các thông tin cần thiết để kết nối tới MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['twitter_db']
collection = db['hashtags']

def process_tweet(tweet):
    tweet_data = json.loads(tweet.value)
    entities = tweet_data.get("entities", {})
    hashtags = entities.get("hashtags", [])
    
    hashtag_texts = [hashtag.get("text") for hashtag in hashtags]
    hashtag_counts = Counter(hashtag_texts)
    
    for hashtag, count in hashtag_counts.items():
        existing_hashtag = collection.find_one({"hashtag": hashtag})
        if existing_hashtag:
            collection.update_one({"_id": existing_hashtag["_id"]}, {"$inc": {"count": count}})
            print(f"Data updated in MongoDB for hashtag: {hashtag}. Total count: {existing_hashtag['count'] + count}")
        else:
            tags = {
                "hashtag": hashtag,
                "count": count
            }
            collection.insert_one(tags)
            print(f"Data inserted into MongoDB for hashtag: {hashtag}. Total count: {count}")


def stream_tweets():
    spark = SparkSession.builder.appName("SparkTwitterAnalysis").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    consumer = KafkaConsumer(
        'topic_twitter',
        bootstrap_servers='172.16.0.209:9092,172.16.0.209:9093,172.16.0.209:9094',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    
    for tweet in consumer:
        process_tweet(tweet)
    
    consumer.close()

if __name__ == '__main__':
    try:
        stream_tweets()
    except KeyboardInterrupt:
        exit("Keyboard Interrupt, Exiting..")
    except Exception as e:
        exit("Error in Spark App")
