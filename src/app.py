import pymongo
from collections import OrderedDict
from flask import Flask, jsonify, request, render_template
import logging
from pymongo import MongoClient


# Cấu hình logger để tắt thông báo của Flask.
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

dataValues = []
categoryValues = []

# Kết nối tới MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['twitter_db']
collection = db['hashtags']

def get_top_players(data, n=20):
    """Get top n players by score.
    Returns a dictionary or an `OrderedDict` if `order` is true.
    """
    top = sorted(data.items(), key=lambda x: x[1], reverse=True)[:n]
    return OrderedDict(top)

@app.route("/")
def home():
    return render_template('index.html', dataValues=dataValues, categoryValues=categoryValues)

@app.route('/refreshData')
def refresh_data():
    global dataValues, categoryValues

    # Truy vấn dữ liệu từ MongoDB
    tags = {}
    for doc in collection.find():
        tags[doc['hashtag']] = doc['count']

    sorted_tags = get_top_players(tags)
    categoryValues = [x for x in sorted_tags]
    dataValues = [tags[x] for x in sorted_tags]

    return jsonify(dataValues=dataValues, categoryValues=categoryValues)

if __name__ == "__main__":
    app.run(host='localhost', port=5001, debug=True)

