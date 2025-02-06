import time
import random
import json
from kafka import KafkaProducer

# Khởi tạo Kafka producer
producer = KafkaProducer(bootstrap_servers='172.16.0.209:9092,172.16.0.209:9093,172.16.0.209:9094')

# Danh sách các hashtag và các mẫu văn bản tương ứng
hashtag_text_mapping = {
    "apple": [
        "Just tried out the new iPhone! It's amazing!",
        "Excited to hear about the latest updates from Apple.",
        "Looking forward to the Apple event next week!",
        "Working on a new app for iOS. #apple"
    ],
    "java": [
        "Spent the day coding in Java. #java",
        "Java programming is both challenging and rewarding.",
        "Learning Java is essential for any software developer.",
        "Can't wait to explore more Java libraries. #programming"
    ],
    "github": [
        "Just pushed some new changes to my GitHub repository.",
        "Collaborating with others on GitHub is so convenient.",
        "Learning version control with Git and GitHub. #github",
        "Exploring open source projects on GitHub."
    ],
    "python": [
        "Python is my favorite programming language!",
        "Just finished a Python project. #python",
        "Exploring Python libraries for data science.",
        "Python makes web development so much fun! #webdev"
    ],
    "Russia": [
        "Interesting news coming out of Russia today.",
        "Exploring Russian culture and history.",
        "Visiting Russia has always been on my bucket list.",
        "What are your thoughts on the situation in Russia? #discussion"
    ],
    "Ukraina": [
        "The situation in Ukraine is concerning.",
        "Sending thoughts and prayers to Ukraine.",
        "Discussing the Ukraine crisis with friends. #ukrainecrisis",
        "Ukraine's history and culture are fascinating."
    ],
    "ElonMusk": [
        "Elon Musk's latest tweets are always intriguing. #ElonMusk",
        "Following Elon Musk's SpaceX developments. #ElonMusk",
        "Elon Musk's vision for the future is inspiring. #ElonMusk",
        "What will Elon Musk do next? #speculation. #ElonMusk"
    ],
    "Samsum": [
        "Just got a new Samsung phone. #samsung",
        "Exploring Samsung's latest innovations. #samsung",
        "Samsung's products never disappoint. #samsung",
        "Comparing Samsung and Apple products. #samsung"
    ],
    "Putin": [
        "Vladimir Putin's latest speech is causing controversy.",
        "Discussing Russian politics and Vladimir Putin. #politics",
        "Putin's influence on world affairs is undeniable.",
        "Analyzing Putin's leadership style. #leadership"
    ],
    "JoeBiden": [
        "President Joe Biden's latest policies are under scrutiny.",
        "Thoughts on Joe Biden's presidency so far? #discussion",
        "Joe Biden's infrastructure plan is ambitious.",
        "Following Joe Biden's diplomatic efforts. #diplomacy"
    ],
    "NATO": [
        "NATO's role in global security is vital.",
        "Discussing NATO's future. #security",
        "NATO's response to recent events is being debated.",
        "Why is NATO important? #discussion"
    ]
}

# Danh sách tên người dùng
user_names = ['John Doe', 'Jane Smith', 'Michael Johnson', 'Emily Davis', 'David Brown']

# Hàm tạo văn bản tweet phù hợp với hashtag được chọn
def generate_tweet_text(hashtag):
    if hashtag in hashtag_text_mapping:
        return random.choice(hashtag_text_mapping[hashtag])
    else:
        return f"Exploring new topics. #{hashtag}"

# Hàm tạo dữ liệu ngẫu nhiên
def generate_random_data():
    tweet_id = random.randint(1000000000000000000, 9999999999999999999)
    user_id = random.randint(100000, 999999)
    followers_count = random.randint(100, 10000)
    friends_count = random.randint(50, 500)
    hashtags = ["apple", "java", "github", "python", "Russia","Ukraina","ElonMusk","Samsum","Putin","JoeBiden","NATO"]
    random_hashtag = random.choice(hashtags)
    random_user_name = random.choice(user_names)  # Chọn tên người dùng ngẫu nhiên

    tweet = {
        "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
        "id": tweet_id,
        "text": generate_tweet_text(random_hashtag),
        "user": {
            "id": user_id,
            "screen_name": f"example_user_{user_id}",
            "name": random_user_name,  # Sử dụng tên người dùng ngẫu nhiên
            "followers_count": followers_count,
            "friends_count": friends_count
        },
        "entities": {
            "hashtags": [
                {
                    "text": random_hashtag,
                    "indices": [20, 26]
                }
            ],
            "user_mentions": [],
            "urls": []
        },
        "favorite_count": random.randint(0, 100),  # Lượt thích ngẫu nhiên
        "retweet_count": random.randint(0, 50),  # Lượt không thích ngẫu nhiên
        "extended_entities": {
            "media": [
                {
                    "type": "photo",
                    "media_url": "https://example.com/image.jpg"
                }
            ]
        },
        "comments": generate_random_comments(),  # Bình luận ngẫu nhiên
        "created_date": time.strftime("%Y-%m-%d")  # Ngày tạo tweet
    }

    return tweet

# Hàm tạo bình luận ngẫu nhiên
def generate_random_comments():
    comments = []
    num_comments = random.randint(1, 3)

    for _ in range(num_comments):
        comment = {
            "user_id": random.randint(100000, 999999),
            "user_name": "Anonymous",
            "comment_text": generate_comment_text()
        }
        comments.append(comment)

    return comments

# Hàm tạo nội dung bình luận ngẫu nhiên theo kiểu tích cực, tiêu cực và trung lập
def generate_comment_text():
    sentiment = random.choice(["positive", "negative", "neutral"])

    if sentiment == "positive":
        return random.choice([
            "Great post!",
            "I disagree with you.",
            "Interesting perspective.",
            "Thanks for sharing!",
            "I totally agree!",
            "Well said!",
            "I have a different opinion.",
            "Can you provide more information?",
            "This is inspiring.",
            "I'm not sure about that."
        ])
    elif sentiment == "negative":
        return random.choice([
            "I disagree with you.",
            "This is not accurate.",
            "Poorly argued."
        ])
    else:
        return random.choice([
            "This is not convincing at all.",
            "I strongly disagree with your point of view.",
            "Your argument is flawed.",
            "I find this post to be misleading.",
            "What a terrible post!",
            "I can't believe you think that way.",
            "This is a complete waste of time.",
            "I'm appalled by the content of this tweet.",
            "You couldn't be more wrong.",
            "This is utter nonsense."
        ])

count = 0

# Vòng lặp liên tục
while True:
    random_tweet = generate_random_data()
    tweet_json = json.dumps(random_tweet).encode('utf-8')
    
    # Gửi dữ liệu vào Kafka topic
    producer.send('topic_twitter', value=tweet_json)
    producer.flush()
    count += 1

    # Hiển thị thông báo đếm
    print(f"amount of data put into kafka: {count}")

    time.sleep(2)

