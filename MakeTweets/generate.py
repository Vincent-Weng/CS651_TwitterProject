import os
import sys
import time
import re
from typing import List, Tuple
import random

# Result
# 0: user
# 1: content
# 2: age
# 3: gender
def parse_tweets(tweet_line: str) -> List[str]:
    tokens = [re.sub(r'\s+', r' ', t.replace('"', '').strip()) for t in tweet_line.split('","')]
    return tokens[4:]
    
with open("./data/tweets.csv", "r") as tweets:
    f = open("./logs/tweet.log", "w+",encoding="ISO-8859-1")
    for line in tweets:
        try:
            result = parse_tweets(line)
            age = random.randint(15,60)
            gender = random.sample(["f","m","o"],1)[0]
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            tweet = time_str + "\t" + result[0] + "\t" + result[1] + "\t" + str(age)+ "\t" + gender
            f.write(tweet + "\n")
            time.sleep(0.001)
            f.flush()
        except:
            continue


        