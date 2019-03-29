import os
import sys
import time
import re
from typing import List, Tuple

# Result
# 0 user
# 1: content
def parse_tweets(tweet_line: str) -> List[str]:
    tokens = [re.sub(r'\s+', r' ', t.replace('"', '').strip()) for t in tweet_line.split('","')]
    return tokens[4:]
    
with open("./data/tweets.csv", "r") as tweets:
    f = open("./logs/tweet.log", "w+")
    for line in tweets:
        result = parse_tweets(line)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        tweet = time_str + "\t" + result[0] + "\t" + result[1]
        f.write(tweet + "\n")
        time.sleep(0.5)
        f.flush()

        