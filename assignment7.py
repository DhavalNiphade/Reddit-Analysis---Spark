"""Assignment 7: Spark

Please add your code where indicated by "YOUR CODE HERE". You may conduct a
superficial test of your code by executing this file in a python interpreter.

This assignment asks you to analyze a large number of social media posts and
calculate a number of summary statistics. You are encouraged to use pyspark for
this.

"""
import json,time, operator, pyspark
from datetime import datetime
from collections import Counter



# These are the files you will be working with. They contain reddit posts from
# 2014 and 2015 respectively.
#
# Each file is a sequence of JSON-encoded lines.
reddit_data_filenames = [
    '/nobackup/riddella/public/2015_reddit_comments_corpus/RC_2014-01.bz2',
    '/nobackup/riddella/public/2015_reddit_comments_corpus/RC_2015-01.bz2',
]


sc = pyspark.SparkContext()


def example_comments():
    """Two examples from the corpus.

    This function suggests how you can load individual records from the data.

    """
    contents = """{"ups":1,"author":"znb239","parent_id":"t3_1u4kbf","link_id":"t3_1u4kbf","edited":false,"subreddit":"AskReddit","author_flair_text":null,"controversiality":0,"created_utc":"1388534400","name":"t1_ceefsvy","subreddit_id":"t5_2qh1i","score_hidden":false,"author_flair_css_class":null,"archived":true,"body":"Yes, I won't get into what. But it went fine. Used PayPal, money was taken out. And then it showed up a few days later. No issues. Pm If you want details.","id":"ceefsvy","distinguished":null,"score":1,"gilded":0,"retrieved_on":1427910702,"downs":0}\n{"score_hidden":false,"subreddit_id":"t5_2cneq","name":"t1_ceefsvz","controversiality":0,"created_utc":"1388534400","retrieved_on":1427910702,"downs":0,"score":5,"gilded":0,"id":"ceefsvz","distinguished":null,"author_flair_css_class":null,"archived":true,"body":"What? Bloomberg alone way outspends the NRA.","link_id":"t3_1u1ajt","edited":false,"parent_id":"t1_cedt5zw","author":"diablo_man","ups":5,"author_flair_text":null,"subreddit":"politics"}"""
    return [json.loads(line) for line in contents.split('\n')]


def count_comments(filename):
    """Count the number of comments in the given filename."""
    # YOUR CODE HERE

    retval = 0
    lines = sc.textFile(filename)
    subred = lines.map(lambda line : json.loads(line)['subreddit']).countByValue().items()
    subredCount = Counter(dict(subred))

    for comments in subredCount:
        retval+=subredCount[comments]

    return retval

def top10_subreddits(filename):
    """Find the top 10 subreddits.

    Measure popularity by the number of comments.

    You might use this to compare the top subreddits at the beginning of 2014 and 2015.

    """
    # YOUR CODE HERE

    lines = sc.textFile(filename)
    subreddit_items = lines.map(lambda line: json.loads(line)['subreddit']).countByValue().items()
    subreddit_201401_counter = collections.Counter(dict(subreddit_items))
    subreddits = sorted(list(subreddit_201401_counter.values()))
    popular = []
    for i in range(1, 11):
        popular.append(subreddits[-i])
    return popular


def count_comments_by_hour(filename):
    """Count the number of comments in the given filename by hour.

    Returns a dictionary where hours are keys and values are comment counts.
    """
    # YOUR CODE HERE
    lines = sc.textFile(filename)
    commentsByHour = lines.map(lambda line: datetime.utcfromtimestamp(int(json.loads(line)['created_utc'])).hour).countByValue().items()
    return Counter(dict(commentsByHour))


def peak_reddit_hours(filename):
    """Return top 4 hours during which most redditors post.

    You likely want to use your results from `count_comments_by_hour`.

    Return a sequence of integers of length 4. Remember that hours are recorded in UTC time.

    """
    # YOUR CODE HERE
    commentsByHour = count_comments_by_hour(filename)
    sortedCBH = sorted(commentsByHour.items(),key=operator.itemgetter(1),reverse=True)

    peak_hours = []
    # print(time_dict)
    for i in commentsByHour:
        for j in range(1, 5):
            if (commentsByHour[i] == sortedCBH[-j]):
                peak_hours.append(i)
    # print(peak_hours)
    return peak_hours


# DO NOT EDIT CODE BELOW THIS LINE

import unittest


class TestAssignment7(unittest.TestCase):

    def test_count_comments(self):
        filename = '/nobackup/riddella/public/2015_reddit_comments_corpus/RC_2014-01.bz2'
        self.assertEqual(count_comments(filename), 42420655)

    def test_top10_subreddits(self):
        filename = '/nobackup/riddella/public/2015_reddit_comments_corpus/RC_2014-01.bz2'
        self.assertNotNone(top10_subreddits(filename))

    def test_count_comments_by_hour(self):
        filename = '/nobackup/riddella/public/2015_reddit_comments_corpus/RC_2014-01.bz2'
        results = count_comments_by_hour(filename)
        self.assertEqual(len(results), 24)
        self.assertEqual(sum(results.values()), 42420655)

    def test_peak_reddit_hours(self):
        filename = '/nobackup/riddella/public/2015_reddit_comments_corpus/RC_2014-01.bz2'
        results = peak_reddit_hours(filename)
        self.assertIn(13, results)


if __name__ == '__main__':
    unittest.main()
