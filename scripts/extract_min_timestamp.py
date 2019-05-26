import argparse
import os
import sys

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

parser = argparse.ArgumentParser(description='Extract Minimum Timestamp from cleaned Post, Comment and Like stream')
parser.add_argument('--streamdir', help='directory of streams')

args = parser.parse_args()

with open(f"{args.streamdir}/post_event_stream_cleaned.csv", encoding='utf-8') as post_stream:
    header = post_stream.readline().strip().split("|")
    first_post = dict(zip(header, post_stream.readline().strip().split("|")))

with open(f"{args.streamdir}/comment_event_stream_cleaned.csv", encoding='utf-8') as comment_stream:
    header = comment_stream.readline().strip().split("|")
    first_comment = dict(zip(header, comment_stream.readline().strip().split("|")))

with open(f"{args.streamdir}/likes_event_stream_cleaned.csv", encoding='utf-8') as like_stream:
    header = like_stream.readline().strip().split("|")
    first_like = dict(zip(header, like_stream.readline().strip().split("|")))


min_timestamp = min(first_post['creationDate'], first_comment['creationDate'], first_like['creationDate'])

print(min_timestamp)
