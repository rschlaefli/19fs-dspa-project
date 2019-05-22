from datetime import datetime
from datetime import timedelta

import csv
from tqdm import tqdm

def main():

    print("Calculating Test Stream User Stats:")
    calc_person_activity_stats(post_stream_path="./social-network-analysis/src/test/java/resources/post_event_stream.csv",
        comment_stream_path="./social-network-analysis/src/test/java/resources/comment_event_stream.csv",
        like_stream_path="./social-network-analysis/src/test/java/resources/likes_event_stream.csv",
        output_file="./data/test_stream_user_activity_stats.csv",
        n_posts=66, n_comments=136, n_likes=47)

    print("Calculating 1k User Stats:")
    calc_person_activity_stats(post_stream_path="./data/1k-users-sorted/streams/post_event_stream_cleaned.csv",
        comment_stream_path="./data/1k-users-sorted/streams/comment_event_stream_cleaned.csv",
        like_stream_path="./data/1k-users-sorted/streams/likes_event_stream_cleaned.csv",
        output_file="./data/1kuser_activity_stats.csv",
        n_posts=173402, n_comments=543469, n_likes=651172)
        
    print("\nCalculating 10k User Stats:")
    calc_person_activity_stats(post_stream_path="./data/10k-users-sorted/streams/post_event_stream_cleaned.csv",
        comment_stream_path="./data/10k-users-sorted/streams/comment_event_stream_cleaned.csv",
        like_stream_path="./data/10k-users-sorted/streams/likes_event_stream_cleaned.csv",
        output_file="./data/10kuser_activity_stats.csv",
        n_posts=5520842, n_comments=17246965, n_likes=20770737)

def calc_person_activity_stats(post_stream_path, comment_stream_path, like_stream_path, output_file, n_posts, n_comments, n_likes):
    person_interactions = []

    post_stream = open(post_stream_path, 'r', encoding='utf-8')
    post_header_line = post_stream.readline()
    post_header = post_header_line.strip().split("|")

    print("Processing Post Stream...")
    for post_line in tqdm(post_stream, total=n_posts):
        post = dict(zip(post_header, post_line.strip().split("|")))
        timestamp = datetime.strptime(post['creationDate'], '%Y-%m-%dT%H:%M:%SZ')
        interaction = {"personId": post['personId'], "timestamp": timestamp, "type": "post"}
        person_interactions.append(interaction)

    post_stream.close()

    comment_stream = open(comment_stream_path, 'r', encoding='utf-8')
    comment_header_line = comment_stream.readline()
    comment_header = comment_header_line.strip().split("|")

    print("Processing Comment Stream...")
    for comment_line in tqdm(comment_stream, total=n_comments):
        comment = dict(zip(comment_header, comment_line.strip().split("|")))
        timestamp = datetime.strptime(comment['creationDate'], '%Y-%m-%dT%H:%M:%SZ')
        interaction = {"personId": comment['personId'], "timestamp": timestamp, "type": "comment"}
        person_interactions.append(interaction)

    comment_stream.close()

    like_stream = open(like_stream_path, 'r', encoding='utf-8')
    like_header_line = like_stream.readline()
    like_header = like_header_line.strip().split("|")

    print("Processing Likes Stream...")
    for like_line in tqdm(like_stream, total=n_likes):
        like = dict(zip(like_header, like_line.strip().split("|")))
        timestamp = datetime.strptime(like['creationDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
        interaction = {"personId": like['Person.id'], "timestamp": timestamp, "type": "like"}
        person_interactions.append(interaction)

    like_stream.close()

    print("Sorting Interactions...")
    person_interactions = sorted(person_interactions, key = lambda i: i['timestamp']) 


    persons = dict()


    def init_person():
        person = {}
        person['post'] = 0
        person['comment'] = 0
        person['like'] = 0
        person['total_count'] = 0
        person['max_delta'] = timedelta()
        person['last_person_event'] = first_event['timestamp']
        return person

    # init all with first event
    first_event = person_interactions[0]
        
    print("Building Person Activity Statistics...")
    for event in tqdm(person_interactions):
        if event['personId'] not in persons:
            persons[event['personId']] = init_person()

        # increase event type count
        persons[event['personId']][event['type']] += 1
        # increase event total count
        persons[event['personId']]['total_count'] += 1

        # find max delta between user interactions
        last_person_event = persons[event['personId']]['last_person_event']
        delta = event['timestamp'] - last_person_event
        if delta > persons[event['personId']]['max_delta']:
            persons[event['personId']]['max_delta'] = delta
            persons[event['personId']]['max_delta_period_start'] = last_person_event
            persons[event['personId']]['max_delta_period_end'] = event['timestamp']
        
        # update last event
        persons[event['personId']]['last_person_event'] = event['timestamp']

    # update max_delta with last event of stream
    person_list = []
    last_event = person_interactions[-1]
    for personId, person in persons.items():
        last_person_event = person['last_person_event']
        delta = last_event['timestamp'] - last_person_event
        if delta > person['max_delta']:
            person['max_delta'] = delta
            person['max_delta_period_start'] = last_person_event
            person['max_delta_period_end'] = event['timestamp']
        
        person['id'] = personId
        person_list.append(person)

    print("Sorting Person Activity Statistics...")
    person_list = sorted(person_list, key = lambda i: i['max_delta']) 

    print("Writing Person Activity Statistics to CSV...")
    keys = person_list[0].keys()
    with open(output_file, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(person_list)

if __name__ == "__main__":
    main()