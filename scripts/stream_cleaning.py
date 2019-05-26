import sys

from tqdm import tqdm

if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")

def main():
    print("Cleaning 1k User Streams:")
    clean_streams(post_stream_path="./data/1k-users-sorted/streams/post_event_stream.csv",
        comment_stream_path="./data/1k-users-sorted/streams/comment_event_stream.csv",
        like_stream_path="./data/1k-users-sorted/streams/likes_event_stream.csv",
        post_stream_cleaned_path="./data/1k-users-sorted/streams/post_event_stream_cleaned.csv",
        comment_stream_cleaned_path="./data/1k-users-sorted/streams/comment_event_stream_cleaned.csv",
        like_stream_cleaned_path="./data/1k-users-sorted/streams/likes_event_stream_cleaned.csv",
        n_posts=173402, n_comments=632043, n_likes=662891)

    print("\nCleaning 10k User Streams:")
    clean_streams(post_stream_path="./data/10k-users-sorted/streams/post_event_stream.csv",
        comment_stream_path="./data/10k-users-sorted/streams/comment_event_stream.csv",
        like_stream_path="./data/10k-users-sorted/streams/likes_event_stream.csv",
        post_stream_cleaned_path="./data/10k-users-sorted/streams/post_event_stream_cleaned.csv",
        comment_stream_cleaned_path="./data/10k-users-sorted/streams/comment_event_stream_cleaned.csv",
        like_stream_cleaned_path="./data/10k-users-sorted/streams/likes_event_stream_cleaned.csv",
        n_posts=5520842, n_comments=20096288, n_likes=21148771)

def clean_streams(post_stream_path, comment_stream_path, like_stream_path, post_stream_cleaned_path, comment_stream_cleaned_path, like_stream_cleaned_path, n_posts, n_comments, n_likes):

    comment_stream = open(comment_stream_path, encoding='mac-roman')
    post_stream = open(post_stream_path, encoding='mac-roman')
    like_stream = open(like_stream_path, encoding='mac-roman')

    post_stream_cleaned = open(post_stream_cleaned_path, 'w', encoding='utf-8', newline="\n")
    comment_stream_cleaned = open(comment_stream_cleaned_path, 'w', encoding='utf-8', newline="\n")
    like_stream_cleaned = open(like_stream_cleaned_path, 'w', encoding='utf-8', newline="\n")

    comment_header_line = comment_stream.readline()
    comment_header = comment_header_line.strip().split("|")

    post_header_line = post_stream.readline()
    post_header = post_header_line.strip().split("|")

    like_header_line = like_stream.readline()
    like_header = like_header_line.strip().split("|")

    post_stream_cleaned.write(f"{post_header_line}")
    comment_stream_cleaned.write(f"{comment_header_line}")
    like_stream_cleaned.write(f"{like_header_line}")

    comment_line = comment_stream.readline()
    comment = dict(zip(comment_header, comment_line.strip().split("|")))

    post_line = post_stream.readline()
    post = dict(zip(post_header, post_line.strip().split("|")))

    like_line = like_stream.readline()
    like = dict(zip(like_header, like_line.strip().split("|")))

    posts = set()
    comments = set()

    like_blacklist = []
    comment_blacklist = []
    reply_blacklist = []

    like_ok_count = 0
    comment_ok_count = 0
    reply_ok_count = 0

    print("Cleaning Event Streams...")
    pbar = tqdm(total=n_posts+n_comments+n_likes)
    while True:
        pbar.update(1)
        post_creation_date = None
        comment_creation_date = None
        like_creation_date = None

        if post and post['id']:
            post_creation_date = post['creationDate']
        if comment and comment['id']:
            comment_creation_date = comment['creationDate']
        if like and like['Person.id']:
            like_creation_date = like['creationDate']

            # break if all streams are at the end
        if not post_creation_date and not comment_creation_date and not like_creation_date:
            break

        min_creation_date = min(value for value in [post_creation_date, comment_creation_date, like_creation_date] if value is not None)

        if post_creation_date == min_creation_date:
            # process post and take next post
            posts.add(post['id'])
            post_stream_cleaned.write(f"{post_line}")

            post_line = post_stream.readline()
            post = dict(zip(post_header, post_line.strip().split("|")))


        elif comment_creation_date == min_creation_date:
            # process comment and take next comment
            if comment['reply_to_postId']:
                if comment['reply_to_postId'] not in posts:
                    comment_blacklist.append(comment['id'])
                else:
                    comment_ok_count += 1
                    comment_stream_cleaned.write(f"{comment_line}")
                    comments.add(comment['id'])

            elif comment['reply_to_commentId']:
                if comment['reply_to_commentId'] not in comments:
                    reply_blacklist.append(comment['id'])
                else:
                    reply_ok_count += 1
                    comment_stream_cleaned.write(f"{comment_line}")
                    comments.add(comment['id'])

            comment_line = comment_stream.readline()
            comment = dict(zip(comment_header, comment_line.strip().split("|")))
        else:
            # process like and take next like
            if like['Post.id'] not in posts:
                like_blacklist.append(like['Person.id'] + "_" + like['Post.id'])
            else:
                like_ok_count += 1
                like_stream_cleaned.write(f"{like_line}")
            like_line = like_stream.readline()
            like = dict(zip(like_header, like_line.strip().split("|")))

    pbar.close()
    comment_error_count = len(comment_blacklist)
    reply_error_count = len(reply_blacklist)
    like_error_count = len(like_blacklist)
    total_comment_error_count = comment_error_count + reply_error_count


    print("\n===================================================")
    print("Summary")
    print("===================================================")
    print(f"Written UTF-8 Version of Streams containing only events with timestamps respecting the order")
    print(f"    (Post before Like, Post before Top Level Comment, Comment before Reply)")

    print(f"Top Level Comment: ErrorCount = {comment_error_count} OkCount = {comment_ok_count}")
    print(f"Reply Comment: ErrorCount = {reply_error_count} OkCount = {reply_ok_count}")
    print(f"Comment Stream Discard Percentage: {total_comment_error_count/(comment_ok_count + reply_ok_count + total_comment_error_count):.2}")

    print(f"\nLike: ErrorCount = {like_error_count} OkCount = {like_ok_count}")
    print(f"Like Stream Discard Percentage: {like_error_count/(like_ok_count+like_error_count):.2}")

    comment_stream.close()
    post_stream.close()
    like_stream.close()

    post_stream_cleaned.close()
    comment_stream_cleaned.close()
    like_stream_cleaned.close()

if __name__ == "__main__":
    main()
