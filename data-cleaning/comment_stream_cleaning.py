from tqdm import tqdm

# input
comment_stream_1k_path = "./../data/1k-users-sorted/streams/comment_event_stream.csv"
comment_stream_10k_path = "./../data/10k-users-sorted/streams/comment_event_stream.csv"

# for tqdm progress
n_comments_1k = 632042
n_comments_10k = 20096288

# output
blacklist_1k_path = "./../data/1k-users-sorted/streams/comment_blacklist.csv"
cleaned_comment_stream_1k_path = "./../data/1k-users-sorted/streams/comment_event_stream_cleaned.csv"

blacklist_10k_path = "./../data/10k-users-sorted/streams/comment_blacklist.csv"
cleaned_comment_stream_10k_path = "./../data/10k-users-sorted/streams/comment_event_stream_cleaned.csv"


def build_blacklist(comment_stream_file, stream_length):
    d = {}
    blacklist = set()

    with open(comment_stream_file, encoding='mac-roman') as comment_stream:
        header = comment_stream.readline().split("|")

        for line in tqdm(comment_stream, total=stream_length):
            event = dict(zip(header, line.split("|")))
            if event["reply_to_commentId"]:
                parent_id = event["reply_to_commentId"]

                if parent_id in d:
                    # take postId from parent
                    d[event["id"]] = d[parent_id]
                else:
                    # parent not there => wrong ordering
                    blacklist.add(event["id"])

            else: # root comment
                d[event["id"]] = event["reply_to_postId"]

    print(f"   Blacklist: {len(blacklist)}")
    print(f"   Whitelist: {len(d)}")

    print(f"   Percentage Blacklist: {len(blacklist)/(len(blacklist) + len(d)):.2f}")

    return blacklist

def write_output(header, output, output_path):
    with open(output_path, 'w') as f:
        f.write(f"{header}\n")
        for item in output:
            f.write(f"{item}\n")

def clean_comment_stream(comment_stream_file, stream_length, blacklist, output_path):
    with open(output_path, 'w', encoding='mac-roman') as f:
        with open(comment_stream_file, encoding='mac-roman') as comment_stream:
            header = comment_stream.readline()
            f.write(f"{header}")
            for line in tqdm(comment_stream, total=stream_length):
                comment_id = line.split("|")[0]

                if comment_id not in blacklist:
                    f.write(f"{line}")




# write output files
print("Building Blacklist 1k...")
blacklist_1k = build_blacklist(comment_stream_file=comment_stream_1k_path, stream_length=n_comments_1k)



print(f"Writing Blacklist 1k to: {blacklist_1k_path}")
write_output(header="commentId", output=blacklist_1k, output_path=blacklist_1k_path)

print(f"Writing Cleaned Comment Stream 1k to: {blacklist_1k_path}")
clean_comment_stream(comment_stream_file=comment_stream_1k_path,
                     stream_length=n_comments_1k,
                     blacklist=blacklist_1k,
                     output_path=cleaned_comment_stream_1k_path)



print("Building Blacklist 10k...")
blacklist_10k = build_blacklist(comment_stream_file=comment_stream_10k_path, stream_length=n_comments_10k)

print(f"Writing Blacklist 10k to: {blacklist_10k_path}")
write_output(header="commentId", output=blacklist_10k, output_path=blacklist_10k_path)

print(f"Writing Cleaned Comment Stream 10k to: {blacklist_10k_path}")
clean_comment_stream(comment_stream_file=comment_stream_10k_path,
                     stream_length=n_comments_10k,
                     blacklist=blacklist_10k,
                     output_path=cleaned_comment_stream_10k_path)
