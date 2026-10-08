# %%
import re
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-cleaner',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False}  # read the topic from the start on every run

consumer = Consumer(conf)

# %%
topic = 'book-lines'
consumer.subscribe([topic])

# %% Text cleaning (same steps as in the word count lab)
STOPWORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'if', 'of', 'to', 'in', 'on', 'at', 'by',
    'for', 'with', 'from', 'as', 'is', 'are', 'was', 'were', 'be', 'been', 'am',
    'it', 'its', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'we',
    'they', 'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his', 'our', 'their',
    'not', 'no', 'so', 'do', 'did', 'does', 'have', 'has', 'had', 'will', 'would',
    'can', 'could', 'which', 'who', 'what', 'when', 'there',
}


def clean(line):
    line = line.lower()                    # lower case
    line = re.sub(r"['’]", '', line)       # don't -> dont
    line = re.sub(r'[^a-z\s]', ' ', line)  # remove punctuation and digits
    return [w for w in line.split() if w not in STOPWORDS]  # remove stopwords


# %%
# Configuration
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0
received = 0
output_file = 'book_clean.txt'

with open(output_file, 'w', encoding='utf-8') as out:
    while True:
        msg = consumer.poll(1.0)

        # 1. Handle "No Message" (Timeout)
        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        # 2. Handle Errors
        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        # 3. Handle Success
        # Reset counters when we actually get data
        empty_polls = 0
        error_count = 0
        received += 1

        # Clean the message and write it to the output file
        words = clean(msg.value().decode('utf-8'))
        if words:
            out.write(' '.join(words) + '\n')

# Clean up
consumer.close()
print(f"{received} messages received, cleaned text written to {output_file}")
