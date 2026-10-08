# %%
import os
import socket
import urllib.request
from confluent_kafka import Producer

# %% Download the book from Project Gutenberg (only if not already there)
book_url = 'https://www.gutenberg.org/cache/epub/1342/pg1342.txt'  # Pride and Prejudice
book_file = 'book.txt'

if not os.path.exists(book_file):
    urllib.request.urlretrieve(book_url, book_file)

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %% Read the book line by line and send each line to the topic
topic = 'book-lines'
sent = 0

with open(book_file, encoding='utf-8-sig') as f:
    for line in f:
        line = line.strip()
        if not line:  # skip blank lines
            continue
        producer.produce(topic=topic, value=line)
        producer.poll(0)  # serve delivery reports and free the local queue
        sent += 1

producer.flush()
print(f"{sent} lines sent to '{topic}'")
