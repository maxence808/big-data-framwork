# Kafka lab

A Project Gutenberg book is sent line by line to a Kafka topic, then consumed, cleaned and written to a new file.

- `admin.py`: creates the `book-lines` topic
- `producer.py`: downloads the book and sends each line to the topic
- `consumer.py`: reads the topic, cleans each line (lower case, punctuation and stopwords removed) and writes the result to `book_clean.txt`

## Run

Start Kafka:

```bash
docker run -p 9092:9092 apache/kafka-native:4.1.1
```

In another terminal, in a Python environment:

```bash
pip install confluent-kafka
python admin.py
python producer.py
python consumer.py
```
