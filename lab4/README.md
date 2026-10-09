# Lab 4 - Structured Streaming

The Wikimedia `recentchange` events (fr.wikipedia.org edits by default) are sent to the Kafka topic `wikistreams`, then processed with Spark Structured Streaming in a Jupyter notebook.

- `compose.yaml`: Kafka, the PySpark notebook and a `producer` service
- `admin.py`: creates the `wikistreams` topic
- `wikistream_producer.py`: reads the Wikimedia stream and sends each event to the topic, with filters on the command line
- `notebooks/wikistream_pyspark.ipynb`: the Spark consumer (demo + explorations with tumbling and sliding windows)
- `data/`: parquet output of the notebook, `jobs/`: empty
- `requirements.txt`: Python packages of the producer

## Run

1. Start Kafka and the notebook:

```bash
docker compose up -d
```

2. Get the Jupyter link (with the token) in the logs and open it:

```bash
docker logs pyspark_notebook
```

3. Create the topic and start the producer (10 minutes by default). It runs in a Docker container, so nothing has to be installed on the host:

```bash
docker compose run --rm producer
```

4. In Jupyter, open `work/wikistream_pyspark.ipynb` and run all the cells **except the last one** (it stops the queries).

5. Follow the output of the queries:

```bash
docker logs -f pyspark_notebook
```

6. At the end, run the last cell of the notebook, then:

```bash
docker compose down
```

### Run the producer on the host instead

Kafka is also exposed on `localhost:9092`:

```bash
pip install -r requirements.txt
python admin.py
python wikistream_producer.py
```

On this PC, Windows Smart App Control blocks the `confluent_kafka` DLL ("Une stratégie de contrôle d'application a bloqué ce fichier"), which is why the producer runs in Docker by default.

## Producer filters

The filters are applied in the producer, so only the selected events reach Kafka. They can be combined; the arguments after `producer` are passed to the script.

| Option | Effect | Example |
|--------|--------|---------|
| `--wiki` | wikis to follow (default `fr.wikipedia.org`) | `--wiki en.wikipedia.org de.wikipedia.org` |
| `--type` | change types (default `edit`) | `--type edit new` |
| `--title` | only these pages | `--title Paris "Emmanuel Macron"` |
| `--namespace` | only these namespaces (0 = articles) | `--namespace 0` |
| `--humans-only` / `--bots-only` | drop / keep only bot edits | `--humans-only` |
| `--minutes` | duration of the stream (default 10) | `--minutes 30` |
| `--since-minutes` | replay the last N minutes first | `--since-minutes 15` |
| `--topic` | Kafka topic (default `wikistreams`) | `--topic wiki-en` |
| `--quiet` | do not print every event | |

```bash
docker compose run --rm producer --wiki fr.wikipedia.org en.wikipedia.org --humans-only --namespace 0
```

## Streaming queries in the notebook

| # | Query | Window | Output mode |
|---|-------|--------|-------------|
| 0 | Demo: edits by bot / human | tumbling 1 hour | complete |
| 1 | Activity per minute: edits, bot and minor edits, users, pages, bytes added / removed, average and biggest edit | **tumbling** 1 min, watermark 2 min | update |
| 2 | Most edited pages | **sliding** 10 min every 2 min (overlapping) | complete |
| 3 | Most active human users | **sliding** 5 min every 1 min (overlapping) | complete |
| 4 | Edit size categories (removal, small, medium, large) | **tumbling** 2 min, watermark 2 min | update |
| 5 | Activity per wiki (with several `--wiki`) | **tumbling** 1 min, watermark 2 min | update |
| 6 | Alerts on edits of 5 000 bytes or more, also saved in parquet in `data/big_edits` | none (stateless) | append |

Each table printed in the logs starts with a `query` column to tell the queries apart.

- **Tumbling windows** do not overlap: each edit is counted once.
- **Sliding windows** overlap: with 10 min every 2 min, each edit is counted in 5 windows, which smooths the ranking.
- The **watermark** lets Spark drop the state of the windows older than 2 minutes; in `update` mode only the windows changed by the micro-batch are printed. Sorting (pages, users) is only possible in `complete` mode.
- The Kafka connector version is taken from `pyspark.__version__`: the `jupyter/pyspark-notebook` image now ships Spark 4.2.0, and the 4.1.0 connector of the original demo fails with a `NoSuchMethodError`.
