# %% Dependencies
import argparse
import os
import socket
import json
from confluent_kafka import Producer
from datetime import datetime, timedelta, timezone

# pywikibot looks for a user-config.py file; we only need EventStreams
os.environ.setdefault('PYWIKIBOT_NO_USER_CONFIG', '1')
from pywikibot.comms.eventstreams import EventStreams

# %% Command line options (filters at the producer level)
# Examples:
#   python wikistream_producer.py                                  -> fr.wikipedia edits, 10 minutes
#   python wikistream_producer.py --wiki en.wikipedia.org de.wikipedia.org
#   python wikistream_producer.py --title Paris "Emmanuel Macron"  -> only these pages
#   python wikistream_producer.py --humans-only --namespace 0      -> human edits on articles
#   python wikistream_producer.py --type edit new --minutes 5 --since-minutes 30
parser = argparse.ArgumentParser(description='Send Wikimedia recent changes to Kafka')
parser.add_argument('--wiki', nargs='+', default=['fr.wikipedia.org'],
                    help='server_name(s) to follow (default: fr.wikipedia.org)')
parser.add_argument('--type', nargs='+', default=['edit'],
                    help='change type(s): edit, new, log, categorize (default: edit)')
parser.add_argument('--title', nargs='+', default=None,
                    help='only follow these page titles')
parser.add_argument('--namespace', nargs='+', type=int, default=None,
                    help='only follow these namespaces (0 = articles)')
bots = parser.add_mutually_exclusive_group()
bots.add_argument('--humans-only', action='store_true', help='drop bot edits')
bots.add_argument('--bots-only', action='store_true', help='keep only bot edits')
parser.add_argument('--minutes', type=float, default=10,
                    help='streaming duration in minutes (default: 10)')
parser.add_argument('--since-minutes', type=float, default=None,
                    help='replay the events of the last N minutes before going live')
parser.add_argument('--topic', default='wikistreams', help='Kafka topic (default: wikistreams)')
parser.add_argument('--quiet', action='store_true', help='do not print every event')
args = parser.parse_args()

# %% Helper Functions
# Serializer function to change message from python dict to json
value_serializer = lambda val: json.dumps(val).encode('utf-8')

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')

# %% Producer Instantiation
# Set the producer configuration
# localhost:9092 from the host, kafka:19092 from a container (see compose.yaml)
conf = {'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'),
        'client.id': socket.gethostname(),
        'compression.type': 'lz4'
}

# Instantiate the producer
producer = Producer(conf)

# %% Create wikistreams query
# Only the recentchange stream is used: its schema (title, user, bot, length...)
# is the one parsed by the Spark notebook
since = None
if args.since_minutes:
    since = (datetime.now(timezone.utc) - timedelta(minutes=args.since_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ')
stream = EventStreams(streams=['recentchange'], since=since)

# Every registered filter must match (ftype='all')
stream.register_filter(server_name=args.wiki, type=args.type)
if args.title:
    stream.register_filter(title=args.title)
if args.namespace is not None:
    stream.register_filter(namespace=args.namespace)
if args.humans_only:
    stream.register_filter(bot=False)
if args.bots_only:
    stream.register_filter(bot=True)

print(f'Following {args.wiki} | types={args.type} | titles={args.title} | '
      f'namespaces={args.namespace} | humans_only={args.humans_only} | '
      f'bots_only={args.bots_only} -> topic "{args.topic}"')

# %% Query EventStream
# Run a single query and inspect raw and example formatted output
change = next(stream)
print('Raw Message: ' + str(change))
print(
  '\n' # Add line break between outputs
  'Formatted Message: {type} on page "{title}" by "{user}" at {meta[dt]}.'
  .format(**change)
)

# %% Streaming Query
duration = args.minutes # Streaming window in minutes
start_time = datetime.now() # Current clock time
stop_time = start_time + timedelta(minutes=duration) #start+duration=stop

# Query stream for <duration> minutes
sent = 0
try:
  while datetime.now() < stop_time:
    change = next(stream)
    producer.produce(
      topic=args.topic,
      key=change.get('wiki'),
      value=value_serializer(change),
      callback=delivery_report
    )
    producer.poll(0) # serve the delivery callbacks
    sent += 1
    if not args.quiet:
      length = change.get('length') or {}
      delta = (length.get('new') or 0) - (length.get('old') or 0)
      print(f"[{change['meta']['dt']}] {change['wiki']} | bot={change['bot']} | "
            f"{delta:+d} bytes | {change['title']} by {change['user']}")
except KeyboardInterrupt:
  print('\n stopped by user')

# Wait for the last messages and close the producer
print(f'\n {sent} events sent, waiting for the last messages')
producer.flush(60)
print('\n producer closed')
