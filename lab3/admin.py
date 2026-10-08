# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config = {
    'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %% Create the new topic and wait for the broker's answer
topic = 'book-lines'
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

for t, future in futures.items():
    try:
        future.result()
        print(f"Topic '{t}' created")
    except Exception as e:
        print(f"Topic '{t}' not created: {e}")

# %% List the topics
x = admin_client.list_topics(timeout=10)
for t in x.topics.keys():
    print(t)

# %%
#admin_client.delete_topics([topic])
