import json
import asyncio
from aiokafka import AIOKafkaProducer
from faker import Faker
import csv
import random
import time

# Initialize Faker library
fake = Faker()

# Kafka topic
topic = 'spotify-testing'

# Kafka producer serializer function
def serializer(value):
    return json.dumps(value).encode()

# Function to read songs from CSV file
def read_songs_from_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        songs = [row['name'] for row in reader]
    return songs

# Function to generate a random song
def generate_random_song(songs):
    return random.choice(songs)

# Function to produce Kafka messages
async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=serializer,
        compression_type="gzip")

    await producer.start()

    # Read songs from CSV file
    songs = read_songs_from_csv('spotify-songs.csv')


    generated = False

    while True:
        if not antonis_faros_generated:
            listener_name = "Vyronas Katsos"
            generated = True
        else:
            listener_name = fake.name()

        # Generate a random song
        song_name = generate_random_song(songs)

        # Get current time
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')

        # Prepare message payload
        data = {"listener": listener_name, "song": song_name, "time": current_time}

        # Send message to Kafka
        await producer.send(topic, data)

        # Sleep for random time between 10 and 20 seconds
        await asyncio.sleep(random.randint(10, 20))

    await producer.stop()

# Run the producer
loop = asyncio.get_event_loop()
result = loop.run_until_complete(produce())

