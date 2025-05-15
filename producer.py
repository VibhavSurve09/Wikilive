import json
from confluent_kafka import Producer
import asyncio
import aiohttp
import os
from dotenv import load_dotenv
load_dotenv()


config={
'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP_SERVER'),
'client.id':'python-producer'
}

WIKIMEDIA_STREAM_URL='https://stream.wikimedia.org/v2/stream/recentchange'
KAFKA_TOPIC=os.getenv('KAFKA_TOPIC')



producer=Producer(config)

def delivery_report(errmsg, msg):
    """
    Reports the Failure or Success of a message delivery.
    Args:
        errmsg  (KafkaError): The Error that occurred while message producing.
        msg    (Actual message): The message that was produced.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """   
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
async def produce_to_kafka(parsed_event_json):
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(parsed_event_json),
            key='wiki_event',
            callback=delivery_report
        )
        producer.poll(0)

    except BufferError:
        producer.flush()

async def stream_events():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(WIKIMEDIA_STREAM_URL) as response:
                    async for line in response.content:
                            decoded_line=line.decode('utf-8')
                            if "data:" in decoded_line:
                                _,_,event_json=decoded_line.partition('data:')
                                parsed_event=json.loads(event_json)

                                parsed_event_json = {
                                    'type': parsed_event.get('type'),
                                    'is_bot': parsed_event.get('bot', False),
                                    'server_name': parsed_event.get('server_name'),
                                    'user_name': parsed_event.get('user', 'unknown'),
                                    'minor': parsed_event.get('minor', False),
                                    'title_url': parsed_event.get('title_url'),
                                    'namespace': parsed_event.get('namespace'),
                                    'comment': parsed_event.get('comment', ''),
                                }
                                await produce_to_kafka(parsed_event_json)
        except aiohttp.ClientError as e:
            await asyncio.sleep(5)

        except Exception as e:
            await asyncio.sleep(5)
            

        
def main():
    try:
        asyncio.run(stream_events())
    except KeyboardInterrupt:
        producer.flush()


if __name__=='__main__':
    main()
        