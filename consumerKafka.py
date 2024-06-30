from confluent_kafka import Consumer, KafkaException, KafkaError
import re

def consume_messages(consumer, topic, parameter_name,  parameter_to_be_checked, parameter_value):
    consumer.subscribe([topic])
    print(f'Consuming messages from topic: {topic}')
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Reached end of partition at offset {msg.offset}')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            data = msg.value().decode('utf-8')
            match = re.search(r'\{.*\}', data) #Outline Remover for the value
            if match:
                finaldata = eval(match.group())
                if finaldata["parameter"] == parameter_name and finaldata[parameter_to_be_checked] == parameter_value:
                    print(f'{finaldata}')
def main():
    bootstrap_server = input("Enter the bootstrap-server: ")
    topic_name = input("Enter the topic name: ")
    zookeeper_server = input("Enter the zookeeper server: ")
    group_id = input("Enter the group id: ")
    parameter_name = input("Enter the parameter name: ")
    parameter_to_be_checked = input("Enter the parameter to be checked: ")
    parameter_value = int(input("Enter the parameter value: "))

    consumer_conf = {
        'bootstrap.servers': bootstrap_server,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    try:
        consume_messages(consumer, topic_name, parameter_name, parameter_to_be_checked, parameter_value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
