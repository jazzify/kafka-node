import Kafka from 'node-rdkafka';

export class Consumer {
  _stream: Kafka.ConsumerStream;
  
  constructor(broker_list: string, group_id: string, topics: string) {
    this._stream = Kafka.KafkaConsumer.createReadStream({
      'metadata.broker.list': broker_list,
      'group.id': group_id,
      'enable.auto.commit': false
    }, {} , {
      topics: topics,
      waitInterval: 0,
      objectMode: false
    });

    this._stream.on('error', (err: Kafka.LibrdKafkaError): never => {
      if (err) console.log(err);
      process.exit(1);
    });

    this._stream.consumer.on('event.error', (err: Kafka.LibrdKafkaError): void => console.log(err));
  }

  public get stream() : Kafka.ConsumerStream {
    return this._stream; 
  }
} 

