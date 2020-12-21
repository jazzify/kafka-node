import Kafka from 'node-rdkafka';

export class Producer {
    _producer: Kafka.Producer;

    constructor(broker_list = '', dr = false) {
      this._producer = new Kafka.Producer({
        'metadata.broker.list': broker_list,
        dr_cb: dr, // delivery report callback
      });
      this._producer.on('event.error', (err: Kafka.LibrdKafkaError): void => {
        console.error('Error from producer:', err);
      });
      this._producer.on('delivery-report', (err: Kafka.LibrdKafkaError, report: Kafka.DeliveryReport): void => {
        if (err) console.log(err);
        console.log(`delivery-report: ${JSON.stringify(report)}`);
      });
      this._producer.on('disconnected', (): void => {
        console.log('Producer disconnected.');
      });
      this._producer.connect();
    }

    get producer(): Kafka.Producer {
      return this._producer;
    }
}
