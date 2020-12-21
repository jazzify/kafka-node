import {
  forEach as _forEach,
  lowerFirst as _lowerFirst,
  now as _now,
  replace as _replace,
  split as _split
} from 'lodash';
import mongoose from 'mongoose';
import { Consumer } from './controllers/consumer';
import { Producer } from './controllers/producer';
import { User } from './models';

//  Util function (TODO: REMOVE)
const sleep = (ms: number): Promise<NodeJS.Timeout> => new Promise((r) => setTimeout(r, ms));

// MongoDB Conn and Event Susbcribers
const conn = mongoose.connection;
mongoose.connect('mongodb://127.0.0.1:27017/test?authSource=admin', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  user: 'root',
  pass: 'example',
});
conn.once('open', () => console.log('Connected to DB'));
conn.on('error', (err: mongoose.Error) => console.log('Error on DB', err));


// Kafka Implementation
const brokerList = 'localhost:9092';
const topicName = 'topicpoc';
const groupId = 'librd-test';

// Util function
const parseMessage = (message: Buffer, id: number): void => {
  const msg = JSON.parse(message.toString('utf-8'));
  msg['created_at'] = new Date(_now());
  _forEach(msg.books, (book) => {
    book.name = _replace(_lowerFirst(book.name), 'a', '@');
    book.isbn = _split(book.isbn, '-');
  });
  console.log(`working on ${id}: `, msg);
};

// Kafka Consumer
const { stream : stream1 } = new Consumer(brokerList, groupId, topicName);
const { stream : stream2 } = new Consumer(brokerList, groupId, topicName);
stream1.on('data', (message: Buffer) => parseMessage(message, 1));
stream2.on('data', (message: Buffer) => parseMessage(message, 2));

// Kafka Producer
const { producer } = new Producer(brokerList, true);
producer.on('ready', async () => {
  // if partition is set to -1, librdkafka will use the default partitioner
  const partition = -1;
  const mdocs = await User.find({});
  let counter = 1;

  for (let i = 0; i < mdocs.length; i += 1) {
    const value = Buffer.from(JSON.stringify(mdocs[i]));
    const key = `key-${i}`;
    // if partition is set to -1, librdkafka will use the default partitioner
    producer.produce(topicName, partition, value, key, Date.now());
    counter += 1;
    // eslint-disable-next-line no-await-in-loop
    await sleep(1000);
  }

  // Need to keep polling for a while to ensure the delivery reports are received
  const pollLoop = setInterval((): void => {
    producer.poll();
    if (counter === mdocs.length) {
      clearInterval(pollLoop);
      producer.disconnect();
      stream1.removeAllListeners();
      stream2.removeAllListeners();
      process.exit();
    }
  }, 1000);
});

// Disconnect the producer on CTRL+C
process.on('SIGINT', () => {
  producer.disconnect();
  process.exit();
});
