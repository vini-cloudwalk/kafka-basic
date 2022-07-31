const { Kafka, Partitioners } = require("kafkajs");
const pino = require("pino");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const logger = pino({
  level: "info",
  transport: {
    target: "pino-pretty",
    options: {
      colorize: true,
    },
  },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});
const consumer = kafka.consumer({ groupId: "test-group" });

const sendMessage = async ({ topic, messages = [] }) => {
  logger.info("Starting connection with PRODUCER!");
  await producer.connect();
  logger.info("Connection with PRODUCER completed!");
  await producer.send({
    topic,
    messages,
  });
  logger.info("Message send with success!");
  await producer.disconnect();
};

const getMessages = async (topic) => {
  logger.info("Starting connection with CONSUMER!");
  await consumer.connect();
  logger.info("Connection with CONSUMER completed!");
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        value: JSON.parse(message.value),
      });
    },
  });
  //   await consumer.disconnect();
};

const start = async () => {
  const topic = "created-user";
  const user = {
    firstName: "Vinicius",
    lastName: "Branco",
    age: 23,
    email: "viniciusoliveirabranco@hotmail.com",
  };

  // write message
  await sendMessage({
    topic,
    messages: [
      {
        value: JSON.stringify(user),
      },
    ],
  });

  // read message
  await getMessages(topic);
};

start();
