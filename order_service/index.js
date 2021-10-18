// TODO: Implement the order service
const amqp = require('amqplib/callback_api');
const fs = require('fs');

const messageBrokerInfo = {
    exchanges: {
        order: 'order_exchange'
    },
    queres: {
        addQueue: 'order_queue'
    },
    routingKeys: {
        createOrder: "create_order",
        input: "log_route"
    }
}

const createMessageBrokerConnection = () => new Promise((resolve, reject) => {
    amqp.connect('amqp://localhost', (err, conn) => {
        if (err) { reject(err); }
        resolve(conn);
    });
});

const configureMessageBroker = channel => {
    const { exchanges, queues, routingKeys } = messageBrokerInfo;

    channel.assertExchange(exchanges.order, 'direct', { durable: true });
    channel.assertQueue(queues.addQueue, { durable: true });
    channel.bindQueue(queues.addQueue, exchanges.order, routingKeys.input);
}

const createChannel = connection => new Promise((resolve, reject) => {
    connection.createChannel((err, channel) => {
        if (err) { reject(err); }
        configureMessageBroker(channel);
        resolve(channel);
    });
});

(async () => {
    const connection = await createMessageBrokerConnection();
    const channel = await createChannel(connection);

    const { order } = messageBrokerInfo.exchanges;
    const { addQueue } = messageBrokerInfo.queues;
    const { createOrder } = messageBrokerInfo.routingKeys;

    channel.consume(addQueue, data => {
        console.log(data.content.toString());
        const dataJson = JSON.parse(data.content.toString());
        const addResult = add(dataJson.a, dataJson.b);
        channel.publish(order, createOrder, new Buffer(JSON.stringify(addResult)));

        console.log(`[x] Sent: ${JSON.stringify(dataJson)}`);
    }, { noAck: true });
})().catch(e => console.error(e));