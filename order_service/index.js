// TODO: Implement the order service
var amqp = require('amqplib/callback_api');

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
    amqp.connect('ampq://localhost', (err, conn) => {
        if (err) { reject(err); }
        resolve(conn);
    });
});

const createChannel = connection => new Promise((resolve, reject) => {
    connection.createChannel((err, channel) => {
        if(err) { reject(err);}
        resolve(channel);
    });
});

const configureMessageBroker = channel => {
    const { order } = messageBrokerInfo.exchanges;
    const { addQueue} = messageBrokerInfo.queues;
    const { input } = messageBrokerInfo.routingKeys;

    channel.assertExchange(order, 'direct', { durable: true});
    channel.assertQueue(addQueue, {durable: true});
    channel.bindQueue(addQueue, order, input);
};

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