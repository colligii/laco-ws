import { createClient } from "redis";
import { PubSubListener } from '@redis/client/dist/lib/client/pub-sub';

export class RedisPubSub {

    publisher = createClient({
        url: process.env.REDIS_URL
    });

    subscriber = createClient({
        url: process.env.REDIS_URL
    });

    async init(listener: PubSubListener<false>) {

        await this.publisher.connect();
        await this.subscriber.connect();

        await this.subscriber.subscribe("event", (message, channel) => {
            try {
                listener(JSON.parse(message), channel);
            } catch {
                console.log("invalid message");
            }
        });
    }

    async publish(data: any) {
        const payload = JSON.stringify(data);
        await this.publisher.publish("event", payload);
    }

    async close() {
        await this.publisher.quit();
        await this.subscriber.quit();
    }
}