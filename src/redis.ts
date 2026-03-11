import * as redis from 'redis';
import 'dotenv/config';
import { RedisClientType } from '@redis/client';

class RedisService {
    private redis: RedisClientType;

    constructor() {
        this.redis = redis.createClient({
            url: process.env.REDIS_URL
        });
    }

    async get(key: string) {
        if (!this.redis.isOpen) {
            await this.redis.connect();
        }
    
        const data = await this.redis.get(key);
    
        return data;
    }

}

export const redisService = new RedisService();