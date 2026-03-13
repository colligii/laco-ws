import * as redis from 'redis';
import 'dotenv/config';

class RedisService {
    private client: redis.RedisClientType;
    
    constructor() {
        this.client = redis.createClient({ url: process.env.REDIS_URL });
        
        this.client.connect().catch(console.error);
    }

    async get(key: string) { return await this.client.get(key); }

    async rpush(key: string, value: string) { return await this.client.rPush(key, value); }
    
    async blpop(key: string, timeout: number = 0) { 
        return await this.client.blPop(key, timeout); 
    }
}

export const redisService = new RedisService();