import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import 'dotenv/config'
import { redisService } from './redis'
import { Message } from './message'
import axios from 'axios'
import { RedisPubSub } from './redispubsub'

interface ChatMessage {
  senderId: string
  text: string
  timestamp: number
}

const allowedOrigins = [
  'http://localhost:3000',
  'https://laco.mahevini.com.br'
]

const fastify = Fastify({ logger: true })

fastify.register(websocket)

const main = async () => {


}


const start = async () => {
  try {

    const redisPubSub = new RedisPubSub();

    const clients = new Set<any>();

    await redisPubSub.init((data: any, channel) => {
      if (data.type === 'created-message') {
        for(const connection of clients) {
          if (connection.readyState === 1) {
            connection.send(Message.getMessage('receive-message', data.data));
          } else {
            try {
              connection.close()
            } catch(e) {}
            clients.delete(connection)
          }
        }
      }
    })



    fastify.register(async (instance) => {
      instance.get('/ws', { websocket: true }, async (connection, req) => {

        const origin = req.headers.origin

        if (!origin || !allowedOrigins.includes(origin)) {
          connection.close()
          return
        }

        const { session } = req.query as { session?: string }

        if (typeof session !== 'string') {
          connection.close()
          return
        }

        const userId = await redisService.get(`session:${session}`);

        if (!userId) {
          connection.close()
          return
        }

        let messages = 0

        const interval = setInterval(() => {
          messages = 0
        }, 60000)


        clients.add(connection);

        connection.on('close', () => {
          console.log('Cliente desconectou')
          clients.delete(connection);
          clearInterval(interval);
        })

        const closeConnection = async () => {
          connection.close();
        }

        connection.on('message', async (message: any) => {
          messages++;

          if (typeof message !== 'string' && !Buffer.isBuffer(message)) {
            console.log('conexão fechada')
            await closeConnection();
            return;
          }

          if (message.length > 5000) {
            await closeConnection();
            return;
          }

          if (messages > 100) {
            await closeConnection();
            return;
          }

          try {
            const parsedMessage = Message.parseMessage(message as Buffer);

            switch (parsedMessage.type) {
              case 'send-message':
                console.log({
                  id: parsedMessage.data.uuid,
                  event_id: parsedMessage.data.eventId,
                  message: parsedMessage.data.message,
                  user_id: userId,
                })
                const { data: message } = await axios.post(`${process.env.APP_URL!}/api/chat/create-message`, {
                  id: parsedMessage.data.uuid,
                  event_id: parsedMessage.data.eventId,
                  message: parsedMessage.data.message,
                  user_id: userId,
                }, {
                  headers: {
                    accessToken: process.env.ACCESS_TOKEN
                  }
                })

                await redisPubSub.publish({
                  type: 'created-message',
                  data: message
                })

                break;
              case 'ping':
                break;
              default:
                throw new Error('Not implemented')
            }

          } catch (e) {
            await closeConnection();
          }
        })

      })
    })

    await fastify.listen({ port: 3001, host: '0.0.0.0' })
    console.log('Servidor WebSocket rodando na porta 3001')
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()