import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import 'dotenv/config'
import { redisService } from './redis'
import { Message } from './message'
import axios from 'axios'
import { RedisPubSub } from './redispubsub'
import * as fs from "fs"
import * as path from 'path'
import ffmpeg from 'fluent-ffmpeg';

interface ChatMessage {
  senderId: string
  text: string
  timestamp: number
}

const allowedOrigins = [
  'http://localhost:3000',
  'https://lacos.mahevini.com.br'
]

const fastify = Fastify({ logger: true })

fastify.register(websocket)

const start = async () => {
  try {

    const redisPubSub = new RedisPubSub();

// ... dentro da função videoWorker ...

    const clients = new Set<any>();

    await redisPubSub.init((data: any, channel) => {
      console.log('Pub sub received', data.type, data.data)
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
      } else if (data.type === 'upload-finish') {
        for(const connection of clients) {
          if (connection.readyState === 1) {
            console.log('Conexão aberta', data.type, data.data)
            
            const conn = connection as any;
            if(conn.session === data.data.session) {
              console.log('Conexão encontrada', data.type, data.data)
              connection.send(Message.getMessage('upload-finish', data.data));
            }
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

        console.log(origin, allowedOrigins)
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

        const conn = connection as any;
        conn.session = session; 

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

            console.log('received message', parsedMessage)

            switch (parsedMessage.type) {
              case 'send-message':
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
              case 'temp-file': 
              console.log('comando temp-file recebido')
              const { data: { tempFile, uploadUrl } } = await axios.get(`${process.env.APP_URL!}/api/s3/temp/schedule-conversion/${parsedMessage.data.id}`, {
                headers: {
                  accessToken: process.env.ACCESS_TOKEN
                }
              })
              console.log(tempFile, uploadUrl)

                if(!tempFile || !uploadUrl)
                  return connection.close();

                await redisService.rpush('video-queue', JSON.stringify({session, tempFile, uploadUrl, storyId: parsedMessage.data.storyId, postId: parsedMessage.data.postId, type: parsedMessage.data.type }));
                break;
              default:
                throw new Error('Not implemented')
            }

          } catch (e) {
            console.log(e)
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