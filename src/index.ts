import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import 'dotenv/config'
import { redisService } from './redis'

interface ChatMessage {
  senderId: string
  text: string
  timestamp: number
}

const allowedOrigins = [
  'http://localhost:3000',
  'https://app.seusite.com'
]

const fastify = Fastify({ logger: true })

fastify.register(websocket)

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
    
    const getSession = await redisService.get(`session:${session}`);

    if(!getSession) {
      connection.close()
      return
    }

      let messages = 0

      setInterval(() => {
        messages = 0
      }, 60000)

    connection.on('message', (message) => {
      messages++;
      
      if (typeof message !== 'string' && !Buffer.isBuffer(message)) {
        connection.close()
        return;
      }

      if (message.length > 5000) {
        connection.close()
        return;
      }

      if (messages > 100) {
        connection.close()
        return;
      }
    })

    connection.send(JSON.stringify({ pong: 'pong' }))

    connection.on('close', () => {
      console.log('Cliente desconectou')
    })
  })
})

const start = async () => {
  try {
    await fastify.listen({ port: 3001, host: '0.0.0.0' })
    console.log('Servidor WebSocket rodando na porta 3001')
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()