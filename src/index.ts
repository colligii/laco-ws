import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import 'dotenv/config'

interface ChatMessage {
  senderId: string
  text: string
  timestamp: number
}

const fastify = Fastify({ logger: true })

fastify.register(websocket)

fastify.register(async (instance) => {
  instance.get('/chat', { websocket: true }, (connection, req) => {

    connection.on('message', (message: Buffer) => {
      const data: ChatMessage = JSON.parse(message.toString())

      console.log(`Mensagem de ${data.senderId}: ${data.text}`)

      connection.send(
        JSON.stringify({
          status: 'ok',
          receivedAt: Date.now()
        })
      )
    })

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