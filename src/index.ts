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

const videoWorker = async () => {
  console.log("Worker de vídeo aguardando tarefas...");

  while (true) {
    const result: any = await redisService.blpop('fila-de-video', 0);
    
    // Adicionado log para ver o que chegou da fila
    console.log("Tarefa recebida na fila:", result[1]);

    const {session, tempFile, uploadUrl} = JSON.parse(result[1]);

    const localInputPath = path.join('./temp', `${tempFile.id}.webm`);
    const localOutputPath = path.join('./temp', `${tempFile.id}.mp4`);

    try {
      console.log(`Iniciando download do arquivo: ${tempFile.path}`);
      
      const writer = fs.createWriteStream(localInputPath);
      const response = await axios({ url: tempFile.path, method: 'GET', responseType: 'stream' });
      response.data.pipe(writer);
      
      await new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
      });

      console.log(`Download concluído: ${localInputPath}. Iniciando conversão FFmpeg...`);

      await new Promise((resolve, reject) => {
        ffmpeg(localInputPath)
          .outputOptions(['-c:v libx264', '-movflags +faststart', '-pix_fmt yuv420p'])
          .save(localOutputPath)
          .on('end', () => {
            console.log("Conversão FFmpeg finalizada com sucesso.");
            resolve(null);
          })
          .on('error', (err) => {
            console.error("Erro durante o processamento FFmpeg:", err);
            reject(err);
          });
      });

      const stats = fs.statSync(localOutputPath);
      console.log(`Arquivo convertido pronto (${stats.size} bytes). Iniciando upload para S3/Servidor.`);

      const fileStream = fs.createReadStream(localOutputPath);

      await axios.put(uploadUrl, fileStream, {
        headers: {
          'Content-Type': 'video/mp4', 
          'Content-Length': stats.size 
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity
      });

      console.log(`Upload finalizado para o arquivo: ${tempFile.id}`);
      
      await redisPubSub.publish({
        type: 'upload-finish',
        data: {session, tempFile}
      })
    } catch (err) {
      console.error("Erro fatal no processamento do vídeo:", err);
    } finally {
      // LIMPEZA: Isso roda sempre, deu erro ou não!
      if (fs.existsSync(localInputPath)) fs.unlinkSync(localInputPath);
      if (fs.existsSync(localOutputPath)) fs.unlinkSync(localOutputPath);
      console.log(`Arquivos temporários limpos para o vídeo: ${tempFile.id}`);
    }
  }
}

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
      } else if (data.type === 'upload-finish') {
        for(const connection of clients) {
          if (connection.readyState === 1) {
            const conn = connection as any;
            if(conn.session === data.data.session) {
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
              const { data: { tempFile, uploadUrl } } = await axios.get(`${process.env.APP_URL!}/s3/temp/schedule-conversion/${parsedMessage.data.id}`, {
                headers: {
                  accessToken: process.env.ACCESS_TOKEN
                }
              })
              console.log(tempFile, uploadUrl)

                if(!tempFile || !uploadUrl)
                  return connection.close();

                await redisService.rpush('video-queue', JSON.stringify({session, tempFile, uploadUrl}));
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

  videoWorker().catch(console.error);

    await fastify.listen({ port: 3001, host: '0.0.0.0' })
    console.log('Servidor WebSocket rodando na porta 3001')
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()