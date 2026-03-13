import { redisService } from "./redis";
import { RedisPubSub } from "./redispubsub";
import * as path from 'path'
import * as fs from 'fs'
import axios from 'axios'
import ffmpeg from 'fluent-ffmpeg'

const redisPubSub = new RedisPubSub();

const videoWorker = async () => {
  console.log("Worker de vídeo aguardando tarefas...");

    const tempDir = path.resolve('./temp');
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
    console.log(`Diretório ${tempDir} criado.`);
  }

  while (true) {
    const result: any = await redisService.blpop('video-queue', 0);
    
    // Adicionado log para ver o que chegou da fila
    console.log("Tarefa recebida na fila:", result.element);

    const {session, tempFile, uploadUrl, storyId} = JSON.parse(result.element);

    console.log(session, tempFile, uploadUrl, storyId);

    console.log(tempFile.extension)

    const localInputPath = path.join('./temp', `${tempFile.id}${tempFile.extension ?? '.webm'}`);
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

      let orientation: any;

      if(!tempFile?.orientation) {
        const o = Number(tempFile?.orientation);

        if(!isNaN(o) && o === 0) {
          orientation = o;
        }
      }

      const rotateFilter = orientation && orientation !== 0
        ? `rotate=${orientation}*PI/180`
        : null;

      console.log(`Download concluído: ${localInputPath}. Iniciando conversão FFmpeg...`);

      await new Promise((resolve, reject) => {
        let initialRequest = ffmpeg(localInputPath)
          .outputOptions(['-c:v libx264', '-movflags +faststart', '-pix_fmt yuv420p'])
          
          if(rotateFilter) {
            initialRequest = initialRequest.videoFilters(rotateFilter);
          }

          initialRequest
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
      
      console.log('Migrando post temporario para fixo');

      await axios.post(`${process.env.APP_URL!}/api/story/migrate/temp/${storyId}`, {}, {
        headers: {
                  accessToken: process.env.ACCESS_TOKEN
                }
      });

      console.log('Migrado com sucesso')

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

videoWorker().catch(console.error);