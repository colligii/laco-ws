FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

RUN npx tsc

FROM node:20-alpine AS production

WORKDIR /app

COPY package*.json ./

RUN npm install --production

COPY --from=build /app/dist ./dist

RUN apk add --no-cache ffmpeg

EXPOSE 3001
CMD ["node", "dist/index.js"]