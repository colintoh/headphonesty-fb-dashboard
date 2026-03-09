FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY src/ ./src/
COPY public/ ./public/

RUN mkdir -p /app/data

ENV PORT=3000
ENV DB_PATH=/app/data/posts.db

EXPOSE 3000

CMD ["node", "src/server.js"]
