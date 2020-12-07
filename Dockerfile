FROM node:alpine

WORKDIR /app

COPY . .
RUN npm ci --only=production

CMD [ "node" "main.js" ]
