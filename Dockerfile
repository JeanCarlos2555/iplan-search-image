FROM node:20-alpine

RUN apk add --no-cache curl tzdata

RUN ln -snf /usr/share/zoneinfo/America/Fortaleza /etc/localtime && echo America/Fortaleza > /etc/timezone
ENV TZ=America/Fortaleza

WORKDIR /app

COPY package.json package-lock.json ./

RUN npm ci --omit=dev

COPY . .

RUN mkdir -p /app/output

CMD ["npm", "start"]
