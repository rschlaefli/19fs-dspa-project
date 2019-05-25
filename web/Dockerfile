FROM node:10-alpine AS builder

ENV NODE_ENV=production

WORKDIR /tmp

COPY frontend/package.json .
COPY frontend/package-lock.json .

RUN npm ci --only=production

COPY frontend/src/ src/
COPY frontend/public/ public/

RUN npm run build


FROM node:10-alpine

ENV NODE_ENV=production

WORKDIR /usr/src/app

COPY backend/package.json .
COPY backend/package-lock.json .

RUN npm ci --only=production

COPY --from=builder /tmp/build/ public/
COPY wait-for.sh /wait-for.sh
COPY backend/src/ src/

RUN chmod +x /wait-for.sh

EXPOSE 4000

ENTRYPOINT ["/wait-for.sh", "npm"]
CMD ["start"]
