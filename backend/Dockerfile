# syntax=docker/dockerfile:1.7

FROM node:20-alpine AS prod-deps
WORKDIR /app/backend

COPY backend/package.json backend/package-lock.json ./
RUN npm ci --omit=dev

FROM node:20-alpine AS build
WORKDIR /app/backend
COPY backend/package.json backend/package-lock.json ./
RUN npm ci
COPY backend/tsconfig.json ./
COPY backend/src ./src
RUN npm run build

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY --from=prod-deps /app/backend/node_modules ./backend/node_modules
COPY backend/package.json backend/package-lock.json ./backend/
COPY --from=build /app/backend/dist ./backend/dist
RUN mkdir -p ./backend/data

EXPOSE 8787
CMD ["node", "backend/dist/server.js"]
