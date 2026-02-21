# syntax=docker/dockerfile:1.7

FROM node:20-alpine AS build
WORKDIR /workspace
COPY . .
RUN set -eux; \
  if [ -f /workspace/backend/package.json ]; then \
    APP_DIR=/workspace/backend; \
  else \
    APP_DIR=/workspace; \
  fi; \
  cd "$APP_DIR"; \
  npm ci
RUN set -eux; \
  if [ -f /workspace/backend/package.json ]; then \
    APP_DIR=/workspace/backend; \
  else \
    APP_DIR=/workspace; \
  fi; \
  cd "$APP_DIR"; \
  npm run build; \
  mkdir -p /out; \
  cp package.json /out/package.json; \
  cp package-lock.json /out/package-lock.json; \
  cp -R dist /out/dist; \
  if [ -d data ]; then cp -R data /out/data; else mkdir -p /out/data; fi

FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

RUN mkdir -p /app/backend
COPY --from=build /out/package.json /out/package-lock.json /app/backend/
RUN cd /app/backend && npm ci --omit=dev && npm cache clean --force
COPY --from=build /out/dist /app/backend/dist
COPY --from=build /out/data /app/backend/data

EXPOSE 8787
CMD ["node", "backend/dist/server.js"]
