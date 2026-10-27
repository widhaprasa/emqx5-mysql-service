# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder stage
# ---------------------------------------------------------------------------
# Node.js 24.22.1 (LTS "Krypton") is pinned explicitly so builds are
# reproducible. The -alpine variant keeps the image small.
FROM node:24.22.1-alpine AS deps

WORKDIR /usr/src/app

# Copy only the manifests first. This layer is cached until the npm
# dependencies actually change.
COPY package.json package-lock.json ./

# Install exactly what the lockfile specifies, production dependencies only.
RUN npm ci --omit=dev

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM node:24.22.1-alpine AS runtime

# curl is used by the HEALTHCHECK below.
RUN apk add --no-cache curl

# Defaults for the service. Override at run time with `-e HOST=... -e PORT=...`.
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000

WORKDIR /usr/src/app

# Reuse the production node_modules built in the previous stage.
COPY --from=deps --chown=node:node /usr/src/app/node_modules ./node_modules

# Copy only the files the service needs at run time. Copying explicitly
# avoids leaking the local node_modules, .git or tests into the image.
COPY --chown=node:node package.json package-lock.json ./
COPY --chown=node:node server.js query.js parse.js config.js ./

# Run as the unprivileged `node` user that ships with the official image.
USER node

# The HTTP port is configurable through PORT (see config.js); 3000 is the default.
EXPOSE 3000

# Mark the container unhealthy if /health stops responding.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS "http://127.0.0.1:${PORT}/health" || exit 1

LABEL org.opencontainers.image.title="emqx5-mysql-service" \
      org.opencontainers.image.description="EMQX 5 MySQL/MariaDB account service"

CMD ["node", "server.js"]
