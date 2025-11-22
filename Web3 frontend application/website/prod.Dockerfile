FROM node:20-alpine AS base

FROM base AS builder

WORKDIR /app

COPY package.json  package-lock.json* ./

RUN  npm install

COPY src ./src
COPY messages ./messages
COPY public ./public
COPY next.config.ts .
COPY eslint.config.mjs .
COPY tsconfig.json .
COPY next-env.d.ts .

ARG ENV_VARIABLE
ENV ENV_VARIABLE=${ENV_VARIABLE}
ARG NEXT_PUBLIC_ENV_VARIABLE
ENV NEXT_PUBLIC_ENV_VARIABLE=${NEXT_PUBLIC_ENV_VARIABLE}


RUN mkdir uploads

RUN npm run build 


FROM base AS runner

WORKDIR /app


RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
USER nextjs

COPY --from=builder /app/public ./public


COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static


ARG ENV_VARIABLE
ENV ENV_VARIABLE=${ENV_VARIABLE}
ARG NEXT_PUBLIC_ENV_VARIABLE
ENV NEXT_PUBLIC_ENV_VARIABLE=${NEXT_PUBLIC_ENV_VARIABLE}



CMD ["node", "server.js"]
