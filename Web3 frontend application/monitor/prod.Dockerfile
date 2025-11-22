
FROM node:18-alpine
ADD https://github.com/eficode/wait-for/raw/master/wait-for /usr/local/bin/wait-for
RUN chmod +x /usr/local/bin/wait-for
WORKDIR /app
COPY ["package.json", "package-lock.json*", "./"]
RUN npm install -g npm@10.3.0
RUN npm install  --omit=dev
COPY . .
CMD ["wait-for", "mysqldb:3306", "--", "node", "app.js"]
