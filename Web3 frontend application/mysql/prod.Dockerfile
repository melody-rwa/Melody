FROM docker.io/mysql 
 
EXPOSE 3306

#Define the directory that will be automatically executed by the container
ENV AUTO_RUN_DIR /docker-entrypoint-initdb.d

#Define initialization SQL file
ENV INIT_SQL admin.sql

#Place the SQL file to be executed in the/dockerentrypoint-initdb. d/directory, and the container will automatically execute this SQL file
COPY admin.sql ./$INIT_SQL $AUTO_RUN_DIR/

#Add executable permissions to the executable file
RUN chmod 777 $AUTO_RUN_DIR/$INIT_SQL

