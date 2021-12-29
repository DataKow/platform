FROM openjdk:11-jdk

RUN apt install git
RUN mkdir /logs

COPY target/config-server.jar /opt/datakow/
COPY config-repo /opt/datakow/config-repo

WORKDIR /opt/datakow/config-repo
RUN git config --global user.email "kevonoff@gmail.com"
RUN git config --global user.name "Kevin Off"
RUN git init
RUN git add *
RUN git commit -m "Initializing"
EXPOSE 8888 9001

ARG GRAYLOG_SERVER_HOST=udp:graylog
ARG GRAYLOG_SERVER_PORT=12201
ARG EUREKA_DEFAULT_ZONE=http://eureka-server:8761/eureka/

ENV GRAYLOG_SERVER_HOST=${GRAYLOG_SERVER_HOST}
ENV GRAYLOG_SERVER_PORT=${GRAYLOG_SERVER_PORT}
ENV EUREKA_DEFAULT_ZONE=${EUREKA_DEFAULT_ZONE}

WORKDIR /

ENTRYPOINT ["java","-jar","/opt/datakow/config-server.jar"]