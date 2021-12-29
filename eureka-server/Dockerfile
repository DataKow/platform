FROM openjdk:11-jdk
COPY target/eureka-server.jar /opt/datakow/

ARG GRAYLOG_SERVER_HOST=udp:graylog
ARG GRAYLOG_SERVER_PORT=12201

ENV GRAYLOG_SERVER_HOST=${GRAYLOG_SERVER_HOST}
ENV GRAYLOG_SERVER_PORT=${GRAYLOG_SERVER_PORT}

EXPOSE 8761
ENTRYPOINT ["java","-jar","/opt/datakow/eureka-server.jar"]