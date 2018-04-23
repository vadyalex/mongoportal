FROM java:8-jre-alpine

MAINTAINER Vladyslav Aleksakhin <vadyalex@gmail.com>

ADD target/mongoportal.jar /data/

WORKDIR /data

ENTRYPOINT ["java", "-jar", "mongoportal.jar"]