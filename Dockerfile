FROM openjdk:8-jdk-alpine
EXPOSE 4567/tcp
ARG MAVEN_VERSION=3.3.9
ARG USER_HOME_DIR="/root"
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"
ENV MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"

RUN apk add --no-cache curl tar bash && \
    mkdir -p /usr/share/maven && \
    curl -fsSL http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar -xzC /usr/share/maven --strip-components=1 && \
    ln -s /usr/share/maven/bin/mvn /usr/bin/mvn && \
    mkdir -p /usr/src/kafkaTweets

WORKDIR /usr/src/kafkaTweets
COPY ./pom.xml /usr/src/kafkaTweets/pom.xml

#RUN /usr/bin/mvn  dependency:resolve \
#    && /usr/bin/mvn  verify

COPY ./src /usr/src/kafkaTweets/src
RUN /usr/bin/mvn package

COPY ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["java", "-jar", "target/kafkaTweets-jar-with-dependencies.jar"]
