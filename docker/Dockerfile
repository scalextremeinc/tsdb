FROM docker-registry.scalextreme.com/jdk:8

RUN yum install -y make hostname gnuplot

ADD . /opt/tsdb
WORKDIR /opt/tsdb
RUN ./build.sh distclean
RUN ./build.sh

ENV TSDB_HOME=/opt/tsdb LOG_DIR=/volume/log CACHE_DIR=/volume/tsdb-cache JAVA_HOME=/opt/jdk
VOLUME /volume

EXPOSE 4242

CMD /opt/tsdb/docker/start-nosql.sh
