FROM centos:7
RUN yum install -y make hostname gnuplot

ADD third_party/jdk-7u79-linux-x64.tar.gz /opt/jdk/
WORKDIR /opt/jdk
RUN ln -s /opt/jdk/jdk1.7.0_79/bin/* /usr/local/sbin/

ADD . /opt/tsdb
WORKDIR /opt/tsdb
RUN ./build.sh distclean
RUN ./build.sh

ENV TSDB_HOME=/opt/tsdb LOG_DIR=/volume/log CACHE_DIR=/volume/tsdb-cache JAVA_HOME=/opt/jdk/jdk1.7.0_79
VOLUME /volume

EXPOSE 4242

CMD /opt/tsdb/start-nosql.sh
