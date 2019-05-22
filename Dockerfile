FROM centos:7
MAINTAINER Mansoor Baba Shaik "mansoorbabashaik@outlook.com"

ENV container=docker
ENV MYSQL_PORT=3306
ENV JAVA_HOME=/usr/lib/jvm/jre
ENV DATA_DIR=/data

RUN yum -y install wget && \
    wget http://repo.mysql.com/mysql-community-release-el7-5.noarch.rpm && \
    rpm -ivh mysql-community-release-el7-5.noarch.rpm && \
    rm -f mysql-community-release-el7-5.noarch.rpm && \
    yum -y update && \
    yum -y install mysql-community-server

RUN yum -y install python-setuptools && \
    easy_install pip && \
    pip install supervisor

RUN yum -y install java-1.8.0-openjdk

RUN yum -y clean all; rm -rf /var/yum/cache

RUN mkdir -p /etc/supervisor.d /var/log/supervisor /var/log/app /data /apps/dataengineering
RUN chmod -R 777 /data

COPY . /apps/dataengineering

RUN cp /apps/dataengineering/configs/my.cnf /etc/
RUN cp /apps/dataengineering/configs/supervisord.conf /etc/supervisor.d/
RUN cp /apps/dataengineering/configs/stop-supervisor.sh /
RUN chmod +x /stop-supervisor.sh

EXPOSE $MYSQL_PORT

ENTRYPOINT ["/usr/bin/supervisord", "-c", "/etc/supervisor.d/supervisord.conf"]