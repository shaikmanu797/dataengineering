FROM centos:7
MAINTAINER Mansoor Baba Shaik "mansoorbabashaik@outlook.com"

ENV container=docker
ENV MYSQL_PORT=3306

RUN yum -y install wget && \
    wget http://repo.mysql.com/mysql-community-release-el7-5.noarch.rpm && \
    rpm -ivh mysql-community-release-el7-5.noarch.rpm && \
    rm -f mysql-community-release-el7-5.noarch.rpm && \
    yum -y update && \
    yum -y install mysql-community-server

RUN yum -y install python-setuptools && \
    easy_install pip && \
    pip install supervisor

RUN yum -y clean all; rm -rf /var/yum/cache

COPY my.cnf /etc

RUN mkdir -p /etc/supervisor.d /var/log/supervisord /var/log/dataengineering
COPY supervisord.conf /etc/supervisor.d

EXPOSE $MYSQL_PORT

ENTRYPOINT ["/usr/bin/supervisord", "-c", "/etc/supervisor.d/supervisord.conf"]