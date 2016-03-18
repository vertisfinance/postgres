FROM vertisfinance/baseimage

MAINTAINER Richard Bann "richard.bann@vertis.com"

RUN apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8

ENV PG_MAJOR 9.5
ENV PG_VERSION 9.5.1-1.pgdg80+1

RUN echo 'deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main' $PG_MAJOR > /etc/apt/sources.list.d/pgdg.list

RUN set -ex \
    && apt-get update \
    && apt-get install -y postgresql-common \
    && sed -ri 's/#(create_main_cluster) .*$/\1 = false/' /etc/postgresql-common/createcluster.conf \
    && apt-get install -y \
        postgresql-$PG_MAJOR=$PG_VERSION \
        postgresql-contrib-$PG_MAJOR=$PG_VERSION \
    && rm -rf /var/lib/apt/lists/*

ENV PATH /usr/lib/postgresql/$PG_MAJOR/bin:$PATH

RUN rmdir /run/postgresql
RUN rmdir /var/lib/postgresql
RUN rmdir /var/log/postgresql

RUN userdel postgres

COPY entry.py /entry.py
# COPY postgresql.conf /usr/share/postgresql/$PG_MAJOR/postgresql.conf.sample
# COPY pg_hba.conf /usr/share/postgresql/$PG_MAJOR/pg_hba.conf.sample
COPY postgresql.conf /postgresql.conf
COPY pg_hba.conf /pg_hba.conf
