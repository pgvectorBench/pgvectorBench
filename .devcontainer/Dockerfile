ARG LC_ALL=C.UTF-8
ARG LANG=C.UTF-8

FROM debian:stable

RUN apt update
RUN apt install -y build-essential git cmake llvm-dev clang ninja-build libpq-dev vim lsb-release wget
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb -P /tmp
RUN apt install -y -V /tmp/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt update
RUN apt install -y libparquet-dev

ARG LC_ALL
ARG LANG

ENV LC_ALL=$LC_ALL LANG=$LANG

WORKDIR /opt/pgvectorbench
