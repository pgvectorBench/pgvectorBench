FROM debian:bookworm AS builder

RUN apt update
RUN apt install -y build-essential cmake git ninja-build libpq-dev wget
RUN wget https://packages.apache.org/artifactory/arrow/debian/apache-arrow-apt-source-latest-bookworm.deb -P /tmp
RUN apt install -y /tmp/apache-arrow-apt-source-latest-bookworm.deb
RUN apt update
RUN apt install -y libparquet-dev

WORKDIR /opt/pgvectorbench
COPY . /opt/pgvectorbench/

RUN cmake --preset system-arrow
RUN cmake --build --preset system-arrow --parallel

FROM debian:bookworm-slim

RUN apt update && apt install -y libpq-dev wget
RUN wget https://packages.apache.org/artifactory/arrow/debian/apache-arrow-apt-source-latest-bookworm.deb -P /tmp
RUN apt install -y /tmp/apache-arrow-apt-source-latest-bookworm.deb && apt update && apt install -y libparquet-dev

COPY --from=builder /opt/pgvectorbench/build-system/pgvectorbench /usr/local/bin

LABEL maintainer="Junwang Zhao <zhjwpku@gmail.com>"

ENTRYPOINT [ "sh", "-c", "pgvectorbench $@" ]
