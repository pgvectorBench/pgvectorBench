FROM debian:bookworm as builder

RUN apt update
RUN apt install -y build-essential git cmake ninja-build libpq-dev wget
RUN wget https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-bookworm.deb -P /tmp
RUN apt install -y /tmp/apache-arrow-apt-source-latest-bookworm.deb
RUN apt update
RUN apt install -y libparquet-dev

WORKDIR /opt/pgvectorbench
COPY . /opt/pgvectorbench/

RUN git submodule update --init --recursive
RUN cmake -G Ninja -S . -B build
RUN cmake --build build -j

FROM debian:bookworm-slim

RUN apt update && apt install -y libpq-dev wget
RUN wget https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-bookworm.deb -P /tmp
RUN apt install -y /tmp/apache-arrow-apt-source-latest-bookworm.deb && apt update && apt install -y libparquet-dev

COPY --from=builder /opt/pgvectorbench/build/pgvectorbench /usr/local/bin

LABEL maintainer="Junwang Zhao <zhjwpku@gmail.com>"

ENTRYPOINT [ "sh", "-c", "pgvectorbench $@" ]
