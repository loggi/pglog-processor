# pgbadger (PostgreSQL log Analyzer) is written in Perl
FROM perl
MAINTAINER Marcio Ribeiro <binary@b1n.org>

# Install pgbadger and JSON::XS
ENV PGBADGER_VERSION 7.1
ENV PGBADGER_URL https://github.com/dalibo/pgbadger/archive/v${PGBADGER_VERSION}.tar.gz
RUN curl -L ${PGBADGER_URL} | tar -C /opt -xzf - \
    && cd /opt/pgbadger-${PGBADGER_VERSION} \
    && perl Makefile.PL \
    && make install \
    && cpanm JSON::XS

# Install golang
ENV GOROOT /opt/go
ENV GOPATH /opt/local/go
ENV GOBIN ${GOPATH}/bin
ENV PROJ_PATH  ${GOPATH}/src/pg_logger
ENV PATH ${PATH}:${GOROOT}/bin:${GOBIN}
ENV GO_VERSION 1.4.2
ENV GO_FILENAME go${GO_VERSION}.linux-amd64
ENV GO_TARNAME ${GO_FILENAME}.tar.gz
ENV GO_URL https://storage.googleapis.com/golang/${GO_TARNAME}
RUN mkdir -p ${GOROOT}
RUN curl -L ${GO_URL} | tar -C /opt -xzf - \
    && mkdir -p ${GOPATH}/src \
    && mkdir -p ${GOPATH}/bin \
    && mkdir -p ${GOPATH}/pkg

# Copy & Run pg_logger
COPY main.go ${PROJ_PATH}/
WORKDIR ${PROJ_PATH}
RUN go get && go install
ENTRYPOINT ["pg_logger"]
