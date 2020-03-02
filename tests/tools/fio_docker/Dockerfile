FROM golang:1

RUN apt-get update \
    #
    # Install fio
    && apt-get install fio -y

WORKDIR "/fio-data"
ENTRYPOINT [ "fio" ]
