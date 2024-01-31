# Yandex.Tank
#
# VERSION 0.0.4

FROM ubuntu:focal
MAINTAINER Yandex Load Team <load-public@yandex-team.ru>
# Version for desription
ARG VERSION
# You may specify tag instead of branch to build for specific tag
ARG BRANCH=master

LABEL Description="Fresh Yandex.Tank from github master branch with phantom" \
    Vendor="Yandex" \
    maintainer="direvius@yandex-team.ru" \
    YandexTank.version="${VERSION}" \
    Telegraf.version="${TELEGRAF_VERSION}"

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -q && \
    apt-get install --no-install-recommends -yq \
        sudo     \
        vim      \
        wget     \
        curl     \
        less     \
        iproute2 \
        software-properties-common \
        telnet   \
        atop     \
        openssh-client \
        git            \
        gpg-agent      \
        python3-pip  && \
    add-apt-repository ppa:yandex-load/main -y && \
    apt-get update -q && \
    apt-get install -yq \
        phantom \
        phantom-ssl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /var/tmp/*

ENV TELEGRAF_VERSION=1.26.1-1

RUN wget https://dl.influxdata.com/telegraf/releases/telegraf_${TELEGRAF_VERSION}_amd64.deb && \
    dpkg -i telegraf_${TELEGRAF_VERSION}_amd64.deb && \
    rm -f telegraf_${TELEGRAF_VERSION}_amd64.deb*

ENV BUILD_DEPS="python3-dev build-essential gfortran libssl-dev libffi-dev"
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -yq --no-install-recommends ${BUILD_DEPS} && \
    pip3 install --upgrade setuptools && \
    pip3 install --upgrade pip && \
    pip3 install https://api.github.com/repos/yandex/yandex-tank/tarball/${BRANCH} && \
    apt-get autoremove -y ${BUILD_DEPS} && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* /tmp/* /var/tmp/* /root/.cache/*

RUN curl -L -v -o pandora https://github.com/yandex/pandora/releases/download/v0.5.3/pandora_0.5.3_linux_amd64 && \
    chmod +x ./pandora && \
    mv ./pandora /usr/local/bin/

RUN rm -rf /usr/lib/python3/dist-packages/OpenSSL && \
    pip3 install pyopenssl && \
    pip3 install pyopenssl --upgrade

COPY files/bashrc /root/.bashrc
COPY files/inputrc /root/.inputrc
RUN apt update && apt install -y golang-1.18
COPY . /root/tank
RUN bash -c 'cd /root/tank; /usr/lib/go-1.18/bin/go get && /usr/lib/go-1.18/bin/go build'
run cp /root/tank/tank /usr/local/bin


VOLUME ["/var/loadtest"]
WORKDIR /var/loadtest
ENTRYPOINT ["/usr/local/bin/yandex-tank"]
