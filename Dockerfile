FROM ubuntu:20.04

# 设置环境变量 
ENV DEBIAN_FRONTEND noninteractive

# 设置时区
ARG TZ=Asia/Shanghai
ENV TZ ${TZ}

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 用 root 用户操作
USER root


RUN sed -i "s/security.ubuntu.com/mirrors.aliyun.com/" /etc/apt/sources.list && \
    sed -i "s/archive.ubuntu.com/mirrors.aliyun.com/" /etc/apt/sources.list && \
    sed -i "s/security-cdn.ubuntu.com/mirrors.aliyun.com/" /etc/apt/sources.list
RUN  apt-get clean

RUN apt update -y
RUN apt install -y man vim curl wget gcc gdb lsof net-tools build-essential make sudo software-properties-common
RUN apt update -y

RUN sudo add-apt-repository ppa:longsleep/golang-backports
RUN sudo apt update -y
RUN sudo apt install -y golang-go

# RUN rm -rf /var/lib/apt/lists/*

RUN mkdir mit-6.824-labs

CMD ["/bin/bash"]
