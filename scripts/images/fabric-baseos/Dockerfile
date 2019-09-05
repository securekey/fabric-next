#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
ARG FABRIC_BASE_OS_IMAGE=hyperledger/fabric-baseos
ARG ARCH=amd64
ARG FABRIC_BASE_VERSION=0.4.8
FROM ${FABRIC_BASE_OS_IMAGE}:${ARCH}-${FABRIC_BASE_VERSION}
LABEL maintainer=sk-dev-team

#RUN export OS=`awk -F= '/^NAME/{print $2}' /etc/os-release | awk '{print $1}' | tr -d '"' | tr '[[:upper:]]' '[[:lower:]]'` \
#    && sed -i -e "s/http:\/\/archive/mirror:\/\/mirrors/" -e "s/\/$OS\//\/mirrors.txt/" /etc/apt/sources.list \
#    && sed -i -e "s/http:\/\/security/mirror:\/\/mirrors/" -e "s/\/$OS\//\/mirrors.txt/" /etc/apt/sources.list

RUN apt-get update -qq \
    && apt-get install -y libltdl7 aria2 \
    && apt-get -y --print-uris dist-upgrade | egrep -o -e "http://[^\']+" | aria2c -c -d /var/cache/apt/archives -i -; apt-get -y dist-upgrade \
    && apt-get clean autoclean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/cache /var/lib/log
