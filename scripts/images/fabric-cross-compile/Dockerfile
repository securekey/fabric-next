#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

# Start from the sk dynamic ccenv for now, but will be fabric-ccenv image in future
ARG FABRIC_CCENV_IMAGE=securekey/fabric-dynamic-ccenv
ARG FABRIC_CCENV_TAG=x86_64-latest
FROM ${FABRIC_CCENV_IMAGE}:${FABRIC_CCENV_TAG}

# LABELS
LABEL maintainer=sk-dev-team

# Mark current sources for amd64 arch
RUN sed -i "s/deb /deb [arch=amd64] /" /etc/apt/sources.list

# Add the ubuntu ports source for the s390x architecture
RUN echo  'deb [arch=s390x] http://ports.ubuntu.com/ubuntu-ports xenial main' >> /etc/apt/sources.list

# Add the s390x architecture
RUN dpkg --add-architecture s390x

# Update list of packages
RUN apt update

# Downgrade libc so that it does not break libc:s390x
RUN apt-get install -y --allow-downgrades libc6=2.23-0ubuntu3

# Install essential s390x lib tools
RUN apt-get install -y \
    libc6:s390x \
    libltdl7:s390x \
    libltdl-dev:s390x

# Install aptitude so that it handles dependencies for gcc
RUN apt-get install -y aptitude

# Install gcc compiler for s390x
RUN aptitude install -y gcc-multilib-s390x-linux-gnu


