# Workspace image for this ProjLab.
# One image backs GitHub Codespaces, VS Code Dev Containers and plain `docker compose up`,
# so everything the project needs is installed here (Dev Container features only add editor
# conveniences such as the GitHub CLI).
ARG BASE_IMAGE=mcr.microsoft.com/devcontainers/python:1-3.12-bookworm
FROM ${BASE_IMAGE}

# Java 17 for Spark, copied from the official Eclipse Temurin JRE image. This works the same in
# Codespaces, Dev Containers and plain `docker compose up` (Dev Container features would only
# apply to the first two), needs no apt repositories, and is multi-arch (amd64 and arm64).
COPY --from=eclipse-temurin:17.0.20.1_1-jre-jammy /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# The base image ships a global pipx `pytest` that would shadow the pinned one from requirements.txt.
RUN rm -f /usr/local/py-utils/bin/pytest /usr/local/py-utils/bin/py.test

COPY requirements.txt /tmp/projlab/requirements.txt
RUN pip install --no-cache-dir -r /tmp/projlab/requirements.txt && rm -rf /tmp/projlab
