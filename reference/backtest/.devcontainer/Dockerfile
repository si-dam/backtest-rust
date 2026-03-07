FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

# Work around expired/missing Yarn apt key in the base image.
# Devcontainer features run apt commands; removing this source prevents build failure.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /etc/apt/sources.list.d/yarn.list.save
