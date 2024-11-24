FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye

ENV PYTHONUNBUFFERED 1

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends gcc g++ musl-dev libffi-dev libpq-dev libhdf5-dev python3-tables graphviz libgraphviz-dev curl

ENV VIRTUAL_ENV=/usr/local
ADD --chmod=755 https://astral.sh/uv/install.sh /install.sh
RUN /install.sh && rm /install.sh

ENV PATH="/root/.local/bin/:$PATH"

# pyright requires Node
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash \
    && . $HOME/.bashrc \
    && nvm install node \
    && npm install -g pyright

COPY . .

RUN uv pip install --no-cache -e . --system
