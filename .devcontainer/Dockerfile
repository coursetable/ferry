FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye

ENV PYTHONUNBUFFERED 1

# Remove Yarn repo - base image has it configured but GPG key is expired.
# Bullseye is archived, and deb.debian.org returns 404 for security packages.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && sed -i 's|http://deb.debian.org|http://archive.debian.org|g' /etc/apt/sources.list \
    && printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99no-check-valid-until \
    && apt-get update && export DEBIAN_FRONTEND=noninteractive \
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
