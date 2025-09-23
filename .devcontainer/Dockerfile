FROM python:3.12.9

# === general ===
SHELL ["/bin/bash", "-c"]
ARG username=kupo

# === python settings ===
ENV TZ=Europe/Vienna

# === poetry settings ===
ENV POETRY_VERSION=1.8.4 \
    POETRY_HOME="/usr/local/" \
    POETRY_VIRTUALENVS_IN_PROJECT=1 

# === poetry installation ===
RUN python -m pip install --upgrade pip setuptools

RUN curl -sSL https://install.python-poetry.org | python - && poetry completions bash >> ~/.bash_completion  

RUN useradd --create-home ${username}

USER ${username}

CMD ["/bin/bash", "-c", "echo 'Just sleeping here ...'; while sleep 1000; do :; done"]