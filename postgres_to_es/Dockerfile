FROM python:3.10

RUN apt-get update && \
    apt-get clean && \
    pip install --upgrade pip && \
    pip install poetry

COPY poetry.lock pyproject.toml /app/
WORKDIR /app

COPY . /app
RUN poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi



RUN ls

CMD [ "python", "main.py" ]