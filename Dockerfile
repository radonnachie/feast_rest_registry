# syntax=docker/dockerfile:1
FROM python:3.8

WORKDIR /work/

# layer install the requirements to expedite image rebuilds during development
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN pip install .

EXPOSE 80
CMD ["feast_rest_registry", "--host", "0.0.0.0", "--port", "80", "postgresql+psycopg2://feast:feast@db:5432/feast", "-l", "feast_rest_registry.log", "-vvv"]
