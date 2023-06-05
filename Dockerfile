# syntax=docker/dockerfile:1
FROM python:3.8

RUN pip install psycopg2 feast fastapi "uvicorn[standard]"

WORKDIR /work/src/feast_rest_registry

EXPOSE 80
CMD ["uvicorn", "feastregistry_restserver:app", "--host", "0.0.0.0", "--port", "80"]
