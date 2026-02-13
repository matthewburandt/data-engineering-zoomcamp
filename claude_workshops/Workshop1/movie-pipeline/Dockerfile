FROM python:3.13-slim

RUN pip install\
    pandas\
    pyarrow

WORKDIR /app

RUN mkdir -p data

COPY generate_data.py generate_data.py

ENTRYPOINT ["python", "generate_data.py"]

