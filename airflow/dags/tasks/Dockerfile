#Used to make Image for ETL script task to run with DockerOperator

FROM python:3.10

WORKDIR /code

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY etlScript.py .

ENTRYPOINT ["python", "etlScript.py"]
