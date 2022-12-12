FROM python:3.9.13-slim-buster

RUN adduser --disabled-password --gecos '' appuser
USER appuser

WORKDIR /home/appuser
COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "app.py"]
