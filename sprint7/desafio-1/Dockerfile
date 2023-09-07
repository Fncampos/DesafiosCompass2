FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install boto3

RUN pip install python-dotenv

RUN mkdir /app/dados

CMD ["python", "uploadLocal-s3.py"]