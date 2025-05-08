FROM python:3.12.8

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "data_extraction.py"]
