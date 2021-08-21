FROM python:3.8-bullseye

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .

CMD [ "python", "main.py" ]