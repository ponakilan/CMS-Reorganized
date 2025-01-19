FROM ponakilan/cmsbase:latest

WORKDIR /app

COPY . /app/
COPY ./requirements.txt /app/

RUN pip install -r requirements.txt
CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]
