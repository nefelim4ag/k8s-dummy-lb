FROM python:3.14
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app.py /usr/bin/
CMD /usr/bin/app.py
