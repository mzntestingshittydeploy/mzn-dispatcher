FROM python:3.9

RUN useradd -m mzn-dispatcher
USER mzn-dispatcher

COPY requirements.txt .
ENV PATH="${PATH}:/home/mzn-dispatcher/.local/bin"
RUN pip install --user -r requirements.txt

COPY main.py .

CMD python3 main.py
