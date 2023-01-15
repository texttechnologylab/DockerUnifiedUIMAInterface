FROM python:3.8

WORKDIR /usr/src/app

RUN pip install setuptools wheel
RUN pip install --upgrade pip
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-deps -r ./requirements.txt

RUN pip install --no-binary :all: nmslib
RUN pip install langdetect

# service script
COPY ./python/dkpro-core-types.xml ./dkpro-core-types.xml
COPY ./python/communication.lua ./communication.lua
COPY ./python/duui_languagedetection.py ./duui_languagedetection.py

ENTRYPOINT ["uvicorn", "duui_languagedetection:app", "--host", "0.0.0.0", "--port", "9714"]
CMD ["--workers", "1"]