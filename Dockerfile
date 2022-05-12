FROM python:3.9.12

ADD requirements.txt .
RUN python -m pip install -r requirements.txt

ADD portescap_excel_ftp /portescap_excel_ftp/
WORKDIR /portescap_excel_ftp/
CMD python /portescap_excel_ftpmain.py