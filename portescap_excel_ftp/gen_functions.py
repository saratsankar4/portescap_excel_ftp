from config import config as cfg
from db_connection import read_from_db, save_to_db
import os, os.path
import numpy as np
import pandas as pd
from datetime import datetime, time
import schedule
import time
import datetime
import pysftp
import ftplib

cnopts = pysftp.CnOpts()

table_details = cfg('table_details')

db_details = cfg('db')

f_path = cfg('folder_path')

ftp = cfg('sftp')


def get_datetime_format(date_time):
    # convert to datetime object
    date_time = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
    # convert to human readable date time string
    return date_time.strftime('%Y-%m-%d %H:%M:%S')


def get_size_format(n, suffix="B"):
    # converts bytes to scaled format (e.g KB, MB, etc.)
    for unit in ["", "K", "M", "G", "T", "P"]:
        if n < 1024:
            return f"{n:.2f}{unit}{suffix}"
        n /= 1024


def get_last_time(file_name):
    records = read_from_db("select max(last_modified) from "+table_details['folder_data'] +
                           " where file_name = " + "'" + str(file_name) + "'", cfg('db'))

    print(records['max'][0], file_name)
    return records


def update_table(file_name, temp_dte):
    df = pd.DataFrame()

    df = df.append({'file_name': file_name, 'last_modified': temp_dte}, ignore_index=True)

    save_to_db(table_details['folder_data'], "update", cfg('db'), df)


def check_update(file_name, meta):

    modificationtime = get_datetime_format(meta.get("modify"))

    last_sav_dte = get_last_time(file_name)

    if last_sav_dte['max'][0]:
        last_sav = pd.to_datetime(last_sav_dte['max'][0], format='%Y-%m-%d %H:%M:%S')
        last_sav = last_sav.strftime('%Y-%m-%d %H:%M:%S')
        print(modificationtime, last_sav)
        if modificationtime != last_sav:
            update_table(file_name, last_sav)
            return True
        else:
            return False
    else:
        return True


def insert_file_info(file_name, meta, file_size):
    df = pd.DataFrame()
    src_file_name = file_name

    modificationtime = get_datetime_format(meta.get("modify"))

    file_size = get_size_format(file_size)

    file_type = meta.get("type")

    end_date = '31/12/2999'
    df = df.append(
        {'file_name': src_file_name, 'type': file_type, 'last_modified': modificationtime, 'size': file_size,
         'end_date': end_date},
        ignore_index=True)

    save_to_db(table_details['folder_data'], "append", cfg('db'), df)


def write_file_data(a):

    dest_file_path = f_path['destination_path']

    file_name = dest_file_path + '/' + a

    df = pd.read_excel(file_name)
    df.rename(columns={'NO. ARTICLE': 'article_no', 'DESIGNATION': 'designation', 'NO. O.F.': 'no_of', "DATE ": 'date',
                       'NO.MESURE': 'no_mesure', 'R (OHMS)': 'r_ohms', 'I (mA)': 'i_ma', 'V (T/min)': 'v_t_min',
                       'K (mNm/A)': 'k_mnm_a', 'UD (V)': 'ud_v', 'OND (%)': 'ond'}, inplace=True)

    df = df.dropna(how='all')
    df['article_no'] = file_name[:8]
    df['rep_date'] = file_name[11:21]
    df['rep_time'] = file_name[-9:-7] + ":" + file_name[-6:-4]

    save_to_db(table_details['files_data'], "append", cfg('db'), df)


def copy_remote_local(src_path, dest_file_path):
    myhostname = ftp['host']
    myusername = ftp['user']
    mypassword = ftp['pass']

    # os.chdir(dest_file_path)
    src_file_path = src_path
    print(src_path)

    ftp_server = ftplib.FTP(myhostname, myusername, mypassword)
    ftp_server.cwd(src_path)

    for file_data in ftp_server.mlsd():
        # extract returning data
        file_name, meta = file_data
        # i.e directory, file or link, etc
        file_type = meta.get("type")
        if file_type == "file":

            ftp_server.voidcmd("TYPE I")
            file_size = ftp_server.size(file_name)
            if check_update(file_name, meta):
                insert_file_info(file_name, meta, file_size)

                dest_file = os.path.join(dest_file_path, file_name)

                with open(dest_file, "wb") as file:
                    # use FTP's RETR command to download the file
                    ftp_server.retrbinary(f"RETR {file_name}", file.write)

                write_file_data(file_name)
    ftp_server.quit()


def job():
    src_path = f_path['source_path']
    dest_file_path = f_path['destination_path']
    # files = os.listdir(str(folder_path))
    # f_info = read_from_db("select * from " + table_details['folder_data'], cfg('db'))

    copy_remote_local(src_path, dest_file_path)

# schedule.every(5).seconds.do(job)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
