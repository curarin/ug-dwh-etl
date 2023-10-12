def hello_world(message):
    message = "started"
    return message
print(hello_world("message"))

import pandas as pd
import datetime
from datetime import date
import numpy as np
import os
from imbox import Imbox # pip install imbox
import imbox
import traceback
from datetime import datetime
import datetime
import re
import requests
from google.oauth2 import service_account
import pandas_gbq



def login(user_name):
    print(user_name)
    host = "###"
    username = "###"
    password = '###'
    download_folder = "/tmp/attachments"
    now = datetime.datetime.now()
    y = now.strftime("%Y")
    y = int(y)
    m = now.strftime("%m")
    m = int(m)
    d = now.strftime("%d")
    d = int(d)

    if not os.path.isdir(download_folder):
        os.makedirs(download_folder, exist_ok=True)

    mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
    # holt die email raus, die am heutigen tag geschickt wurde
    messages = mail.messages(sent_from="###", sent_to="###", date__on=datetime.date(y, m, d), folder="all", label="###")
    for uid, message in messages:
        mail.mark_seen(uid) # optional, mark message as read

        for idx, attachment in enumerate(message.attachments):
            try:
                att_fn = attachment.get("filename")
                download_path = f"{download_folder}/{att_fn}"
                print(download_path)
                with open(download_path, "wb") as fp:
                    fp.write(attachment.get('content').read())
            except:
                print(traceback.print_exc())
    mail.logout()
    df = pd.read_csv(download_path, delimiter=";", encoding="latin-1")
    #### data manipulation ###
    ### Daten Bereinigen
    ### String to Date
    df["CRS (Standard) Reisebeginn"] = pd.to_datetime(df["CRS (Standard) Reisebeginn"], format="%d.%m.%Y")
    df["CRS (Standard) Reiseende"] = pd.to_datetime(df["CRS (Standard) Reiseende"], format="%d.%m.%Y")
    # Replace the period (.) with an empty string, replace the comma (,) with a dot (.) and then convert to float
    df["Leistung Element Preis"] = df["Leistung Element Preis"].str.replace('\.', '', regex=True).str.replace(',', '.', regex=False).astype(float)
    df["Leistung Initialer Preis"] = df["Leistung Initialer Preis"].str.replace('\.', '', regex=True).str.replace(',', '.', regex=False).astype(float)
    df["Leistung Anlagedatum"] = pd.to_datetime(df["Leistung Anlagedatum"], format="%d.%m.%Y")
    df["Zeit von Buchung bis Start"] = (df["CRS (Standard) Reisebeginn"] - df["Leistung Anlagedatum"]).dt.days # berechnet Zeit zwischen Buchung und reisebeginn
    df["CRS (Standard) original Buchungsnummer"] = pd.to_numeric(
        df["CRS (Standard) original Buchungsnummer"],
        errors='coerce',
        downcast='integer'
    ).fillna(0).astype(int)

    column_name_mapping = {
        'Leistung Anlagedatum': 'buchungsdatum',
        'Auftrag Vermittler (Auftrag)': 'vermittler',
        'Leistung Element Preis': 'preis',
        'CRS (Standard) Reisebeginn': 'reisebeginn',
        'CRS (Standard) Reiseende': 'reiseende',
        'Leistung Abflughafen Beschreibung': 'abflughafen',
        'Leistung Rückflug Abflughafen Beschreibung': 'landeflughafen',
        'Leistung Hotelort': 'hotelort',
        'Leistung Land Beschreibung': 'land',
        'Leistung Beschreibung': 'leistungbeschreibung',
        'Leistung Kategorie': 'sterne',
        'CRS (Standard) Personenzahl': 'personen',
        'CRS (Standard) Status': 'buchungs_status',
        'CRS (Standard) Stornodatum': 'stornodatum',
        'Leistungsattribut Wert': 'agent',
        'Leistung Initialer Preis': 'preis_initial',
        'CRS (Standard) ExtId': 'ext_id',
        'CRS (Standard) original Buchungsnummer': 'buchungsnummer',
        'Zeit von Buchung bis Start': 'tage_bis_reisestart'
    }

    # Rename the columns using the defined mapping
    df = df.rename(columns=column_name_mapping)
    columns_to_keep = (list(column_name_mapping.values()))
    df = df[columns_to_keep]


    bq = "###"
    credentials = service_account.Credentials.from_service_account_file(
            'key.json',
        )

                    
    ### push to gbq
    pandas_gbq.to_gbq(df, bq, project_id="###", if_exists="append", credentials=credentials)
    print("Data pushed to GBQ.")
    string_to_return = "Success"
    return string_to_return

string_to_return = login("Login Function started")
print(string_to_return)