from flask import Flask, render_template, redirect, url_for
import connect
import pandas as pd
import datetime
import time
import json
from constants import *
import gspread
import os

app = Flask(__name__)
gc = gspread.service_account()

wait_time_in_minutes = 5
wait_time_in_seconds = 60 * wait_time_in_minutes


@app.route('/')
def home():
    return render_template("index.html")


@app.route('/reservas')
def get_reservas():
    if start_update():
        return redirect(url_for('downloaded'))
    else:
        return redirect(url_for('try_later'))


@app.route('/tente-mais-tarde')
def try_later():
    return render_template("too-soon.html", wait=wait_time_in_minutes)


@app.route('/atualizado')
def downloaded():
    return render_template("download.html")


def is_too_soon(new_request_time):
    """ Returns True if it is Too Soon to call again """
    with open(LOG_FILE) as f:
        json_data = json.load(f)
    last_request_time = int(json_data['logs'][-1][EPOCH_TIME])
    if new_request_time - last_request_time < wait_time_in_seconds:
        # returns True because it is too soon
        return True
    else:
        return False


def get_timestamp():
    """ Returns the timestamp from Now """
    now = datetime.datetime.now()
    return [datetime.datetime.strftime(now, '%d/%m %Hh%M'), time.time()]


def log_to_json(response, timestamp):
    """ Salva todas requisições num Log """
    new_data = {
        EPOCH_TIME: timestamp[1],
        TIMESTAMP: timestamp[0],
        RESPONSE: response,
    }

    with open(LOG_FILE) as f:
        json_data = json.load(f)
        with open("backup-log.json", "w") as b_file:
            json.dump(json_data, b_file, indent=4)
        temp = json_data["logs"]
        temp.append(new_data)

    with open(LOG_FILE, 'w') as f:
        json.dump(json_data, f, indent=4)


def start_update():
    """ Returns True if started the Update """
    timestamp = get_timestamp()
    if not is_too_soon(timestamp[1]):
        update_data(timestamp)
        return True
    else:
        return False


def update_data(stamp):
    # Fazendo query no banco de dados para extrair valores
    reservas_data = connect.query_reservas()

    # Tratamento para erro de Datetime. Transformando datetime em string
    new_reservas = []
    for reserva in reservas_data:
        temp_reserva = []
        for data in reserva:
            if isinstance(data, datetime.time):
                data = data.strftime('%Hh%M')
            temp_reserva.append(data)
        tup_temp = tuple(temp_reserva)
        new_reservas.append(tup_temp)
    reservas_data = new_reservas

    # Nome do arquivo onde ficará salvo o response da query
    response_filename = f'{round(stamp[1])}.txt'
    # Salvando response na pasta de response_files
    with open(f'./response_files/{response_filename}', 'w') as f:
        f.write(str(reservas_data))

    # Criando novo log com os dados do request e arquivo correspondente
    log_to_json(response_filename, stamp)

    # Transforma os dados do DB em DataFrame Pandas
    google_uploader(reservas_data, stamp[0])


def google_uploader(data, timestamp):
    """ Gets DataFrame and uploads to Google Sheet"""
    df = make_df(data)
    attempt = 1
    try:
        sh = gc.open("Reservas Seller")
        new_worksheet = sh.add_worksheet(title=f"{timestamp}", rows="4000", cols="15")
    except gspread.exceptions.APIError:
        new_worksheet = sh.add_worksheet(title=f"{timestamp}-{attempt}", rows="4000", cols="15")
        attempt += 1
    finally:
        new_worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def make_df(d):
    """ Transforma os dados vindos do database em DataFrame Pandas"""
    df = pd.DataFrame.from_records(
        d,
        columns=[
            'ID da Reserva',
            'Data do Turno',
            'Início do Turno',
            'Final do Turno',
            'Hora de Cadastro da Reserva',
            'Posição',
            'Turno',
            'Corretor',
            'Imóvel'
        ]
    )
    return df


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
