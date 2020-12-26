from flask import Flask, send_file, render_template, redirect, request, url_for
import secrets
import connect
import pandas as pd
import datetime
import time
import json
from constants import *

app = Flask(__name__)

wait_time_in_minutes = 5
wait_time_in_seconds = 60 * wait_time_in_minutes


@app.route('/')
def home():
    return render_template("index.html")


@app.route('/tente-mais-tarde')
def try_later():
    return render_template("too-soon.html", wait=wait_time_in_minutes)


@app.route('/reservas')
def get_reservas():
    if request.form.get('reservas_key') == secrets.RESERVAS_KEY:
        timestamp = get_timestamp()
        if not is_too_soon(timestamp[1]):
            reservas_data = connect.query_reservas()
            log_to_json(reservas_data, timestamp)
            file = excel_maker(reservas_data, timestamp[0])
            print('redirect to home')
        else:
            return redirect(url_for('try_later'))
    else:
        return jsonify({"error": "Sorry, that's not allowed. Make sure you have the correct key"}), 403


def excel_maker(data, time):
    df = pd.DataFrame.from_records(
        data,
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
    print(df)
    excel_file = df.to_excel(f"Reservas {time}.xlsx", header=True, index=False)
    return excel_file


# Returns True if it is Too Soon to call again
def is_too_soon(new_request_time):
    with open(LOG_FILE) as f:
        json_data = json.load(f)
        last_request_time = json_data[REQUEST_LOGS][-1][EPOCH_TIME]
    if new_request_time - last_request_time < wait_time_in_seconds:
        # returns True because it is too soon
        return True
    else:
        return False


def get_timestamp():
    now = datetime.datetime.now()
    return [datetime.datetime.strftime(now, '%d-%m-%Y %H:%M'), time.time()]


def log_to_json(response, timestamp):
    new_data = {
        EPOCH_TIME: timestamp[1]
        TIMESTAMP: timestamp[0],
        RESPONSE: response,
    }
    with open(LOG_FILE) as f:
        json_data = json.load(f)

    data = json_data[REQUEST_LOGS].append(new_data)

    with open(LOG_FILE, 'w') as write_file:
        json.dump(data, write_file)


if __name__ == "__main__":
    app.run(debug=True)