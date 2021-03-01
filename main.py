import connect
import pandas as pd
import datetime
import time
import sys
import json
from constants import *
import gspread


gc = gspread.service_account()
wait_time_in_seconds = 300


def main():
    result = get_reservas()
    # print(result)



def get_reservas():
    if start_update():
        return "Success"
    else:
        return "Error"


def start_update():
    """ Returns True if started the Update """
    timestamp = get_timestamp()
    update_data(timestamp)
    return True



#def is_too_soon(new_request_time):
#    """ Returns True if it is Too Soon to call again """
#    with open(LOG_FILE) as f:
#        json_data = json.load(f)
#        print(json_data['logs'][-1])
#        last_request_time = int(json_data['logs'][-1]['EPOCH_TIME'])
#    # returns True when it is too soon
#    if new_request_time - last_request_time < wait_time_in_seconds:
#        return True
#    else:
#        return False


def get_timestamp():
    """ Returns the timestamp from Now """
    now = datetime.datetime.now() - datetime.timedelta(hours=3)
    return [datetime.datetime.strftime(now, '%d/%m %Hh%M'), time.time()]


#def log_to_json(response, timestamp):
#    """ Salva todas requisições num Log """
#    new_data = {
#        'EPOCH_TIME': timestamp[1],
#        'TIMESTAMP': timestamp[0],
#        'RESPONSE': response,
#    }
#
#    with open(LOG_FILE) as f:
#        json_data = json.load(f)
#        with open("backup-log.json", "w") as b_file:
#            json.dump(json_data, b_file, indent=4)
#        temp = json_data["logs"]
#        temp.append(new_data)
#
#    with open(LOG_FILE, 'w') as f:
#        json.dump(json_data, f, indent=4)


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
            elif type(data) == int:
                data = data
            else:
                data = data.strip()
            temp_reserva.append(data)
        tup_temp = tuple(temp_reserva)
        new_reservas.append(tup_temp)
    reservas_data = new_reservas

    # Criando novo log com os dados do request e arquivo correspondente
#    log_to_json(reservas_data, stamp)

    # Transforma os dados do DB em DataFrame Pandas
    google_uploader(reservas_data, stamp[0])


def google_uploader(data, timestamp):
    """ Gets DataFrame and uploads to Google Sheet"""
    df_from_json = make_df(data)
    detailed_output = detailed_data_treatment(df_from_json)
    summary_output = summary_data_treatment(df_from_json)
    attempt = 1
    new_worksheet = ''
    try:
        sh = gc.open("Reservas Seller")
        new_worksheet = sh.add_worksheet(title=f"{timestamp}", rows="4000", cols="15")
    except gspread.exceptions.APIError:
        sh = gc.open("Reservas Seller")
        new_worksheet = sh.add_worksheet(title=f"{timestamp}-{attempt}", rows="4000", cols="15")
        attempt += 1
    finally:
        new_worksheet.update([detailed_output.columns.values.tolist()] + detailed_output.values.tolist())


def make_df(data):
    """ Transforma os dados vindos do database em DataFrame Pandas"""
    df = pd.DataFrame.from_records(
        data,
        columns=[
            'Data do Turno',
            'Hora de Cadastro da Reserva',
            'Posição',
            'Turno',
            'Corretor',
            'Imóvel'
        ]
    )
    return df


def summary_data_treatment(df):
    agents_list = df.groupby(['Corretor']).groups.keys()
    summary_count = df.groupby(['Corretor', 'Posição'], as_index=False)[['Turno']].count()
    positions = df.groupby(['Posição']).groups.keys()

    agents_summary = pd.DataFrame(columns=["Nome", *positions, "Total"])

    for agent in agents_list:
        total = 0
        agent_sum_dict = {"Nome": agent}

        for position in positions:
            reservations = summary_count[
                (summary_count['Corretor'] == agent) &
                (summary_count['Posição'] == position)
            ]['Turno'].sum()

            agent_sum_dict[position] = reservations
            total += reservations

        agent_sum_dict["Total"] = total
        agents_summary = agents_summary.append(agent_sum_dict, ignore_index=True)

    return agents_summary


def detailed_data_treatment(df):
    estates_list = df.groupby(['Imóvel']).groups.keys()

    details_header = ['Imóvel', 'Data do Turno', 'Turno', 'Corretor']
    details_groupby = df.groupby(details_header, as_index=False)
    details_toframe = pd.DataFrame(list(details_groupby.groups.keys()))
    details_toframe.columns = details_header

    shifts = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]
    # shifts_toframe = pd.DataFrame(shifts)

    dates_groupby = df.groupby(['Data do Turno'], as_index=False)
    dates_toframe = list(dates_groupby.groups.keys())
    print(dates_toframe)

    header = []
    formatted_header = []
    for date in dates_toframe:
        for shift in shifts:
            header.append(date + " " + shift)
            formatted_header.append(str(date).replace("/2021", "") + shift.replace("Turno da", ""))

    reservations_detailed = pd.DataFrame(columns=["Imóvel", *header])

    for estate in estates_list:
        estate_reservations_dict = {"Imóvel": estate}
        for shift in header:
            res_date = shift.split()[0]
            res_shift = " ".join(shift.split()[1:])
            agent_reserved = details_toframe[
                (details_toframe['Imóvel'] == estate) &
                (details_toframe['Data do Turno'] == res_date) &
                (details_toframe['Turno'] == res_shift)
            ]['Corretor'].values
            if agent_reserved.size > 0:
                for i, agent in enumerate(agent_reserved):
                    if i == 0:
                        estate_reservations_dict[shift] = agent
                    else:
                        estate_reservations_dict[shift] += "\n" + agent
            else:
                estate_reservations_dict[shift] = ""
        reservations_detailed = reservations_detailed.append(estate_reservations_dict, ignore_index=True)

    reservations_detailed.columns = ["Imóveis", *formatted_header]
    print(reservations_detailed)

    return reservations_detailed


if __name__ == "__main__":
    main()