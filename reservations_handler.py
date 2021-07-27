# -*- coding: utf-8 -*-
from connect import Connect
import pandas as pd
import gspread
import gspread_formatting as gsf
import date_calculator
from agent import Agent
from pdvs import Pdv
from pdv_types import PDV_TYPES
from shift_day import ShiftDay
from reservation import Reservation
from merging import merge_days
from copy_format import copy_format
from formatting import formatting_sheet
from rename_sheet import rename_sheet
from agents_abbreviations import ABBREVIATIONS
import datetime
import time
import math


class ReservationsHandler:

    def __init__(self, sheet):
        self.header_len = 0
        self.db_conn = Connect(2)
        self.seats = None
        self.detailed_worksheet = None
        self.sheet_name = ""
        self.raw_worksheet = None
        self.days_header = [""]
        self.shifts_header = []
        self.gc = gspread.service_account()
        self.sh = self.gc.open(sheet)
        self.days = date_calculator.get_next_week_from_monday(0)
        self.weekdays = ['', 'SEG', '', 'TER', '', 'QUA', '', 'QUI', '', 'SEX', '', 'SÁB', '', 'DOM', '']
        self.shift_times_sede = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]
        self.shift_times = ["Turno da Manhã", "Turno da Tarde"]
        self.current_sheets_list = None
        self.pdvs = PDV_TYPES.keys()

        self.pdv_reservations = {}
        self.get_current_time()
        self.data_handler()
        self.outputs_handler()

        self.calendar_formatting()
        self.update_calendar_values()

    def refresh_data(self):
        self.days_header = [""]
        self.get_current_time()
        self.data_handler()
        self.outputs_handler()
        self.rename_sheet()
        self.update_calendar_values()

    def rename_sheet(self):
        sheet_name = "%s | Reservas" % (self.timestamp)
        rename_body = rename_sheet(self.sh.worksheets()[-1].id, sheet_name)
        self.detailed_worksheet._properties['title'] = sheet_name
        res = self.sh.batch_update(rename_body)
        print(res)

    def get_current_time(self):
        now = datetime.datetime.now()
        self.current_sheets_list = self.sh.worksheets()
        stamp = [datetime.datetime.strftime(now, '%d/%m %Hh%Mm%Ss'), time.time()]
        self.timestamp = stamp[0]

    def data_handler(self):
        """ Fetch Data from DB """
        self.reservations_json = self.fetch_data()
        self.dataframe = self.make_dataframe()
        self.agents_list = self.get_agents()

        self.reservations_objects = self.create_reservations_objects()
        self.pdv_reservations = self.populate_table(self.shift_times)
        self.pdv_capacity = self.capacity_calculation()

    def outputs_handler(self):
        self.detailed_output = self.detailed_reservations()
        self.summary_output = self.summary_reservations()
        self.no_reservations_output = self.no_reservations_broker()

    def fetch_data(self):
        seats = self.db_conn.query(self.db_conn.pre_reservas)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        seats_df = pd.DataFrame.from_records(
            seats,
            columns=[
                'Vagas',
                'Posição',
                'Turno',
                'Dia da Semana',
                'Imóvel'
            ]
        )
        self.seats = seats_df

        data = self.db_conn.query(self.db_conn.query_reservas)
        return data

    def make_dataframe(self):
        dataframe = pd.DataFrame.from_records(
            self.reservations_json,
            columns=[
                'ID',
                'Corretor',
                'Data do Turno',
                'Hora de Cadastro',
                'Posição',
                'Turno',
                'Imóvel'
            ]
        )
        return dataframe

    def get_agents(self):
        agents = []
        agents_names = self.dataframe.groupby(['Corretor']).groups.keys()
        for agent_name in agents_names:
            new_agent = Agent(str(agent_name).strip())
            agents.append(new_agent)
        return agents

    def get_pdvs(self):
        pdvs = []
        pdvs_names = self.dataframe.groupby(['Imóvel']).groups.keys()
        for pdv_name in pdvs_names:
            new_pdv = Pdv(str(pdv_name).strip())
            pdvs.append(new_pdv)
        return pdvs

    def get_days(self):
        days_list = []
        days = self.dataframe.groupby(['Data do Turno']).groups.keys()
        for day in days:
            d = ShiftDay(str(day).strip())
            days_list.append(d)
        return days_list

    def create_reservations_objects(self):
        reservations = []
        for index, row in self.dataframe.iterrows():
            res_id = row['ID']
            pdv = next((pdv for pdv in self.pdvs if pdv == str(row['Imóvel']).strip()), row['Imóvel'])
            day = next((day for day in self.days if day == str(row['Data do Turno']).strip()), row['Data do Turno'])
            shift_time = next((shift for shift in self.shift_times if shift == str(row['Turno']).strip()), row['Turno'])
            agent = next((agent for agent in self.agents_list if agent.name == str(row['Corretor']).strip()),
                         row['Corretor'])
            reservation_time = row['Hora de Cadastro']
            position = row['Posição']

            res = Reservation(res_id, pdv, day, shift_time, agent, reservation_time, position)
            reservations.append(res)
        return reservations

    def populate_table(self, times):
        pdv_reservations = {}
        '''Colocar um if para pegar o caso onde o PDV seja SEDE SELLER, mas sem aceitar POSIÇÃO TELEFONE'''
        for pdv in self.pdvs:
            if pdv != "Sede Seller":
                day_reservations = {}
                for day in self.days:
                    shift_time_reservations = {}
                    for shift_time in times:
                        reservations = []
                        for res in self.reservations_objects:
                            if res.pdv == pdv and res.day == day and res.shift_time == shift_time:
                                reservations.append(res.agent.abbreviation)

                        if reservations:
                            shift_time_reservations[shift_time] = reservations
                    day_reservations[day] = shift_time_reservations
                pdv_reservations[pdv] = day_reservations
            else:
                telefone_reservations = {}
                day_reservations = {}
                for day in self.days:
                    telefone = {'Turno da Manhã': ['', ''], 'Turno da Tarde': ['', '']}
                    shift_time_reservations = {}
                    for shift_time in times:
                        reservations = []
                        for res in self.reservations_objects:
                            if res.pdv == pdv and res.day == day and res.shift_time == shift_time:
                                if str(res.position).strip() == "Online":
                                    reservations.append(res.agent.abbreviation)
                                else:
                                    telefone[shift_time] = [res.agent.abbreviation, '']

                        if reservations:
                            shift_time_reservations[shift_time] = reservations

                    telefone_reservations[day] = telefone
                    day_reservations[day] = shift_time_reservations
                pdv_reservations[pdv] = day_reservations
                pdv_reservations['telefone'] = telefone_reservations

        return pdv_reservations

    def capacity_calculation(self):
        capacity = {}
        sede_reservations = self.populate_table(self.shift_times_sede)

        pdvs_capacity = {}
        for pdv in PDV_TYPES.keys():
            pdvs_capacity[pdv] = 0

        cap = self.seats.groupby('Imóvel')['Vagas'].max()
        for key, value in cap.items():
            pdvs_capacity[key] = value

        print(pdvs_capacity)
        for pdv, reservations in self.pdv_reservations.items():
            if str(pdv).strip() != "Sede Seller" and str(pdv).strip() != "8652" and str(
                    pdv).lower().strip() != "telefone" and str(pdv).strip() != "8600":
                capacity[pdv] = pdvs_capacity[pdv]
            elif str(pdv).strip() == "Sede Seller":
                for shift in self.shift_times_sede:
                    shift_max = 0
                    capacity["Chat Manhã"] = 12
                    capacity["Chat Tarde"] = 8
                    capacity["Chat Noite"] = 6

                    for day, reservation in sede_reservations["Sede Seller"].items():
                        for s, agents in reservation.items():
                            if s == shift:
                                if len(agents) > shift_max:
                                    shift_max = len(agents)
                    if shift_max != 0:
                        capacity[f"Chat {str(shift).replace('Turno da ', '')}"] = shift_max
        print(capacity)
        return capacity

    def make_header(self):
        header = []
        self.days_header = [""]
        self.shifts_header = []
        for day in self.days:
            formatted_date = str(day).replace("/2021", "")
            self.days_header.append(formatted_date)
            self.days_header.append("")
            self.shifts_header.append("MAN")
            self.shifts_header.append("TAR")
            for shift in self.shift_times:
                header.append(day.strip() + " - " + shift)
        return header

    def detailed_reservations(self):
        header = self.make_header()
        self.reservations_detailed = None
        self.reservations_detailed = pd.DataFrame(columns=["Imóvel", *header])
        for pdv, reservations in self.pdv_reservations.items():
            if pdv != "Sede Seller" and pdv != "8652" and pdv != "8600" and pdv != "telefone":
                self.reservations_detailed = self.reservations_detailed.append(pd.Series(), ignore_index=True)
                try:
                    lines = self.pdv_capacity[pdv]
                except KeyError:
                    lines = 0
                if lines > 0:
                    for i in range(lines):
                        pdv_res = self.pdv_reservations[pdv]
                        pdv_dict = {"Imóvel": pdv}
                        for day in self.days:
                            for shift_date, shifts in pdv_res.items():
                                if shift_date == day:
                                    if len(shifts) > 0:
                                        for shift, agents in shifts.items():
                                            try:
                                                pdv_dict[day.strip() + " - " + shift] = agents[i]
                                            except IndexError:
                                                continue
                                    else:
                                        for shift, agents in shifts.items():
                                            pdv_dict[day.strip() + " - " + shift] = ""
                        self.reservations_detailed = self.reservations_detailed.append(pdv_dict, ignore_index=True)
                        self.reservations_detailed.fillna('', inplace=True)
                else:
                    pdv_dict = {"Imóvel": pdv}
                    for day in self.days:
                        for shift in self.shift_times:
                            pdv_dict[day.strip() + " - " + shift] = "-"
                    self.reservations_detailed = self.reservations_detailed.append(pdv_dict, ignore_index=True)
                    self.reservations_detailed.fillna('', inplace=True)
        self.reservations_detailed = self.reservations_detailed.append(pd.Series(), ignore_index=True)
        days_df = pd.DataFrame([self.days_header], columns=self.reservations_detailed.columns)
        weekdays_df = pd.DataFrame([self.weekdays], columns=self.reservations_detailed.columns)

        self.reservations_detailed = self.reservations_detailed.append(days_df, ignore_index=True)
        self.reservations_detailed = self.reservations_detailed.append(weekdays_df, ignore_index=True)

        sede_reservations = self.get_sede_reservations()
        self.reservations_detailed = self.reservations_detailed.append(sede_reservations, ignore_index=True)
        self.reservations_detailed.fillna('', inplace=True)
        self.reservations_detailed.columns = ["Imóvel", *self.shifts_header]

        self.header_len = len(self.reservations_detailed.columns)
        return self.reservations_detailed

    def get_sede_reservations(self):
        sede_reservations = self.populate_table(self.shift_times_sede)

        sede_res = sede_reservations["Sede Seller"]
        shifts = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]
        dates_groupby = self.dataframe.groupby(['Data do Turno'], as_index=False)
        sede_list = []

        for shift in shifts:
            shift_list = []
            turno = str(shift).replace("Turno da ", "")
            sede_shift_tag = f"Chat {turno}"
            lines = math.ceil(self.pdv_capacity[sede_shift_tag] / 2)
            for line in range(lines):
                shift_list = [sede_shift_tag]
                counter = 0
                for day in self.days:
                    shift_res = []
                    day_shifts = sede_res[day]
                    if day_shifts:
                        try:
                            shift_res = day_shifts[shift]
                        except KeyError:
                            pass

                        for i in range(2):
                            if len(day_shifts) < 0:
                                shift_list.append("")
                            else:
                                try:
                                    r = shift_res[(line * 2) + i]
                                    shift_list.append(r)
                                except:
                                    shift_list.append("")
                                finally:
                                    counter += 1
                    else:
                        shift_list.append("")
                        shift_list.append("")

                sede_list.append(shift_list)
            shift_length = len(shift_list)
            s_len = [""] * shift_length
            sede_list.append(s_len)

        telefone_res = sede_reservations['telefone']
        for s in ["Turno da Manhã", "Turno da Tarde"]:
            turno = str(s).replace("Turno da ", "")
            telefone_shifts_tag = f"Telefone {turno}"
            telefone_shifts_list = [telefone_shifts_tag]
            for day in self.days:
                day_shifts = telefone_res[day]
                if day_shifts:
                    try:
                        telefone_shifts_list.append(day_shifts[s][0])
                        telefone_shifts_list.append("")
                    except KeyError:
                        pass
                else:
                    telefone_shifts_list.append("")
                    telefone_shifts_list.append("")

            sede_list.append(telefone_shifts_list)
            sede_list.append(s_len)

        sede = pd.DataFrame(sede_list, columns=self.reservations_detailed.columns)
        sede.fillna('', inplace=True)
        return sede

    def summary_reservations(self):
        agents_list = self.dataframe.groupby(['Corretor']).groups.keys()
        summary_count = self.dataframe.groupby(['Corretor', 'Posição'], as_index=False)[['Turno']].count()
        positions_raw = self.dataframe.groupby(['Posição']).groups.keys()
        positions = [str(p).upper() for p in positions_raw]
        agents_summary = pd.DataFrame(columns=["Nome", "Sigla", *positions_raw, "Total"])
        for agent in agents_list:
            total = 0
            try:
                sigla = ABBREVIATIONS[str(agent).strip()]
            except KeyError:
                sigla = agent
            agent_sum_dict = {"Nome": agent, "Sigla": sigla}
            for position in positions_raw:
                reservations = summary_count[
                    (summary_count['Corretor'] == agent) &
                    (summary_count['Posição'] == position)
                    ]['Turno'].sum()
                agent_sum_dict[position] = reservations
                total += reservations
            agent_sum_dict["Total"] = total
            agents_summary = agents_summary.append(agent_sum_dict, ignore_index=True)
        agents_summary.columns = ["NOME", "SIGLA", *positions, "TOTAL"]
        return agents_summary

    def no_reservations_broker(self):
        agents_list = self.dataframe.groupby(['Corretor']).groups.keys()
        brokers = []
        for a in agents_list:
            brokers.append(str(a).strip())

        no_reservation_brokers = pd.DataFrame(columns=["Nome", "Sigla"])
        for agent in ABBREVIATIONS.keys():
            if agent not in brokers:
                agent_sum_dict = {"Nome": agent, "Sigla": ABBREVIATIONS[agent]}
                no_reservation_brokers = no_reservation_brokers.append(agent_sum_dict, ignore_index=True)
        no_reservation_brokers.columns = ["CORRETORES SEM RESERVA", "SIGLA"]
        for i in range(60):
            no_reservation_brokers = no_reservation_brokers.append(pd.Series(), ignore_index=True)
        no_reservation_brokers.fillna('', inplace=True)

        return no_reservation_brokers

    def calendar_formatting(self):

        rows = 2000

        std_format = gsf.cellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        header_format = gsf.cellFormat(backgroundColor=gsf.color(0.0455, 0.343, 0.6116),
                                       textFormat=gsf.textFormat(bold=True, fontSize=12,
                                                                 foregroundColor=gsf.color(1, 1, 1)))
        index_format = gsf.cellFormat(textFormat=gsf.textFormat(bold=True))

        attempt = 0
        self.sheet_name = f"{self.timestamp} | Reservas"
        try:
            self.detailed_worksheet = self.sh.add_worksheet(title=self.sheet_name, rows=str(rows),
                                                            cols="35")
        except gspread.exceptions.APIError:
            attempt += 1
            self.detailed_worksheet = self.sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Reservas",
                                                            rows="2000", cols="35")
        finally:
            gsf.format_cell_range(self.detailed_worksheet, '1:1000', std_format)
            gsf.format_cell_range(self.detailed_worksheet, 'A', index_format)
            gsf.format_cell_range(self.detailed_worksheet, '1:2', index_format)

            header_range = f"A4:{chr(ord('a') + self.header_len - 1)}4"
            gsf.format_cell_range(self.detailed_worksheet, header_range, header_format)

            gsf.set_frozen(self.detailed_worksheet, rows=4, cols=1)

            gsf.set_row_height(self.detailed_worksheet, '1:1000', 24)
            gsf.set_column_widths(self.detailed_worksheet,
                                  [('A', 150), ('B:O', 50), ('Q', 225), ('R:X', 100), ('Z', 225)])

            self.detailed_worksheet.update('A1', [])
            self.detailed_worksheet.update('A2', [self.days_header])
            self.detailed_worksheet.update('A3', [self.weekdays])

        gsf.format_cell_range(self.detailed_worksheet, 'Q', index_format)
        gsf.format_cell_range(self.detailed_worksheet, 'Q4:X4', header_format)

        gsf.format_cell_range(self.detailed_worksheet, 'Z', index_format)
        gsf.format_cell_range(self.detailed_worksheet, 'Z4:AA4', header_format)

        self.sh.batch_update(
            copy_format(src_sheet_id=self.sh.worksheet("Layout").id, dest_sheet_id=self.detailed_worksheet.id))

    def get_layout(self):
        srcSpradsheet = self.gc.open("Layout Reservas")
        srcSheetName = "Layout"
        srcSheet = srcSpradsheet.worksheet(srcSheetName)

    def update_calendar_values(self):

        res_det = self.detailed_worksheet.update('A4', [
            self.detailed_output.columns.values.tolist()] + self.detailed_output.values.tolist())
        res_sum = self.detailed_worksheet.update("Q4", [
            self.summary_output.columns.values.tolist()] + self.summary_output.values.tolist())
        res_no = self.detailed_worksheet.update("Z4", [
            self.no_reservations_output.columns.values.tolist()] + self.no_reservations_output.values.tolist())

    def upload_raw(self):
        std_format = gsf.cellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        header_format = gsf.cellFormat(backgroundColor=gsf.color(0.0455, 0.343, 0.6116),
                                       textFormat=gsf.textFormat(bold=True, fontSize=12,
                                                                 foregroundColor=gsf.color(1, 1, 1)))
        index_format = gsf.cellFormat(textFormat=gsf.textFormat(bold=True))

        attempt = 1
        raw_worksheet = ''
        try:
            self.raw_worksheet = self.sh.add_worksheet(title=f"{self.timestamp} | Lista", rows='4000', cols="15")
        except gspread.exceptions.APIError:
            attempt += 1
            self.raw_worksheet = self.sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Lista", rows="4000",
                                                       cols="15")
        finally:
            gsf.format_cell_range(self.raw_worksheet, '1:1000', std_format)
            gsf.format_cell_range(self.raw_worksheet, 'B', index_format)
            gsf.format_cell_range(self.raw_worksheet, 'A1:G1', header_format)

            gsf.set_frozen(self.raw_worksheet, rows=1)

            gsf.set_row_height(self.raw_worksheet, '1:1000', 35)
            gsf.set_column_widths(self.raw_worksheet, [('A:O', 150), ('B', 200)])

            self.raw_worksheet.update([self.dataframe.columns.values.tolist()] + self.dataframe.values.tolist())

    def merging_cells(self):
        merge_sheet_id = int(self.detailed_worksheet._properties['sheetId'])
        body = merge_days(merge_sheet_id)
        res = self.sh.batch_update(body)

    def formatting_specific_cells(self):
        formatting_sheet_id = int(self.raw_worksheet._properties['sheetId'])
        formatting_body = formatting_sheet(formatting_sheet_id)
        res = self.sh.batch_update(formatting_body)

    def wipe_old_sheets(self, tabs_num):
        if len(self.sh.worksheets()) >= tabs_num:
            for sheet in self.sh.worksheets()[-tabs_num:]:
                self.sh.del_worksheet(sheet)

    def wipe_single_sheet(self, nth):
        if len(self.sh.worksheets()) > 1:
            self.sh.del_worksheet(self.sh.worksheets()[nth])
