from connect import Connect
import pandas as pd
import datetime
import time
import gspread
import gspread_formatting as gsf
import math
import date_calculator
from agent import Agent
from pdvs import Pdv
from pdv_types import PDV_TYPES
from shift_day import ShiftDay
from reservation import Reservation
from merging import merge_days
from formatting import formatting_sheet
from agents_abbreviations import ABBREVIATIONS
from credentials import credentials

class ReservationsHandler:

    def __init__(self):
        self.header_len = 0
        self.gc = gspread.authorize(credentials)
        now = datetime.datetime.now()
        self.days_header = [""]
        self.seats = None
        self.shifts_header = []
        self.detailed_worksheet = None
        self.raw_worksheet = None
        self.sh = None
        stamp = [datetime.datetime.strftime(now, '%d/%m %Hh%M'), time.time()]
        self.timestamp = stamp[0]
        self.reservations_json = self.fetch_data()
        self.dataframe = self.make_dataframe()
        self.agents_list = self.get_agents()
        self.pdvs = PDV_TYPES.keys()
        self.days = date_calculator.get_next_week_from_monday(-7)
        # self.days = ['19/04/2021', '20/04/2021', '21/04/2021', '22/04/2021', '23/04/2021', '24/04/2021', '25/04/2021']
        self.weekdays = ['','SEG', '', 'TER', '', 'QUA', '', 'QUI', '', 'SEX', '', 'SÁB', '', 'DOM', '']
        self.shift_times_sede = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]
        self.shift_times = ["Turno da Manhã", "Turno da Tarde"]
        self.reservations_objects = self.create_reservations_objects()
        self.pdv_reservations = {}
        self.pdv_reservations = self.populate_table(self.shift_times)
        print(self.pdv_reservations)
        self.pdv_capacity = self.capacity_calculation()
        self.current_sheets_list = None
        self.upload_raw()
        self.upload_calendar()
        # self.wipe_old_sheets(2)

    def fetch_data(self):
        print("opening db")
        conn = Connect(10)
        seats = conn.query(conn.pre_reservas)
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
        # print(data)

        data = conn.query(conn.query_reservas)
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
        print(dataframe)
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
            # print(day)
        return days_list

    def create_reservations_objects(self):
        reservations = []
        for index, row in self.dataframe.iterrows():
            res_id = row['ID']
            pdv = next((pdv for pdv in self.pdvs if pdv == str(row['Imóvel']).strip()), row['Imóvel'])
            day = next((day for day in self.days if day == str(row['Data do Turno']).strip()),row['Data do Turno'])
            shift_time = next((shift for shift in self.shift_times if shift == str(row['Turno']).strip()), row['Turno'])
            agent = next((agent for agent in self.agents_list if agent.name == str(row['Corretor']).strip()),row['Corretor'])
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
                tel8652_reservations = {}
                tel8600_reservations = {}
                day_reservations = {}
                for day in self.days:
                    tel_8652 = {}
                    tel_8600 = {}
                    shift_time_reservations = {}
                    for shift_time in times:
                        reservations = []
                        for res in self.reservations_objects:
                            # print(res.position)
                            if res.pdv == pdv and res.day == day and res.shift_time == shift_time:
                                if str(res.position).strip() == "Online":
                                    reservations.append(res.agent.abbreviation)
                                elif str(res.position).strip() == "Telefone 8652":
                                    tel_8652[shift_time] = [res.agent.abbreviation, '']
                                else:
                                    tel_8600[shift_time] = [res.agent.abbreviation, '']

                        if reservations:
                            shift_time_reservations[shift_time] = reservations

                    tel8652_reservations[day] = tel_8652
                    tel8600_reservations[day] = tel_8600
                    day_reservations[day] = shift_time_reservations
                pdv_reservations[pdv] = day_reservations
                pdv_reservations['8652'] = tel8652_reservations
                pdv_reservations['8600'] = tel8600_reservations

        # print(pdv_reservations.keys())
        return pdv_reservations

    def capacity_calculation(self):
        capacity = {}
        sede_reservations = self.populate_table(self.shift_times_sede)
        pdvs_capacity = self.seats.groupby('Imóvel')['Vagas'].max()
        for pdv, reservations in self.pdv_reservations.items():
            if str(pdv).strip() != "Sede Seller" and str(pdv).strip() != "8652" and str(pdv).strip() != "8600":
                # for day, reservation in reservations.items():
                #     for shifts, agents in reservation.items():
                #         if len(agents) > pdv_max:
                #             pdv_max = len(agents)
                capacity[pdv] = pdvs_capacity[pdv]
            elif str(pdv).strip() == "Sede Seller":
                for shift in self.shift_times_sede:
                    shift_max = 0
                    for day, reservation in sede_reservations["Sede Seller"].items():
                        for s, agents in reservation.items():
                            if s == shift:
                                if len(agents) > shift_max:
                                    shift_max = len(agents)
                    capacity[f"Chat {str(shift).replace('Turno da ', '')}"] = shift_max
        # print(capacity)
        return capacity

    def make_header(self):
        header = []
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
        # print(self.shifts_header)
        self.reservations_detailed = pd.DataFrame(columns=["Imóvel", *header])
        # print(len(["Imóvel", *header]))
        for pdv, reservations in self.pdv_reservations.items():
            if pdv != "Sede Seller" and pdv != "8652" and pdv != "8600":
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

        # print(len(self.shifts_header))
        sede_res = sede_reservations["Sede Seller"]
        print(sede_res)
        shifts = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]
        dates_groupby = self.dataframe.groupby(['Data do Turno'], as_index=False)
        # print("Data Sede")
        # print(sede_res)
        sede_list = []

        for shift in shifts:
            shift_list = []
            turno = str(shift).replace("Turno da ", "")
            sede_shift_tag = f"Chat {turno}"
            lines = math.ceil(self.pdv_capacity[sede_shift_tag] / 2)
            shift_length = 0
            for line in range(lines):
                shift_list = [sede_shift_tag]
                counter = 0
                for day in self.days:
                    shift_res = []
                    day_shifts = sede_res[day]
                    # print(date)
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
                                    # print(len(shift_list))

                sede_list.append(shift_list)
                print(sede_list)
            shift_length = len(shift_list)
            s_len = [""] * shift_length
            sede_list.append(s_len)


        tel8652_res = sede_reservations["8652"]
        for s in ["Turno da Manhã", "Turno da Tarde"]:
            turno = str(s).replace("Turno da ", "")
            tel8652_shifts_tag = f"8652 {turno}"
            tel8652_shifts_list = [tel8652_shifts_tag]
            for day in self.days:
                day_shifts = tel8652_res[day]
                if day_shifts:
                    try:
                        tel8652_shifts_list.append(day_shifts[s][0])
                        tel8652_shifts_list.append("")
                    except KeyError:
                        pass
            sede_list.append(tel8652_shifts_list)
            sede_list.append(s_len)

        tel8600_res = sede_reservations["8600"]
        for s in ["Turno da Manhã", "Turno da Tarde"]:
            turno = str(s).replace("Turno da ", "")
            tel8600_shifts_tag = f"8600 {turno}"
            tel8600_shifts_list = [tel8600_shifts_tag]
            for day in self.days:
                day_shifts = tel8600_res[day]
                if day_shifts:
                    try:
                        tel8600_shifts_list.append(day_shifts[s][0])
                        tel8600_shifts_list.append("")
                    except KeyError:
                        pass

            sede_list.append(tel8600_shifts_list)
            sede_list.append(s_len)

        print("sede_list")
        print(sede_list)
        # print(self.reservations_detailed.columns)
        print("self.reservations_detailed.columns")
        print(self.reservations_detailed.columns)
        sede = pd.DataFrame(sede_list, columns=self.reservations_detailed.columns)
        sede.fillna('', inplace=True)
        # print(sede)
        return sede


    def summary_reservations(self):
        agents_list = self.dataframe.groupby(['Corretor']).groups.keys()
        summary_count = self.dataframe.groupby(['Corretor', 'Posição'], as_index=False)[['Turno']].count()
        positions_raw = self.dataframe.groupby(['Posição']).groups.keys()
        positions = [p.upper() for p in positions_raw]
        agents_summary = pd.DataFrame(columns=["Nome", "Sigla", *positions_raw, "Total"])
        for agent in agents_list:
            total = 0
            agent_sum_dict = {"Nome": agent, "Sigla": ABBREVIATIONS[str(agent).strip()]}
            for position in positions_raw:
                reservations = summary_count[
                    (summary_count['Corretor'] == agent) &
                    (summary_count['Posição'] == position)
                    ]['Turno'].sum()
                agent_sum_dict[position] = reservations
                total += reservations
            agent_sum_dict["Total"] = total
            agents_summary = agents_summary.append(agent_sum_dict, ignore_index=True)
        # print(agents_summary)
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
        print(no_reservation_brokers)
        no_reservation_brokers.columns = ["CORRETORES SEM RESERVA", "SIGLA"]
        return no_reservation_brokers

    def upload_calendar(self):
        detailed_output = self.detailed_reservations()
        # print(detailed_output)
        summary_output = self.summary_reservations()
        no_reservations = self.no_reservations_broker()
        rows = 2000

        std_format = gsf.cellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        header_format = gsf.cellFormat(backgroundColor=gsf.color(0.0455, 0.343, 0.6116),
                                       textFormat=gsf.textFormat(bold=True, fontSize=12,
                                                                 foregroundColor=gsf.color(1, 1, 1)))
        index_format = gsf.cellFormat(textFormat=gsf.textFormat(bold=True))

        attempt = 0
        try:
            self.detailed_worksheet = self.sh.add_worksheet(title=f"{self.timestamp} | Reservas Seller", rows=str(rows),
                                                            cols="35")
        except gspread.exceptions.APIError:
            attempt += 1
            self.detailed_worksheet = self.sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Reservas Seller",
                                                            rows="2000", cols="35")
        finally:
            self.merging_and_formatting()
            gsf.format_cell_range(self.detailed_worksheet, '1:1000', std_format)
            gsf.format_cell_range(self.detailed_worksheet, 'A', index_format)
            gsf.format_cell_range(self.detailed_worksheet, '1:2', index_format)

            header_range = f"A4:{chr(ord('a') + self.header_len - 1)}4"
            gsf.format_cell_range(self.detailed_worksheet, header_range, header_format)

            gsf.set_frozen(self.detailed_worksheet, rows=4, cols=1)

            gsf.set_row_height(self.detailed_worksheet, '1:1000', 24)
            gsf.set_column_widths(self.detailed_worksheet, [('A', 150), ('B:O', 50), ('Q', 200), ('R:X', 100), ('Z', 200)])

            self.detailed_worksheet.update('A1', [])
            self.detailed_worksheet.update('A2', [self.days_header])
            self.detailed_worksheet.update('A3', [self.weekdays])
            self.detailed_worksheet.update('A4',
                                           [detailed_output.columns.values.tolist()] + detailed_output.values.tolist())
            # print("Successfully Created Detailed Worksheet")

        attempt = 0
        summary_worksheet = ''
        # print("Creating Summary Worksheet")

        gsf.format_cell_range(self.detailed_worksheet, 'Q', index_format)
        gsf.format_cell_range(self.detailed_worksheet, 'Q4:X4', header_format)
        self.detailed_worksheet.update("Q4", [summary_output.columns.values.tolist()] + summary_output.values.tolist())

        gsf.format_cell_range(self.detailed_worksheet, 'Z', index_format)
        gsf.format_cell_range(self.detailed_worksheet, 'Z4:AA4', header_format)
        self.detailed_worksheet.update("Z4", [no_reservations.columns.values.tolist()] + no_reservations.values.tolist())

        # print("Successfully Created Summary Worksheet")

    def upload_raw(self):
        self.sh = self.gc.open("Reservas Seller")

        self.current_sheets_list = self.sh.worksheets()

        std_format = gsf.cellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        header_format = gsf.cellFormat(backgroundColor=gsf.color(0.0455, 0.343, 0.6116),
                                       textFormat=gsf.textFormat(bold=True, fontSize=12,
                                                                 foregroundColor=gsf.color(1, 1, 1)))
        index_format = gsf.cellFormat(textFormat=gsf.textFormat(bold=True))

        attempt = 1
        raw_worksheet = ''
        # print("Creating Raw Worksheet")
        try:
            self.raw_worksheet = self.sh.add_worksheet(title=f"{self.timestamp} | Lista", rows='4000', cols="15")
        except gspread.exceptions.APIError:
            attempt += 1
            self.raw_worksheet = self.sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Lista", rows="4000", cols="15")
        finally:
            gsf.format_cell_range(self.raw_worksheet, '1:1000', std_format)
            gsf.format_cell_range(self.raw_worksheet, 'B', index_format)
            gsf.format_cell_range(self.raw_worksheet, 'A1:G1', header_format)

            gsf.set_frozen(self.raw_worksheet, rows=1)

            gsf.set_row_height(self.raw_worksheet, '1:1000', 35)
            gsf.set_column_widths(self.raw_worksheet, [('A:O', 150), ('B', 200)])

            self.raw_worksheet.update([self.dataframe.columns.values.tolist()] + self.dataframe.values.tolist())
            # print("Successfully Created Raw Worksheet")

    def merging_and_formatting(self):
        merge_sheet_id = int(self.detailed_worksheet._properties['sheetId'])
        body = merge_days(merge_sheet_id)
        res = self.sh.batch_update(body)
        # print(res)

        formatting_sheet_id = int(self.raw_worksheet._properties['sheetId'])
        formatting_body = formatting_sheet(formatting_sheet_id)
        res = self.sh.batch_update(formatting_body)
        # print(res)

    def wipe_old_sheets(self, tabs_num):

        if len(self.current_sheets_list) >= tabs_num:
            # print("Deleting Old Tabs")
            for sheet in self.current_sheets_list[-tabs_num:]:
                self.sh.del_worksheet(sheet)
            # print("Old Tabs Deleted")


if __name__ == "__main__":
    new_reservations = ReservationsHandler()
