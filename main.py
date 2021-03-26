from connect import Connect
import pandas as pd
import datetime
import time
import gspread
import gspread_formatting as gsf
import math
from agent import Agent
from pdvs import Pdv
from shift_day import ShiftDay
from reservation import Reservation
from merging import merge_days
from formatting import formatting_sheet
from agents_abbreviations import ABBREVIATIONS


class ReservationsHandler:

    def __init__(self):
        self.header_len = 0
        self.gc = gspread.service_account()
        now = datetime.datetime.now()
        self.days_header = [""]
        self.shifts_header = []
        self.detailed_worksheet = None
        self.raw_worksheet = None
        self.sh = None
        stamp = [datetime.datetime.strftime(now, '%d/%m %Hh%M'), time.time()]
        self.timestamp = stamp[0]
        self.reservations_json = self.fetch_data()
        self.dataframe = self.make_dataframe()
        self.agents_list = self.get_agents()
        self.pdvs = self.get_pdvs()
        self.days = self.get_days()

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

    @staticmethod
    def fetch_data():
        conn = Connect()
        data = conn.query()
        # print(data)
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
            pdv = next((pdv for pdv in self.pdvs if pdv.name == str(row['Imóvel']).strip()), row['Imóvel'])
            day = next((day for day in self.days if day.date == str(row['Data do Turno']).strip()),row['Data do Turno'])
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
            if pdv.name != "Sede Seller":
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
                    day_reservations[day.date] = shift_time_reservations
                pdv_reservations[pdv.name] = day_reservations
            else:
                telefone_reservations = {}
                day_reservations = {}
                for day in self.days:
                    telefone = {}
                    shift_time_reservations = {}
                    for shift_time in times:
                        reservations = []
                        for res in self.reservations_objects:
                            # print(res.position)
                            if res.pdv == pdv and res.day == day and res.shift_time == shift_time:
                                if str(res.position).strip() == "Online":
                                    reservations.append(res.agent.abbreviation)
                                else:
                                    telefone[shift_time] = [res.agent.abbreviation, '']
                        if reservations:
                            shift_time_reservations[shift_time] = reservations

                    telefone_reservations[day.date] = telefone
                    day_reservations[day.date] = shift_time_reservations
                pdv_reservations[pdv.name] = day_reservations
                pdv_reservations['8652'] = telefone_reservations

        print(pdv_reservations)
        return pdv_reservations

    def capacity_calculation(self):
        capacity = {}
        sede_reservations = self.populate_table(self.shift_times_sede)

        for pdv, reservations in self.pdv_reservations.items():
            if str(pdv).strip() != "Sede Seller" and str(pdv).strip() != "8652":
                pdv_max = 0
                for day, reservation in reservations.items():
                    for shifts, agents in reservation.items():
                        if len(agents) > pdv_max:
                            pdv_max = len(agents)
                capacity[pdv] = pdv_max
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
            formatted_date = str(day.date).replace("/2021", "")
            self.days_header.append(formatted_date)
            self.days_header.append("")
            self.shifts_header.append("MAN")
            self.shifts_header.append("TAR")
            for shift in self.shift_times:
                header.append(day.date.strip() + " - " + shift)
        return header

    def detailed_reservations(self):
        header = self.make_header()
        # print(self.shifts_header)
        self.reservations_detailed = pd.DataFrame(columns=["Imóvel", *header])
        # print(len(["Imóvel", *header]))
        for pdv, reservations in self.pdv_reservations.items():
            if pdv != "Sede Seller" and pdv != "8652":
                self.reservations_detailed = self.reservations_detailed.append(pd.Series(), ignore_index=True)
                lines = self.pdv_capacity[pdv]
                for i in range(lines):
                    pdv_res = self.pdv_reservations[pdv]
                    pdv_dict = {"Imóvel": pdv}
                    for day in self.days:
                        for shift_date, shifts in pdv_res.items():
                            if shift_date == day.date:
                                if len(shifts) > 0:
                                    for shift, agents in shifts.items():
                                        try:
                                            pdv_dict[day.date.strip() + " - " + shift] = agents[i]
                                        except IndexError:
                                            continue
                                else:
                                    for shift, agents in shifts.items():
                                        pdv_dict[day.date.strip() + " - " + shift] = ""
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

        shifts = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]
        dates_groupby = self.dataframe.groupby(['Data do Turno'], as_index=False)
        dates = list(dates_groupby.groups.keys())
        # print("Data Sede")
        # print(sede_res)
        sede_list = []

        for shift in shifts:
            turno = str(shift).replace("Turno da ", "")
            sede_shift_tag = f"Chat {turno}"
            lines = math.ceil(self.pdv_capacity[sede_shift_tag] / 2)
            shift_length = 0
            for line in range(lines):
                shift_list = [sede_shift_tag]
                counter = 0
                for day in range(len(dates)):
                    date = dates[day]
                    shift_res = []
                    day_shifts = sede_res[date]
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
            shift_length = len(shift_list)
            # print(shift_length)
            l = [""] * shift_length
            sede_list.append(l)


        telefone_res = sede_reservations["8652"]
        # telefone_list = []
        for s in ["Turno da Manhã", "Turno da Tarde"]:
            turno = str(s).replace("Turno da ", "")
            telefone_shifts_tag = f"8652 {turno}"
            telefone_shifts_list = [telefone_shifts_tag]
            for day in range(len(dates)):
                date = dates[day]
                day_shifts = telefone_res[date]
                if day_shifts:
                    try:
                        telefone_shifts_list.append(day_shifts[s][0])
                        telefone_shifts_list.append("")
                    except KeyError:
                        pass

            sede_list.append(telefone_shifts_list)
            sede_list.append(l)
        print(sede_list)
        # print(self.reservations_detailed.columns)
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

    def upload_calendar(self):
        detailed_output = self.detailed_reservations()
        # print(detailed_output)
        summary_output = self.summary_reservations()
        rows = 2000

        std_format = gsf.cellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        header_format = gsf.cellFormat(backgroundColor=gsf.color(0.0455, 0.343, 0.6116),
                                       textFormat=gsf.textFormat(bold=True, fontSize=12,
                                                                 foregroundColor=gsf.color(1, 1, 1)))
        index_format = gsf.cellFormat(textFormat=gsf.textFormat(bold=True))

        attempt = 0
        try:
            self.detailed_worksheet = self.sh.add_worksheet(title=f"{self.timestamp} | Teste", rows=str(rows),
                                                            cols="35")
        except gspread.exceptions.APIError:
            attempt += 1
            self.detailed_worksheet = self.sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Calendário",
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
            gsf.set_column_widths(self.detailed_worksheet, [('A', 150), ('B:S', 50), ('T', 200), ('U:Z', 100)])

            self.detailed_worksheet.update('A1', [])
            self.detailed_worksheet.update('A2', [self.days_header])
            self.detailed_worksheet.update('A3', [self.weekdays])
            self.detailed_worksheet.update('A4',
                                           [detailed_output.columns.values.tolist()] + detailed_output.values.tolist())
            # print("Successfully Created Detailed Worksheet")

        attempt = 0
        summary_worksheet = ''
        # print("Creating Summary Worksheet")

        gsf.format_cell_range(self.detailed_worksheet, 'T', index_format)
        gsf.format_cell_range(self.detailed_worksheet, 'T4:Z4', header_format)

        self.detailed_worksheet.update("T4", [summary_output.columns.values.tolist()] + summary_output.values.tolist())
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
