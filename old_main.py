from connect import Connect
import pandas as pd
import datetime
import time
import gspread
import gspread_formatting as gsf

gc = gspread.service_account()
wait_time_in_seconds = 300


class Reserves:

    def __init__(self):
        now = datetime.datetime.now()
        stamp = [datetime.datetime.strftime(now, '%d/%m %Hh%M'), time.time()]
        self.timestamp = stamp[0]
        self.reserves_data = []
        self.df = None

    def get_reserves(self):
        if self.update_data():
            return "Success"
        else:
            return "Error"

    def update_data(self):
        # Fazendo query no banco de dados para extrair valores
        conn = Connect()
        self.reserves_data = conn.query()

        # Tratamento para erro de Datetime. Transformando datetime em string
        # for reserve in reserves_data_raw:
        #     temp_reserva = []
        #     for data in reserve:
        #         if isinstance(data, datetime.time):
        #             data = data.strftime('%Hh%M')
        #         elif type(data) == int:
        #             data = data
        #         else:
        #             data = data.strip()
        #         temp_reserva.append(data)
        #     tup_temp = tuple(temp_reserva)
        #     self.reserves_data.append(tup_temp)

        """ Transforma os dados do DB em DataFrame Pandas """
        self.google_uploader()

    def google_uploader(self):
        """ Gets DataFrame and uploads to Google Sheet"""
        self.make_df()
        detailed_output = self.detailed_data_treatment()
        summary_output = self.summary_data_treatment()

        sh = gc.open("Reservas Seller")

        rows = 2000
        col_width = 150
        current_sheets_list = sh.worksheets()
        std_format = gsf.cellFormat(
            horizontalAlignment='CENTER',
            verticalAlignment='MIDDLE'
        )
        header_format = gsf.cellFormat(
            backgroundColor=gsf.color(0.0455, 0.343, 0.6116),
            textFormat=gsf.textFormat(bold=True, fontSize=12, foregroundColor=gsf.color(1, 1, 1))
        )

        index_format = gsf.cellFormat(
            textFormat=gsf.textFormat(bold=True)
        )

        # attempt = 1
        # raw_worksheet = ''
        # print("Creating Raw Worksheet")
        # try:
        #     raw_worksheet = sh.add_worksheet(title=f"{self.timestamp} | Lista", rows=str(rows), cols="15")
        # except gspread.exceptions.APIError:
        #     raw_worksheet = sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Lista", rows="4000", cols="15")
        #     attempt += 1
        # finally:
        #     gsf.format_cell_range(raw_worksheet, '1:1000', std_format)
        #     gsf.format_cell_range(raw_worksheet, 'A', index_format)
        #     gsf.format_cell_range(raw_worksheet, 'A1:G1', header_format)
        #
        #     gsf.set_frozen(raw_worksheet, rows=1)
        #
        #     gsf.set_row_height(raw_worksheet, '1:1000', 35)
        #     gsf.set_column_widths(raw_worksheet, [('A:O', 150), ('B', 200)])
        #
        #     raw_worksheet.update([self.df.columns.values.tolist()] + self.df.values.tolist())
        #     print("Successfully Created Raw Worksheet")

        # attempt = 1
        # summary_worksheet = ''
        # print("Creating Summary Worksheet")
        # try:
        #     summary_worksheet = sh.add_worksheet(title=f"{self.timestamp} | Resumo", rows=str(rows), cols="15")
        # except gspread.exceptions.APIError:
        #     summary_worksheet = sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Resumo", rows="4000", cols="15")
        # finally:
        #     gsf.format_cell_range(summary_worksheet, '1:1000', std_format)
        #     gsf.format_cell_range(summary_worksheet, 'A', index_format)
        #     gsf.format_cell_range(summary_worksheet, 'A1:F1', header_format)
        #
        #     gsf.set_frozen(summary_worksheet, rows=1)
        #
        #     gsf.set_row_height(summary_worksheet, '1:1000', 35)
        #     gsf.set_column_widths(summary_worksheet, [('A:O', 150), ('C', 200)])
        #
        #     summary_worksheet.update([summary_output.columns.values.tolist()] + summary_output.values.tolist())
        #     print("Successfully Created Summary Worksheet")

        attempt = 1
        detailed_worksheet = ''
        print("Creating Detailed Worksheet")
        try:
            detailed_worksheet = sh.add_worksheet(title=f"{self.timestamp} | Calendário", rows=str(rows), cols="15")
        except gspread.exceptions.APIError:
            detailed_worksheet = sh.add_worksheet(title=f"{self.timestamp}-{attempt} | Calendário", rows="4000", cols="15")
        finally:
            gsf.format_cell_range(detailed_worksheet, '1:1000', std_format)
            gsf.format_cell_range(detailed_worksheet, 'A:B', index_format)
            gsf.format_cell_range(detailed_worksheet, 'A1:I1', header_format)

            gsf.set_frozen(detailed_worksheet, rows=1)

            # gsformat.set_row_height(detailed_worksheet, '1:1000', 35)
            gsf.set_column_widths(detailed_worksheet, [('A:O', 150), ('B', 200)])

            detailed_worksheet.update([detailed_output.columns.values.tolist()] + detailed_output.values.tolist())
            print("Successfully Created Detailed Worksheet")

        if len(current_sheets_list) >= 3:
            print("Deleting Old Tabs")
            # for sheet in current_sheets_list[-3:]:
            #     sh.del_worksheet(sheet)
            print("Old Tabs Deleted")

    def make_df(self):
        """ Transforma os dados vindos do database em DataFrame Pandas"""
        self.df = pd.DataFrame.from_records(
            self.reserves_data,
            columns=[
                'ID',
                'Corretor',
                'Data do Turno',
                'Hora de Cadastro da Reserva',
                'Posição',
                'Turno',
                'Imóvel'
            ]
        )

    def summary_data_treatment(self):
        agents_list = self.df.groupby(['Corretor']).groups.keys()
        summary_count = self.df.groupby(['Corretor', 'Posição'], as_index=False)[['Turno']].count()
        positions = self.df.groupby(['Posição']).groups.keys()

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

    def detailed_data_treatment(self):
        estates_list = self.df.groupby(['Imóvel']).groups.keys()

        details_header = ['Imóvel', 'Data do Turno', 'Turno', 'Corretor']
        details_groupby = self.df.groupby(details_header, as_index=False)
        details_toframe = pd.DataFrame(list(details_groupby.groups.keys()))
        details_toframe.columns = details_header

        shifts = ["Turno da Manhã", "Turno da Tarde", "Turno da Noite"]

        dates_groupby = self.df.groupby(['Data do Turno'], as_index=False)
        dates = list(dates_groupby.groups.keys())

        formatted_dates = []
        for date in dates:
            formatted_dates.append(str(date).replace("/2021", ""))

        reservations_detailed = pd.DataFrame(columns=["Imóvel", "Turno", *dates])
        # print(reservations_detailed)

        for estate in estates_list:
            for shift in shifts:
                formatted_shift = shift.replace("Turno da", "")
                estate_reservations_dict = {"Imóvel": estate, "Turno": formatted_shift}
                for date in dates:
                    agents_reserved = details_toframe[
                        (details_toframe['Imóvel'] == estate) &
                        (details_toframe['Data do Turno'] == date) &
                        (details_toframe['Turno'] == shift)
                        ]['Corretor'].values
                    if agents_reserved.size > 0:
                        for i, agent in enumerate(agents_reserved):
                            if i == 0:
                                estate_reservations_dict[date] = agent
                            else:
                                estate_reservations_dict[date] += "\n" + agent
                    else:
                        estate_reservations_dict[date] = ""

                reservations_detailed = reservations_detailed.append(estate_reservations_dict, ignore_index=True)

        reservations_detailed.columns = ["Imóvel", "Turno", *formatted_dates]
        return reservations_detailed


class Main:

    def __init__(self):
        new_res = Reserves()
        result = new_res.get_reserves()


if __name__ == "__main__":
    main = Main()
