from connect import Connect
import pandas as pd
import gspread


class ReservationsHandler:

    def __init__(self):
        self.reservations_json = self.fetch_data()
        self.df = self.make_dataframe()
        self.gc = gspread.service_account()
        self.upload_to_sheet()

    @staticmethod
    def fetch_data():
        conn2 = Connect(0)

        data = conn2.query(conn2.pre_reservas)
        # print(data)
        return data

    def make_dataframe(self):
        # print(self.reservations_json)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        dataframe = pd.DataFrame.from_records(
            self.reservations_json,
            columns=[
                'Vagas',
                'Posição',
                'Turno',
                'Dia da Semana',
                'Imóvel'
            ]
        )
        # print(dataframe)
        dataframe["PDV"] = dataframe['Imóvel'] + " " + dataframe['Posição']
        dataframe['Shift'] = dataframe['Dia da Semana'] + " " + dataframe['Turno']
        dataframe = dataframe.drop(['Imóvel', 'Posição', 'Dia da Semana', 'Turno'], axis=1)
        dataframe = dataframe.pivot(index='PDV', columns='Shift', values='Vagas')
        dataframe.fillna('', inplace=True)

        print(dataframe)
        return dataframe

    def upload_to_sheet(self):
        sh = self.gc.open("Posições da Semana")
        sheets_list = sh.worksheets()
        sheets_num = len(sheets_list)
        new_sheet = sh.add_worksheet(title=f"Planilha{sheets_num + 1}", rows='1000', cols='35')
        new_sheet.update([self.df.columns.values.tolist()] + self.df.values.tolist())


if __name__ == "__main__":
    new_reservations = ReservationsHandler()