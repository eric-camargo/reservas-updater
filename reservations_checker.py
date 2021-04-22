from connect import Connect
import pandas as pd


class ReservationsHandler:

    def __init__(self):
        self.fetch_data()
        self.df = self.make_dataframe()

    @staticmethod
    def fetch_data():
        conn2 = Connect()
        data = conn2.query(conn2.pre_reservas)
        # print(data)
        return data

    def make_dataframe(self):
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
        return dataframe


if __name__ == "__main__":
    new_reservations = ReservationsHandler()