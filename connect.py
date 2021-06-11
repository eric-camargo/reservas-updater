import psycopg2
import secrets


# query_todas_reservas = "SELECT * from timetable_reservation"
"""res.date > CURRENT_TIMESTAMP + INTERVAL '2 days' AND """
''' AND res.deleted_at is NULL'''
class Connect:

    def __init__(self, days):
        self.query_reservas = """SELECT res.id, CONCAT (usr.first_name,' ',usr.last_name) AS Usuario,
                                TO_CHAR(res.date, 'DD/MM/YYYY') AS dia,
                                to_char(res.created_at - INTERVAL '3 hours', 'DD/MM/YYYY HH24:MI:SS') AS HoraDeCadastro,
                                pos.title AS posição,
                                shi.title AS Turno,
                                obj.short_title AS imovel FROM
                                timetable_reservation AS res
                                INNER JOIN timetable_position AS pos ON res.position_id = pos.id
                                INNER JOIN timetable_shiftschedule AS shc ON res.shiftschedule_id = shc.id
                                INNER JOIN timetable_shift AS shi ON shi.id = shc.shift_id
                                INNER JOIN core_user AS usr ON usr.id = res.user_id
                                INNER JOIN explore_exploreobject AS obj ON res.exploreobject_id = obj.id
                                WHERE res.created_at > CURRENT_TIMESTAMP - INTERVAL '%s days'
                                ORDER BY Usuario ASC;""" % (days)

        self.pre_reservas = """SELECT seats.seats AS Vagas, 
                                positions.title AS Posição, 
                                shift.title AS Turno, 
                                CASE schedule.weekday
                                       WHEN 0 THEN 'Segunda'
                                       WHEN 1 THEN 'Terça'
                                       WHEN 2 THEN 'Quarta'
                                       WHEN 3 THEN 'Quinta'
                                       WHEN 4 THEN 'Sexta'
                                       WHEN 5 THEN 'Sábado'
                                       WHEN 6 THEN 'Domingo'
                                    END weekday, 
                                    imovel.short_title AS Imóvel 
                                FROM timetable_seats seats
                                INNER JOIN explore_exploreobject imovel
                                ON seats.exploreobject_id = imovel.id
                                INNER JOIN timetable_position positions
                                ON seats.position_id = positions.id
                                INNER JOIN timetable_shiftschedule schedule
                                ON seats.shiftschedule_id = schedule.id
                                INNER JOIN timetable_shift shift
                                ON schedule.shift_id = shift.id
                                INNER JOIN timetable_schedulingwindow janela
                                ON seats.exploreobject_id = janela.exploreobject_id
                                WHERE janela.scheduling_start_datetime > '2021-04-23'
                                ORDER BY imovel.short_title ASC, schedule.weekday
                                ;"""

        self.connection = None
        try:
            self.connection = psycopg2.connect(
                host=secrets.pg_host,
                database=secrets.pg_database,
                user=secrets.pg_user,
                password=secrets.pg_password
            )
            self.cursor = self.connection.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def query(self, query):
        """ Connect to the PostgreSQL database server """
        todas_reservas = None
        try:
            self.cursor.execute(query)
            todas_reservas = self.cursor.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            return todas_reservas

    def close(self):
        # close communication with the PostgreSQL
        self.cursor.close()
        if self.connection is not None:
            self.connection.close()
            print("Database connection closed")