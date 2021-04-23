import psycopg2
import secrets


# query_todas_reservas = "SELECT * from timetable_reservation"
"""res.date > CURRENT_TIMESTAMP + INTERVAL '2 days' AND """
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
                                WHERE res.created_at > CURRENT_TIMESTAMP - INTERVAL '%s days' AND
                                res.deleted_at is NULL
                                ORDER BY Usuario ASC;""" % (days)

        self.pre_reservas = """SELECT seats.seats AS Vagas, 
                                positions.title AS Posição, 
                                shift.title AS Turno, 
                                CASE schedule.weekday
                                    WHEN 0 THEN 'Domingo'
                                    WHEN 1 THEN 'Segunda'
                                    WHEN 2 THEN 'Terça'
                                    WHEN 3 THEN 'Quarta'
                                    WHEN 4 THEN 'Quinta'
                                    WHEN 5 THEN 'Sexta'
                                    WHEN 6 THEN 'Sábado'
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
                                WHERE janela.scheduling_start_datetime > '2021-04-04'
                                ORDER BY imovel.short_title ASC, schedule.weekday
                                ;"""

    def query(self, query):
        """ Connect to the PostgreSQL database server """
        conn = None
        todas_reservas = None
        try:
            # connect to the PostgreSQL server
            conn = psycopg2.connect(
                host=secrets.pg_host,
                database=secrets.pg_database,
                user=secrets.pg_user,
                password=secrets.pg_password
            )

            # create a cursor
            cur = conn.cursor()
            # print(cur)

            # execute select_all
            cur.execute(query)
            todas_reservas = cur.fetchall()
            # print(todas_reservas)

            # close communication with the PostgreSQL
            cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()
                print("Database connection closed")
            # print(todas_reservas)
            return todas_reservas
