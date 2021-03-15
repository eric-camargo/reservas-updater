import psycopg2
import secrets


# query_todas_reservas = "SELECT * from timetable_reservation"

class Connect:

    def __init__(self):
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
                                WHERE res.created_at > CURRENT_TIMESTAMP - INTERVAL '6 days'AND
                                res.deleted_at IS NULL
                                ORDER BY Usuario ASC;"""

    def query(self):
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

            # execute select_all
            cur.execute(self.query_reservas)
            todas_reservas = cur.fetchall()

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


if __name__ == "__main__":
    conn = Connect()
    conn.query_reservas()
