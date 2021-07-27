import datetime
import time
import math
from reservations_handler import ReservationsHandler


def reservation_script():
    start_time = time.time()
    real_time_sheet = ReservationsHandler(sheet="Reservas Seller Real Time")
    real_time_sheet.wipe_single_sheet(-2)
    while True:
        real_time_sheet.refresh_data()
        wait_seconds = 60
        new_update = time.time()
        print("New Update at: %s" % new_update)
        elapsed = math.floor(new_update - start_time)
        if elapsed < 60:
            wait_seconds = 4
        elif elapsed < 360:
            wait_seconds = 6 + elapsed * 0.01
        elif elapsed < 900:
            wait_seconds = 9 + elapsed * 0.01
        elif elapsed < 1200:
            wait_seconds = 12 + elapsed * 0.01
        elif elapsed > 3600:
            break

        time.sleep(wait_seconds)
    real_time_sheet.db_conn.close()
    print("FINISHED IT")


def check_if_reservation_day():
    if datetime.datetime.today().weekday() == 4 or datetime.datetime.today().weekday() == 3:
        reservation_script()
    else:
        print("Hoje não é Sexta")


if __name__ == "__main__":
    reservation_script()
