import datetime


def get_next_week_from_monday(days_offset):
    week_list = []
    d = datetime.date.today()+datetime.timedelta(days_offset)
    print(d)

    while d.weekday() != 0:
        d += datetime.timedelta(1)

    for i in range(7):
        new_day = d + datetime.timedelta(i)

        week_list.append(new_day.strftime("%d/%m/%Y"))

    print(week_list)
    return week_list
