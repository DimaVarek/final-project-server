import datetime
import calendar


def get_week_by_last_day(current_date: datetime.date):
    first_day = current_date - datetime.timedelta(days=6)
    first_day = datetime.datetime(first_day.year, first_day.month, first_day.day).timestamp()
    last_day = datetime.datetime(current_date.year, current_date.month, current_date.day) + \
               datetime.timedelta(hours=23, minutes=59, seconds=59)
    last_day = last_day.timestamp()
    return [first_day, last_day]


def get_full_day(current_date: datetime.date):
    start_day = datetime.datetime(current_date.year, current_date.month, current_date.day).timestamp()
    end_day = (datetime.datetime(current_date.year, current_date.month, current_date.day) +
               datetime.timedelta(hours=23, minutes=59, seconds=59)).timestamp()
    return [start_day, end_day]


def first_day_of_month(current_date: datetime.date):
    return datetime.datetime(current_date.year, current_date.month, 1)


def last_day_of_month(current_date: datetime.date):
    day_next_month = datetime.date(current_date.year, current_date.month, 28) + datetime.timedelta(days=5)
    return first_day_of_month(day_next_month) - datetime.timedelta(days=1) + \
        datetime.timedelta(hours=23, minutes=59, seconds=59)


def first_day_of_previous_month(current_date: datetime.date):
    first_day = first_day_of_month(current_date)
    return first_day - datetime.timedelta(seconds=1)


def month_name(current_date: datetime.date):
    return calendar.month_name[current_date.month]


def get_last_six_months():
    last_six_months = []
    today = datetime.date.today()
    last_day = last_day_of_month(today)
    first_day = first_day_of_month(today)
    last_six_months.append([month_name(today), first_day.timestamp(), last_day.timestamp()])
    for i in range(5):
        last_day = first_day_of_previous_month(first_day)
        first_day = first_day_of_month(last_day)
        last_six_months.append([month_name(last_day), first_day.timestamp(), last_day.timestamp()])

    return last_six_months[::-1]


def get_last_four_week():
    last_four_week = []
    today = datetime.date.today()
    last_four_week.append(['week4', *get_week_by_last_day(today)])
    for i in range(3):
        today = today - datetime.timedelta(days=7)
        last_four_week.append([f'week{3-i}', *get_week_by_last_day(today)])
    return last_four_week[::-1]


def get_last_week():
    last_week = []
    today = datetime.date.today()
    last_week.append([calendar.day_name[today.weekday()], *get_full_day(today)])
    for i in range(6):
        today -= datetime.timedelta(days=1)
        last_week.append([calendar.day_name[today.weekday()], *get_full_day(today)])
    return last_week[::-1]
# for i in get_last_four_week():
#     print(i)
# today = datetime.date.today()
# print(today)
# print(first_day_of_month(today))
# print(last_day_of_month(today))
# print(month_name(today))
# print(first_day_of_previous_month(today))
