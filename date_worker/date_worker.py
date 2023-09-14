import datetime
import calendar


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

    return last_six_months


for i in get_last_six_months():
    print(i)
# today = datetime.date.today()
# print(today)
# print(first_day_of_month(today))
# print(last_day_of_month(today))
# print(month_name(today))
# print(first_day_of_previous_month(today))
