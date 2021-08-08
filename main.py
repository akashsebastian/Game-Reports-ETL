import datetime
from utils import get_dates
from report import Report
from scores import Scores

def back_populator():
    # Start date of season
    # start_date = datetime.date(2020, 12, 22)
    # start_date = datetime.date(2021, 4, 11)
    start_date = datetime.date(2021, 5, 18)
    end_date, _ = get_dates()
    Report().generate_reports(start_date, end_date)

def poll():
    _, yesterday = get_dates()
    print(f"Polling for date: {yesterday}")
    Report().generate_reports(yesterday, yesterday)
    Scores(yesterday).poll()

if __name__ == '__main__':
    back_populator()