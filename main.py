import datetime
from utils import get_dates
from report import Report
from scores import Scores

def back_populator():
    # Start date of season
    # start_date = datetime.date(2020, 12, 22)
    start_date = datetime.date(2021, 4, 2)
    end_date, _ = get_dates()
    while start_date <= end_date:
        Report().generate_reports(start_date)
        Scores(start_date).poll()
        start_date = start_date + datetime.timedelta(days=1)

if __name__ == '__main__':
    back_populator()