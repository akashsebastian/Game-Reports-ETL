import datetime
from utils import get_dates
from video_status import VideoStatus
from shots_closest_defender_teams import ShotsClosestDefenderTeams
from drives import Drives
from catch_and_shoot import CatchAndShoot
from pull_up_shooting import PullUpShooting
from scores import Scores

def handler(event, context):
    today, yesterday = get_dates()
    ShotsClosestDefenderTeams().poll()
    VideoStatus(yesterday).poll()
    VideoStatus(today).poll()
    ShotsClosestDefenderTeams(yesterday, yesterday).poll()
    ShotsClosestDefenderTeams(today, today).poll()
    return {'result':'success'}

def get_data_for_date(date):
    print(f"VideoStatus - {date}")
    VideoStatus(date).poll()
    print(f"Scores - {date}")
    Scores(date).poll()
    print(f"ShotsClosestDefenderTeams - {date}")
    ShotsClosestDefenderTeams(date, date).poll()
    print(f"Drives - {date}")
    Drives(date, date).poll()
    print(f"CatchAndShoot - {date}")
    CatchAndShoot(date, date).poll()
    print(f"PullUpShooting - {date}")
    PullUpShooting(date, date).poll()

def get_data():
    print(f"ShotsClosestDefenderTeams - All")
    ShotsClosestDefenderTeams().poll()
    print(f"Drives - All")
    Drives().poll()
    print(f"CatchAndShoot - All")
    CatchAndShoot().poll()
    print(f"PullUpShooting - All")
    PullUpShooting().poll()

def main_test():
    today, yesterday = get_dates()
    get_data_for_date(yesterday)
    get_data_for_date(today)
    get_data()
    return {'result':'success'}

def back_populator():
    # start_date = datetime.date(2020, 12, 22)
    start_date = datetime.date(2021, 1, 25)
    end_date, _ = get_dates()
    while start_date <= end_date:
        get_data_for_date(start_date)
        start_date = start_date + datetime.timedelta(days=1)
    get_data()

if __name__ == '__main__':
    main_test()