import requests
import json
import datetime
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db, get_dates


class Scores(AbstractNBAConnect):

    def __init__(self, game_date):
        # self.game_date = datetime.datetime.strptime(game_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        self.game_date = game_date.strftime('%Y-%m-%d')
        # self.game_date

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO scores values " +
                    args_str +
                    "ON CONFLICT (game_id) DO UPDATE SET (game_id, date, home_team_id, away_team_id, status, home_team_score, away_team_score, home_team_wins, away_team_wins, home_team_losses, away_team_losses, game_string) = (EXCLUDED.game_id, EXCLUDED.date, EXCLUDED.home_team_id, EXCLUDED.away_team_id, EXCLUDED.status, EXCLUDED.home_team_score, EXCLUDED.away_team_score, EXCLUDED.home_team_wins, EXCLUDED.away_team_wins, EXCLUDED.home_team_losses, EXCLUDED.away_team_losses, EXCLUDED.game_string)"
                )
                conn.commit()
            return rows
        except:
            return []

    def get_data(self):
        params = (
            ('LeagueID', '00'),
            ('GameDate', self.game_date),
        )
        return json.loads(requests.get('https://stats.nba.com/stats/scoreboardv3', headers=self.headers, params=params).content)

    def parse_data(self, raw_data):
        data = []
        print(raw_data)
        date = raw_data['scoreboard']['gameDate']
        for game in raw_data['scoreboard']['games']:
            obj = [
                int(game['gameId']),
                date,
                int(game['homeTeam']['teamId']),
                int(game['awayTeam']['teamId']),
                game['gameStatusText'],
                int(game['homeTeam']['score']),
                int(game['awayTeam']['score']),
                int(game['homeTeam']['wins']),
                int(game['awayTeam']['wins']),
                int(game['homeTeam']['losses']),
                int(game['awayTeam']['losses']),
                f"{game['awayTeam']['teamName']} at {game['homeTeam']['teamName']}"
            ]
            data.append(obj)
        return data

    def upload_data(self, parsed_data):
        rows = self.upload_to_db(parsed_data)
        return parsed_data

    def poll(self):
        raw_data = self.get_data()
        parsed_data = self.parse_data(raw_data)
        data = self.upload_data(parsed_data)

if __name__ == '__main__':
    # print(Scores('04/2/2021').poll())
    start_date = datetime.date(2020, 12, 22)
    # start_date = datetime.date(2021, 4, 2)
    end_date, _ = get_dates()
    while start_date <= end_date:
        Scores(start_date).poll()
        start_date = start_date + datetime.timedelta(days=1)

