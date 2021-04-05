import requests
import json
from requests.models import parse_url
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db

# Base class to handle connecting to the NBA API and getting the catch and shoot data
class CatchAndShoot(AbstractNBAConnect):

    def __init__(self, date_from='', date_to=''):
        # mm/dd/yyyy
        self.date_from = date_from
        self.date_to = date_to

    def get_params(self):
        return (
            ('College', ''),
            ('Conference', ''),
            ('Country', ''),
            ('DateFrom', self.date_from),
            ('DateTo', self.date_to),
            ('Division', ''),
            ('DraftPick', ''),
            ('DraftYear', ''),
            ('GameScope', ''),
            ('GameSegment', ''),
            ('Height', ''),
            ('LastNGames', '0'),
            ('LeagueID', '00'),
            ('Location', ''),
            ('Month', '0'),
            ('OpponentTeamID', '0'),
            ('Outcome', ''),
            ('PORound', '0'),
            ('PerMode', 'PerGame'),
            ('Period', '0'),
            ('PlayerExperience', ''),
            ('PlayerOrTeam', self.playerorteam),
            ('PlayerPosition', ''),
            ('PtMeasureType', 'CatchShoot'),
            ('Season', '2020-21'),
            ('SeasonSegment', ''),
            ('SeasonType', 'Regular Season'),
            ('StarterBench', ''),
            ('TeamID', '0'),
            ('VsConference', ''),
            ('VsDivision', ''),
            ('Weight', ''),
        )

    def get_data(self):
        return json.loads(requests.get('https://stats.nba.com/stats/leaguedashptstats', headers=self.headers, params=self.get_params()).content)

# Class to handle getting the catch and shoot data for all players
class PlayerTotalCatchAndShoot(CatchAndShoot):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'Player'
        self.table_name = 'player_total_catch_and_shoot'
        self.headers_list = []

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name  + " VALUES " +
                    args_str +
                    "ON CONFLICT (player_id) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, catch_shoot_fgm, catch_shoot_fga, catch_shoot_fg_pct, catch_shoot_pts, catch_shoot_fg3m, catch_shoot_fg3a, catch_shoot_fg3_pct, catch_shoot_efg_pct) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.catch_shoot_fgm, EXCLUDED.catch_shoot_fga, EXCLUDED.catch_shoot_fg_pct, EXCLUDED.catch_shoot_pts, EXCLUDED.catch_shoot_fg3m, EXCLUDED.catch_shoot_fg3a, EXCLUDED.catch_shoot_fg3_pct, EXCLUDED.catch_shoot_efg_pct)"
                )
            conn.commit()
            return rows
        except Exception as e:
            print(e)
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data['resultSets'][0]['headers']]
        self.headers_list = headers_list
        for row in raw_data['resultSets'][0]['rowSet']:
            data.append(row)
        return data

# Class to handle getting the catch and shoot data for players on a given day
class PlayerDailyCatchAndShoot(CatchAndShoot):

    def __init__(self, date_from = '', date_to = ''):
        super().__init__(date_from, date_to)
        self.playerorteam = 'Player'
        self.table_name = 'player_daily_catch_and_shoot'
        self.headers_list = []

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name  + " VALUES " +
                    args_str +
                    "ON CONFLICT (player_id, date) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, catch_shoot_fgm, catch_shoot_fga, catch_shoot_fg_pct, catch_shoot_pts, catch_shoot_fg3m, catch_shoot_fg3a, catch_shoot_fg3_pct, catch_shoot_efg_pct, date) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.catch_shoot_fgm, EXCLUDED.catch_shoot_fga, EXCLUDED.catch_shoot_fg_pct, EXCLUDED.catch_shoot_pts, EXCLUDED.catch_shoot_fg3m, EXCLUDED.catch_shoot_fg3a, EXCLUDED.catch_shoot_fg3_pct, EXCLUDED.catch_shoot_efg_pct, EXCLUDED.date)"
                )
            conn.commit()
            return rows
        except Exception as e:
            print(e)
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data['resultSets'][0]['headers']]
        headers_list.append('date')
        self.headers_list = headers_list
        for row in raw_data['resultSets'][0]['rowSet']:
            row.append(self.date_from)
            data.append(row)
        return data

# Class to handle getting the catch and shoot data for all teams
class TeamTotalCatchAndShoot(CatchAndShoot):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'Team'
        self.table_name = 'team_total_catch_and_shoot'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id) DO UPDATE SET (team_id, team_abbreviation, team_name, gp, w, l, min, catch_shoot_fgm, catch_shoot_fga, catch_shoot_fg_pct, catch_shoot_pts, 	catch_shoot_fg3m, catch_shoot_fg3a, catch_shoot_fg3_pct, catch_shoot_efg_pct) = (EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_name, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.catch_shoot_fgm, EXCLUDED.catch_shoot_fga, EXCLUDED.catch_shoot_fg_pct, EXCLUDED.catch_shoot_pts, EXCLUDED.catch_shoot_fg3m, EXCLUDED.catch_shoot_fg3a, EXCLUDED.catch_shoot_fg3_pct, EXCLUDED.catch_shoot_efg_pct)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB: {e}")
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data['resultSets'][0]['headers']]
        self.headers_list = headers_list
        for row in raw_data['resultSets'][0]['rowSet']:
            data.append(row)
        return data

# Class to handle getting the catch and shoot data for teams on a given day
class TeamDailyCatchAndShoot(CatchAndShoot):

    def __init__(self, date_from, date_to):
        super().__init__(date_from, date_to )
        self.playerorteam = 'Team'
        self.table_name = 'team_daily_catch_and_shoot'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id, date) DO UPDATE SET (team_id, team_abbreviation, team_name, gp, w, l, min, catch_shoot_fgm, catch_shoot_fga, catch_shoot_fg_pct, catch_shoot_pts, catch_shoot_fg3m, catch_shoot_fg3a, catch_shoot_fg3_pct, catch_shoot_efg_pct, date) = (EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_name, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.catch_shoot_fgm, EXCLUDED.catch_shoot_fga, EXCLUDED.catch_shoot_fg_pct, EXCLUDED.catch_shoot_pts, EXCLUDED.catch_shoot_fg3m, EXCLUDED.catch_shoot_fg3a, EXCLUDED.catch_shoot_fg3_pct, EXCLUDED.catch_shoot_efg_pct, EXCLUDED.date)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB: {e}")
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data['resultSets'][0]['headers']]
        headers_list.append('date')
        self.headers_list = headers_list
        for row in raw_data['resultSets'][0]['rowSet']:
            row.append(self.date_from)
            data.append(row)
        return data

if __name__ == '__main__':
    # print(CatchAndShoot("01/20/2021", "01/20/2021").poll())
    # print(CatchAndShoot().poll())
    print(PlayerTotalCatchAndShoot().poll())
    # print(PlayerDailyCatchAndShoot("01/20/2021", "01/20/2021").poll())
    # print(TeamTotalCatchAndShoot().poll())
    # print(TeamDailyCatchAndShoot("01/20/2021", "01/20/2021").poll())
