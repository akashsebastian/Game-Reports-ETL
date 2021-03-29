import requests
import json
from requests.models import parse_url
from nba_connectors import AbstractNBAConnect
from utils import upload_rows, delete_rows, connect_to_db


class PullUpShooting(AbstractNBAConnect):

    def __init__(self, date_from='', date_to=''):
        # mm/dd/yyyy
        if not date_from:
            self.table_name = 'pull_up'
        else:
            self.table_name = 'pull_up_daily'
        self.date_from = date_from
        self.date_to = date_to

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                # if self.table_name == 'pull_up':
                #     args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                #     args_str = ', '.join(args)
                #     cur.execute(
                #         "INSERT INTO pull_up VALUES" +
                #         args_str +
                #         "ON CONFLICT (team_id) DO UPDATE SET (team_id,gp,points,fg_m,fg_a,fg_pct,fg3_m,fg3_a,fg3_pct) = (EXCLUDED.team_id,EXCLUDED.gp,EXCLUDED.points,EXCLUDED.fg_m,EXCLUDED.fg_a,EXCLUDED.fg_pct,EXCLUDED.fg3_m,EXCLUDED.fg3_a,EXCLUDED.fg3_pct)"
                #     )
                # else:
                #     args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                #     args_str = ', '.join(args)
                #     cur.execute(
                #         "INSERT INTO pull_up_daily VALUES" +
                #         args_str +
                #         "ON CONFLICT (team_id, date) DO UPDATE SET (team_id,gp,points,fg_m,fg_a,fg_pct,fg3_m,fg3_a,fg3_pct,date) = (EXCLUDED.team_id,EXCLUDED.gp,EXCLUDED.points,EXCLUDED.fg_m,EXCLUDED.fg_a,EXCLUDED.fg_pct,EXCLUDED.fg3_m,EXCLUDED.fg3_a,EXCLUDED.fg3_pct,EXCLUDED.date)"
                #     )
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO player_pull_up VALUES" +
                    args_str +
                    "ON CONFLICT (player_id, date) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, pull_up_fgm, pull_up_fga, pull_up_fg_pct, pull_up_pts, pull_up_fg3m, pull_up_fg3a, pull_up_fg3_pct, pull_up_efg_pct, date) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.pull_up_fgm, EXCLUDED.pull_up_fga, EXCLUDED.pull_up_fg_pct, EXCLUDED.pull_up_pts, EXCLUDED.pull_up_fg3m, EXCLUDED.pull_up_fg3a, EXCLUDED.pull_up_fg3_pct, EXCLUDED.pull_up_efg_pct, EXCLUDED.date)"
                )
            conn.commit()
            return rows
        except Exception as e:
            print(e)
            return []

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
            ('PtMeasureType', 'PullUpShot'),
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

    def parse_data(self, raw_data):
        data = []
        for row in raw_data['resultSets'][0]['rowSet']:
            obj = [
                int(row[0]),
                int(row[3]),
                int(row[10]),
                int(row[7]),
                int(row[8]),
                float(row[9]) if row[9] else 0,
                int(row[11]) if row[11] else 0,
                int(row[12]) if row[12] else 0,
                float(row[13]) if row[13] else 0,
            ]
            if self.table_name == 'pull_up_daily':
                obj.append(self.date_from)
            data.append(obj)
        return data

class PlayerTotalPullUpShooting(PullUpShooting):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'Player'
        self.headers_list = []
        self.table_name = 'player_total_pull_up'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " VALUES " +
                    args_str +
                    "ON CONFLICT (player_id) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, pull_up_fgm, pull_up_fga, pull_up_fg_pct, pull_up_pts, pull_up_fg3m, pull_up_fg3a, pull_up_fg3_pct, pull_up_efg_pct) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.pull_up_fgm, EXCLUDED.pull_up_fga, EXCLUDED.pull_up_fg_pct, EXCLUDED.pull_up_pts, EXCLUDED.pull_up_fg3m, EXCLUDED.pull_up_fg3a, EXCLUDED.pull_up_fg3_pct, EXCLUDED.pull_up_efg_pct)"
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

class PlayerDailyPullUpShooting(PullUpShooting):

    def __init__(self, date_from, date_to):
        super().__init__(date_from, date_to)
        self.playerorteam = 'Player'
        self.headers_list = []
        self.table_name = 'player_daily_pull_up'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " VALUES " +
                    args_str +
                    "ON CONFLICT (player_id, date) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, pull_up_fgm, pull_up_fga, pull_up_fg_pct, pull_up_pts, pull_up_fg3m, pull_up_fg3a, pull_up_fg3_pct, pull_up_efg_pct, date) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.pull_up_fgm, EXCLUDED.pull_up_fga, EXCLUDED.pull_up_fg_pct, EXCLUDED.pull_up_pts, EXCLUDED.pull_up_fg3m, EXCLUDED.pull_up_fg3a, EXCLUDED.pull_up_fg3_pct, EXCLUDED.pull_up_efg_pct, EXCLUDED.date)"
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

class TeamTotalPullUpShooting(PullUpShooting):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'Team'
        self.table_name = 'team_total_pull_up_shooting'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id) DO UPDATE SET (team_id, team_abbreviation, team_name, gp, w, l, min, pull_up_shooting_fgm, pull_up_shooting_fga, pull_up_shooting_fg_pct, pull_up_shooting_pts, 	pull_up_shooting_fg3m, pull_up_shooting_fg3a, pull_up_shooting_fg3_pct, pull_up_shooting_efg_pct) = (EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_name, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.pull_up_shooting_fgm, EXCLUDED.pull_up_shooting_fga, EXCLUDED.pull_up_shooting_fg_pct, EXCLUDED.pull_up_shooting_pts, EXCLUDED.pull_up_shooting_fg3m, EXCLUDED.pull_up_shooting_fg3a, EXCLUDED.pull_up_shooting_fg3_pct, EXCLUDED.pull_up_shooting_efg_pct)"
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


class TeamDailyPullUpShooting(PullUpShooting):

    def __init__(self, date_from, date_to):
        super().__init__(date_from, date_to)
        self.playerorteam = 'Team'
        self.table_name = 'team_daily_pull_up_shooting'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id, date) DO UPDATE SET (team_id, team_abbreviation, team_name, gp, w, l, min, pull_up_shooting_fgm, pull_up_shooting_fga, pull_up_shooting_fg_pct, pull_up_shooting_pts, pull_up_shooting_fg3m, pull_up_shooting_fg3a, pull_up_shooting_fg3_pct, pull_up_shooting_efg_pct, date) = (EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_name, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.pull_up_shooting_fgm, EXCLUDED.pull_up_shooting_fga, EXCLUDED.pull_up_shooting_fg_pct, EXCLUDED.pull_up_shooting_pts, EXCLUDED.pull_up_shooting_fg3m, EXCLUDED.pull_up_shooting_fg3a, EXCLUDED.pull_up_shooting_fg3_pct, EXCLUDED.pull_up_shooting_efg_pct, EXCLUDED.date)"
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
    # print(PullUpShooting("01/20/2021", "01/20/2021").poll())
    # print(PullUpShooting().poll())
    print(PlayerTotalPullUpShooting().poll())
    # print(PlayerDailyPullUpShooting("01/20/2021", "01/20/2021").poll())
    # print(TeamTotalPullUpShooting().poll())
    # print(TeamDailyPullUpShooting("01/20/2021", "01/20/2021").poll())
