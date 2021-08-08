import requests
import json
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db

# Base class to handle connecting to the NBA API and getting the drives data
class Drives(AbstractNBAConnect):

    def __init__(self, date_from='', date_to='', season_type='Regular Season'):
        # mm/dd/yyyy
        self.date_from = date_from
        self.date_to = date_to
        self.season_type = season_type

    def get_data(self):
        params = (
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
            ('PtMeasureType', 'Drives'),
            ('Season', '2020-21'),
            ('SeasonSegment', ''),
            ('SeasonType', self.season_type),
            ('StarterBench', ''),
            ('TeamID', '0'),
            ('VsConference', ''),
            ('VsDivision', ''),
            ('Weight', ''),
        )
        return json.loads(requests.get('https://stats.nba.com/stats/leaguedashptstats', headers=self.headers, params=params).content)

# Class to handle getting the drives data for all players
class PlayerTotalDrives(Drives):
    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'Player'
        self.headers_list = []
        self.table_name = 'player_total_drives'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values  " +
                    args_str +
                    "ON CONFLICT (player_id) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, drives, drive_fgm, drive_fga, drive_fg_pct, drive_ftm, drive_fta, drive_ft_pct, drive_pts, drive_pts_pct, drive_passes, drive_passes_pct, drive_ast, drive_ast_pct, drive_tov, drive_tov_pct, drive_pf, drive_pf_pct) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.drives, EXCLUDED.drive_fgm, EXCLUDED.drive_fga, EXCLUDED.drive_fg_pct, EXCLUDED.drive_ftm, EXCLUDED.drive_fta, EXCLUDED.drive_ft_pct, EXCLUDED.drive_pts, EXCLUDED.drive_pts_pct, EXCLUDED.drive_passes, EXCLUDED.drive_passes_pct, EXCLUDED.drive_ast, EXCLUDED.drive_ast_pct, EXCLUDED.drive_tov, EXCLUDED.drive_tov_pct, EXCLUDED.drive_pf, EXCLUDED.drive_pf_pct)"
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

# Class to handle getting the drives for all players on a given day
class PlayerDailyDrives(Drives):
    def __init__(self, date_from, date_to, season_type):
        super().__init__(date_from, date_to, season_type)
        self.playerorteam = 'Player'
        self.headers_list = []
        self.table_name = 'player_daily_drives'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values  " +
                    args_str +
                    "ON CONFLICT (player_id, date) DO UPDATE SET (player_id, player_name, team_id, team_abbreviation, gp, w, l, min, drives, drive_fgm, drive_fga, drive_fg_pct, drive_ftm, drive_fta, drive_ft_pct, drive_pts, drive_pts_pct, drive_passes, drive_passes_pct, drive_ast, drive_ast_pct, drive_tov, drive_tov_pct, drive_pf, drive_pf_pct, date) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.drives, EXCLUDED.drive_fgm, EXCLUDED.drive_fga, EXCLUDED.drive_fg_pct, EXCLUDED.drive_ftm, EXCLUDED.drive_fta, EXCLUDED.drive_ft_pct, EXCLUDED.drive_pts, EXCLUDED.drive_pts_pct, EXCLUDED.drive_passes, EXCLUDED.drive_passes_pct, EXCLUDED.drive_ast, EXCLUDED.drive_ast_pct, EXCLUDED.drive_tov, EXCLUDED.drive_tov_pct, EXCLUDED.drive_pf, EXCLUDED.drive_pf_pct, EXCLUDED.date)"
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

# Class to handle getting the drives data for all teams
class TeamTotalDrives(Drives):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'Team'
        self.table_name = 'team_total_drives'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id) DO UPDATE SET (team_id, team_abbreviation, team_name, gp, w, l, min, drives, drive_fgm, drive_fga, drive_fg_pct, drive_ftm, drive_fta, drive_ft_pct, drive_pts, drive_pts_pct, drive_passes, drive_passes_pct, drive_ast, drive_ast_pct, drive_tov, drive_tov_pct, drive_pf, drive_pf_pct) = (EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_name, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.drives, EXCLUDED.drive_fgm, EXCLUDED.drive_fga, EXCLUDED.drive_fg_pct, EXCLUDED.drive_ftm, EXCLUDED.drive_fta, EXCLUDED.drive_ft_pct, EXCLUDED.drive_pts, EXCLUDED.drive_pts_pct, EXCLUDED.drive_passes, EXCLUDED.drive_passes_pct, EXCLUDED.drive_ast, EXCLUDED.drive_ast_pct, EXCLUDED.drive_tov, EXCLUDED.drive_tov_pct, EXCLUDED.drive_pf, EXCLUDED.drive_pf_pct)"
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

# Class to handle getting the drives data for all teams on a given day
class TeamDailyDrives(Drives):

    def __init__(self, date_from, date_to, season_type):
        super().__init__(date_from, date_to, season_type)
        self.playerorteam = 'Team'
        self.table_name = 'team_daily_drives'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id, date) DO UPDATE SET (team_id, team_abbreviation, team_name, gp, w, l, min, drives, drive_fgm, drive_fga, drive_fg_pct, drive_ftm, drive_fta, drive_ft_pct, drive_pts, drive_pts_pct, drive_passes, drive_passes_pct, drive_ast, drive_ast_pct, drive_tov, drive_tov_pct, drive_pf, drive_pf_pct, date) = (EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_name, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.drives, EXCLUDED.drive_fgm, EXCLUDED.drive_fga, EXCLUDED.drive_fg_pct, EXCLUDED.drive_ftm, EXCLUDED.drive_fta, EXCLUDED.drive_ft_pct, EXCLUDED.drive_pts, EXCLUDED.drive_pts_pct, EXCLUDED.drive_passes, EXCLUDED.drive_passes_pct, EXCLUDED.drive_ast, EXCLUDED.drive_ast_pct, EXCLUDED.drive_tov, EXCLUDED.drive_tov_pct, EXCLUDED.drive_pf, EXCLUDED.drive_pf_pct, EXCLUDED.date)"
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
    # print(Drives('01/20/2021', '01/20/2021').poll())
    # print(Drives().poll())
    print(PlayerTotalDrives().poll())
    # print(PlayerDailyDrives('01/20/2021', '01/20/2021').poll())
    # print(TeamTotalDrives().poll())
    # print(TeamDailyDrives('01/20/2021', '01/20/2021').poll())