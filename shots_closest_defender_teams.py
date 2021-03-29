import requests
import json

from requests.models import parse_url
from nba_connectors import AbstractNBAConnect
from utils import upload_rows, delete_rows, connect_to_db


class ShotsClosestDefenderTeams(AbstractNBAConnect):

    def __init__(self, date_from='', date_to=''):
        # mm/dd/yyyy
        if not date_from:
            self.table_name = 'shots_closest_defender_teams'
        else:
            self.table_name = 'shots_closest_defender_teams_daily'
        self.date_from = date_from
        self.date_to = date_to

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                # if self.table_name == 'shots_closest_defender_teams':
                #     args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                #     args_str = ', '.join(args)
                #     cur.execute(
                #         "INSERT INTO shots_closest_defender_teams VALUES" +
                #         args_str +
                #         "ON CONFLICT (team_id, def_distance) DO UPDATE SET (team_id,gp,g,fga_frequency,fgm,fga,fg_pct,efg_pct,fg2a_frequency,fg2m,fg2a,fg2_pct,fg3a_frequency,fg3m,fg3a,fg3_pct,def_distance) = (EXCLUDED.team_id,EXCLUDED.gp,EXCLUDED.g,EXCLUDED.fga_frequency,EXCLUDED.fgm,EXCLUDED.fga,EXCLUDED.fg_pct,EXCLUDED.efg_pct,EXCLUDED.fg2a_frequency,EXCLUDED.fg2m,EXCLUDED.fg2a,EXCLUDED.fg2_pct,EXCLUDED.fg3a_frequency,EXCLUDED.fg3m,EXCLUDED.fg3a,EXCLUDED.fg3_pct,EXCLUDED.def_distance)"
                #     )
                # else:
                #     args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                #     args_str = ', '.join(args)
                #     cur.execute(
                #         "INSERT INTO shots_closest_defender_teams_daily VALUES" +
                #         args_str +
                #         "ON CONFLICT (team_id, def_distance, date) DO UPDATE SET (team_id,gp,g,fga_frequency,fgm,fga,fg_pct,efg_pct,fg2a_frequency,fg2m,fg2a,fg2_pct,fg3a_frequency,fg3m,fg3a,fg3_pct,def_distance,date) = (EXCLUDED.team_id,EXCLUDED.gp,EXCLUDED.g,EXCLUDED.fga_frequency,EXCLUDED.fgm,EXCLUDED.fga,EXCLUDED.fg_pct,EXCLUDED.efg_pct,EXCLUDED.fg2a_frequency,EXCLUDED.fg2m,EXCLUDED.fg2a,EXCLUDED.fg2_pct,EXCLUDED.fg3a_frequency,EXCLUDED.fg3m,EXCLUDED.fg3a,EXCLUDED.fg3_pct,EXCLUDED.def_distance,EXCLUDED.date)"
                #     )
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO player_shots_closest_defender VALUES" +
                    args_str +
                    "ON CONFLICT (player_id, def_dist, date) DO UPDATE SET (player_id, player_name, player_last_team_id, player_last_team_abbreviation, age, gp, g, fga_frequency, fgm, fga, fg_pct, efg_pct, fg2a_frequency, fg2m, fg2a, fg2_pct, fg3a_frequency, fg3m, fg3a, fg3_pct, def_dist, date) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.player_last_team_id, EXCLUDED.player_last_team_abbreviation, EXCLUDED.age, EXCLUDED.gp, EXCLUDED.g, EXCLUDED.fga_frequency, EXCLUDED.fgm, EXCLUDED.fga, EXCLUDED.fg_pct, EXCLUDED.efg_pct, EXCLUDED.fg2a_frequency, EXCLUDED.fg2m, EXCLUDED.fg2a, EXCLUDED.fg2_pct, EXCLUDED.fg3a_frequency, EXCLUDED.fg3m, EXCLUDED.fg3a, EXCLUDED.fg3_pct, EXCLUDED.def_dist, EXCLUDED.date)"
                )
            conn.commit()
            return rows
        except Exception as e:
            print(e)
            return []

    def_range_map = {
        0: '0-2 Feet - Very Tight',
        1: '2-4 Feet - Tight',
        2: '4-6 Feet - Open',
        3: '6+ Feet - Wide Open'
    }

    def get_params(self, def_range):
        return (
            ('CloseDefDistRange', def_range),
            ('College', ''),
            ('Conference', ''),
            ('Country', ''),
            ('DateFrom', self.date_from),
            ('DateTo', self.date_to),
            ('Division', ''),
            ('DraftPick', ''),
            ('DraftYear', ''),
            ('DribbleRange', ''),
            ('GameScope', ''),
            ('GameSegment', ''),
            ('GeneralRange', ''),
            ('Height', ''),
            ('LastNGames', '0'),
            ('LeagueID', '00'),
            ('Location', ''),
            ('Month', '0'),
            ('OpponentTeamID', '0'),
            ('Outcome', ''),
            ('PORound', '0'),
            ('PaceAdjust', 'N'),
            ('PerMode', 'PerGame'),
            ('Period', '0'),
            ('PlayerExperience', ''),
            ('PlayerPosition', ''),
            ('PlusMinus', 'N'),
            ('Rank', 'N'),
            ('Season', '2020-21'),
            ('SeasonSegment', ''),
            ('SeasonType', 'Regular Season'),
            ('ShotClockRange', ''),
            ('ShotDistRange', ''),
            ('StarterBench', ''),
            ('TeamID', '0'),
            ('TouchTimeRange', ''),
            ('VsConference', ''),
            ('VsDivision', ''),
            ('Weight', ''),
        )

    def get_data(self):
        raw_data = []
        for i in range(4):
            raw_data.append(json.loads(requests.get(f'https://stats.nba.com/stats/leaguedash{self.playerorteam}ptshot', headers=self.headers, params=self.get_params(self.def_range_map[i])).content))
        return raw_data

    def parse_data(self, raw_data):
        data = []
        for def_range, raw_shooting_data in enumerate(raw_data):
            for row in raw_shooting_data['resultSets'][0]['rowSet']:
                obj = []
                for i in range(len(raw_shooting_data['resultSets'][0]['headers'])):
                    # Team name and abbreviation
                    if i in [1,2]:
                        continue
                    obj.append(0 if row[i] is None else float(row[i]))
                obj.append(self.def_range_map[def_range])
                if self.table_name == 'shots_closest_defender_teams_daily':
                    obj.append(self.date_from)
                data.append(obj)
        return data

class PlayerTotalShotsClosestDefender(ShotsClosestDefenderTeams):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'player'
        self.headers_list = []
        self.table_name = 'player_total_shots_closest_defender'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " VALUES " +
                    args_str +
                    "ON CONFLICT (player_id, def_dist) DO UPDATE SET (player_id, player_name, player_last_team_id, player_last_team_abbreviation, age, gp, g, fga_frequency, fgm, fga, fg_pct, efg_pct, fg2a_frequency, fg2m, fg2a, fg2_pct, fg3a_frequency, fg3m, fg3a, fg3_pct, def_dist) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.player_last_team_id, EXCLUDED.player_last_team_abbreviation, EXCLUDED.age, EXCLUDED.gp, EXCLUDED.g, EXCLUDED.fga_frequency, EXCLUDED.fgm, EXCLUDED.fga, EXCLUDED.fg_pct, EXCLUDED.efg_pct, EXCLUDED.fg2a_frequency, EXCLUDED.fg2m, EXCLUDED.fg2a, EXCLUDED.fg2_pct, EXCLUDED.fg3a_frequency, EXCLUDED.fg3m, EXCLUDED.fg3a, EXCLUDED.fg3_pct, EXCLUDED.def_dist)"
                )
            conn.commit()
            return rows
        except Exception as e:
            print(e)
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data[0]['resultSets'][0]['headers']]
        headers_list.append('def_dist')
        self.headers_list = headers_list
        for def_range, raw_shooting_data in enumerate(raw_data):
            for row in raw_shooting_data['resultSets'][0]['rowSet']:
                row.append(self.def_range_map[def_range])
                data.append(row)
        return data

class PlayerDailyShotsClosestDefender(ShotsClosestDefenderTeams):

    def __init__(self, date_from, date_to):
        super().__init__(date_from, date_to)
        self.playerorteam = 'player'
        self.headers_list = []
        self.table_name = 'player_daily_shots_closest_defender'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " VALUES " +
                    args_str +
                    "ON CONFLICT (player_id, def_dist, date) DO UPDATE SET (player_id, player_name, player_last_team_id, player_last_team_abbreviation, age, gp, g, fga_frequency, fgm, fga, fg_pct, efg_pct, fg2a_frequency, fg2m, fg2a, fg2_pct, fg3a_frequency, fg3m, fg3a, fg3_pct, def_dist, date) = (EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.player_last_team_id, EXCLUDED.player_last_team_abbreviation, EXCLUDED.age, EXCLUDED.gp, EXCLUDED.g, EXCLUDED.fga_frequency, EXCLUDED.fgm, EXCLUDED.fga, EXCLUDED.fg_pct, EXCLUDED.efg_pct, EXCLUDED.fg2a_frequency, EXCLUDED.fg2m, EXCLUDED.fg2a, EXCLUDED.fg2_pct, EXCLUDED.fg3a_frequency, EXCLUDED.fg3m, EXCLUDED.fg3a, EXCLUDED.fg3_pct, EXCLUDED.def_dist, EXCLUDED.date)"
                )
            conn.commit()
            return rows
        except Exception as e:
            print(e)
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data[0]['resultSets'][0]['headers']]
        headers_list.extend(['def_dist', 'date'])
        self.headers_list = headers_list
        for def_range, raw_shooting_data in enumerate(raw_data):
            for row in raw_shooting_data['resultSets'][0]['rowSet']:
                row.append(self.def_range_map[def_range])
                row.append(self.date_from)
                data.append(row)
        return data

class TeamTotalShotsClosestDefender(ShotsClosestDefenderTeams):

    def __init__(self):
        super().__init__(date_from = '', date_to = '')
        self.playerorteam = 'team'
        self.table_name = 'team_total_shots_closest_defender'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id, def_dist) DO UPDATE SET (team_id, team_name, team_abbreviation, gp, g, fga_frequency, fgm, fga, fg_pct, efg_pct, fg2a_frequency, fg2m, fg2a, fg2_pct, fg3a_frequency, fg3m, fg3a, fg3_pct, def_dist) = (EXCLUDED.team_id, EXCLUDED.team_name, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.g, EXCLUDED.fga_frequency, EXCLUDED.fgm, EXCLUDED.fga, EXCLUDED.fg_pct, EXCLUDED.efg_pct, EXCLUDED.fg2a_frequency, EXCLUDED.fg2m, EXCLUDED.fg2a, EXCLUDED.fg2_pct, EXCLUDED.fg3a_frequency, EXCLUDED.fg3m, EXCLUDED.fg3a, EXCLUDED.fg3_pct, EXCLUDED.def_dist)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB: {e}")
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data[0]['resultSets'][0]['headers']]
        headers_list.append('def_dist')
        self.headers_list = headers_list
        for def_range, raw_shooting_data in enumerate(raw_data):
            for row in raw_shooting_data['resultSets'][0]['rowSet']:
                row.append(self.def_range_map[def_range])
                data.append(row)
        return data

class TeamDailyShotsClosestDefender(ShotsClosestDefenderTeams):

    def __init__(self, date_from, date_to):
        super().__init__(date_from, date_to)
        self.playerorteam = 'team'
        self.table_name = 'team_daily_shots_closest_defender'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (team_id, def_dist, date) DO UPDATE SET (team_id, team_name, team_abbreviation, gp, g, fga_frequency, fgm, fga, fg_pct, efg_pct, fg2a_frequency, fg2m, fg2a, fg2_pct, fg3a_frequency, fg3m, fg3a, fg3_pct, def_dist, date) = (EXCLUDED.team_id, EXCLUDED.team_name, EXCLUDED.team_abbreviation, EXCLUDED.gp, EXCLUDED.g, EXCLUDED.fga_frequency, EXCLUDED.fgm, EXCLUDED.fga, EXCLUDED.fg_pct, EXCLUDED.efg_pct, EXCLUDED.fg2a_frequency, EXCLUDED.fg2m, EXCLUDED.fg2a, EXCLUDED.fg2_pct, EXCLUDED.fg3a_frequency, EXCLUDED.fg3m, EXCLUDED.fg3a, EXCLUDED.fg3_pct, EXCLUDED.def_dist, EXCLUDED.date)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB: {e}")
            return []

    def parse_data(self, raw_data):
        data = []
        headers_list = [header.lower() for header in raw_data[0]['resultSets'][0]['headers']]
        headers_list.extend(['def_dist', 'date'])
        self.headers_list = headers_list
        for def_range, raw_shooting_data in enumerate(raw_data):
            for row in raw_shooting_data['resultSets'][0]['rowSet']:
                row.append(self.def_range_map[def_range])
                row.append(self.date_from)
                data.append(row)
        return data

if __name__ == '__main__':
    # print(ShotsClosestDefenderTeams("01/27/2021", "01/27/2021").poll())
    # print(ShotsClosestDefenderTeams().poll())
    print(PlayerTotalShotsClosestDefender().poll())
    # print(PlayerDailyShotsClosestDefender("01/20/2021", "01/20/2021").poll())
    # print(TeamTotalShotsClosestDefender().poll())
    # print(TeamDailyShotsClosestDefender("01/20/2021", "01/20/2021").poll())