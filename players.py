import requests
import json
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db

class Players(AbstractNBAConnect):

    def __init__(self, team_id):
        self.team_id = team_id
        self.table_name = 'players'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify("(" + ', '.join(['%s'] * len(self.headers_list)) + ")", x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO " + self.table_name + " values " +
                    args_str +
                    "ON CONFLICT (player_id) DO UPDATE SET (team_id, season, leagueid, player, player_slug, num, position, height, weight, birth_date, age, exp, school, player_id) = (EXCLUDED.team_id, EXCLUDED.season, EXCLUDED.leagueid, EXCLUDED.player, EXCLUDED.player_slug, EXCLUDED.num, EXCLUDED.position, EXCLUDED.height, EXCLUDED.weight, EXCLUDED.birth_date, EXCLUDED.age, EXCLUDED.exp, EXCLUDED.school, EXCLUDED.player_id)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB. Error: {e}")
            return []

    def get_data(self):
        params = (
            ('LeagueID', '00'),
            ('Season', '2020-21'),
            ('TeamID', self.team_id)
        )
        return json.loads(requests.get('https://stats.nba.com/stats/commonteamroster', headers=self.headers, params=params).content)

    def parse_data(self, raw_data):
        data = []
        self.headers_list = [header.lower() if header != 'teamid' else 'team_id' for header in raw_data['resultSets'][0]['headers']]
        for row in raw_data['resultSets'][0]['rowSet']:
            data.append(row)
        return data
