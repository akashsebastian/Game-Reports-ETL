import requests
import json
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db


class BoxScore(AbstractNBAConnect):

    def __init__(self, game_id):
        self.game_id = game_id

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify("(" + ', '.join(['%s'] * len(self.headers_list)) + ")", x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO box_score values " +
                    args_str +
                    "ON CONFLICT (game_id, player_id) DO UPDATE SET (game_id, team_id, team_abbreviation, team_city, player_id, player_name, start_position, comment, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, pts, plus_minus) = (EXCLUDED.game_id, EXCLUDED.team_id, EXCLUDED.team_abbreviation, EXCLUDED.team_city, EXCLUDED.player_id, EXCLUDED.player_name, EXCLUDED.start_position, EXCLUDED.comment, EXCLUDED.min, EXCLUDED.fgm, EXCLUDED.fga, EXCLUDED.fg_pct, EXCLUDED.fg3m, EXCLUDED.fg3a, EXCLUDED.fg3_pct, EXCLUDED.ftm, EXCLUDED.fta, EXCLUDED.ft_pct, EXCLUDED.oreb, EXCLUDED.dreb, EXCLUDED.reb, EXCLUDED.ast, EXCLUDED.stl, EXCLUDED.blk, EXCLUDED.tov, EXCLUDED.pf, EXCLUDED.pts, EXCLUDED.plus_minus)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB. Error: {e}")
            return []

    def get_data(self):
        params = (
            ('EndPeriod', '1'),
            ('EndRange', '0'),
            ('GameID', self.game_id),
            ('RangeType', '0'),
            ('StartPeriod', '1'),
            ('StartRange', '0')
        )
        return json.loads(requests.get('https://stats.nba.com/stats/boxscoretraditionalv2', headers=self.headers, params=params).content)

    def parse_data(self, raw_data):
        data = []
        self.headers_list = [header.lower() if header != 'TO' else 'tov' for header in raw_data['resultSets'][0]['headers']]
        for row in raw_data['resultSets'][0]['rowSet']:
            data.append(row)
        return data

    def poll(self):
        print(f"Polling box score for game {self.game_id}")
        raw_data = self.get_data()
        parsed_data = self.parse_data(raw_data)
        data = self.upload_data(parsed_data)

if __name__ == '__main__':
    print(BoxScore('0022000216').poll())
