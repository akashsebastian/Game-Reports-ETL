import requests
import json
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db


class VideoStatus(AbstractNBAConnect):

    def __init__(self, game_date):
        self.game_date = game_date

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify('(%s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO video_status values " +
                    args_str +
                    "ON CONFLICT (game_id) DO UPDATE SET (game_id, date, away_team_id, home_team_id, is_available, game_string) = (EXCLUDED.game_id, EXCLUDED.date, EXCLUDED.away_team_id, EXCLUDED.home_team_id, EXCLUDED.is_available, EXCLUDED.game_string)"
                )
                conn.commit()
            return rows
        except:
            return []

    def get_data(self):
        params = (
            ('LeagueID', '00'),
            ('gameDate', self.game_date),
        )
        return json.loads(requests.get('https://stats.nba.com/stats/videoStatus', headers=self.headers, params=params).content)
    
    def parse_data(self, raw_data):
        data = []
        for row in raw_data['resultSets'][0]['rowSet']:
            obj = []
            obj.append(int(row[0]))
            obj.append(row[1])
            obj.append(int(row[2]))
            obj.append(int(row[6]))
            obj.append(int(row[13]))
            obj.append(row[4] + ' at ' + row[8])
            data.append(obj)
        return data

    # def upload_data(self, parsed_data):
    #     rows = self.upload_to_db(parsed_data)
    #     return rows

if __name__ == '__main__':
    print(VideoStatus('01/28/2021').poll())
