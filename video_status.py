import requests
import json
from nba_connectors import AbstractNBAConnect
from utils import connect_to_db
from box_score import BoxScore
from scores import Scores


class VideoStatus(AbstractNBAConnect):

    def __init__(self, game_date):
        self.game_date = game_date

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify("(" + ', '.join(['%s'] * len(self.headers_list)) + ")", x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO video_status values " +
                    args_str +
                    "ON CONFLICT (game_id) DO UPDATE SET (game_id, game_date, visitor_team_id, visitor_team_city, visitor_team_name, visitor_team_abbreviation, home_team_id, home_team_city, home_team_name, home_team_abbreviation, game_status, game_status_text, is_available, pt_xyz_available) = (EXCLUDED.game_id, EXCLUDED.game_date, EXCLUDED.visitor_team_id, EXCLUDED.visitor_team_city, EXCLUDED.visitor_team_name, EXCLUDED.visitor_team_abbreviation, EXCLUDED.home_team_id, EXCLUDED.home_team_city, EXCLUDED.home_team_name, EXCLUDED.home_team_abbreviation, EXCLUDED.game_status, EXCLUDED.game_status_text, EXCLUDED.is_available, EXCLUDED.pt_xyz_available)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB. Error: {e}")
            return []

    def get_data(self):
        params = (
            ('LeagueID', '00'),
            ('gameDate', self.game_date),
        )
        return json.loads(requests.get('https://stats.nba.com/stats/videoStatus', headers=self.headers, params=params).content)

    def parse_data(self, raw_data):
        data = []
        self.headers_list = [header.lower() for header in raw_data['resultSets'][0]['headers']]
        for row in raw_data['resultSets'][0]['rowSet']:
            data.append(row)
            BoxScore(row[0]).poll()
        return data

    def poll(self):
        raw_data = self.get_data()
        parsed_data = self.parse_data(raw_data)
        data = self.upload_data(parsed_data)
        print(f"Polled Video Status date: {self.game_date}")
        return data

if __name__ == '__main__':
    print(VideoStatus('01/20/2021').poll())
