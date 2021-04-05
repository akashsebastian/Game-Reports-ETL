import requests
import json
from nba_connectors import AbstractNBAConnect
from players import Players
from utils import connect_to_db


class TeamDetails(AbstractNBAConnect):

    season = '2020-21'
    season_type = 'Regular Season'

    def upload_to_db(self, rows):
        try:
            conn = connect_to_db()
            with conn.cursor() as cur:
                args = [cur.mogrify("(%s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in rows]
                args_str = ', '.join(args)
                cur.execute(
                    "INSERT INTO teams values " +
                    args_str +
                    "ON CONFLICT (team_id) DO UPDATE SET (team_id, city, name, slug, conference, division) = (EXCLUDED.team_id, EXCLUDED.city, EXCLUDED.name, EXCLUDED.slug, EXCLUDED.conference, EXCLUDED.division)"
                )
                conn.commit()
            return rows
        except Exception as e:
            print(f"Error uploading to DB. Error: {e}")
            return []

    def get_data(self):
        params = (
            ('LeagueID', '00'),
            ('Season', self.season),
            ('SeasonType', self.season_type),
        )
        return json.loads(requests.get('https://stats.nba.com/stats/leaguestandingsv3', headers=self.headers, params=params).content)

    def parse_data(self, raw_data):
        data = []
        for row in raw_data['resultSets'][0]['rowSet']:
            obj = []
            obj.append(row[2])
            obj.append(row[3])
            obj.append(row[4])
            obj.append(row[5])
            obj.append(row[6])
            obj.append(row[10])
            data.append(tuple(obj))
            Players(obj[0]).poll()
        return data

    def poll(self):
        raw_data = self.get_data()
        parsed_data = self.parse_data(raw_data)
        data = self.upload_data(parsed_data)


if __name__ == '__main__':
    print(TeamDetails().poll())
