import requests
import json
from nba_connectors import AbstractNBAConnect
from utils import upload_rows


class TeamDetails(AbstractNBAConnect):

    season = '2020-21'
    season_type = 'Regular Season'

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
        return data

    # def upload_data(self, parsed_data):
    #     rows = upload_rows('teams', parsed_data)
    #     return rows


if __name__ == '__main__':
    print(TeamDetails().poll())
