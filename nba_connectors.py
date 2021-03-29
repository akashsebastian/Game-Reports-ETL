import pandas as pd


class AbstractNBAConnect():

    headers = {
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
        'Accept': 'application/json, text/plain, */*',
        'x-nba-stats-token': 'true',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'x-nba-stats-origin': 'stats',
        'Origin': 'https://www.nba.com',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.nba.com/',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    def get_data():
        raise NotImplementedError(
            'Child classes of AbstractNBAConnect should implement the get_data() method.'
        )

    def parse_data():
        raise NotImplementedError(
            'Child classes of AbstractNBAConnect should implement the parse_data() method.'
        )

    def upload_data(self, parsed_data):
        if parsed_data:
            rows = self.upload_to_db(parsed_data)
            return rows
        return []

    def poll(self):
        raw_data = self.get_data()
        parsed_data = self.parse_data(raw_data)
        data = self.upload_data(parsed_data)
        print(data)
        return self.to_dataframe(data)

    def to_dataframe(self, data):
        return pd.DataFrame(data, columns = self.headers_list)
