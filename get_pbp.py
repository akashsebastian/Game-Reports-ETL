from nba_api.stats.endpoints import playbyplay

df = playbyplay.PlayByPlay('0042000212').get_data_frames()[0]
df.to_csv('test.csv', index=False)