import pandas as pd
from catch_and_shoot import CatchAndShoot, TeamDailyCatchAndShoot, TeamTotalCatchAndShoot, PlayerDailyCatchAndShoot, PlayerTotalCatchAndShoot
from drives import Drives, TeamDailyDrives, TeamTotalDrives, PlayerDailyDrives, PlayerTotalDrives
from pull_up_shooting import PullUpShooting, TeamDailyPullUpShooting, TeamTotalPullUpShooting, PlayerDailyPullUpShooting, PlayerTotalPullUpShooting
from shots_closest_defender_teams import ShotsClosestDefenderTeams, TeamDailyShotsClosestDefender, TeamTotalShotsClosestDefender, PlayerDailyShotsClosestDefender, PlayerTotalShotsClosestDefender
from utils import divide, connect_to_db


class Report():

    table_names = {
        0: 'player_report_catch_and_shoot',
        1: 'player_report_drives',
        2: 'player_report_pull_up_shooting',
        3: 'player_report_shots_closest_defender'
    }


    def parse_player_shots(self, player_shots_df):
        return player_shots_df[(player_shots_df['def_dist'] == '0-2 Feet - Very Tight') | (player_shots_df['def_dist'] == '2-4 Feet - Tight')], player_shots_df[(player_shots_df['def_dist'] == '4-6 Feet - Open') | (player_shots_df['def_dist'] == '6+ Feet - Wide Open')]

    def get_player_rows(self, table_name, players_ids_list):
        players_ids_list = ','.join(map(str, players_ids_list))
        conn = connect_to_db()
        return pd.read_sql_query(f'select * from {table_name} where player_id in ({players_ids_list})', con=conn)

    def update_player_drives_report(self, df):
        table_name = self.table_names[1]
        player_ids_list = df['player_id'].tolist()
        df_player_reports = self.get_player_rows(table_name, player_ids_list)
        drop_columns_list = [x for x in df_player_reports.columns.tolist() if '_pct' in x]
        df.drop(drop_columns_list, axis=1, inplace=True)
        df.drop('date', axis = 1, inplace=True)
        df.drop(['player_name', 'team_id', 'team_abbreviation'], axis=1, inplace=True)
        columns_order = df_player_reports.columns.tolist()
        df_player_reports = df_player_reports.drop(drop_columns_list, axis=1)
        grouped_df = pd.concat([df, df_player_reports], axis=0).groupby('player_id')
        df_new = pd.concat([grouped_df['gp','w', 'l', 'drives', 'drive_fgm','drive_fga','drive_ftm', 'drive_fta', 'drive_pts', 'drive_passes','drive_ast','drive_tov','drive_pf'].sum(),  grouped_df['min'].sum()], axis=1).reset_index()
        df_new['drive_fg_pct'] = df_new.apply(lambda row: divide(row.drive_fgm,row.drive_fga), axis=1)
        df_new['drive_ft_pct'] = df_new.apply(lambda row: divide(row.drive_ftm,row.drive_fta), axis=1)
        df_new['drive_pts_pct'] = df_new.apply(lambda row: divide(row.drive_pts,row.drives), axis=1)
        df_new['drive_passes_pct'] = df_new.apply(lambda row: divide(row.drive_passes,row.drives), axis=1)
        df_new['drive_pts_pct'] = df_new.apply(lambda row: divide(row.drive_pts,row.drives), axis=1)
        df_new['drive_ast_pct'] = df_new.apply(lambda row: divide(row.drive_ast,row.drives), axis=1)
        df_new['drive_tov_pct'] = df_new.apply(lambda row: divide(row.drive_tov,row.drives), axis=1)
        df_new['drive_pf_pct'] = df_new.apply(lambda row: divide(row.drive_pf,row.drives), axis=1)
        df_new.fillna(0, inplace=True)
        df_new = df_new[columns_order]
        rows = list(list(x) for x in zip(*(df_new[x].values.tolist() for x in df_new.columns)))
        conn = connect_to_db()
        with conn.cursor() as cur:
            args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
            args_str = ', '.join(args)
            cur.execute(
                "INSERT INTO player_report_drives values " +
                args_str +
                "ON CONFLICT (player_id) DO UPDATE SET (player_id, gp, w, l, min, drives, drive_fgm, drive_fga, drive_fg_pct, drive_ftm, drive_fta, drive_ft_pct, drive_pts, drive_pts_pct, drive_passes, drive_passes_pct, drive_ast, drive_ast_pct, drive_tov, drive_tov_pct, drive_pf, drive_pf_pct) = (EXCLUDED.player_id, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.drives, EXCLUDED.drive_fgm, EXCLUDED.drive_fga, EXCLUDED.drive_fg_pct, EXCLUDED.drive_ftm, EXCLUDED.drive_fta, EXCLUDED.drive_ft_pct, EXCLUDED.drive_pts, EXCLUDED.drive_pts_pct, EXCLUDED.drive_passes, EXCLUDED.drive_passes_pct, EXCLUDED.drive_ast, EXCLUDED.drive_ast_pct, EXCLUDED.drive_tov, EXCLUDED.drive_tov_pct, EXCLUDED.drive_pf, EXCLUDED.drive_pf_pct)"
            )
            conn.commit()

    def update_player_catch_and_shoot_report(self, df):
        table_name = self.table_names[0]
        player_ids_list = df['player_id'].tolist()
        df_player_reports = self.get_player_rows(table_name, player_ids_list)
        drop_columns_list = [x for x in df_player_reports.columns.tolist() if '_pct' in x]
        df.drop(drop_columns_list, axis=1, inplace=True)
        df.drop('date', axis = 1, inplace=True)
        df.drop(['player_name', 'team_id', 'team_abbreviation'], axis=1, inplace=True)
        columns_order = df_player_reports.columns.tolist()
        df_player_reports = df_player_reports.drop(drop_columns_list, axis=1)
        df.fillna(0, inplace=True)
        df_player_reports.fillna(0, inplace=True)
        cols = df.columns.drop('min')
        df[cols] = df[cols].astype(int)
        df['min'] = df['min'].astype(float)
        cols = df_player_reports.columns.drop('min')
        df_player_reports[cols] = df_player_reports[cols].astype(int)
        df_player_reports['min'] = df_player_reports['min'].astype(float)
        grouped_df = pd.concat([df, df_player_reports], axis=0).groupby('player_id')
        df_new = pd.concat([grouped_df['gp','w', 'l', 'catch_shoot_fgm', 'catch_shoot_fga', 'catch_shoot_pts','catch_shoot_fg3m', 'catch_shoot_fg3a'].sum(),  grouped_df['min'].sum()], axis=1).reset_index()
        df_new['catch_shoot_fg_pct'] = df_new.apply(lambda row: divide(row.catch_shoot_fgm,row.catch_shoot_fga), axis=1)
        df_new['catch_shoot_fg3_pct'] = df_new.apply(lambda row: divide(row.catch_shoot_fg3m,row.catch_shoot_fg3a), axis=1)
        df_new['catch_shoot_efg_pct'] = df_new.apply(lambda row: divide((row.catch_shoot_fg3m*0.5 + row.catch_shoot_fgm),row.catch_shoot_fga), axis=1)
        df_new.fillna(0, inplace=True)
        df_new = df_new[columns_order]
        rows = list(list(x) for x in zip(*(df_new[x].values.tolist() for x in df_new.columns)))
        conn = connect_to_db()
        with conn.cursor() as cur:
            args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
            args_str = ', '.join(args)
            cur.execute(
                "INSERT INTO player_report_catch_and_shoot values " +
                args_str +
                "ON CONFLICT (player_id) DO UPDATE SET (player_id, gp, w, l, min, catch_shoot_fgm, catch_shoot_fga, catch_shoot_fg_pct, catch_shoot_pts, catch_shoot_fg3m, catch_shoot_fg3a, catch_shoot_fg3_pct, catch_shoot_efg_pct) = (EXCLUDED.player_id, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.catch_shoot_fgm, EXCLUDED.catch_shoot_fga, EXCLUDED.catch_shoot_fg_pct, EXCLUDED.catch_shoot_pts, EXCLUDED.catch_shoot_fg3m, EXCLUDED.catch_shoot_fg3a, EXCLUDED.catch_shoot_fg3_pct, EXCLUDED.catch_shoot_efg_pct)"
            )
            conn.commit()

    def update_player_pull_up_shooting_report(self, df):
        table_name = self.table_names[2]
        player_ids_list = df['player_id'].tolist()
        df_player_reports = self.get_player_rows(table_name, player_ids_list)
        drop_columns_list = [x for x in df_player_reports.columns.tolist() if '_pct' in x]
        df.drop(drop_columns_list, axis=1, inplace=True)
        df.drop('date', axis = 1, inplace=True)
        df.drop(['player_name', 'team_id', 'team_abbreviation'], axis=1, inplace=True)
        columns_order = df_player_reports.columns.tolist()
        df_player_reports = df_player_reports.drop(drop_columns_list, axis=1)
        df.fillna(0, inplace=True)
        df_player_reports.fillna(0, inplace=True)
        cols = df.columns.drop('min')
        df[cols] = df[cols].astype(int)
        df['min'] = df['min'].astype(float)
        cols = df_player_reports.columns.drop('min')
        df_player_reports[cols] = df_player_reports[cols].astype(int)
        df_player_reports['min'] = df_player_reports['min'].astype(float)
        grouped_df = pd.concat([df, df_player_reports], axis=0).groupby('player_id')
        df_new = pd.concat([grouped_df['gp','w', 'l', 'catch_shoot_fgm', 'catch_shoot_fga', 'catch_shoot_pts','catch_shoot_fg3m', 'catch_shoot_fg3a'].sum(),  grouped_df['min'].sum()], axis=1).reset_index()
        df_new['catch_shoot_fg_pct'] = df_new.apply(lambda row: divide(row.catch_shoot_fgm,row.catch_shoot_fga), axis=1)
        df_new['catch_shoot_fg3_pct'] = df_new.apply(lambda row: divide(row.catch_shoot_fg3m,row.catch_shoot_fg3a), axis=1)
        df_new['catch_shoot_efg_pct'] = df_new.apply(lambda row: divide((row.catch_shoot_fg3m*0.5 + row.catch_shoot_fgm),row.catch_shoot_fga), axis=1)
        df_new.fillna(0, inplace=True)
        df_new = df_new[columns_order]
        rows = list(list(x) for x in zip(*(df_new[x].values.tolist() for x in df_new.columns)))
        conn = connect_to_db()
        with conn.cursor() as cur:
            args = [cur.mogrify('(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', x).decode('utf-8') for x in rows]
            args_str = ', '.join(args)
            cur.execute(
                "INSERT INTO player_report_catch_and_shoot values " +
                args_str +
                "ON CONFLICT (player_id) DO UPDATE SET (player_id, gp, w, l, min, catch_shoot_fgm, catch_shoot_fga, catch_shoot_fg_pct, catch_shoot_pts, catch_shoot_fg3m, catch_shoot_fg3a, catch_shoot_fg3_pct, catch_shoot_efg_pct) = (EXCLUDED.player_id, EXCLUDED.gp, EXCLUDED.w, EXCLUDED.l, EXCLUDED.min, EXCLUDED.catch_shoot_fgm, EXCLUDED.catch_shoot_fga, EXCLUDED.catch_shoot_fg_pct, EXCLUDED.catch_shoot_pts, EXCLUDED.catch_shoot_fg3m, EXCLUDED.catch_shoot_fg3a, EXCLUDED.catch_shoot_fg3_pct, EXCLUDED.catch_shoot_efg_pct)"
            )
            conn.commit()

    def update_player_shots_closest_defender_report(self, df):
        pass

    def update_player_report(self, player_catch_and_shoot, player_drives, player_pull_up_shooting, player_shots_closest_defender_open, player_shots_closest_defender_tight):
        # self.update_player_drives_report(player_drives)
        # self.update_player_catch_and_shoot_report(player_catch_and_shoot)
        self.update_player_pull_up_shooting_report(player_pull_up_shooting)


    # Legit working starts from here
    # *********************************
    # <<<<< Teams >>>>>>>
    def upload_team_report(self, final_df):
        rows = list(list(x) for x in zip(*(final_df[x].values.tolist() for x in final_df.columns)))
        conn = connect_to_db()
        with conn.cursor() as cur:
            args = [cur.mogrify("(" + ', '.join(['%s'] * 119) + ")", x).decode('utf-8') for x in rows]
            args_str = ', '.join(args)
            cur.execute(
                "INSERT INTO team_report values " +
                args_str +
                "ON CONFLICT (team_id, date) DO UPDATE SET (team_id, drives_daily, drive_fgm_daily, drive_fga_daily, drive_fg_pct_daily, drive_ftm_daily, drive_fta_daily, drive_ft_pct_daily, drive_pts_daily, drive_pts_pct_daily, drive_passes_daily, drive_passes_pct_daily, drive_ast_daily, drive_ast_pct_daily, drive_tov_daily, drive_tov_pct_daily, drive_pf_daily, drive_pf_pct_daily, drives_total, drive_fgm_total, drive_fga_total, drive_fg_pct_total, drive_ftm_total, drive_fta_total, drive_ft_pct_total, drive_pts_total, drive_pts_pct_total, drive_passes_total, drive_passes_pct_total, drive_ast_total, drive_ast_pct_total, drive_tov_total, drive_tov_pct_total, drive_pf_total, drive_pf_pct_total, drives_diff, drive_fgm_diff, drive_fga_diff, drive_fg_pct_diff, drive_ftm_diff, drive_fta_diff, drive_ft_pct_diff, drive_pts_diff, drive_pts_pct_diff, drive_passes_diff, drive_passes_pct_diff, drive_ast_diff, drive_ast_pct_diff, drive_tov_diff, drive_tov_pct_diff, drive_pf_diff, drive_pf_pct_diff, catch_shoot_fgm_daily, catch_shoot_fga_daily, catch_shoot_fg_pct_daily, catch_shoot_pts_daily, catch_shoot_fg3m_daily, catch_shoot_fg3a_daily, catch_shoot_fg3_pct_daily, catch_shoot_efg_pct_daily, catch_shoot_fgm_total, catch_shoot_fga_total, catch_shoot_fg_pct_total, catch_shoot_pts_total, catch_shoot_fg3m_total, catch_shoot_fg3a_total, catch_shoot_fg3_pct_total, catch_shoot_efg_pct_total, catch_shoot_fgm_diff, catch_shoot_fga_diff, catch_shoot_fg_pct_diff, catch_shoot_pts_diff, catch_shoot_fg3m_diff, catch_shoot_fg3a_diff, catch_shoot_fg3_pct_diff, catch_shoot_efg_pct_diff, pull_up_fgm_daily, pull_up_fga_daily, pull_up_fg_pct_daily, pull_up_pts_daily, pull_up_fg3m_daily, pull_up_fg3a_daily, pull_up_fg3_pct_daily, pull_up_efg_pct_daily, pull_up_fgm_total, pull_up_fga_total, pull_up_fg_pct_total, pull_up_pts_total, pull_up_fg3m_total, pull_up_fg3a_total, pull_up_fg3_pct_total, pull_up_efg_pct_total, pull_up_fgm_diff, pull_up_fga_diff, pull_up_fg_pct_diff, pull_up_pts_diff, pull_up_fg3m_diff, pull_up_fg3a_diff, pull_up_fg3_pct_diff, pull_up_efg_pct_diff, fg3m_daily_tight, fg3a_daily_tight, fg3_pct_daily_tight, fg3m_total_tight, fg3a_total_tight, fg3_pct_total_tight, fg3m_diff_tight, fg3a_diff_tight, fg3_pct_diff_tight, fg3m_daily_open, fg3a_daily_open, fg3_pct_daily_open, fg3m_total_open, fg3a_total_open, fg3_pct_total_open, fg3m_diff_open, fg3a_diff_open, fg3_pct_diff_open, date) = (EXCLUDED.team_id, EXCLUDED.drives_daily, EXCLUDED.drive_fgm_daily, EXCLUDED.drive_fga_daily, EXCLUDED.drive_fg_pct_daily, EXCLUDED.drive_ftm_daily, EXCLUDED.drive_fta_daily, EXCLUDED.drive_ft_pct_daily, EXCLUDED.drive_pts_daily, EXCLUDED.drive_pts_pct_daily, EXCLUDED.drive_passes_daily, EXCLUDED.drive_passes_pct_daily, EXCLUDED.drive_ast_daily, EXCLUDED.drive_ast_pct_daily, EXCLUDED.drive_tov_daily, EXCLUDED.drive_tov_pct_daily, EXCLUDED.drive_pf_daily, EXCLUDED.drive_pf_pct_daily, EXCLUDED.drives_total, EXCLUDED.drive_fgm_total, EXCLUDED.drive_fga_total, EXCLUDED.drive_fg_pct_total, EXCLUDED.drive_ftm_total, EXCLUDED.drive_fta_total, EXCLUDED.drive_ft_pct_total, EXCLUDED.drive_pts_total, EXCLUDED.drive_pts_pct_total, EXCLUDED.drive_passes_total, EXCLUDED.drive_passes_pct_total, EXCLUDED.drive_ast_total, EXCLUDED.drive_ast_pct_total, EXCLUDED.drive_tov_total, EXCLUDED.drive_tov_pct_total, EXCLUDED.drive_pf_total, EXCLUDED.drive_pf_pct_total, EXCLUDED.drives_diff, EXCLUDED.drive_fgm_diff, EXCLUDED.drive_fga_diff, EXCLUDED.drive_fg_pct_diff, EXCLUDED.drive_ftm_diff, EXCLUDED.drive_fta_diff, EXCLUDED.drive_ft_pct_diff, EXCLUDED.drive_pts_diff, EXCLUDED.drive_pts_pct_diff, EXCLUDED.drive_passes_diff, EXCLUDED.drive_passes_pct_diff, EXCLUDED.drive_ast_diff, EXCLUDED.drive_ast_pct_diff, EXCLUDED.drive_tov_diff, EXCLUDED.drive_tov_pct_diff, EXCLUDED.drive_pf_diff, EXCLUDED.drive_pf_pct_diff, EXCLUDED.catch_shoot_fgm_daily, EXCLUDED.catch_shoot_fga_daily, EXCLUDED.catch_shoot_fg_pct_daily, EXCLUDED.catch_shoot_pts_daily, EXCLUDED.catch_shoot_fg3m_daily, EXCLUDED.catch_shoot_fg3a_daily, EXCLUDED.catch_shoot_fg3_pct_daily, EXCLUDED.catch_shoot_efg_pct_daily, EXCLUDED.catch_shoot_fgm_total, EXCLUDED.catch_shoot_fga_total, EXCLUDED.catch_shoot_fg_pct_total, EXCLUDED.catch_shoot_pts_total, EXCLUDED.catch_shoot_fg3m_total, EXCLUDED.catch_shoot_fg3a_total, EXCLUDED.catch_shoot_fg3_pct_total, EXCLUDED.catch_shoot_efg_pct_total, EXCLUDED.catch_shoot_fgm_diff, EXCLUDED.catch_shoot_fga_diff, EXCLUDED.catch_shoot_fg_pct_diff, EXCLUDED.catch_shoot_pts_diff, EXCLUDED.catch_shoot_fg3m_diff, EXCLUDED.catch_shoot_fg3a_diff, EXCLUDED.catch_shoot_fg3_pct_diff, EXCLUDED.catch_shoot_efg_pct_diff, EXCLUDED.pull_up_fgm_daily, EXCLUDED.pull_up_fga_daily, EXCLUDED.pull_up_fg_pct_daily, EXCLUDED.pull_up_pts_daily, EXCLUDED.pull_up_fg3m_daily, EXCLUDED.pull_up_fg3a_daily, EXCLUDED.pull_up_fg3_pct_daily, EXCLUDED.pull_up_efg_pct_daily, EXCLUDED.pull_up_fgm_total, EXCLUDED.pull_up_fga_total, EXCLUDED.pull_up_fg_pct_total, EXCLUDED.pull_up_pts_total, EXCLUDED.pull_up_fg3m_total, EXCLUDED.pull_up_fg3a_total, EXCLUDED.pull_up_fg3_pct_total, EXCLUDED.pull_up_efg_pct_total, EXCLUDED.pull_up_fgm_diff, EXCLUDED.pull_up_fga_diff, EXCLUDED.pull_up_fg_pct_diff, EXCLUDED.pull_up_pts_diff, EXCLUDED.pull_up_fg3m_diff, EXCLUDED.pull_up_fg3a_diff, EXCLUDED.pull_up_fg3_pct_diff, EXCLUDED.pull_up_efg_pct_diff, EXCLUDED.fg3m_daily_tight, EXCLUDED.fg3a_daily_tight, EXCLUDED.fg3_pct_daily_tight, EXCLUDED.fg3m_total_tight, EXCLUDED.fg3a_total_tight, EXCLUDED.fg3_pct_total_tight, EXCLUDED.fg3m_diff_tight, EXCLUDED.fg3a_diff_tight, EXCLUDED.fg3_pct_diff_tight, EXCLUDED.fg3m_daily_open, EXCLUDED.fg3a_daily_open, EXCLUDED.fg3_pct_daily_open, EXCLUDED.fg3m_total_open, EXCLUDED.fg3a_total_open, EXCLUDED.fg3_pct_total_open, EXCLUDED.fg3m_diff_open, EXCLUDED.fg3a_diff_open, EXCLUDED.fg3_pct_diff_open, EXCLUDED.date)"
            )
            conn.commit()

    # Legit Working
    def generate_diff_drives(self, team_daily_drives, team_total_drives):
        team_total_drives.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_total_drives.drop('team_id', axis = 1).columns]
        team_total_drives.columns = [str(col) + '_total' if col != 'team_id' else str(col) for col in team_total_drives.columns]
        team_daily_drives.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        team_daily_drives.columns = [str(col) + '_daily' if col != 'team_id' else str(col) for col in team_daily_drives.columns]
        merged_df = team_daily_drives.merge(team_total_drives,how='inner',on = 'team_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('team_id')
        return merged_df

    # Legit Working
    def generate_diff_catch_and_shoot(self, team_daily_catch_and_shoot, team_total_catch_and_shoot):
        team_total_catch_and_shoot.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_total_catch_and_shoot.drop('team_id', axis = 1).columns]
        team_total_catch_and_shoot.columns = [str(col) + '_total' if col != 'team_id' else str(col) for col in team_total_catch_and_shoot.columns]
        team_daily_catch_and_shoot.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        team_daily_catch_and_shoot.columns = [str(col) + '_daily' if col != 'team_id' else str(col) for col in team_daily_catch_and_shoot.columns]
        merged_df = team_daily_catch_and_shoot.merge(team_total_catch_and_shoot,how='inner',on = 'team_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('team_id')
        return merged_df

    def generate_diff_pull_up_shooting(self, team_daily_pull_up_shooting, team_total_pull_up_shooting):
        team_total_pull_up_shooting.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_total_pull_up_shooting.drop('team_id', axis = 1).columns]
        team_total_pull_up_shooting.columns = [str(col) + '_total' if col != 'team_id' else str(col) for col in team_total_pull_up_shooting.columns]
        team_daily_pull_up_shooting.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        team_daily_pull_up_shooting.columns = [str(col) + '_daily' if col != 'team_id' else str(col) for col in team_daily_pull_up_shooting.columns]
        merged_df = team_daily_pull_up_shooting.merge(team_total_pull_up_shooting,how='inner',on = 'team_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('team_id')
        return merged_df

    def generate_diff_shots_closest_defender(self, team_daily_shots_closest_defender, team_total_shots_closest_defender):
        team_daily_shots_closest_defender = team_daily_shots_closest_defender[['team_id', 'fg3m', 'fg3a', 'def_dist']]
        team_total_shots_closest_defender = team_total_shots_closest_defender[['team_id', 'fg3m', 'fg3a', 'def_dist']]
        # Tight
        team_daily_shots_closest_defender_tight = team_daily_shots_closest_defender[(team_daily_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (team_daily_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_daily_shots_closest_defender_tight['fg3_pct'] = team_daily_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_daily_shots_closest_defender_tight.columns = [str(col) + '_daily_tight' if col != 'team_id' else col for col in team_daily_shots_closest_defender_tight]
        team_total_shots_closest_defender_tight = team_total_shots_closest_defender[(team_total_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (team_total_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_total_shots_closest_defender_tight['fg3_pct'] = team_total_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_total_shots_closest_defender_tight.columns = [str(col) + '_total_tight' if col != 'team_id' else col for col in team_total_shots_closest_defender_tight]
        merged_df_tight = team_daily_shots_closest_defender_tight.merge(team_total_shots_closest_defender_tight, how='inner', on = 'team_id')
        merged_df_tight['fg3m_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3m_daily_tight - row.fg3m_total_tight, axis = 1)
        merged_df_tight['fg3a_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3a_daily_tight - row.fg3a_total_tight, axis = 1)
        merged_df_tight['fg3_pct_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3_pct_daily_tight - row.fg3_pct_total_tight, axis = 1)
        # Open
        team_daily_shots_closest_defender_open = team_daily_shots_closest_defender[(team_daily_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (team_daily_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_daily_shots_closest_defender_open['fg3_pct'] = team_daily_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_daily_shots_closest_defender_open.columns = [str(col) + '_daily_open' if col != 'team_id' else col for col in team_daily_shots_closest_defender_open]
        team_total_shots_closest_defender_open = team_total_shots_closest_defender[(team_total_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (team_total_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_total_shots_closest_defender_open['fg3_pct'] = team_total_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_total_shots_closest_defender_open.columns = [str(col) + '_total_open' if col != 'team_id' else col for col in team_total_shots_closest_defender_open]
        merged_df_open = team_daily_shots_closest_defender_open.merge(team_total_shots_closest_defender_open, how='inner', on = 'team_id')
        merged_df_open['fg3m_diff_open'] = merged_df_open.apply(lambda row: row.fg3m_daily_open - row.fg3m_total_open, axis = 1)
        merged_df_open['fg3a_diff_open'] = merged_df_open.apply(lambda row: row.fg3a_daily_open - row.fg3a_total_open, axis = 1)
        merged_df_open['fg3_pct_diff_open'] = merged_df_open.apply(lambda row: row.fg3_pct_daily_open - row.fg3_pct_total_open, axis = 1)
        return merged_df_tight.merge(merged_df_open, how='inner', on='team_id')


    def generate_team_reports(self, date):
        # try:
        #     # player_catch_and_shoot = PlayerCatchAndShoot(date, date).poll()
        #     player_catch_and_shoot = None
        #     # player_drives = PlayerDrives(date, date).poll()
        #     player_drives = None
        #     player_pull_up_shooting = PlayerPullUpShooting(date, date).poll()
        #     # player_pull_up_shooting = None
        #     # player_shots_closest_defender_tight, player_shots_closest_defender_open = self.parse_player_shots(PlayerShotsClosestDefenderTeams(date).poll())
        #     player_shots_closest_defender_open = None
        #     player_shots_closest_defender_tight = None
        #     self.update_player_report(player_catch_and_shoot, player_drives, player_pull_up_shooting, player_shots_closest_defender_open, player_shots_closest_defender_tight)
        #     # self.update_team_report(player_catch_and_shoot, player_drives, player_pull_up_shooting, player_shots_closest_defender_open, player_shots_closest_defender_tight)
        # except Exception as e:
            # print(f"Error while polling: {e}")
        team_daily_drives = TeamDailyDrives(date, date).poll()
        team_total_drives = TeamTotalDrives().poll()
        team_diff_drives = self.generate_diff_drives(team_daily_drives, team_total_drives)
        team_daily_catch_and_shoot = TeamDailyCatchAndShoot(date, date).poll()
        team_total_catch_and_shoot = TeamTotalCatchAndShoot().poll()
        team_diff_catch_and_shoot = self.generate_diff_catch_and_shoot(team_daily_catch_and_shoot, team_total_catch_and_shoot)
        team_daily_pull_up_shooting = TeamDailyPullUpShooting(date, date).poll()
        team_total_pull_up_shooting = TeamTotalPullUpShooting().poll()
        team_diff_pull_up_shooting = self.generate_diff_pull_up_shooting(team_daily_pull_up_shooting, team_total_pull_up_shooting)
        team_daily_shots_closest_defender = TeamDailyShotsClosestDefender(date, date).poll()
        team_total_shots_closest_defender = TeamTotalShotsClosestDefender().poll()
        team_diff_shots_closest_defender = self.generate_diff_shots_closest_defender(team_daily_shots_closest_defender, team_total_shots_closest_defender)
        final_df = team_diff_drives.merge(team_diff_catch_and_shoot, how='inner', on='team_id').merge(team_diff_pull_up_shooting, how='inner', on='team_id').merge(team_diff_shots_closest_defender, how='inner', on='team_id')
        final_df['date'] = date
        final_df.fillna(0, inplace=True)
        self.upload_team_report(final_df)

    # <<<<<< Players >>>>>>>>>
    def upload_player_report(self, final_df):
        rows = list(list(x) for x in zip(*(final_df[x].values.tolist() for x in final_df.columns)))
        conn = connect_to_db()
        with conn.cursor() as cur:
            args = [cur.mogrify("(" + ', '.join(['%s'] * 119) + ")", x).decode('utf-8') for x in rows]
            args_str = ', '.join(args)
            cur.execute(
                "INSERT INTO player_report values " +
                args_str +
                "ON CONFLICT (player_id, date) DO UPDATE SET (player_id, drives_daily, drive_fgm_daily, drive_fga_daily, drive_fg_pct_daily, drive_ftm_daily, drive_fta_daily, drive_ft_pct_daily, drive_pts_daily, drive_pts_pct_daily, drive_passes_daily, drive_passes_pct_daily, drive_ast_daily, drive_ast_pct_daily, drive_tov_daily, drive_tov_pct_daily, drive_pf_daily, drive_pf_pct_daily, drives_total, drive_fgm_total, drive_fga_total, drive_fg_pct_total, drive_ftm_total, drive_fta_total, drive_ft_pct_total, drive_pts_total, drive_pts_pct_total, drive_passes_total, drive_passes_pct_total, drive_ast_total, drive_ast_pct_total, drive_tov_total, drive_tov_pct_total, drive_pf_total, drive_pf_pct_total, drives_diff, drive_fgm_diff, drive_fga_diff, drive_fg_pct_diff, drive_ftm_diff, drive_fta_diff, drive_ft_pct_diff, drive_pts_diff, drive_pts_pct_diff, drive_passes_diff, drive_passes_pct_diff, drive_ast_diff, drive_ast_pct_diff, drive_tov_diff, drive_tov_pct_diff, drive_pf_diff, drive_pf_pct_diff, catch_shoot_fgm_daily, catch_shoot_fga_daily, catch_shoot_fg_pct_daily, catch_shoot_pts_daily, catch_shoot_fg3m_daily, catch_shoot_fg3a_daily, catch_shoot_fg3_pct_daily, catch_shoot_efg_pct_daily, catch_shoot_fgm_total, catch_shoot_fga_total, catch_shoot_fg_pct_total, catch_shoot_pts_total, catch_shoot_fg3m_total, catch_shoot_fg3a_total, catch_shoot_fg3_pct_total, catch_shoot_efg_pct_total, catch_shoot_fgm_diff, catch_shoot_fga_diff, catch_shoot_fg_pct_diff, catch_shoot_pts_diff, catch_shoot_fg3m_diff, catch_shoot_fg3a_diff, catch_shoot_fg3_pct_diff, catch_shoot_efg_pct_diff, pull_up_fgm_daily, pull_up_fga_daily, pull_up_fg_pct_daily, pull_up_pts_daily, pull_up_fg3m_daily, pull_up_fg3a_daily, pull_up_fg3_pct_daily, pull_up_efg_pct_daily, pull_up_fgm_total, pull_up_fga_total, pull_up_fg_pct_total, pull_up_pts_total, pull_up_fg3m_total, pull_up_fg3a_total, pull_up_fg3_pct_total, pull_up_efg_pct_total, pull_up_fgm_diff, pull_up_fga_diff, pull_up_fg_pct_diff, pull_up_pts_diff, pull_up_fg3m_diff, pull_up_fg3a_diff, pull_up_fg3_pct_diff, pull_up_efg_pct_diff, fg3m_daily_tight, fg3a_daily_tight, fg3_pct_daily_tight, fg3m_total_tight, fg3a_total_tight, fg3_pct_total_tight, fg3m_diff_tight, fg3a_diff_tight, fg3_pct_diff_tight, fg3m_daily_open, fg3a_daily_open, fg3_pct_daily_open, fg3m_total_open, fg3a_total_open, fg3_pct_total_open, fg3m_diff_open, fg3a_diff_open, fg3_pct_diff_open, date) = (EXCLUDED.player_id, EXCLUDED.drives_daily, EXCLUDED.drive_fgm_daily, EXCLUDED.drive_fga_daily, EXCLUDED.drive_fg_pct_daily, EXCLUDED.drive_ftm_daily, EXCLUDED.drive_fta_daily, EXCLUDED.drive_ft_pct_daily, EXCLUDED.drive_pts_daily, EXCLUDED.drive_pts_pct_daily, EXCLUDED.drive_passes_daily, EXCLUDED.drive_passes_pct_daily, EXCLUDED.drive_ast_daily, EXCLUDED.drive_ast_pct_daily, EXCLUDED.drive_tov_daily, EXCLUDED.drive_tov_pct_daily, EXCLUDED.drive_pf_daily, EXCLUDED.drive_pf_pct_daily, EXCLUDED.drives_total, EXCLUDED.drive_fgm_total, EXCLUDED.drive_fga_total, EXCLUDED.drive_fg_pct_total, EXCLUDED.drive_ftm_total, EXCLUDED.drive_fta_total, EXCLUDED.drive_ft_pct_total, EXCLUDED.drive_pts_total, EXCLUDED.drive_pts_pct_total, EXCLUDED.drive_passes_total, EXCLUDED.drive_passes_pct_total, EXCLUDED.drive_ast_total, EXCLUDED.drive_ast_pct_total, EXCLUDED.drive_tov_total, EXCLUDED.drive_tov_pct_total, EXCLUDED.drive_pf_total, EXCLUDED.drive_pf_pct_total, EXCLUDED.drives_diff, EXCLUDED.drive_fgm_diff, EXCLUDED.drive_fga_diff, EXCLUDED.drive_fg_pct_diff, EXCLUDED.drive_ftm_diff, EXCLUDED.drive_fta_diff, EXCLUDED.drive_ft_pct_diff, EXCLUDED.drive_pts_diff, EXCLUDED.drive_pts_pct_diff, EXCLUDED.drive_passes_diff, EXCLUDED.drive_passes_pct_diff, EXCLUDED.drive_ast_diff, EXCLUDED.drive_ast_pct_diff, EXCLUDED.drive_tov_diff, EXCLUDED.drive_tov_pct_diff, EXCLUDED.drive_pf_diff, EXCLUDED.drive_pf_pct_diff, EXCLUDED.catch_shoot_fgm_daily, EXCLUDED.catch_shoot_fga_daily, EXCLUDED.catch_shoot_fg_pct_daily, EXCLUDED.catch_shoot_pts_daily, EXCLUDED.catch_shoot_fg3m_daily, EXCLUDED.catch_shoot_fg3a_daily, EXCLUDED.catch_shoot_fg3_pct_daily, EXCLUDED.catch_shoot_efg_pct_daily, EXCLUDED.catch_shoot_fgm_total, EXCLUDED.catch_shoot_fga_total, EXCLUDED.catch_shoot_fg_pct_total, EXCLUDED.catch_shoot_pts_total, EXCLUDED.catch_shoot_fg3m_total, EXCLUDED.catch_shoot_fg3a_total, EXCLUDED.catch_shoot_fg3_pct_total, EXCLUDED.catch_shoot_efg_pct_total, EXCLUDED.catch_shoot_fgm_diff, EXCLUDED.catch_shoot_fga_diff, EXCLUDED.catch_shoot_fg_pct_diff, EXCLUDED.catch_shoot_pts_diff, EXCLUDED.catch_shoot_fg3m_diff, EXCLUDED.catch_shoot_fg3a_diff, EXCLUDED.catch_shoot_fg3_pct_diff, EXCLUDED.catch_shoot_efg_pct_diff, EXCLUDED.pull_up_fgm_daily, EXCLUDED.pull_up_fga_daily, EXCLUDED.pull_up_fg_pct_daily, EXCLUDED.pull_up_pts_daily, EXCLUDED.pull_up_fg3m_daily, EXCLUDED.pull_up_fg3a_daily, EXCLUDED.pull_up_fg3_pct_daily, EXCLUDED.pull_up_efg_pct_daily, EXCLUDED.pull_up_fgm_total, EXCLUDED.pull_up_fga_total, EXCLUDED.pull_up_fg_pct_total, EXCLUDED.pull_up_pts_total, EXCLUDED.pull_up_fg3m_total, EXCLUDED.pull_up_fg3a_total, EXCLUDED.pull_up_fg3_pct_total, EXCLUDED.pull_up_efg_pct_total, EXCLUDED.pull_up_fgm_diff, EXCLUDED.pull_up_fga_diff, EXCLUDED.pull_up_fg_pct_diff, EXCLUDED.pull_up_pts_diff, EXCLUDED.pull_up_fg3m_diff, EXCLUDED.pull_up_fg3a_diff, EXCLUDED.pull_up_fg3_pct_diff, EXCLUDED.pull_up_efg_pct_diff, EXCLUDED.fg3m_daily_tight, EXCLUDED.fg3a_daily_tight, EXCLUDED.fg3_pct_daily_tight, EXCLUDED.fg3m_total_tight, EXCLUDED.fg3a_total_tight, EXCLUDED.fg3_pct_total_tight, EXCLUDED.fg3m_diff_tight, EXCLUDED.fg3a_diff_tight, EXCLUDED.fg3_pct_diff_tight, EXCLUDED.fg3m_daily_open, EXCLUDED.fg3a_daily_open, EXCLUDED.fg3_pct_daily_open, EXCLUDED.fg3m_total_open, EXCLUDED.fg3a_total_open, EXCLUDED.fg3_pct_total_open, EXCLUDED.fg3m_diff_open, EXCLUDED.fg3a_diff_open, EXCLUDED.fg3_pct_diff_open, EXCLUDED.date)"
            )
            conn.commit()
    # Done
    def generate_diff_player_drives(self, team_daily_drives, team_total_drives):
        team_total_drives.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_total_drives.drop('player_id', axis = 1).columns]
        team_total_drives.columns = [str(col) + '_total' if col != 'player_id' else str(col) for col in team_total_drives.columns]
        team_daily_drives.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        team_daily_drives.columns = [str(col) + '_daily' if col != 'player_id' else str(col) for col in team_daily_drives.columns]
        merged_df = team_daily_drives.merge(team_total_drives,how='inner',on = 'player_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('player_id')
        return merged_df

    def generate_diff_player_catch_and_shoot(self, team_daily_catch_and_shoot, team_total_catch_and_shoot):
        team_total_catch_and_shoot.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_total_catch_and_shoot.drop('player_id', axis = 1).columns]
        team_total_catch_and_shoot.columns = [str(col) + '_total' if col != 'player_id' else str(col) for col in team_total_catch_and_shoot.columns]
        team_daily_catch_and_shoot.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        team_daily_catch_and_shoot.columns = [str(col) + '_daily' if col != 'player_id' else str(col) for col in team_daily_catch_and_shoot.columns]
        merged_df = team_daily_catch_and_shoot.merge(team_total_catch_and_shoot,how='inner',on = 'player_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('player_id')
        return merged_df

    def generate_diff_player_pull_up_shooting(self, team_daily_pull_up_shooting, team_total_pull_up_shooting):
        team_total_pull_up_shooting.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_total_pull_up_shooting.drop('player_id', axis = 1).columns]
        team_total_pull_up_shooting.columns = [str(col) + '_total' if col != 'player_id' else str(col) for col in team_total_pull_up_shooting.columns]
        team_daily_pull_up_shooting.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        team_daily_pull_up_shooting.columns = [str(col) + '_daily' if col != 'player_id' else str(col) for col in team_daily_pull_up_shooting.columns]
        merged_df = team_daily_pull_up_shooting.merge(team_total_pull_up_shooting,how='inner',on = 'player_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('player_id')
        return merged_df

    def generate_diff_player_shots_closest_defender(self, team_daily_shots_closest_defender, team_total_shots_closest_defender):
        team_daily_shots_closest_defender = team_daily_shots_closest_defender[['player_id', 'fg3m', 'fg3a', 'def_dist']]
        team_total_shots_closest_defender = team_total_shots_closest_defender[['player_id', 'fg3m', 'fg3a', 'def_dist']]
        # Tight
        team_daily_shots_closest_defender_tight = team_daily_shots_closest_defender[(team_daily_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (team_daily_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        team_daily_shots_closest_defender_tight['fg3_pct'] = team_daily_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_daily_shots_closest_defender_tight.columns = [str(col) + '_daily_tight' if col != 'player_id' else col for col in team_daily_shots_closest_defender_tight]
        team_total_shots_closest_defender_tight = team_total_shots_closest_defender[(team_total_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (team_total_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        team_total_shots_closest_defender_tight['fg3_pct'] = team_total_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_total_shots_closest_defender_tight.columns = [str(col) + '_total_tight' if col != 'player_id' else col for col in team_total_shots_closest_defender_tight]
        merged_df_tight = team_daily_shots_closest_defender_tight.merge(team_total_shots_closest_defender_tight, how='inner', on = 'player_id')
        merged_df_tight['fg3m_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3m_daily_tight - row.fg3m_total_tight, axis = 1)
        merged_df_tight['fg3a_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3a_daily_tight - row.fg3a_total_tight, axis = 1)
        merged_df_tight['fg3_pct_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3_pct_daily_tight - row.fg3_pct_total_tight, axis = 1)
        # Open
        team_daily_shots_closest_defender_open = team_daily_shots_closest_defender[(team_daily_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (team_daily_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        team_daily_shots_closest_defender_open['fg3_pct'] = team_daily_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_daily_shots_closest_defender_open.columns = [str(col) + '_daily_open' if col != 'player_id' else col for col in team_daily_shots_closest_defender_open]
        team_total_shots_closest_defender_open = team_total_shots_closest_defender[(team_total_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (team_total_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        team_total_shots_closest_defender_open['fg3_pct'] = team_total_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_total_shots_closest_defender_open.columns = [str(col) + '_total_open' if col != 'player_id' else col for col in team_total_shots_closest_defender_open]
        merged_df_open = team_daily_shots_closest_defender_open.merge(team_total_shots_closest_defender_open, how='inner', on = 'player_id')
        merged_df_open['fg3m_diff_open'] = merged_df_open.apply(lambda row: row.fg3m_daily_open - row.fg3m_total_open, axis = 1)
        merged_df_open['fg3a_diff_open'] = merged_df_open.apply(lambda row: row.fg3a_daily_open - row.fg3a_total_open, axis = 1)
        merged_df_open['fg3_pct_diff_open'] = merged_df_open.apply(lambda row: row.fg3_pct_daily_open - row.fg3_pct_total_open, axis = 1)
        return merged_df_tight.merge(merged_df_open, how='inner', on='player_id')

    def generate_player_reports(self, date):
        player_daily_drives = PlayerDailyDrives(date, date).poll()
        player_total_drives = PlayerTotalDrives().poll()
        player_diff_drives = self.generate_diff_player_drives(player_daily_drives, player_total_drives)
        player_daily_catch_and_shoot = PlayerDailyCatchAndShoot(date, date).poll()
        player_total_catch_and_shoot = PlayerTotalCatchAndShoot().poll()
        player_diff_catch_and_shoot = self.generate_diff_player_catch_and_shoot(player_daily_catch_and_shoot, player_total_catch_and_shoot)
        player_daily_pull_up_shooting = PlayerDailyPullUpShooting(date, date).poll()
        player_total_pull_up_shooting = PlayerTotalPullUpShooting().poll()
        player_diff_pull_up_shooting = self.generate_diff_player_pull_up_shooting(player_daily_pull_up_shooting, player_total_pull_up_shooting)
        player_daily_shots_closest_defender = PlayerDailyShotsClosestDefender(date, date).poll()
        player_total_shots_closest_defender = PlayerTotalShotsClosestDefender().poll()
        player_diff_shots_closest_defender = self.generate_diff_player_shots_closest_defender(player_daily_shots_closest_defender, player_total_shots_closest_defender)
        final_df = player_diff_drives.merge(player_diff_catch_and_shoot, how='inner', on='player_id').merge(player_diff_pull_up_shooting, how='inner', on='player_id').merge(player_diff_shots_closest_defender, how='left', on='player_id')
        final_df['date'] = date
        final_df.fillna(0, inplace=True)
        self.upload_player_report(final_df)

if __name__ == '__main__':
    # Report().generate_reports("01/20/2021")
    # Report().generate_team_reports("01/21/2021")
    # Report().generate_reports("01/22/2021")
    Report().generate_player_reports("01/20/2021")