import pandas as pd
import datetime
from catch_and_shoot import TeamDailyCatchAndShoot, TeamTotalCatchAndShoot, PlayerDailyCatchAndShoot, PlayerTotalCatchAndShoot
from drives import TeamDailyDrives, TeamTotalDrives, PlayerDailyDrives, PlayerTotalDrives
from pull_up_shooting import TeamDailyPullUpShooting, TeamTotalPullUpShooting, PlayerDailyPullUpShooting, PlayerTotalPullUpShooting
from shots_closest_defender import TeamDailyShotsClosestDefender, TeamTotalShotsClosestDefender, PlayerDailyShotsClosestDefender, PlayerTotalShotsClosestDefender
from video_status import VideoStatus
from scores import Scores
from utils import divide, connect_to_db


class Report():

    # Function to upload the team report
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

    # Function to generate the difference between the daily and total stats for team drives
    def generate_diff_team_drives(self, team_daily_drives, team_total_drives):
        team_daily_drives.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_daily_drives.drop('team_id', axis = 1).columns]
        team_daily_drives.columns = [str(col) + '_daily' if col != 'team_id' else str(col) for col in team_daily_drives.columns]
        merged_df = team_daily_drives.merge(team_total_drives,how='inner',on = 'team_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('team_id')
        return merged_df

    # Function to generate the difference between the daily and total stats for team catch and shoot
    def generate_diff_team_catch_and_shoot(self, team_daily_catch_and_shoot, team_total_catch_and_shoot):
        team_daily_catch_and_shoot.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_daily_catch_and_shoot.drop('team_id', axis = 1).columns]
        team_daily_catch_and_shoot.columns = [str(col) + '_daily' if col != 'team_id' else str(col) for col in team_daily_catch_and_shoot.columns]
        merged_df = team_daily_catch_and_shoot.merge(team_total_catch_and_shoot,how='inner',on = 'team_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('team_id')
        return merged_df

    # Function to generate the difference between the daily and total stats for team pull up shooting
    def generate_diff_team_pull_up_shooting(self, team_daily_pull_up_shooting, team_total_pull_up_shooting):
        team_daily_pull_up_shooting.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in team_daily_pull_up_shooting.drop('team_id', axis = 1).columns]
        team_daily_pull_up_shooting.columns = [str(col) + '_daily' if col != 'team_id' else str(col) for col in team_daily_pull_up_shooting.columns]
        merged_df = team_daily_pull_up_shooting.merge(team_total_pull_up_shooting,how='inner',on = 'team_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('team_id')
        return merged_df

    # Function to generate the difference between the daily and total stats for team shots closest defender
    def generate_diff_team_shots_closest_defender(self, team_daily_shots_closest_defender, team_total_shots_closest_defender_tight, team_total_shots_closest_defender_open):
        team_daily_shots_closest_defender = team_daily_shots_closest_defender[['team_id', 'fg3m', 'fg3a', 'def_dist']]
        # Tight
        team_daily_shots_closest_defender_tight = team_daily_shots_closest_defender[(team_daily_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (team_daily_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_daily_shots_closest_defender_tight['fg3_pct'] = team_daily_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_daily_shots_closest_defender_tight.columns = [str(col) + '_daily_tight' if col != 'team_id' else col for col in team_daily_shots_closest_defender_tight]
        merged_df_tight = team_daily_shots_closest_defender_tight.merge(team_total_shots_closest_defender_tight, how='inner', on = 'team_id')
        merged_df_tight['fg3m_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3m_daily_tight - row.fg3m_total_tight, axis = 1)
        merged_df_tight['fg3a_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3a_daily_tight - row.fg3a_total_tight, axis = 1)
        merged_df_tight['fg3_pct_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3_pct_daily_tight - row.fg3_pct_total_tight, axis = 1)
        # Open
        team_daily_shots_closest_defender_open = team_daily_shots_closest_defender[(team_daily_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (team_daily_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_daily_shots_closest_defender_open['fg3_pct'] = team_daily_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_daily_shots_closest_defender_open.columns = [str(col) + '_daily_open' if col != 'team_id' else col for col in team_daily_shots_closest_defender_open]
        merged_df_open = team_daily_shots_closest_defender_open.merge(team_total_shots_closest_defender_open, how='inner', on = 'team_id')
        merged_df_open['fg3m_diff_open'] = merged_df_open.apply(lambda row: row.fg3m_daily_open - row.fg3m_total_open, axis = 1)
        merged_df_open['fg3a_diff_open'] = merged_df_open.apply(lambda row: row.fg3a_daily_open - row.fg3a_total_open, axis = 1)
        merged_df_open['fg3_pct_diff_open'] = merged_df_open.apply(lambda row: row.fg3_pct_daily_open - row.fg3_pct_total_open, axis = 1)
        return merged_df_tight.merge(merged_df_open, how='inner', on='team_id')

    # Function to generate the team report
    def generate_team_reports(self, date, season_type, team_total_drives, team_total_catch_and_shoot, team_total_pull_up_shooting, team_total_shots_closest_defender_tight, team_total_shots_closest_defender_open):
        team_daily_drives = TeamDailyDrives(date, date, season_type).poll()
        team_diff_drives = self.generate_diff_team_drives(team_daily_drives, team_total_drives)
        team_daily_catch_and_shoot = TeamDailyCatchAndShoot(date, date, season_type).poll()
        team_diff_catch_and_shoot = self.generate_diff_team_catch_and_shoot(team_daily_catch_and_shoot, team_total_catch_and_shoot)
        team_daily_pull_up_shooting = TeamDailyPullUpShooting(date, date, season_type).poll()
        team_diff_pull_up_shooting = self.generate_diff_team_pull_up_shooting(team_daily_pull_up_shooting, team_total_pull_up_shooting)
        team_daily_shots_closest_defender = TeamDailyShotsClosestDefender(date, date, season_type).poll()
        team_diff_shots_closest_defender = self.generate_diff_team_shots_closest_defender(team_daily_shots_closest_defender, team_total_shots_closest_defender_tight, team_total_shots_closest_defender_open)
        final_df = team_diff_drives.merge(team_diff_catch_and_shoot, how='inner', on='team_id').merge(team_diff_pull_up_shooting, how='inner', on='team_id').merge(team_diff_shots_closest_defender, how='inner', on='team_id')
        final_df['date'] = date
        final_df.fillna(0, inplace=True)
        self.upload_team_report(final_df)

    # Function to upload player report
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

    # Function to generate the difference between the daily and total stats for player drives
    def generate_diff_player_drives(self, player_daily_drives, player_total_drives):
        player_daily_drives.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in player_daily_drives.drop('player_id', axis = 1).columns]
        player_daily_drives.columns = [str(col) + '_daily' if col != 'player_id' else str(col) for col in player_daily_drives.columns]
        merged_df = player_daily_drives.merge(player_total_drives,how='inner',on = 'player_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('player_id')
        return merged_df

    # Function to generate the difference between the daily and total stats for player catch and shoot
    def generate_diff_player_catch_and_shoot(self, player_daily_catch_and_shoot, player_total_catch_and_shoot):
        player_daily_catch_and_shoot.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in player_daily_catch_and_shoot.drop('player_id', axis = 1).columns]
        player_daily_catch_and_shoot.columns = [str(col) + '_daily' if col != 'player_id' else str(col) for col in player_daily_catch_and_shoot.columns]
        merged_df = player_daily_catch_and_shoot.merge(player_total_catch_and_shoot,how='inner',on = 'player_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('player_id')
        return merged_df

    # Function to generate the difference between the daily and total stats for player pull up shooting
    def generate_diff_player_pull_up_shooting(self, player_daily_pull_up_shooting, player_total_pull_up_shooting):
        player_daily_pull_up_shooting.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min', 'date'], inplace=True, axis=1)
        final_col_names = [column + '_diff' for column in player_daily_pull_up_shooting.drop('player_id', axis = 1).columns]
        player_daily_pull_up_shooting.columns = [str(col) + '_daily' if col != 'player_id' else str(col) for col in player_daily_pull_up_shooting.columns]
        merged_df = player_daily_pull_up_shooting.merge(player_total_pull_up_shooting,how='inner',on = 'player_id')
        for column in final_col_names:
            column_base = column.rsplit('_', 1)[0]
            merged_df[column] = merged_df.apply(lambda row: row[column_base + '_daily'] - row[column_base + '_total'], axis = 1)
        final_col_names.append('player_id')
        return merged_df

    # Function to generate the difference between the daily and total stats for player shots closest defender
    def generate_diff_player_shots_closest_defender(self, player_daily_shots_closest_defender, player_total_shots_closest_defender_tight, player_total_shots_closest_defender_open):
        player_daily_shots_closest_defender = player_daily_shots_closest_defender[['player_id', 'fg3m', 'fg3a', 'def_dist']]
        # Tight
        player_daily_shots_closest_defender_tight = player_daily_shots_closest_defender[(player_daily_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (player_daily_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        player_daily_shots_closest_defender_tight['fg3_pct'] = player_daily_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        player_daily_shots_closest_defender_tight.columns = [str(col) + '_daily_tight' if col != 'player_id' else col for col in player_daily_shots_closest_defender_tight]
        merged_df_tight = player_daily_shots_closest_defender_tight.merge(player_total_shots_closest_defender_tight, how='inner', on = 'player_id')
        merged_df_tight['fg3m_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3m_daily_tight - row.fg3m_total_tight, axis = 1)
        merged_df_tight['fg3a_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3a_daily_tight - row.fg3a_total_tight, axis = 1)
        merged_df_tight['fg3_pct_diff_tight'] = merged_df_tight.apply(lambda row: row.fg3_pct_daily_tight - row.fg3_pct_total_tight, axis = 1)
        # Open
        player_daily_shots_closest_defender_open = player_daily_shots_closest_defender[(player_daily_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (player_daily_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        player_daily_shots_closest_defender_open['fg3_pct'] = player_daily_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        player_daily_shots_closest_defender_open.columns = [str(col) + '_daily_open' if col != 'player_id' else col for col in player_daily_shots_closest_defender_open]
        merged_df_open = player_daily_shots_closest_defender_open.merge(player_total_shots_closest_defender_open, how='inner', on = 'player_id')
        merged_df_open['fg3m_diff_open'] = merged_df_open.apply(lambda row: row.fg3m_daily_open - row.fg3m_total_open, axis = 1)
        merged_df_open['fg3a_diff_open'] = merged_df_open.apply(lambda row: row.fg3a_daily_open - row.fg3a_total_open, axis = 1)
        merged_df_open['fg3_pct_diff_open'] = merged_df_open.apply(lambda row: row.fg3_pct_daily_open - row.fg3_pct_total_open, axis = 1)
        return merged_df_tight.merge(merged_df_open, how='inner', on='player_id')

    # Function to generate the player report
    def generate_player_reports(self, date, season_type, player_total_drives, player_total_catch_and_shoot, player_total_pull_up_shooting, player_total_shots_closest_defender_tight, player_total_shots_closest_defender_open):
        player_daily_drives = PlayerDailyDrives(date, date, season_type).poll()
        player_diff_drives = self.generate_diff_player_drives(player_daily_drives, player_total_drives)
        player_daily_catch_and_shoot = PlayerDailyCatchAndShoot(date, date, season_type).poll()
        player_diff_catch_and_shoot = self.generate_diff_player_catch_and_shoot(player_daily_catch_and_shoot, player_total_catch_and_shoot)
        player_daily_pull_up_shooting = PlayerDailyPullUpShooting(date, date, season_type).poll()
        player_diff_pull_up_shooting = self.generate_diff_player_pull_up_shooting(player_daily_pull_up_shooting, player_total_pull_up_shooting)
        player_daily_shots_closest_defender = PlayerDailyShotsClosestDefender(date, date, season_type).poll()
        player_diff_shots_closest_defender = self.generate_diff_player_shots_closest_defender(player_daily_shots_closest_defender, player_total_shots_closest_defender_tight, player_total_shots_closest_defender_open)
        final_df = player_diff_drives.merge(player_diff_catch_and_shoot, how='inner', on='player_id').merge(player_diff_pull_up_shooting, how='inner', on='player_id').merge(player_diff_shots_closest_defender, how='left', on='player_id')
        final_df['date'] = date
        final_df.fillna(0, inplace=True)
        self.upload_player_report(final_df)

    # Function to generate player and team report
    def generate_report(self, date, season_type, player_total_drives, player_total_catch_and_shoot, player_total_pull_up_shooting, player_total_shots_closest_defender_tight, player_total_shots_closest_defender_open, team_total_drives, team_total_catch_and_shoot, team_total_pull_up_shooting, team_total_shots_closest_defender_tight, team_total_shots_closest_defender_open):
        games = VideoStatus(date).poll()
        if games:
            self.generate_team_reports(date, season_type, team_total_drives, team_total_catch_and_shoot, team_total_pull_up_shooting, team_total_shots_closest_defender_tight, team_total_shots_closest_defender_open)
            self.generate_player_reports(date, season_type, player_total_drives, player_total_catch_and_shoot, player_total_pull_up_shooting, player_total_shots_closest_defender_tight, player_total_shots_closest_defender_open)
        else:
            print(f"No games on date {date}")

    def generate_reports(self, start_date, end_date):
        player_total_drives = PlayerTotalDrives().poll()
        player_total_drives.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        player_total_drives.columns = [str(col) + '_total' if col != 'player_id' else str(col) for col in player_total_drives.columns]
        player_total_catch_and_shoot = PlayerTotalCatchAndShoot().poll()
        player_total_catch_and_shoot.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        player_total_catch_and_shoot.columns = [str(col) + '_total' if col != 'player_id' else str(col) for col in player_total_catch_and_shoot.columns]
        player_total_pull_up_shooting = PlayerTotalPullUpShooting().poll()
        player_total_pull_up_shooting.drop(['player_name', 'team_id', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        player_total_pull_up_shooting.columns = [str(col) + '_total' if col != 'player_id' else str(col) for col in player_total_pull_up_shooting.columns]
        player_total_shots_closest_defender = PlayerTotalShotsClosestDefender().poll()
        player_total_shots_closest_defender = player_total_shots_closest_defender[['player_id', 'fg3m', 'fg3a', 'def_dist']]
        player_total_shots_closest_defender_tight = player_total_shots_closest_defender[(player_total_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (player_total_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        player_total_shots_closest_defender_tight['fg3_pct'] = player_total_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        player_total_shots_closest_defender_tight.columns = [str(col) + '_total_tight' if col != 'player_id' else col for col in player_total_shots_closest_defender_tight]
        player_total_shots_closest_defender_open = player_total_shots_closest_defender[(player_total_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (player_total_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['player_id']).sum().reset_index()
        player_total_shots_closest_defender_open['fg3_pct'] = player_total_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        player_total_shots_closest_defender_open.columns = [str(col) + '_total_open' if col != 'player_id' else col for col in player_total_shots_closest_defender_open]
        team_total_drives = TeamTotalDrives().poll()
        team_total_drives.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        team_total_drives.columns = [str(col) + '_total' if col != 'team_id' else str(col) for col in team_total_drives.columns]
        team_total_catch_and_shoot = TeamTotalCatchAndShoot().poll()
        team_total_catch_and_shoot.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        team_total_catch_and_shoot.columns = [str(col) + '_total' if col != 'team_id' else str(col) for col in team_total_catch_and_shoot.columns]
        team_total_pull_up_shooting = TeamTotalPullUpShooting().poll()
        team_total_pull_up_shooting.drop(['team_name', 'team_abbreviation', 'gp', 'w', 'l', 'min'], inplace=True, axis=1)
        team_total_pull_up_shooting.columns = [str(col) + '_total' if col != 'team_id' else str(col) for col in team_total_pull_up_shooting.columns]
        team_total_shots_closest_defender = TeamTotalShotsClosestDefender().poll()
        team_total_shots_closest_defender = team_total_shots_closest_defender[['team_id', 'fg3m', 'fg3a', 'def_dist']]
        team_total_shots_closest_defender_tight = team_total_shots_closest_defender[(team_total_shots_closest_defender['def_dist'] == '0-2 Feet - Very Tight') | (team_total_shots_closest_defender['def_dist'] == '2-4 Feet - Tight')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_total_shots_closest_defender_tight['fg3_pct'] = team_total_shots_closest_defender_tight.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_total_shots_closest_defender_tight.columns = [str(col) + '_total_tight' if col != 'team_id' else col for col in team_total_shots_closest_defender_tight]
        team_total_shots_closest_defender_open = team_total_shots_closest_defender[(team_total_shots_closest_defender['def_dist'] == '4-6 Feet - Open') | (team_total_shots_closest_defender['def_dist'] == '6+ Feet - Wide Open')].drop('def_dist', axis=1).groupby(['team_id']).sum().reset_index()
        team_total_shots_closest_defender_open['fg3_pct'] = team_total_shots_closest_defender_open.apply(lambda row: divide(row.fg3m,row.fg3a), axis = 1)
        team_total_shots_closest_defender_open.columns = [str(col) + '_total_open' if col != 'team_id' else col for col in team_total_shots_closest_defender_open]
        while start_date <= end_date:
            if start_date == datetime.date(2020, 12, 31) or start_date == datetime.date(2021, 3, 7):
                start_date = start_date + datetime.timedelta(days=1)
                continue
            season_type = 'Regular Season'
            if datetime.date(2021, 5, 18) <= start_date <= datetime.date(2021, 5, 21):
                season_type = 'PlayIn'
            if start_date >= datetime.date(2021, 5, 22):
                season_type = 'Playoffs'
            self.generate_report(start_date, season_type,  player_total_drives, player_total_catch_and_shoot, player_total_pull_up_shooting, player_total_shots_closest_defender_tight, player_total_shots_closest_defender_open, team_total_drives, team_total_catch_and_shoot, team_total_pull_up_shooting, team_total_shots_closest_defender_tight, team_total_shots_closest_defender_open)
            Scores(start_date).poll()
            start_date = start_date + datetime.timedelta(days=1)

if __name__ == '__main__':
    # Report().generate_reports("01/20/2021")
    # Report().generate_reports("01/21/2021")
    # Report().generate_reports("01/22/2021")
    Report().generate_reports("12/24/2020")