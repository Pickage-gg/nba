from nba_api.stats.endpoints import playergamelog, leaguegamefinder, playbyplayv3
from nba_api.stats.static import players, teams
import psycopg2
from sqlalchemy import create_engine
from config import load_config

class get_Gamelogs:
    '''
    This class retrieves and processes game log data for players and teams using the nba_api module.
    '''
    def __init__(self):
        self.player_dict = players.get_active_players()
        self.id_player_pairs = dict([(player['id'], player['full_name']) for player in self.player_dict])
        self.season = 2023

    def format_season(self, season_id):
        start_year = int(str(season_id)[-4:])
        end_year = start_year + 1
        return f"{start_year}-{str(end_year)[-2:]}"

    def process_matchup(self, df, index):
        df.insert(index, 'TEAM', df['MATCHUP'].str[:3])
        df.insert(index+1, 'OPPONENT', df['MATCHUP'].str[-3:])
        df.insert(index+2, 'LOCATION', df['MATCHUP'].apply(lambda x: 'Away' if '@' in x else 'Home'))
        return df

    def player_gamelog(self, playerID):
        player_df = playergamelog.PlayerGameLog(playerID, season=self.season).get_data_frames()[0]
        player_df.insert(1, 'Season', player_df['SEASON_ID'].apply(self.format_season))
        player_df.insert(3, 'Player_Name', player_df['Player_ID'].map(self.id_player_pairs))
        player_df = self.process_matchup(player_df, 7)
        return player_df

    def team_gamelog(self, teamID):
        team_df = leaguegamefinder.LeagueGameFinder(team_id_nullable=teamID).get_data_frames()[0]
        team_df.insert(1, 'Season', team_df['SEASON_ID'].apply(self.format_season))
        team_df = self.process_matchup(team_df, 8)
        return team_df


class PlaybyPlayTables:
    '''
    This class handles the creation of the Play-by-Play tables in a PostgreSQL database.
    '''
    def __init__(self):
        self.db_params = load_config()
        self.engine = create_engine(f"postgresql://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}")

    def drop_table(self, table_name):
        conn = psycopg2.connect(**self.db_params)
        cursor = conn.cursor()
        sql = f'DROP TABLE IF EXISTS "{table_name}"'
        cursor.execute(sql)
        print(f"Table '{table_name}' dropped!")
        conn.commit()
        conn.close()

    def get_gameID(self, season):
        gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable="00")
        games = gamefinder.get_data_frames()[0]
        game_ids = list(set(games['GAME_ID'].tolist()))
        return game_ids

    def create_PbP_table(self, season_gameIDs):
        columns_to_drop = ['actionNumber', 'playerNameI', 'xLegacy', 'yLegacy', 'videoAvailable']
        count = 0
        for gameID in season_gameIDs:
            game_data = playbyplayv3.PlayByPlayV3(gameID).play_by_play.get_data_frame()
            filtered_data = game_data.drop(columns=columns_to_drop)

            if not filtered_data.empty:
                filtered_data.to_sql('PbP_table', self.engine, if_exists='append', index=False)
                count += 1
                print(f"{count}: {gameID} data inserted")
            else:
                print(f"{count}: {gameID} data empty")


class GamelogTables:
    '''
    This class handles the creation of both the team and player gamelog tables in a PostgreSQL database.
    '''
    def __init__(self):
        self.gamelog = get_Gamelogs()
        self.db_params = load_config()
        self.engine = create_engine(f"postgresql://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}")

    def drop_table(self, table_name):
        conn = psycopg2.connect(**self.db_params)
        cursor = conn.cursor()
        sql = f'DROP TABLE IF EXISTS "{table_name}"'
        cursor.execute(sql)
        print(f"Table '{table_name}' dropped!")
        conn.commit()
        conn.close()

    def get_teamIDs(self):
        team_dict = teams.get_teams()
        team_ids = [team['id'] for team in team_dict]
        return team_ids

    def get_playerIDs(self):
        player_dict = players.get_active_players()
        player_ids = [player['id'] for player in player_dict]
        return player_ids

    def create_tmLogs_table(self, teamIDs):
        count = 0
        for teamID in teamIDs:
            team_df = self.gamelog.team_gamelog(teamID)
            if not team_df.empty:
                team_df.to_sql('tmLogs_table', self.engine, if_exists='append', index=False)
                count += 1
                print(f"{count}: {teamID} data inserted")
            else:
                print(f"{count}: {teamID} data empty")

    def create_playlogs_table(self, playerIDs):
        count = 0
        for playerID in playerIDs:
            player_df = self.gamelog.player_gamelog(playerID)
            if not player_df.empty:
                player_df.to_sql('playLogs_table', self.engine, if_exists='append', index=False)
                count += 1
                print(f"{count}: {playerID} data inserted")
            else:
                print(f"{count}: {playerID} data empty")


if __name__ == '__main__':
    pbp_tables = PlaybyPlayTables()
    pbp_tables.drop_table("PbP_table")
    season = "2023-24"
    season_gameIDs = pbp_tables.get_gameID(season)
    pbp_tables.create_PbP_table(season_gameIDs)

    gl_tables = GamelogTables()
    gl_tables.drop_table("tmLogs_table")
    teamIDs = gl_tables.get_teamIDs()
    gl_tables.create_tmLogs_table(teamIDs)

    gl_tables.drop_table("playLogs_table")
    playerIDs = gl_tables.get_playerIDs()
    gl_tables.create_playlogs_table(playerIDs)
