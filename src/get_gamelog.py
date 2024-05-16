"""
Using the nba_api module, get player gamelogs
"""

import datetime
import pandas as pd
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players

def format_season(season_id):
    """
    given a seasonID, format into "XXXX-XX"
    """
    start_year = int(str(season_id)[-4:])
    end_year = start_year + 1
    return f"{start_year}-{str(end_year)[-2:]}"

def player_gamelog(playerID):
    """
    given a playerID gather the gamelogs of that player from the last two years and return a pandas df
    """
    current_year = datetime.datetime.now().year
    seasons_range = range(2022, current_year + 1)
    player_dict = players.get_active_players()
    id_player_pairs = dict([(player['id'], player['full_name']) for player in player_dict])

    player_gamelog = [playergamelog.PlayerGameLog(playerID, season=season).get_data_frames()[0] for season in seasons_range]
    player_df = pd.concat([df for df in player_gamelog if not df.empty])

    player_df.insert(1, 'Season', player_df['SEASON_ID'].apply(format_season))
    player_df.insert(3, 'Player_Name', player_df['Player_ID'].map(id_player_pairs))
    player_df.insert(7, 'TEAM', player_df['MATCHUP'].str[:3])
    player_df.insert(8, 'OPPONENT', player_df['MATCHUP'].str[-3:])
    player_df.insert(9, 'LOCATION', player_df['MATCHUP'].apply(lambda x: 'Away' if '@' in x else 'Home'))

    return player_df