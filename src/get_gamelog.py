"""
Using the nba_api module, get player gamelogs
"""

import datetime
import pandas as pd
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
from nba_api.stats.endpoints import leaguegamefinder


def format_season(season_id):
    """
    given a seasonID, format into "XXXX-XX"
    """
    start_year = int(str(season_id)[-4:])
    end_year = start_year + 1
    return f"{start_year}-{str(end_year)[-2:]}"

def process_matchup(df, index):
    """
    Use the MATCHUP column to extract TEAM, OPPONENT, and LOCATION
    """
    df.insert(index, 'TEAM', df['MATCHUP'].str[:3])
    df.insert(index+1, 'OPPONENT', df['MATCHUP'].str[-3:])
    df.insert(index+2, 'LOCATION', df['MATCHUP'].apply(lambda x: 'Away' if '@' in x else 'Home'))

    return df

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
    player_df = process_matchup(player_df, 7)

    return player_df

def team_gamelog(teamID):
    """
    given a teamID gather the gamelogs of that team and return a pandas df
    """
    team_df = leaguegamefinder.LeagueGameFinder(team_id_nullable=teamID).get_data_frames()[0]
    team_df.insert(1, 'Season', team_df['SEASON_ID'].apply(format_season))
    team_df = process_matchup(team_df, 8)

    return team_df