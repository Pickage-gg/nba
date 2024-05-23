import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from config import load_config
import get_gamelog
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv3

def drop_table(table_name):
    db_params = load_config()
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    sql = f'DROP TABLE IF EXISTS "{table_name}"'
    cursor.execute(sql)
    print(f"Table '{table_name}' dropped!")
    conn.commit()
    conn.close()

def get_gameID(season):
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable="00")
    games = gamefinder.get_data_frames()[0]
    game_ids = list(set(games['GAME_ID'].tolist()))
    return game_ids

def create_tables(season_gameIDs):
    """ 
    Convert pandas df to tables in the PostgreSQL database
    """
    try:
        config = load_config()
        engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        
        columns_to_drop = ['actionNumber', 'playerNameI', 'xLegacy', 'yLegacy', 'videoAvailable']
        for gameID in season_gameIDs[:2]:
            game_data = playbyplayv3.PlayByPlayV3(gameID).play_by_play.get_data_frame()
            filtered_data = game_data.drop(columns=columns_to_drop)
            filtered_data.to_sql('PbP_table', engine, if_exists='append', index=False)

    except (psycopg2.DatabaseError, Exception) as e:
        print(e)

if __name__ == '__main__':
    season = "2023-24" 
    season_gameIDs = get_gameID(season)
    create_tables(season_gameIDs)