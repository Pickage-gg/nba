import psycopg2
from config import load_config
from sqlalchemy import create_engine
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
        count = 0
        for gameID in season_gameIDs[:20]:
            game_data = playbyplayv3.PlayByPlayV3(gameID).play_by_play.get_data_frame()
            filtered_data = game_data.drop(columns=columns_to_drop)

            if not filtered_data.empty:
                filtered_data.to_sql('PbP_table', engine, if_exists='append', index=False)
                count += 1
                print(f"{count}: {gameID} data inserted")
            else:
                print(f"{count}: {gameID} data empty")

    except (psycopg2.DatabaseError, Exception) as e:
        print(e)

if __name__ == '__main__':
    drop_table("PbP_table")
    season = "2023-24" 
    season_gameIDs = get_gameID(season)
    #print(season_gameIDs[:5])
    create_tables(season_gameIDs)