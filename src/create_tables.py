import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from config import load_config
import get_gamelog

def create_tables(playerID):
    """ 
    Convert pandas df to tables in the PostgreSQL database
    """
    try:
        config = load_config()
        engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        # Fetch player game log and store it in the database
        player_df = get_gamelog.player_gamelog(playerID)
        player_df.to_sql('lebron_table', engine, if_exists='replace', index=False)
    except (psycopg2.DatabaseError, Exception) as e:
        print(e)

if __name__ == '__main__':
    create_tables()