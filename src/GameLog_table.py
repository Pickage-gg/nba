import psycopg2
import get_gamelog
from config import load_config
from sqlalchemy import create_engine
from nba_api.stats.static import teams
from nba_api.stats.static import players

def drop_table(table_name):
    db_params = load_config()
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    sql = f'DROP TABLE IF EXISTS "{table_name}"'
    cursor.execute(sql)
    print(f"Table '{table_name}' dropped!")
    conn.commit()
    conn.close()

def get_teamIDs():
    team_dict = teams.get_teams()
    id_team_pairs = [(team['id'], team['full_name']) for team in team_dict]
    team_ids = [pair[0] for pair in id_team_pairs]

    return team_ids

def get_playerIDs():
    player_dict = players.get_active_players()
    id_player_pairs = dict([(player['id'], player['full_name']) for player in player_dict])
    player_ids = [pair for pair in id_player_pairs]

    return player_ids

def create_tmLogs_table(teamIDs):
    """ 
    Convert pandas df to tables in the PostgreSQL database
    """
    try:
        config = load_config()
        engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        
        count = 0
        for teamID in teamIDs:
            team_df = get_gamelog.team_gamelog(teamID)

            if not team_df.empty:
                team_df.to_sql('tmLogs_table', engine, if_exists='append', index=False)
                count += 1
                print(f"{count}: {teamID} data inserted")
            else:
                print(f"{count}: {teamID} data empty")

    except (psycopg2.DatabaseError, Exception) as e:
        print(e)

def create_playlogs_table(playerIDs):
    """ 
    Convert pandas df to tables in the PostgreSQL database
    """
    try:
        config = load_config()
        engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        
        count = 0
        for playerID in playerIDs:
            player_df = get_gamelog.player_gamelog(playerID)

            if not player_df.empty:
                player_df.to_sql('playLogs_table', engine, if_exists='append', index=False)
                count += 1
                print(f"{count}: {playerID} data inserted")
            else:
                print(f"{count}: {playerID} data empty")

    except (psycopg2.DatabaseError, Exception) as e:
        print(e)

if __name__ == '__main__':
    #drop_table("tmLogs_table")
    #teamIDs = get_teamIDs()
    #create_tmLogs_table(teamIDs)

    #drop_table("playLogs_table")
    #playerIDs = get_playerIDs()
    #create_playlogs_table(playerIDs)
    pass