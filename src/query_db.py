import psycopg2
import os
from dotenv import load_dotenv
import pandas.io.sql as sqlio
from tabulate import tabulate

class Database:

    def __init__(self):
        self.connection = self.__open_connection()
        
    def __open_connection(self):
        load_dotenv()
        return psycopg2.connect(user=os.getenv("POSTGRES_USER"),
                        password=os.getenv("POSTGRES_PASSWORD"),
                        host="68.134.23.177",
                        port="54369",
                        database=os.getenv("POSTGRES_DB")
                        )
         
    def __close_connection(self):
         self.connection.close()

    def query(self, received):
            return sqlio.read_sql_query(received, self.connection)

if __name__ == "__main__":
    db = Database()
    tabl = db.query('SELECT * FROM "tmLogs_table" LIMIT 5')
    print(tabulate(tabl, headers = 'keys', tablefmt = 'psql'))





    