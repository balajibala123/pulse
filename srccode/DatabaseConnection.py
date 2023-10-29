import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# MYSQL---------------------------------------------------------------
#  here we're providing mysql related database details
mysql_host_name = os.getenv("MYSQL_HOST_NAME")
mysql_user = os.getenv("MYSQL_USER_NAME")
mysql_password = os.getenv("MYSQL_USER_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE_NAME")

def MyCursor():
    # Database Connection
    db = mysql.connector.connect(
        host =f'{mysql_host_name}' ,
        user = f'{mysql_user}',
        password = f'{mysql_password}',
        database = f'{mysql_database}'
    )

    mycursor = db.cursor(buffered=True)

    db.commit()

    return db, mycursor
