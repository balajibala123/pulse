from pathlib import Path
import pandas as pd
import json
import datetime
import mysql.connector
import numpy as np

# Aggregated Data Transactions

def Data_Transactions():
    root_dir = Path("../data/aggregated/transaction/country/india/state/")
    lis = []
    # rglob is recursive glob - which means searching files recursiverly inside the files and folders
    for i in root_dir.rglob(f"*.json"):
        with open(i, 'r') as file:
            content = file.read()
            dataset = json.loads(content)

            # i will have exact path from that <.parent> is going one step behind and <.name> is getting that subdirectory name
            state_name = i.parent.parent.name
            year = i.parent.name

            # to get the filename 1.json,2.json, 3.json and 4.json 
            quarter = i.stem
            # to change 1.json to Q1, 2.json to Q2 etc
            quarter = f'Q{quarter}'

            lis.append({'quarter':quarter,'year':year,'state':state_name,'data':dataset})

    # print(lis)

    lis2=[]
    for j in lis:
        
        for k in j['data']['data']['transactionData']:
            # I'm doing conversion rounding to two decimal point
            amount_value = k['paymentInstruments'][0]['amount']
            amount_formatted = f'{amount_value: .2f}'

            timestamp_milliseconds = j['data']['responseTimestamp']
            timestamp_seconds = timestamp_milliseconds /1000
            datetime_obj = datetime.datetime.fromtimestamp(timestamp_seconds)
            formatted_timestamp = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

            
            data = dict(quarter = j['quarter'],
                        year = j['year'],
                        state = j['state'],
                        name = k['name'],
                        type = k['paymentInstruments'][0]['type'],
                        count = k['paymentInstruments'][0]['count'],
                        amount = amount_formatted,
                        timestamp = formatted_timestamp)
            lis2.append(data)

    return lis2

dataTransaction = pd.DataFrame(Data_Transactions())
dataTransaction['year'] = dataTransaction['year'].astype(int)
dataTransaction['amount'] = dataTransaction['amount'].astype(float)
# dataTransaction['country'] = 'India'
data_trans = dataTransaction


def MyCursor():
    # Database Connection
    db = mysql.connector.connect(
        host ='localhost',
        user = 'root',
        password ='balaji',
        database = 'pulse'
    )

    mycursor = db.cursor(buffered=True)

    db.commit()

    return db, mycursor

def aggDataTransTable():
    db,mycursor = MyCursor()
    mycursor.execute("create database if not exists pulse")

    mycursor.execute("""create table if not exists aggDataTrans (
                    aggDataTransId int auto_increment primary key,quarter varchar(10), year int(10), 
                    state varchar(255), name varchar(255), type varchar(10), count int(255), amount float,
                    timestamp datetime)
                    """)

    sql = ("""insert into aggDataTrans(aggDataTransId ,quarter ,year ,state ,name, type, count, amount, 
            timestamp) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on duplicate key update
            quarter = values(quarter), year = values(year), state = values(state), name = values(name), 
            type = values(type), count = values(count), amount = values(amount),
            timestamp = values(timestamp)""")

    for i in data_trans.to_records().tolist():
        mycursor.execute(sql,i)

    db.commit()


def aggDataTrans(QUARTER, YEAR):
    db,mycursor = MyCursor()

    mycursor.execute("""select
    quarter,
    state,
    year,
    sum(count) as All_Transactions, 
    concat(round(sum(amount)/ 10000000)) as Total_Payment_Value,
    round(sum(amount)/sum(count)) as Avg_Transactions_Value
    from aggdatatrans
    group by quarter, year, state;""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter', 'state', 'year', 'All_Transactions', 'Total_Payment_Value', 'Avg_Transactions_Value']


    DataTransaction = pd.DataFrame(data, columns=columns)
    DataTransaction = DataTransaction[(DataTransaction['quarter'] == QUARTER) & (DataTransaction['year'] == YEAR)]
    DataTransaction['Total_Payment_Value'] = DataTransaction['Total_Payment_Value'].astype(int)
    DataTransaction['All_Transactions'] = DataTransaction['All_Transactions'].astype(np.int64)

    with open('statesmapping.json', 'r') as file:
        state_mapping = json.load(file)

    # Use the map function to replace old values with new values
    DataTransaction['state'] = DataTransaction['state'].map(state_mapping)
    db.close()
    return DataTransaction
    
# Aggregated Data Users
def AggData_Users():
    root_dir1 = Path("../data/aggregated/user/country/india/state")

    lis = []
    for i in root_dir1.rglob("*.json"):
        # print(i)
        with open(i, 'r') as file:
            content = file.read()
            dataset = json.loads(content)

            state = i.parent.parent.name
            year = i.parent.name
            #  to get 1.josn,2.json,3.json and 4.json
            quarter = i.stem
            #  to convert 1.json to Q1, 2.json to Q2 etc.,
            quarter = f'Q{quarter}'

            lis.append({'quarter':quarter, 'year': year, 'state' : state, 'data': dataset})

    # print(lis)

    lis2 = []
    for j in lis:
        data = {
            'quarter': j['quarter'],
            'year': j['year'],
            'state': j['state'],
            # 'timestamp': j['data'].get('responseTimestamp'),
            'registeredUsers': j['data']['data']['aggregated'].get('registeredUsers', 'NaN'),
            'appOpens': j['data']['data']['aggregated'].get('appOpens', 'NaN')
            
        }

        # Check if 'usersByDevice' data is available
        users_by_device = j['data']['data']['usersByDevice']
        if users_by_device is not None:
            for user_data in users_by_device:
                brand = user_data.get('brand', 'NaN')
                count = user_data.get('count', 'NaN')
                percentage_value = user_data.get('percentage', None)
                percentage_formated = f'{percentage_value: .2f}' if percentage_value is not None else '0'
                lis2.append({
                    **data,  # Include quarter, year, and state in the data
                    'brand': brand,
                    'count': count,
                    'percentage': percentage_formated
                    
                })

        # If 'usersByDevice' is missing or empty, add a row with 'NaN' for brand, count, and percentage
        if not users_by_device:
            lis2.append({
                'quarter': data['quarter'],
                'year': data['year'],
                'state': data['state'],
                'registeredUsers': data['registeredUsers'],
                'appOpens': data['appOpens'],
                'brand': '0',
                'count': '0',
                'percentage': '0'
            })

    return lis2

DataUsers = pd.DataFrame(AggData_Users())
DataUsers['year'] = DataUsers['year'].astype(int)
DataUsers['count'] = DataUsers['count'].astype(int)
DataUsers['percentage'] = DataUsers['percentage'].astype(float)
# DataUsers['timestamp'] = pd.to_datetime(DataUsers['timestamp'], unit='ms')
# DataUsers['timestamp'] = DataUsers['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")
Data_Users = DataUsers

def aggDataUsersTable():
    db, mycursor = MyCursor()
    mycursor.execute("""create table if not exists aggDataUsers (
                    aggDataUsersId int auto_increment primary key, quarter varchar(10), year int(10), 
                    state varchar(255), registeredUsers bigint, appOpens bigint,
                    brand varchar(255), count bigint, percentage float)""")
    
    sql = ("""insert into aggdatausers (aggDataUsersId, quarter, year, state, registeredUsers, appOpens, 
       brand, count, percentage) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
       on duplicate key update
       quarter = values(quarter), year = values(year), state = values(state), 
       registeredUsers = values(registeredUsers), appOpens = values(appOpens), 
       brand = values(brand), count = values(count), percentage = values(percentage)""")

    for i in Data_Users.to_records().tolist():
        mycursor.execute(sql,i)

    db.commit()
    mycursor.close()
    db.close()

def aggDataUsers(QUARTER, YEAR):
    db, mycursor = MyCursor()
    mycursor.execute("""select quarter, year, state, registeredUsers, appOpens 
                    from aggdatausers  
                    group by quarter,year,state;""")

    out = mycursor.fetchall()
    # print(tabulate(out, [i[0] for i in mycursor.description], tablefmt='psql'))

    data = list(out)
    columns = ['quarter', 'year', 'state', 'registeredUsers', 'appOpens']
    UsersData = pd.DataFrame(data, columns= columns)
    UsersData = UsersData[(UsersData['quarter']== QUARTER) & (UsersData['year'] == YEAR)]
    # UsersData

    with open("statesmapping.json", 'r') as file:
        state_mapping = json.load(file)

    UsersData['state']= UsersData['state'].map(state_mapping)

    return UsersData
