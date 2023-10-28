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

# Tranaction by category 
def aggDataTransCategory(QUARTER, YEAR):

    db,mycursor = MyCursor()
    # Payment categories 
    mycursor.execute("""select 
    quarter, 
    year, 
    name as Transactions_Name,
    sum(count) as All_Trans,
    sum(amount) as Total_Pay_value
    from aggdatatrans
    group by quarter, year, name ;""")

    out = mycursor.fetchall()
    data = list(out)

    columns = ['quarter', 'year', 'Transactions_Name', 'Transcations', 'Payment_Value']

    DataTransactionName = pd.DataFrame(data, columns=columns)
    DataTransactionName['Transcations'] = DataTransactionName['Transcations'].astype(np.int64)
    DataTransactionName['Payment_Value'] = np.int64(round(DataTransactionName['Payment_Value'],0))
    DataTransactionName = DataTransactionName[(DataTransactionName['quarter'] == QUARTER) & (DataTransactionName['year'] == YEAR)]


    DataTransactionName['Total_Transactions'] = sum(DataTransactionName['Transcations'])
    DataTransactionName['Total_Payment_Value'] = np.int64(round(sum(DataTransactionName['Payment_Value'])/10000000,0))
    DataTransactionName['Avg_Transaction_Value'] = np.int64(round(sum(DataTransactionName['Payment_Value']) / sum(DataTransactionName['Transcations']),0))

    return DataTransactionName


# MAP STATES TRANS and USERS
# Transaction by Top 10 STATES 
def StateTransactions(QUARTER, YEAR):
    db,mycursor = MyCursor()
    mycursor.execute("""select
        quarter,
        year,
        state,
        sum(count) as count
        from mapdatatrans 
        group by quarter, year, state""")

    out= mycursor.fetchall()
    data = list(out)
    # print(data)
    columns = ['quarter', 'year', 'state', 'Transactions_Count']

    def convert_to_crore_lakh(value):
        if value >= 10000000:
            return f'{value /10000000 : .2f} Cr'
        else:
            return f'{value / 100000 : .2f} L'

    MapTransactionsState = pd.DataFrame(data=data, columns=columns)
    MapTransactionsState['Transactions_Count'] = MapTransactionsState['Transactions_Count'].astype(np.int64)
    MapTransactionsState = MapTransactionsState[(MapTransactionsState['year'] == YEAR) & (MapTransactionsState['quarter'] == QUARTER)]
    MapTransactionsState['Rank'] = MapTransactionsState['Transactions_Count'].rank(ascending= False)
    MapTransactionsState = MapTransactionsState.sort_values(by=['Transactions_Count', 'Rank'], ascending=[False, True])
    MapTransactionsState = MapTransactionsState.reset_index(drop=True)
    MapTransactionsState['Transacations_Count'] = MapTransactionsState['Transactions_Count'].apply(convert_to_crore_lakh)
    MapTransactionsState.drop(columns=['Transactions_Count', 'Rank'], inplace=True)
    TopStateTransactions = MapTransactionsState.head(10)
    
    return TopStateTransactions

# Transactions by Top 10 Districts
def DistrictTransactions(QUARTER, YEAR): 
    db,mycursor = MyCursor()
    mycursor.execute("""select 
        quarter,
        year,
        district_name,
        sum(count) as Transactions_Count
        from topdatatransdistrict
        group by quarter, year, district_name""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter', 'year', 'District_Name', 'Transactions_Count']

    def convert_lakh_crore(value):
        if value >= 10000000:
            return f'{value / 10000000 : .2f} Cr'
        else:
            return f'{value / 100000 : .2f} L'
        
    DistrictTransactions = pd.DataFrame(data=data, columns=columns)
    DistrictTransactions = DistrictTransactions[(DistrictTransactions['quarter']== QUARTER) & (DistrictTransactions['year'] == YEAR)]
    DistrictTransactions['Rank'] = DistrictTransactions['Transactions_Count'].rank(ascending=False)
    DistrictTransactions = DistrictTransactions.sort_values(by=['Transactions_Count', 'Rank'], ascending=[False, True])
    DistrictTransactions = DistrictTransactions.reset_index(drop=True)
    DistrictTransactions['Transactions_Count'] = DistrictTransactions['Transactions_Count'].apply(convert_lakh_crore)
    DistrictTransactions.drop(columns=['Rank'], inplace=True)
    TopDistrict = DistrictTransactions.head(10)
    return TopDistrict

# Transactions by Top 10 Pincodes
def PincodeTransactions(QUARTER, YEAR):
    db,mycursor = MyCursor()
    mycursor.execute("""select
        quarter,
        year,
        pincodes,
        count as Transactions_Count
        from topdatatranspincode
        group by year, quarter, pincodes""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter', 'year', 'Pincodes', 'Transactions_Count']

    def convert_lakh_crore(value):
        if value >= 10000000:
            return f'{value / 10000000 : .2f} Cr'
        else:
            return f'{value / 100000 : .2f} L'

    PincodeTransactions = pd.DataFrame(data=data, columns=columns)
    # PincodeTransactions
    PincodeTransactions = PincodeTransactions[(PincodeTransactions['quarter'] == QUARTER) & (PincodeTransactions['year'] == YEAR)]
    PincodeTransactions['Rank'] = PincodeTransactions['Transactions_Count'].rank(ascending=False)
    PincodeTransactions = PincodeTransactions.sort_values(by=['Transactions_Count', 'Rank'], ascending=[False,True])
    PincodeTransactions = PincodeTransactions.reset_index(drop=True)
    PincodeTransactions['Transactions_Count'] = PincodeTransactions['Transactions_Count'].apply(convert_lakh_crore)
    PincodeTransactions.drop(columns=['Rank'], inplace=True)
    TopPincode = PincodeTransactions.head(10)
    return TopPincode

# Users RegisteredUsers and appOpens in phonepe
def UsersCategory(QUARTER, YEAR): 
    db,mycursor = MyCursor()   
    mycursor.execute("""select
        quarter,
        year,
        sum(registeredUsers),
        sum(appOpens)
        from mapdatausers
        group by quarter, year""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter', 'year', 'RegisteredUsers', 'AppOpens']

    def covert_crore_lakh(value):
        if value >= 10000000:
            return f'{value/10000000 : .7f} cr'
        else:
            return f'{value/100000 : .6f} L'
    UsersAppOpens = pd.DataFrame(data=data, columns=columns)
    UsersAppOpens = UsersAppOpens[(UsersAppOpens['quarter'] == QUARTER) & (UsersAppOpens['year'] == YEAR)]
    UsersAppOpens[['RegisteredUsers', 'AppOpens']] = UsersAppOpens[['RegisteredUsers', 'AppOpens']].applymap(covert_crore_lakh)
    UsersAppOpens['year'] = UsersAppOpens['year'].astype(str)
    return UsersAppOpens

# Users by Top 10 States
def StateUsers(QUARTER, YEAR):
    db,mycursor = MyCursor() 
    mycursor.execute("""select
        quarter,
        year,
        state,
        sum(registeredUsers) as registeredUsers
        from mapdatausers
        group by quarter, year, state;""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter','year','state', 'registeredUsers']

    def convert_to_crore_lakh(value):
        if value >= 10000000:
            return f'{value / 10000000 : .2f} Cr'
        else:
            return f'{value / 100000: .2f} L'

    DataUsersState = pd.DataFrame(data=data, columns=columns)
    DataUsersState = DataUsersState[(DataUsersState['quarter'] == QUARTER) & (DataUsersState['year']== YEAR)]
    DataUsersState['Rank'] = DataUsersState['registeredUsers'].rank(ascending=False)
    DataUsersState = DataUsersState.sort_values(by=['registeredUsers', 'Rank'], ascending=[False, True])
    DataUsersState = DataUsersState.reset_index(drop=True)
    DataUsersState['registeredUsers'] = DataUsersState['registeredUsers'].apply(convert_to_crore_lakh)
    DataUsersState.drop(columns=['Rank'], inplace=True)
    TopStateUsers = DataUsersState.head(10)
    
    return TopStateUsers

# Users By Top 10 district
def TopDistrictUsers(QUARTER, YEAR):
    db,mycursor = MyCursor()
    mycursor.execute("""select 
        quarter,
        year,
        district_name,
        registeredUsers
        from topdatausersdistrict
        group by quarter, year, district_name""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter', 'year', 'District_Name', 'RegisteredUsers']

    def convert_lakh_crore(value):
        if value >= 10000000:
            return f'{value/10000000 : .2f} Cr'
        else:
            return f'{value/100000 : .2f} L'
        
    DistrictUsers = pd.DataFrame(data=data, columns=columns)
    DistrictUsers = DistrictUsers[(DistrictUsers['quarter'] == QUARTER) & (DistrictUsers['year']== YEAR)]
    DistrictUsers['Rank'] = DistrictUsers['RegisteredUsers'].rank(ascending= False)
    DistrictUsers = DistrictUsers.sort_values(by=['RegisteredUsers', 'Rank'], ascending=[False, True])
    DistrictUsers['RegisteredUsers'] = DistrictUsers['RegisteredUsers'].apply(convert_lakh_crore)
    DistrictUsers.drop(columns=['Rank'], inplace=True)
    TopDistrictUsers = DistrictUsers.head(10)
    return TopDistrictUsers

# Users By Top 10 Pincodes
def TopPincodeUsers(QUARTER, YEAR): 
    db,mycursor = MyCursor()
    mycursor.execute("""select 
        quarter,
        year,
        Pincodes,
        RegisteredUsers
        from topdatauserspincode
        group by quarter, year, pincodes""")

    out = mycursor.fetchall()
    data = list(out)
    columns = ['quarter', 'year', 'Pincodes', 'RegisteredUsers']

    def convert_lakh_crore(value):
        if value >= 100000:
            return f'{value/100000 : .2f} L'
        else:
            return f'{value /1000 : .2f} K'

    TopPincodeUsers = pd.DataFrame(data=data, columns=columns)
    TopPincodeUsers = TopPincodeUsers[(TopPincodeUsers['year'] == YEAR) & (TopPincodeUsers['quarter'] == QUARTER)]
    TopPincodeUsers['Rank'] = TopPincodeUsers['RegisteredUsers'].rank(ascending=False)
    # TopPincodeUsers
    TopPincodeUsers = TopPincodeUsers.sort_values(by=['RegisteredUsers', 'Rank'], ascending=[False, True])
    TopPincodeUsers = TopPincodeUsers.reset_index(drop=True)
    TopPincodeUsers['RegisteredUsers'] = TopPincodeUsers['RegisteredUsers'].apply(convert_lakh_crore)
    TopPincodeUsers.drop(columns=['Rank'], inplace=True)
    TopPincodeUsers= TopPincodeUsers.head(10)
    return TopPincodeUsers


