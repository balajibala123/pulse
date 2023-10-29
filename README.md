# PhonePe Pulse - Data #

The Indian digital payments story has truly captured the world’s imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-art payments infrastructure built as Public Goods championed by the central bank and the government. PhonePe started in 2016 and has been a strong beneficiary of the API driven digitisation of payments in India. When we started , we were constantly looking for definitive data sources on digital payments in India without much success. As a way of giving back to the data and developer community, we decided to open the anonymised aggregate data sets that demystify the what, why and how of digital payments in India. Licensed under the [CDLA-Permissive-2.0 open data license](https://github.com/PhonePe/pulse/blob/master/LICENSE), the PhonePe Pulse Dataset API is a first of its kind open data initiative in the payments space.
## Announcements ##
:star2: Data for Q3(_July, August, September_) of 2023 has been added and is available for consumption.

## Table of Contents ##

## cloning into local environment
git clone -- clone the below code to your local environmnet
git clone https://github.com/balajibala123/youtube_data_harvesting.git

## Pre-Requisite
Need to install below python install packages

pip install pathlib pandas json datetime numpy mysql.connector os dotenv

# To setup we need to have few keys and password in .env file

## MYSQL DB DETAILS
MYSQL_HOST_NAME = replace host name
MYSQL_USER_NAME = replace mysql user name
MYSQL_USER_PASSWORD = replace mysql password here
MYSQL_DATABASE_NAME = replace your database name here

## Below functions are used in phonepe data visualisations

Data_Transactions()
aggDataTransTable()
aggDataTrans(QUARTER, YEAR)

AggData_Users()
aggDataUsersTable()
aggDataUsers(QUARTER, YEAR)

# Tranaction by category 
aggDataTransCategory(QUARTER, YEAR)

# MAP STATES TRANS and USERS
# Transaction by Top 10 STATES 
StateTransactions(QUARTER, YEAR)

# Transactions by Top 10 Districts
DistrictTransactions(QUARTER, YEAR)

# Transactions by Top 10 Pincodes
PincodeTransactions(QUARTER, YEAR)

# Users RegisteredUsers and appOpens in phonepe
UsersCategory(QUARTER, YEAR)

# Users by Top 10 States
StateUsers(QUARTER, YEAR)

# Users By Top 10 district
TopDistrictUsers(QUARTER, YEAR)

# Users By Top 10 Pincodes
TopPincodeUsers(QUARTER, YEAR)

# Databases connection
MyCursor()