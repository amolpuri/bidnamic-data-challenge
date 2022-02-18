# New_Cell
pip install pandas

# New_Cell
pip install mysql-connector-python

# New_Cell
import pandas as pd


# We can also read the csv file by uploading it to S3
#  using boto3 library and by mentioning s3 location of the object
campaigns = pd.read_csv(r'C:\Users\amol.puri\Downloads\campaigns.csv', index_col=False, delimiter=',')
#  print(campaigns)

search_terms = pd.read_csv(r'C:\Users\amol.puri\Downloads\search_terms.csv', index_col=False, delimiter=',')
#  print(search_terms)

#  adgroups = pd.read_csv(r'C:\Users\amol.puri\Downloads\adgroups.csv', index_col=False, delimiter=',')
#   print(adgroups)

adgroups = pd.read_csv(r'C:\Users\amol.puri\Downloads\adgroups.csv', index_col=False, delimiter=',')
#  print(adgroups)


# New_Cell
adgroups[['Shift','Shopping','country','campaign structure value','priority','random string','hash']] = adgroups['alias'].str.split(' - ',expand=True)
print(adgroups)


# New_Cell
import mysql.connnector as mysql 
from mysql.connnector import Error

try:
    conn = mysql.connect(host='localhost', user='root')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("Create Database bidnamic")
        print ("Database is created")
except Error as e:
    print("Error while connecting to MySQL, e")



# New_Cell
import mysql.connector as mysql
from mysql.connector import Error
try:
    conn = mysql.connect(host='localhost', database='bidnamic', user='root')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS campaigns;')
        cursor.execute('DROP TABLE IF EXISTS search_terms;')
        cursor.execute('DROP TABLE IF EXISTS adgroups;')
        print('Creating table....')
        
        cursor.execute("CREATE TABLE campaigns(campaign_id varchar(255),structure_value varchar(255),status varchar(255))")
        print("Table is created....")
        
        cursor.execute("CREATE TABLE search_terms(date varchar(255),ad_group_id varchar(255),campaign_id varchar(255),clicks varchar(255),cost varchar(255),conversion_value varchar(255),conversions varchar(255),search_term varchar(255))")
        print("Table is created....")
        
        cursor.execute("CREATE TABLE adgroups(ad_group_id varchar(255),campaign_id varchar(255),alias varchar(255),Shift varchar(255),Shopping varchar(255),country varchar(255),campaign structure value varchar(255),priority varchar(255),random string varchar(255),hash varchar(255),status varchar(255))")
        print("Table is created....")
         
        
        for i,row in campaigns.iterrows():
            sql = "INSERT INTO bidnamic.campaigns VALUES (%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
        for i,row in search_terms.iterrows():
            sql = "INSERT INTO bidnamic.search_terms VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
        for i,row in adgroups.iterrows():
            sql = "INSERT INTO bidnamic.adgroups VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
        
        
        conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)



# New_Cell
sql = "SELECT * FROM bidnamic.campaigns"
cursor.execute(sql)
result = cursor.fetchall()
for i in result:
    print(i)
    
sql = "SELECT * FROM bidnamic.search_terms"
cursor.execute(sql)
result = cursor.fetchall()
for i in result:
    print(i)
    
sql = "SELECT * FROM bidnamic.adgroups"
cursor.execute(sql)
result = cursor.fetchall()
for i in result:
    print(i)



# New_Cell

#  'Shift','Shopping','country','campaign structure value','priority','random string','hash'
#  We sometimes need to know the ROAS aggregated by country and/or by priority.
#  cost column in search term, country and priority column in adgroups

sql = "SELECT st.ad_group_id, st.campaign_id, SUM (st.cost), ag.country, ag.priority\
        FROM search_terms as st\
        FULL OUTER JOIN adgroups as ag\
        ON st.ad_group_id = ag.ad_group_id\
        GROUP BY country, priority"
cursor.execute(sql)
result = cursor.fetchall()
for i in result:
    print(i)
