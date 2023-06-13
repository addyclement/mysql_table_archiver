#Author : Addy Clement
#V1 : 13th, June, 2023
# This code basically archives defined set of rows from a large table into an archive table
# This snippet was last tested on a 700gb (5bn records) table on Aurora MySQL v3
# In 100k chunks, 50m records were copied out in 1hr

import os
import time
# from dotenv import load_dotenv
from mysql.connector import Error
import mysql.connector
import datetime
from datetime import timedelta

print("started at")
ts = datetime.datetime.now()
print(ts)

#declare pagination indexes
start_index=0
end_index=50000
start_date = '2022-08-28 23:59:59'
next_date = start_date
run_cycle=0

#construct pagination here

paging="limit " + str(start_index) + " , " + str(end_index) 

#define a loop to bulk insert data from source to target

for iter in range(500):

#basic connection details for demo, could use environment variables, secret manager etc for prod
    connection = mysql.connector.connect(
    host='dbhost.eu-west-2.rds.amazonaws.com',
    user='archiver_user',
    passwd=$password,
    db='sales_db'
)
#get the next index (last row from the data set) to pass into next get fetch
    query_part_1= """select k.date_created  from (
                     SELECT `id`,`date_created` FROM `sales_db`.`sales_history` FORCE INDEX(`idx_date_created`) 
                     WHERE (seller_id IS NOT NULL AND date_created >= '""" + str(next_date) + "'  and date_created is not null)  ORDER BY `date_created` " + paging + ") k order by 1 desc limit 1;"   
# this part does the bulk insert using the last row from query_1 as paramter   
    query_part_2 = """REPLACE INTO `sales_db`.`sales_history_arch`(`id`,`seller_id`,`order_id`,`channel_id`,`date_created`,`status`,`date_updated`)
                      SELECT /*!40001 SQL_NO_CACHE */ `id`,`seller_id`,`order_id`,`channel_id`,`date_created`,`status`,`date_updated` 
                      FROM `sales_db`.`sales_history` FORCE INDEX(`idx_date_created`) 
                      WHERE (seller_id IS NOT NULL AND 
                      date_created >= '""" + str(next_date)  + "')  ORDER BY `date_created` " + paging


    try:
        if connection.is_connected():
            read_max_id_cursor = connection.cursor(buffered=True)
            read_max_id_cursor.execute(query_part_1)
            next_date_array = read_max_id_cursor.fetchone() 

            # retrieve the date retuned in query 1
            next_date_internal = next_date_array[0]
            
            print("date input for batch insert:" + str(next_date))
            
            #execute the batch insert
            batch_write_cursor = connection.cursor()
            batch_write_cursor.execute(query_part_2) 
        
            connection.commit()

            next_date=next_date_internal  
            print("next date to process :" + str(next_date)) 

            # print("sleep for 1 second(s)")
            run_cycle+=1
            print("Run Cycle " + str(run_cycle) + " Completed at : " + str(datetime.datetime.now()))
            #intentionally allow 1 sec respite
            #consider impact on slaves and activity of the the server (threads connected etc)
            time.sleep(1)

    except Error as e:
        print("Error while connecting to MySQL", e)
        # role back inflight dml if error occurs
        connection.rollback()
    finally:
        connection.close()

endtime = datetime.datetime.now()
print("started : " + str(ts) + " ended : " + str(endtime))

print("total selected :")
print("total inserted")






