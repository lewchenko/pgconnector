import psycopg2
import os
from os import environ

#Example python code to connect to a PG Database, pull back the table and add new records
#really basic

#execute using account equinox on RHEL8 VM

def insert_record(cur,last_id):
    cur.execute("INSERT INTO event (id, app, ac3,ac4,submit,staffid) VALUES \
        (%s, %s, %s, %s, %s, %s)", ((str(int(last_id)+1)),"Jira","BigData","MSS","2021-01-31","04992466"))


def print_table(results):

    for row in results:
            print('id: %s , app: %s , ac3: %s , ac4: %s, submit: %s, staffid %s' % (row[0],row[1],row[2],row[3],row[4],row[5]))
            last_id=row[0]
    return last_id   


def main():
        
        #Connect
        conn = psycopg2.connect("dbname=equinox_db user=equinox")
        cur = conn.cursor()
        
        
        #extract data
        cur.execute("SELECT * FROM event;")
        results=cur.fetchall() 
        
        
        #print table and get last ID
        last_id=print_table(results)
       
        #insert a new record
        insert_record(cur,last_id)

        #Close connection
        conn.commit()        
        cur.close()
        conn.close()

if __name__ == "__main__":

    main()