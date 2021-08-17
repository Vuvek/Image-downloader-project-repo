import mysql.connector as connection
from logg import *

class MySqlDataBase:

    def __init__(self,df_name):

        log = Logger("*** MySql_DB_Logger ***")
        log.logging_info("-----------------  MySql_DB_Log Started --------------------")
        self.logging = log.cassandra()

        try:
            self.con = connection.connect(host="localhost", user="root", password='',database = df_name)
            self.cursor = self.con.cursor()

        except Exception as e:
            print("Error has Occurred ", e)
            self.logging.error("Error has occured while connection database ")
            self.logging.exception(e)
        else:
            self.logging.info("Connection Created to mysql")
            print("Connection established")


    def show_database(self):
        try:
            databases = self.cursor.execute("SHOW DATABASES")
            print(databases)
            self.logging.info(f"all database {databases}")
        except Exception as e:
            print(e)
            self.logging.error("Error occured")
            self. logging.exception(e,"in show databses")



    def use_databse(self,db_name):
        try:
            self.cursor.execute(f"USE {db_name}")
            self.logging.info(f"Database in used now {db_name}")
        except Exception as e:
            print(e)
            self.logging.error("Error occured in use database")
            self.logging.exception(e)

    def create_database(self,db_name):
        try:
            self.cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Database Created {db_name}")
            self.logging.info(f"DataBase Created {db_name}")
        except Exception as e:
            print(e)
            self.logging.error("Error has occured while creating database ")
            self.logging.exception(e)


    def create_table(self,table_name):
        try:
            self.cursor.execute(f"CREATE TABLE {table_name}(id int(100) NOT NULL AUTO_INCREMENT KEY ,img_name text, number_of_image bigint ,schedule_time timestamp, email text);")
            self.logging.info(f"Table Created {table_name}")
            print("Table Created")
        except Exception as e:
            print(e)
            self.logging.error("Error occurred while creating table ")
            self.logging.exception(e)


    def create_table_for_check(self):
        try:
            self.cursor.execute("CREATE TABLE mydb(id int)")
            self.logging.info(f"Table Created check")
            print("Table Created")
        except Exception as e:
            print(e)
            self.logging.error("Error occurred while creating table ")
            self.logging.exception(e)


    def insert_into_schedular_table(self,img_name,number_of_image,schedule_time,email,table_name):

        try:
            self.cursor.execute(f"insert into {table_name}(img_name,number_of_image,schedule_time,email) values(%s,%s,%s,%s)",(img_name,number_of_image,schedule_time,email))
            print(f'Record Inserted in Schedular')
            self.logging.info(f"Record inserted in Schedular")
            self.con.commit()
        except Exception as e:
            print(e)
            self.logging.error(f"Error occured while inserting record in Schedular")
            self.logging.exception(e)


    def insert_into_for_check(self,num):

        try:
            self.cursor.execute(f"insert into mydb values({num});")
            print(f'Record Inserted in Schedular')
            self.logging.info(f"Record inserted in Schedular")
            self.con.commit()
        except Exception as e:
            print(e)
            self.logging.error(f"Error occured while inserting record in Schedular")
            self.logging.exception(e)

    def select_all_record(self,table_name):

        try:
            self.cursor.execute(f"SELECT * FROM advance_image_scrapper.{table_name}")
            row = self.cursor.fetchall()
            for i in row:
                print(i)
                self.logging.info(f"Record Selected {i}")
        except Exception as e:
            print(e)
            self.logging.error("Error occured in select_all_record")
            self.logging.exception(e)

    def select_all_record_with_condition(self,column,value,table_name):

        try:
            self.cursor.execute(f"SELECT * FROM advance_image_scrapper.{table_name} where {column} = '{value}'")
            row = self.cursor.fetchall()
            for i in row:
                print(i)
                self.logging.info(f"Record Selected with condition {i}")
        except Exception as e:
            print(e)
            self.logging.error("Error occured in select_all_record")
            self.logging.exception(e)


    def delete_record_with_condition(self , col_name , value,table_name):
        try:
            self.cursor.execute(f"DELETE FROM advance_image_scrapper.{table_name} where {col_name} = '{value}'")
            print("Record Deleted")
            self.logging.info(f"Record deleted from {table_name}")
            self.con.commit()
        except Exception as e:
            print(e)
            self.logging.error("Error occured while deleting record in delete_record_with_condition")
            self.logging.error(e)


    def delete_all_records(self ,table_name):
        try:
            self.cursor.execute(f"DELETE FROM Schedular")
            print("All Record Deleted")
            self.logging.info(f"Record deleted from {table_name}")
            self.con.commit()
        except Exception as e:
            print(e)
            self.logging.error("Error occured while deleting record in delete_record")
            self.logging.error(e)


    def drop_table(self,table_name):
        try:
            row = self.cursor.execute(f"DROP TABLE advance_image_scrapper.{table_name}")
            print("Successfully Deleted",type(row))
            self.logging.info(f"Table droped successfully :{table_name}")
            self.con.commit()
        except Exception as e:
            print(e)
            self.logging.error("Error Occured while deleting table")
            self.logging.exception(e)



    def count_records(self,table_name):
        try:
            self.cursor.execute(f"Select Count(*) from {table_name}")
            row = self.cursor.fetchone()
            # self.logging.info(f"Total Record {row}")
            if row[0] == 0:
                return False
            else:
                return row[0]
        except Exception as e:
            print(e)
            self.logging.error("Error Occured in count_records")
            self.logging.exception(e)


    def agg_max(self):
        try:
            self.cursor.execute("SELECT img_name,number_of_image,MAX(schedule_time),email FROM advance_image_scrapper.Schedular group by id;")
            row = self.cursor.fetchone()
            print(row[2])
            self.logging.info(f"Max record in table {row}")
            return row
        except Exception as e:
            print(e)
            self.logging.error("Error occured in agg_max")
            self.logging.exception(e)


    def agg_min(self):
        try:
            self.cursor.execute("SELECT img_name,number_of_image,MIN(id),email,schedule_time FROM advance_image_scrapper.Schedular group by id;")
            row = self.cursor.fetchone()
            # self.logging.info(f"Min record in table {row}")
            return row
        except Exception as e:
            print(e)
            self.logging.error("Error occured in agg_min")
            self.logging.exception(e)


con = MySqlDataBase('advance_image_scrapper')

if __name__ == '__main__':


    # con.create_database("advance_image_scrapper")
    # con.use_databse("advance_image_scrapper")
    # con.create_table("Schedular_job_done_table")
    # con.create_table_for_check()
    # con.create_table("Schedular")
    con.insert_into_schedular_table("birds",20,'2013-11-02 10:02;00','vivek.9718470484@gmail.com','Schedular')
    # con.insert_into_for_check(1)
    # con.select_all_record("Schedular")
    # print(con.count_records("mydb"))
    # print(con.count_records("Schedular_job_done_table"))
    # print(con.count_records("Schedular"))
    # con.count_records('Schedular')
    # con.delete_record_with_condition("img_name",'Lions','Schedular')
    # con.select_all_record_with_condition('img_name','birds','Schedular')
    row = con.agg_min()
    print(row)
    # img_name = row[0]
    # Total_image = row[1]
    # schedule_time = row[2]
    # email_receiver = row[3]
    # print(img_name,Total_image,schedule_time,email_receiver)
    # con.drop_table("Schedular_job_done_table")
    # con.drop_table("create_table_for_check")
    # con.delete_all_records('mydb')
    # con.delete_all_records('Schedular')
    # row = con.count_records('Schedular_job_done_table')
    # con.delete_record_with_condition("id", 156, 'Schedular')


