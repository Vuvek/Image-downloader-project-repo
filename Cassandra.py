
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from logg import *



class Cassandra():

    def __init__(self):

        """
           Establishing connection_with_cassandrea and Creating logger object
        """

        self.log = Logger("********************* Cassandra_DB_Logger *********************")
        self.log.logging_info("-----------------  Cassandra_DB Log Started --------------------")
        self.logger = self.log.cassandra()

        self.cloud_config = {'secure_connect_bundle': r'secure-connect-test.zip'}
        self.auth_provider = PlainTextAuthProvider('GyZHUzvSlTSQUrYmCRChkNjT','w1Bzjzuk9e5q3gpyqBGFXS1ohM2XU8_EHhTkE_MGz,hA.mB-.dSxMO23xQ9xpDTR08kQSm1,MqBZq3lbh5L_sgpQ0Z,RUhIkxncPMT_78yzPU.97ZHD5qvcceBmBFZRb')
        self.cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
        self.session = self.cluster.connect()
        try:
            row = self.session.execute("select release_version from system.local").one()
        except Exception as e:
            self.logger.info("Error Occured while establishing the connection")
            print(e)
        else:
            print(row[0])
            self.logger.info("Connection Established Successfully",row[0])


    def create_keyspace(self):
        """
        function to create keyspace
        :return: Nothing
        """

        try:
            self.session.execute("CREATE KEYSPACE advance_image_scrapper WITH replication={'class':'SimpleStrategy', 'replication_factor': 3}").one()
            print("key Space creted")
            self.logger.info("Key Space Created Successfully")
        except Exception as e:
            self.logger.info("Error Occured While Creating Key Space in create_keyspace")
            print(e)


    def use_keyspace(self,key_space):
        """
          function to use default keyspace
          :return: Nothing
        """

        try:
            self.session.execute(f"Use {key_space};")
            self.logger.info(f"{key_space} key space is in used")
        except Exception as e:
            self.logger.info("Error Occured in use_keyspace method")
            print(e)


    def create_table(self,table_name):

        """
           function to create table in keyspace
           :return: nothing
        """

        try:
            row = self.session.execute(
                f"CREATE TABLE advance_image_scrapper.{table_name}(img_name text, number_of_image bigint ,schedule_time timestamp PRIMARY KEY, email text,id int primary key);").one()

            print('table created.')
        except Exception as e:
            print(e)





    def insert_into_schedular_table(self,img_name, number_of_image, schedule_time, email,id,table_name):
        """
        function to insert to record in the table

        :param img_name: image name
        :param number_of_image: number of images download
        :param schedule_time: at which time will be job start
        :param email: email to send images
        :param id: unique id
        :param table_name: table name
        :return: nothing
        """

        try:
            row = self.session.execute(
                f"INSERT INTO advance_image_scrapper.{table_name}(img_name , number_of_image ,schedule_time , email,id) VALUES(%s,%s,%s,%s,%s)",
                (img_name, number_of_image, schedule_time, email,id)).one()
            print('Record Inserted')
            self.logger.info("Reocrd Inserted in the Table Successfully")

        except Exception as e:
            self.logger.info("Error Occured while insertng the record in inserting_into_schedular_table")
            print(e)


    def select_all_record(self):
        """
        function to select all the record from the table
        :return: nothing
        """

        try:
            row = self.session.execute("SELECT * FROM advance_image_scrapper.Schedular")
            self.logger.info("All Records Selected Successfullly")
            print("all record seleted")
            for i in row.all():
                print(i)
        except Exception as e:
            self.logger.info("Error Occured in select_all_record")
            print(e)


    def delete_record_with_condition(self , col_name , value):
        """
        function is to delete record with condition in Schedular table

        :param col_name: column name
        :param value: value to delete that row
        :return: nothing
        """

        try:
            self.session.execute(f"DELETE FROM advance_image_scrapper.Schedular where {col_name} = {value};").one()
            print("Record Deleted")
            self.logger.info("Record delted conditionally")
        except Exception as e:
            self.logger.info("Error Occured in delte_record_with_condition")
            print(e)


    def delete_all_records(self ,table_name):
        """
        function to delete all the reocrd of the table with given name

        :param table_name: table name
        :return: nothing
        """

        try:
            self.session.execute(f"TRUNCATE advance_image_scrapper.{table_name}")
            print("All Record Deleted")
            self.logger.info(f"Record deleted from {table_name}")
        except Exception as e:
            print(e)
            self.logger.error("Error occured while deleting record in delete_record")


    def drop_table(self,table_name):
        """
        function to drop table with given name

        :param table_name: table name
        :return: nothing
        """

        try:
            self.session.execute("DROP TABLE advance_image_scrapper.Schedular")
            print(f"{table_name} Table dropped Successfully")
            self.logger.info(f"{table_name} Table dropped Successfully")
        except Exception as e:
            self.logger.info("Error Occurred in drop_table")
            print(e)


    def count_records(self,table_name):
        """
        function count all the record with given table

        :param table_name: table name
        :return: number of records
        """

        try:
            row = self.session.execute(f"Select Count(*) from advance_image_scrapper.{table_name}")
            print("All reocrd counted Successfully")
            self.logger.info("All reocrd counted Successfully")
            return row.all()
        except Exception as e:
            self.logger.info("Error Occurred in count_reocrds")
            print(e)


    def agg_max(self):
        """
        function return max records with respect to schedule_time column
        :return: max raw
        """

        try:
            max_time = self.session.execute('SELECT img_name,number_of_image,MAX(schedule_time),email,id FROM advance_image_scrapper.Schedular').one()
            if max_time[0] == None:
                print(max_time)
                self.logger.info(max_time)
                return max_time
            row = self.session.execute(
                f"select img_name,number_of_image,schedule_time,email,id from advance_image_scrapper.Schedular where schedule_time = '{max_time[2]}' ALLOW FILTERING;").one()
            print(row)
            self.logger.info("Error Occured in agg_max function")
            return row

        except Exception as e:
            self.logger.info("Error Occured in agg_max method")
            print(e)



    def agg_min(self):
        """
               function return min records with respect to schedule_time column
               :return: min raw
        """

        try:
            min_time = self.session.execute('SELECT img_name,number_of_image,MIN(schedule_time),email,id FROM advance_image_scrapper.Schedular').one()
            if min_time[0] == None:
                print(min_time)
                # self.logger.info(min_time)
                return min_time
            row = self.session.execute(
                f"select img_name,number_of_image,schedule_time,email,id from advance_image_scrapper.Schedular where schedule_time = '{min_time[2]}' ALLOW FILTERING;").one()
            print(f"minimum row {row}")
            # self.logger.info("Error Occured in agg_min function")
            return row

        except Exception as e:
            self.logger.info("Error Occured in agg_min method")
            print(e)
        # min_time = self.session.execute('SELECT img_name,number_of_image,MIN(schedule_time),email,id FROM advance_image_scrapper.Schedular').one()
        # if min_time[0] == None:
        #     return min_time
        # row = self.session.execute(f"select img_name,number_of_image,schedule_time,email,id from advance_image_scrapper.Schedular where schedule_time = '{min_time[2]}' ALLOW FILTERING;").one()
        # return row



"""
Creating object of Cassandra class to import automatically in all the modules
"""

con = Cassandra()



if __name__ == '__main__':

    con.delete_all_records('Schedular')
    con.delete_all_records('Schedular_job_done_table')
    con.select_all_record()
    row = con.agg_min()
    print(row[0])
    print(row[1])
    print(row[2])
    print(row[3])
    print(row[4])
    con.delete_record_with_condition('schedule_time','2013-02-10 10:02:00')


