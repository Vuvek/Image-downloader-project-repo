from Image_Scrapper import *
import zipfile
import datetime
from logg import *
from Cassandra import con
from Upload_to_drive  import upload_to_drive_object
import threading as th



class JobCompletedError(Exception):

    def __init__(self,msg):
        """
        JobCompletedError user defined class inherited from Exception class
        :param msg: Error message
        """
        self.msg = msg



class Timer:

    loop_running_checker = 0
    auto_generate_id = 1

    def __init__(self):
        """
        Timer class, it manage the user's job time(schedular time) , that at which time job will start
        in this class one class method and two class variable present
        """


        log = Logger("*********************** time_handler_Logger *************************")
        log.logging_info("-----------------  time_handler log Started --------------------")
        logger = log.api()


    @classmethod
    def timer(cls,search,count,Time,email):

        """
        Function which is controling all the modules from here
        in this function user job will start at that time at which user want

        :param search: image name
        :param count: number of images user want to download
        :param Time: time at which user want to download
        :param email: email at which images will be send after downloading
        :return: nothing
        """

        if cls.loop_running_checker == 0:

            cls.loop_running_checker  = 1

            print(f"inside if block blocked now {search}")
            con.insert_into_schedular_table(search, count, Time, email,cls.auto_generate_id, "Schedular")
            cls.auto_generate_id += 1
            def working(search_img_name,num_img,time_scheduled,email_to_send,auto_generate_id):
                try:
                    print(f"Started--------------------------------------------------------------{search_img_name}")
                    obj = AdvanceImageDownloader(search_img_name, num_img,'https://www.bing.com/images/search?q=' + search_img_name + '&form=HDRSC2&first=1&tsc=ImageBasicHover',img_links=[],keywords=[])
                    Download(obj)
                    print("Downloading links from website")

                    zipf = zipfile.ZipFile(f'All_img\{search_img_name}.zip', 'w', zipfile.ZIP_DEFLATED)
                    upload_to_drive_object.zip_func('All_img'+'\\' + search_img_name, zipf)
                    zipf.close()
                    print(f"Zip Done {search_img_name}")

                    response = upload_to_drive_object.upload_to_drive(search_img_name+".zip")
                    print(response)
                    print(f"upload done {search_img_name}")
                    print("Upload Done",response)
                    drive_file_id = response.split(":")[2].split(",")[0].strip().strip('"')
                    print(drive_file_id)
                    url = f"https://drive.google.com/file/d/{drive_file_id}/view?usp=sharing"
                    print(url)

                    upload_to_drive_object.send_email(email_to_send,search_img_name,url)
                    print("mail done")

                    print("going to insert into schedular j0b table")
                    con.insert_into_schedular_table(search,count,time_scheduled,email_to_send,auto_generate_id,"Schedular_job_done_table")
                    print("record inserted")

                    con.delete_record_with_condition("id",auto_generate_id )
                    print("Record deleted")

                    print(f'mydb updated {search_img_name}')
                    print(f"All Work Done with {search_img_name}")
                    print(cls.loop_running_checker)

                except Exception as e:
                    con.delete_record_with_condition("id", auto_generate_id)
                    print("Record deleted")
                    print("again same mistake " , e)


            print("calling from main")
            while True:

                try:

                    row = con.agg_min()
                    if (row[0] != None) and (row[2] <= datetime.datetime.now()):

                        work = th.Thread(name='work', target=working, args=(row[0], row[1], row[2], row[3],row[4]))
                        work.start()
                        work.join()

                    elif (row[0] == None):
                        cls.loop_running_checker = 0
                        raise JobCompletedError("All Jobs Completed hihihi")

                except JobCompletedError as e:
                    print(e.msg)
                    break

                except Exception as e:
                    print(e)
                    break


        else:
            print("Outside function recursive")
            time.sleep(1)
            insert = th.Thread(name = 'insert',target= con.insert_into_schedular_table(search, count, Time, email,cls.auto_generate_id, "Schedular"))
            insert.start()
            insert.join()
            cls.auto_generate_id+=1




Timer_object = Timer()