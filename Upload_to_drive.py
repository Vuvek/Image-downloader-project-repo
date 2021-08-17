import os
import sys
import json
import requests
import smtplib
from logg import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText




class Configure_After_Scrapping_Images:

    def __init__(self):

        self.log = Logger("********************* upload_to_drive_Logger *********************")
        self.log.logging_info("-----------------  upload to drive Log Started --------------------")
        self.logger = self.log.upload_to_drive()

        self.__login_Email = "vivek.9718470484@gmail.com"
        self.__login_password = "vivek.9718@"
        self.__port = 587


    def send_email(self,email,search,url):
        """
        function to send mail of images

        :param email: email
        :param search: images name
        :param url: url at which send mail
        :return: nothing
        """
        try:
            loginEmail = self.__login_Email
            loginpassword = self.__login_password
            smtp = smtplib.SMTP(host = 'smtp.gmail.com',port = self.__port)
            smtp.starttls()
            smtp.login(loginEmail,loginpassword)

            recName = search
            recEmail = email
            template = f'Please Download The Images Your Job Is Done  \n {url}'
            message = MIMEMultipart()
            template = template.format(recName)

            message['From'] = loginEmail
            message['To'] = recEmail
            message['Subject'] = "This Mail Is From Advance Images Downloader"

            message.attach(MIMEText(template,'plain'))

            smtp.send_message(message)
            print("mail done")
            self.logger.info("Print Mail Send Successfully")
            del message
            # smtp.quit()
        except Exception as e:
            self.logger.info("Error Occurred while sending the mail")
            print(e)





    def zip_func(self,path, ziph):

        """

        :param path: path of folder you want to zip
        :param ziph: ziph is zipfile handle
        :return: nothing
        """

        try:
            for root, dirs, files in os.walk(path):
                for file in files:
                    ziph.write(os.path.join(root, file),
                               os.path.relpath(os.path.join(root, file),
                                               os.path.join(path, '..')))

            self.logger.info("folder zipped successfully")
        except Exception as e:
            self.logger.info("Error Occured in zip method")
            print(e)





    def upload_to_drive(self,file_name):
        """
         fucntion to upload zip file of images on google drive
         Here in this function i use google drive api version 3
        :param file_name: zip file name
        :return:dictionary
        """
        try:
            headers = {"Authorization": "Bearer ya29.a0ARrdaM-M3Bcoa8Ii6G02EtzGXREMtBuKRdAbrTpR12iXOcbIVwztY0WcAujuvVhgKA3_zzq7E7VCAPsKN9ya0YOBr8z4s4RbThkLayoZIqR6k71Sq5G8sJmfqmS3DDB_YF14-Rq9ycaNmcsaDES4FOxHX0lu"}
            para = {
                "name": file_name,
                "parents": ["1OfoFYiXPF1_rqX-UQ14vBivmW3MhWzyu"]
            }
            files = {
                'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
                'file': open(f"All_img/" + file_name, "rb")
            }
            r = requests.post(
                "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
                headers=headers,
                files=files
            )
            dictionary = r.text
            if "id" in dictionary:
                print("Images uploaded on drive Successfully")
                self.logger.info("Images uploaded on drive Successfully")
                return r.text
            else:
                print("folder id is missing sorry we can not upload images on drive")
                self.logger.info("folder id is missing sorry we can not upload images on drive")
                sys.exit(0)

        except Exception as e:
            self.logger.error("Error Occured in upload_to_drive function")
            print(e)


upload_to_drive_object = Configure_After_Scrapping_Images()



