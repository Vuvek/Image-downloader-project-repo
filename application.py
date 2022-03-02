from flask import Flask,render_template,request
'''
from time_handler import Timer_object
import  threading as th
from logg import *
from datetime import datetime , timedelta
'''



application = Flask(__name__)


@application.route('/')
def home():
    """
    fuction to show the user first web page
    This function is also creating Logger class object
    :return: index.html
    """

    #log = Logger("*********************** API_Logger *************************")
    #log.logging_info("-----------------  API log Started --------------------")
    #logger = log.api()
    try:
        return render_template("index.html")
    except Exception as e:
        #logger.info("Error Occurred in home during calling Login web page")
        pass


@application.route('/download',methods = ['GET','POST'])
def download():
    """
    function to get and post request from the web page
    :return: error.html if user gives time less than the current datetime else return submit.html
    """

    global images_download,object , search, count,Time,email

    if request.method == 'POST':
        search = request.form['image_name']
        count = int(request.form['number_image'])
        Time = request.form['time']
        email = request.form['email']

        date = Time.split('T')[0]
        time_of_web = Time.split('T')[1]
        date_time_str = f"{date.split('-')[2]}/{date.split('-')[1]}/{date.split('-')[0][-2:]} {time_of_web.split(':')[0]}:{time_of_web.split(':')[1]}:00"
        Time = datetime.strptime(date_time_str, '%d/%m/%y %H:%M:%S')

        if (Time+timedelta(seconds=60)) >= (datetime.now()):
            images_dowload = th.Thread(target=Timer_object.timer,args=(search,count,Time,email))
            images_dowload.start()
            return render_template('submit.html',Time = Time)
        else:
            return render_template("error.html")



if __name__ == '__main__':
    application.run()



