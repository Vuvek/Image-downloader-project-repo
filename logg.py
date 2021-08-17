import logging



class Logger():

    """
    Logger Class :
    It will log every thing with level name
    """

    def __init__(self , logger_name):
        logging.basicConfig(filename="image_scrapper.log", level=logging.INFO,
                                format='%(asctime)s %(name)s %(levelname)s %(message)s', filemode='w')

        self.logger_name = logger_name

        self.__console_log = logging.StreamHandler()
        self.__console_log.setLevel(logging.INFO)
        self.__formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s ")
        self.__console_log.setFormatter(self.__formatter)
        logging.getLogger().addHandler(self.__console_log)


    def cassandra(self):
        """
            function to handle cassandra loggers
        """

        loggger_Cassandra_db = logging.getLogger(self.logger_name)
        return loggger_Cassandra_db


    def api(self):
        """
            function to handle api loggers
        """
        api_logger = logging.getLogger(self.logger_name)
        return api_logger

    def image_scrapper(self):
        """
            function to handle Image_Scrapper loggers
        """

        image_scrapper_logger = logging.getLogger(self.logger_name)
        return image_scrapper_logger

    def upload_to_drive(self):
        """
        function to handle upload_to_drive logger
        :return: logging object
        """

        upload_to_drive_logger = logging.getLogger((self.logger_name))
        return upload_to_drive_logger


    def logging_info(self, log):
        """
            This function is for seprating the each logger
        """

        log = "\n\n"+str(log)+"\n\n"
        logging.info(str(log))





