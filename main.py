from urllib3.util import current_time
connect_string = "mongodb://localhost:20001"
import pymongo
from datetime import datetime, date


class Application_log:
    def __init__(self):
        pass
    def log(self, log_file, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H%M%S")
        log_file.write(str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message + "\n")

class mongo:
    def __init__(self, connect_string):
        self.connect = connect_string
        self.logger = Application_log()
    def mongo_connection(self):
        try:
            log_file = open("Logs/database_Connection.txt", 'a+')
            client = pymongo.MongoClient(self.connect)
            self.logger.log(log_file," Database connected successfully!!\t")
            log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t \t")
            return client
        except Exception as e:
            log_file = open("Logs/database_Connection.txt", 'a+')
            self.logger.log(log_file=log_file,log_message="Databasae not connected")
            log_file.write("Current Date :: %s" %date +"\t" +"Current time:: %s" % current_time + "\t \t" + "No connection established"+ "\t \t")
            log_file.close()

    def creating_db_collection(self, DATABASE_NAME,COLLECTION_NAME):
        try:
            Collection_List = self.mongo_connection()[DATABASE_NAME].list_collection_names() ## show collection command
            log_file = open("Logs/database_log.txt", 'a+')
            mongo_connection = self.mongo_connection()
            ListOfAllDatabases = mongo_connection.list_database_names()  ## query give list of present databases
            if DATABASE_NAME not in ListOfAllDatabases:
                Database = mongo_connection[DATABASE_NAME]
                self.logger.log(log_file, " %s: database created :" % DATABASE_NAME)
                Collection = Database[COLLECTION_NAME]  # COLLECTION_NAME is as table name
                self.logger.log(log_file, " %s: table created by name:" % COLLECTION_NAME)
                log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t \t"  + "\n")
                return Collection

            elif DATABASE_NAME in ListOfAllDatabases and COLLECTION_NAME not in Collection_List:
                DATABASE_NAME = mongo_connection[DATABASE_NAME]
                Collection = DATABASE_NAME[COLLECTION_NAME]  # COLLECTION_NAME is as table name
                self.logger.log(log_file, " %s: similar db exist in table created by name:" % COLLECTION_NAME)
                log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t \t" + "\n")
                return Collection

            elif DATABASE_NAME in ListOfAllDatabases and COLLECTION_NAME in Collection_List:
                DATABASE_NAME = mongo_connection[DATABASE_NAME]
                COLLECTION_NAME = DATABASE_NAME[COLLECTION_NAME]  # COLLECTION_NAME is as table name
                self.logger.log(log_file, " %s: similar db exist in table created by name:" % COLLECTION_NAME)
                log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t \t" + "\n")
                return COLLECTION_NAME

            else:
                self.logger.log(log_file,"BUG")
                log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t \t"  + "\n")


        except Exception as e:
            log_file = open("Logs/database_log.txt", 'a+')
            self.logger.log(log_file, "error in database creation:: %s" % e)
            log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t\n" )
            log_file.close()

    def insert_one_record(self,DATABASE_NAME, COLLECTION_NAME, RECORD):
        try:
            log_file = open("Logs/database_log_recordFile.txt", 'a+')
            Record = self.creating_db_collection(DATABASE_NAME,COLLECTION_NAME)
            Record.insert_one(RECORD)
            self.logger.log(log_file, "one_record entered:: %s" % RECORD)
            log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t\n")
            log_file.close()
            print("rec inserted")
        except Exception as e:
            self.logger.log(log_file, "Check db_NAME OR COLLECTION_NAME ALREADY EXIST:: %s" % e)
            log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t\n")
            log_file.close()

    def Insert_Multiple_Record(self,DATABASE_NAME,COLLECTION_NAME,MULTIPLE_RECORD):
        try:
            log_file = open("Logs/database_log_recordFile.txt", 'a+')
            Multiple_Record = self.creating_db_collection(DATABASE_NAME, COLLECTION_NAME)
            Multiple_Record.insert_many(MULTIPLE_RECORD)
            self.logger.log(log_file, "one_record entered:: %s" % MULTIPLE_RECORD)
            log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t\n")
            log_file.close()
            print("rec inserted")
        except Exception as e:
            self.logger.log(log_file, "Error inserting multiple record:: %s" % e)
            log_file.write("Current Date :: %s" % date + "\t" + "Current time:: %s" % current_time + "\t\n")
            log_file.close()














##inserting multiple record into the collection
multiple_record = [
        # record 1
        {
            "Name": "manoj singh negi",
            "Product_using": "AI",
            "note": {"course": "data_sci", "date": "13-06-2022"}
        },

        # record 2
        {
            "Name": "manoj singh negi",
            "Product_using": "AI"
        },

        # record 3
        {
            "note": [{"course": "data_sci", "date": "13-06-2022"}, [{"data": "AI"}]]
        },

        # record 4
        {
            "Name": "manoj singh negi",
            "Product_using": "AI"
        }
]




mongo(connect_string).Insert_Multiple_Record(DATABASE_NAME="DATABASE_LIBRARY_2", COLLECTION_NAME='SELF_2',MULTIPLE_RECORD=multiple_record)