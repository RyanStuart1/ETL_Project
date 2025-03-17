import datetime as dt
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json

# define (local) folders where files will be found / copied / staged / written
WorkingDirectory = "/home/craig/w3c"
LogFiles = WorkingDirectory + "/LogFiles/"
StagingArea = WorkingDirectory + "/StagingArea/"
StarSchema = WorkingDirectory + "/StarSchema/"

# Create a String for a BASH command that will extract / sort unique IP 
# addresses from one file, copying them over into another file
uniqIPsCommand = "sort -u " + StagingArea + "RawIPAddresses.txt > " + StagingArea + "UniqueIPAddresses.txt"

# Another BASH command, this time to extract unique Date values from one file into another
uniqDatesCommand = "sort -u " + StagingArea + "RawDates.txt > " + StagingArea + "UniqueDates.txt"

# Another BASH command, this time to copy the Fact Table that is produced from the Staging area to the resultant folder
copyFactTableCommand = "cp " + StagingArea + "FactTable.txt " + StarSchema + "FactTable.txt"

# prior to any processing, make sure the expected directory structure is in place for files
try:   
   os.mkdir(WorkingDirectory)
except:
   print("Can't make WorkingDirectory")
try:
   os.mkdir(LogFiles)
except:
   print("Can't make LogFiles folder") 
try: 
   os.mkdir(StagingArea)
except:
   print("Can't make StagingArea folder") 
try:
   os.mkdir(StarSchema)
except:
   print("Can't make StarSchema folder") 


# Copy data from a given log file into the staging area.
# The content of the log file will be appended to a file
# in the staging area that will eventually contain the data
# from ALL log files combined.
# Note: the log files may contain comment lines in them, 
# e.g., beginning with a '#' hash. These are ignored / not
# copied to the output file during the copy process
def CopyDataFromLogFileIntoStagingArea(nameOfLogFile):
    print('Copying content from log file', nameOfLogFile)
    #logging.warning('Cleaning '+nameOfLogFile)
    #print (uniqCommand)

    # extra check: get the suffix from the log filename, e.g., '.log'
    suffix = nameOfLogFile[-3:len(nameOfLogFile)]

    # if file suffix is 'log', i.e., it is indeed a 'log' file
    # and not anything else (no point in introducing unwanted data here)
    if (suffix=="log"):
    
        # we have a log file to process
        # it may contain 14 cols or 18 cols
        # we will append the file content into an output file in the StagingArea

        # open output file(s) in the StagingArea to append data into, i.e., to append
        # the lines of data we are reading in from the log file. There is an output file 
        # to store the data being read from the 14-col log files, and another output file
        # to store data being read from the 18-col log files
        OutputFileFor14ColData = open(StagingArea + 'OutputFor14ColData.txt', 'a')
        OutputFileFor18ColData = open(StagingArea + 'OutputFor18ColData.txt', 'a')

        # open the input file, i.e, the log file we want to read data from
        InFile = open(LogFiles + nameOfLogFile, 'r')
    
        # read in the lines / content of the log file
        Lines = InFile.readlines()

        # for each line read in from the log file, one at a time
        for line in Lines:
            # if the line doesn't begins with a '#' character
            if (line[0]!="#"):
                # it is a valid line to process
                
                # check how many cols are in the data
                # each column in a row may be separated by a space
                # split the next line of data in the file based on spaces 
                Split=line.split(" ")
                
                # if the length of the split is 14
                if (len(Split)==14):
                   # write line of data into the output file for 14-col data
                   OutputFileFor14ColData.write(line)
                else:
                   if (len(Split)==18):
                       # write line of data into the output file for 18-col data
                       OutputFileFor18ColData.write(line)
#                        print('Long ',filename,len(Split))
                   else:
                       # unrecognised file format
                       print ("Fault: unrecognised column number " + str(len(Split)))

                # note: the above process can probably be done more efficiently
                # if we know that all lines in the file have either 14 cols or 18 cols,
                # we don't need to keep checking this on a line by line basis
                # however, at the same time, we can't assume that the file isn't malformed,
                # e.g., where some lines may be shorter, some data may be missing


# clear the content of any files in the staging area - opening the file
# with 'write' mode instead of 'append' mode will effectively truncate
# its content to zero
def EmptyOutputFilesInStagingArea():
    OutputFile14Col = open(StagingArea + 'OutputFor14ColData.txt', 'w')
    OutputFile18Col = open(StagingArea + 'OutputFor18ColData.txt', 'w')

# copy (the content of) all log files into the staging area
def CopyLogFilesToStagingArea():
   # get a list of all files in the 'log' files folder - these are the 'raw'
   # input to our process
   arr = os.listdir(LogFiles)
   
   # if no files are found
   if not arr:
      # display an error notification
      print('No files found in Log Files folder')

   # clear/empty the content of output files in the staging area
   # where we will be copying the content of the log files into
   EmptyOutputFilesInStagingArea()

   # for each log file 'f' found in the log files folder
   for f in arr:
       # copy the content of this next log file 'f' over into the output file(s) in the staging area
       CopyDataFromLogFileIntoStagingArea(f)
       
# add / append data from the 14-col files into the Fact table
def Add14ColDataToFactTable():
    # open output file that contains all 14-col data aggregated
    InFile = open(StagingArea + 'OutputFor14ColData.txt','r')

    # open Fact table to write / append into
    OutFact1 = open(StagingArea + 'FactTable.txt', 'a')

    # read in all lines of data from input file (14-col data)
    Lines= InFile.readlines()

    # for each line in the input file
    for line in Lines:
        # split line into columns
        Split=line.split(" ")

        # among other things, the line of data has the following: Date,Time,Browser,IP,ResponseTime

        # do some reformatting of the browser field if required, to remove ',' chars from it
        Browser = Split[9].replace(",","")

        # create line of text to write to output file, made up of the following: Date,Time,Browser,IP,ResponseTime
        OutputLine = Split[0] + "," + Split[1] + "," + Browser + "," + Split[8] + "," +Split[13] + "\n"

        # write line of text to output file
        OutFact1.write(OutputLine)

# add / append data from the 18-col files into the Fact table
def Add18ColDataToFactTable():
    # open output file that contains all 14-col data aggregated
    InFile = open(StagingArea + 'OutputFor18ColData.txt','r')

    # open Fact table to write / append into
    OutFact1 = open(StagingArea + 'FactTable.txt', 'a')

    # read in all lines of data from input file (18-col data)
    Lines = InFile.readlines()

    # for each line in the input file
    for line in Lines:
        # split line into columns
        Split = line.split(" ")

        # do some reformatting of the browser field
        Browser = Split[9].replace(",", "")

        # create line of text to write to output file, made up of the following: Date,Time,Browser,IP,ResponseTime
        Out = Split[0] + "," + Split[1] + "," + Browser + "," + Split[8] + "," + Split[16] + "\n"

        # write line of text to output file
        OutFact1.write(Out)

# build the fact table
def BuildFactTable():
    # write header row into the fact table
    with open(StagingArea + 'FactTable.txt', 'w') as file:
        file.write("Date,Time,Browser,IP,ResponseTime\n")

    # add / append data from 14-col log files into Fact table
    Add14ColDataToFactTable()

    # add / append data from 18-col log files into Fact table
    Add18ColDataToFactTable()

# copy / extract all IP addresses from the Fact Table
# eventually, these will be used to create and populate
# a Dimension table for the IP / Location. This is just
# a first stage in processing to acheive this. Initially,
# ALL ip addresses will be copied from the Fact table 
# which means some of them may be duplicates / non-unique.
# This will be resolved in a subsequent stage
def getIPsFromFactTable():
    # open the fact table (as it contains all rows of data)
    InFile = open(StagingArea + 'FactTable.txt', 'r')

    # open file to write IP data into
    OutputFile = open(StagingArea + 'RawIPAddresses.txt', 'w')

    # read all lines from input file
    Lines = InFile.readlines()

    # treat first line differently, it's a 'header' row
    firstLine = True

    # for each line / row of data
    for line in Lines:
        if firstLine == True:
            # ignore this line, but record we have found it now
            firstLine = False
        else:
            # split the line into its parts
            Split = line.split(",")

            # get the IP address within the parts of the line
            IPAddr = Split[3] + "\n"

            # write IP address to output file
            OutputFile.write(IPAddr)

# copy / extract all dates from the Fact Table
# eventually, these will be used to create and populate
# a Dimension table for the dates. This is just
# a first stage in processing to acheive this. Initially,
# ALL dates will be copied from the Fact table 
# which means some of them may be duplicates / non-unique.
# This will be resolved in a subsequent stage
def getDatesFromFactTable():
    # open fact table
    InFile = open(StagingArea+ 'FactTable.txt', 'r')

    # open output file to write dates into
    OutputFile = open(StagingArea + 'RawDates.txt', 'w')

    # read lines from input file
    Lines= InFile.readlines()

    # treat first line differently, it's a 'header' row
    firstLine = True

    # for each line / row of data
    for line in Lines:

        if firstLine == True:
            # ignore this line, but record we have found it now
            firstLine = False
        else:
            # split into individual elements
            Split = line.split(",")

            # get date component within line of data
            DateInfo = Split[0] + "\n"

            # write data to output file
            OutputFile.write(DateInfo)

# define days of the week - used in routine(s) below
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

# create / build a dimension table for the date information
def makeDateDimension():
    # open file that contains dates extracted from the fact table, subsequently made unique
    InDateFile = open(StagingArea + 'UniqueDates.txt', 'r')   

    # open output file to write date dimension data into
    OutputDateFile = open(StarSchema + 'DimDateTable.txt', 'w')

    # write a header row into the output file for constituent parts of the date
    with OutputDateFile as file:
       file.write("Date,Year,Month,Day,DayofWeek\n")

    # get lines of data from input file (where each 'line' will be a Date string)
    Lines = InDateFile.readlines()
    
    # for each line / date
    for line in Lines:
        # remove any new line that may be present
        line=line.replace("\n","")

        print(line) # remove?

        # if the line isn't empty
        if (len(line) > 0):  
            # try the following
            try:
                # get the date from the line of text, e.g., year, month, day
                date = datetime.strptime(line,"%Y-%m-%d").date()

                # get the weekday as a string, e.g., 'Monday', 'Tuesday', etc.
                weekday = Days[date.weekday()]

                # create line of text to write to output file with the different components of the date
                # each line / row will have the original date string [key], year, month, day, weekday
                out = str(date) + "," + str(date.year) + "," + str(date.month) + "," + str(date.day) + "," + weekday + "\n"

                # write / append the date information to the output file            
                with open(StarSchema + 'DimDateTable.txt', 'a') as file:
                    file.write(out)
            except:
                print("Error with Date") # report error in case of exception

# create / build a dimension table for the 'location' information derived from IP addresses
def makeLocationDimension():
    # define path to the file that will store the location dimension
    DimTablename = StarSchema + 'DimIPLoc.txt'

    # is the following needed?
    # try:
    #    file_stats = os.stat(DimTablename)
    #
    #    if (file_stats.st_size >2):
    #       print("Dim IP Table Exists")
    #       return
    # except:
    #    print("Dim IP Table does not exist, creating one")    
    
    # open file in staging area that contains the uniquie IP addresses extracted from the Fact table
    InFile = open(StagingArea + 'UniqueIPAddresses.txt', 'r')
    #    OutFile=open(StarSchema + 'DimIPLoc.txt', 'w') <- needed?

    # write a header row into the output file for constituent parts of the location
    with open(StarSchema + 'DimIPLoc.txt', 'w') as file:
               file.write("IP, country_code, country_name, city, lat, long\n")
    
    # read in lines / IP addresses from file
    Lines = InFile.readlines()

    # for each line / IP address in the file
    for line in Lines:
        # remove any new line from it
        line = line.replace("\n","")

        # if the line isn't empty
        if (len(line) > 0):
            # define URL of API to send the IP address to, in return for detailed location information
            request_url = 'https://geolocation-db.com/jsonp/' + line

            # Send request and decode the result
            try:
                response = requests.get(request_url)
                result = response.content.decode()
            except:
                print ("Error reponse from geolocation API: " + result)
            
            # process the response
            try:
                # Clean the returned string so it just contains the location data for the IP address
                result = result.split("(")[1].strip(")")
                
                # Convert the location data into a dictionary so that individual fields can be extracted
                result  = json.loads(result)

                # create line of text to write to output file representing the location Dimension
                # each line / row will have the original IP address [key], country code, country name, city, lat, long
                outputLine = line + "," + str(result["country_code"]) + "," + str(result["country_name"]) + "," + str(result["city"]) + "," + str(result["latitude"]) + "," + str(result["longitude"]) + "\n"

                # write / append the line to the output file
                with open(StarSchema + 'DimIPLoc.txt', 'a') as file:
                    file.write(outputLine)
            except:
                print ("error getting location")

# the DAG - required for Apache Airflow
dag = DAG(                                                     
   dag_id="Process_W3_Data",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2023, 2, 24), 
   catchup=False,
)

## TASKS
# A python operator to copy data from the log files into the staging area
task_CopyLogFilesToStagingArea = PythonOperator(
   task_id="task_CopyLogFilesToStagingArea",
   python_callable=CopyLogFilesToStagingArea, 
   dag=dag,
)

# A python operator to copy / extract IP address data from the Fact table
task_getIPsFromFactTable = PythonOperator(
    task_id="task_getIPsFromFactTable",
    python_callable=getIPsFromFactTable,
    dag=dag,
)

# A python operator to copy / extract date information from the Fact table
task_getDatesFromFactTable = PythonOperator(
    task_id="task_getDatesFromFactTable",
    python_callable=getDatesFromFactTable,
    dag=dag,
)

# A python operator to build the Location Dimension based on IP addresses
task_makeLocationDimension = PythonOperator(
    task_id="task_makeLocationDimension",
    python_callable=makeLocationDimension,
    dag=dag,
)

# A python operator to build the Fact table from data contained in the log files
task_BuildFactTable = PythonOperator(
   task_id="task_BuildFactTable",
   python_callable= BuildFactTable,
   dag=dag,
)

# A python operator to build the Date Dimension based on date information
task_makeDateDimension = PythonOperator(
   task_id="task_makeDateDimension",
   python_callable=makeDateDimension, 
   dag=dag,
)

# A bash operator that will transform the complete list of original IP addresses into
# a file containing only unique IP addresses
task_makeUniqueIPs = BashOperator(
    task_id="task_makeUniqueIPs",
    bash_command=uniqIPsCommand,
    dag=dag,
)

# A bash operator that will transform the complete list of original dates into
# a file containing only unique dates
task_makeUniqueDates = BashOperator(
    task_id="task_makeUniqueDates",
    bash_command=uniqDatesCommand,
    dag=dag,
)

# a bash operator that will copy the Fact table from its temporary location in the 
# Staging Area (where it is used during the creation of Dimension tables) into the Star Schema location
task_copyFactTable = BashOperator(
    task_id="task_copyFactTable",
    bash_command=copyFactTableCommand,
#     bash_command="cp /home/airflow/gcs/data/Staging/OutFact1.txt /home/airflow/gcs/data/StarSchema/OutFact1.txt",
    dag=dag,
)
 
# usually, you can set up your ETL pipeline as follows, where each task follows on from the previous, one after another:
# task1 >> task2 >> task3  

# if you want to have tasks working together in parallel (e.g., if we wanted the IP address processing
# to be occurring at the same time as the Date processing was occurring), we need to define the 
# pipeline in a different way, making clear which tasks are 'downstream' of each other (occurring after) 
# or 'upstream' of each other (required to occur before)
# for example, we could define a structure as follows:
#
#                                                        -> task_getDatesFromFactTable -> task_makeUniqueDates -> task_makeDateDimension
# task_CopyLogFilesToStagingArea -> task_BuildFactTable                                                                                     -> task_copyFactTable
#                                                        -> task_getIPsFromFactTable -> task_makeUniqueIPs -> task_makeLocationDimension
#
# In the above, we could say the following: task_copyFactTable is 'downstream' of task_makeDateDimension
# OR, we could say that task_makeDateDimension is 'upstream' of task_copyFactTable
# 
# There are methods we can call to set up these dependencies. E.g., for the above, we could do:
# task_copyFactTable.set_upstream(task_makeDateDimension)
# OR
# task_makeDateDimension.set_downstream(task_copyFactTable)
#
# If TaskA has both TaskB and TaskC upstream of it, TaskA will only commence when BOTH TaskB and TaskC have completed before it.
#
# setting up the tasks below, working back from right to left:

task_copyFactTable.set_upstream(task_makeDateDimension)
task_copyFactTable.set_upstream(task_makeLocationDimension)

task_makeDateDimension.set_upstream(task_makeUniqueDates)
task_makeLocationDimension.set_upstream(task_makeUniqueIPs)

task_makeUniqueDates.set_upstream(task_getDatesFromFactTable)
task_makeUniqueIPs.set_upstream(task_getIPsFromFactTable)

task_getDatesFromFactTable.set_upstream(task_BuildFactTable)
task_getIPsFromFactTable.set_upstream(task_BuildFactTable)

task_BuildFactTable.set_upstream(task_CopyLogFilesToStagingArea)

