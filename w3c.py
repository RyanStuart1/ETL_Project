import datetime as dt
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging


def clean_userAgent(user_agent):
    # Reduces user agent to the browser

    # not in ua ensures that the chrome is used as edge is built into chromium,
    # same applies to safari as it is based on a chrome WebKit.

    ua = user_agent.lower()
    if "chrome" in ua and "edge" not in ua:
        return "chrome"
    elif "firefox" in ua:
        return "firefox"
    elif "safari" in ua and "chrome" not in ua:
        return "safari"
    elif "edge" in ua:
        return "edge"
    elif "googlebot" in ua:
        return "googlebot"
    elif "bingbot" in ua:
        return "bingbot"
    elif "yandex" in ua:
        return "yandexbot"
    elif "mozilla" in ua:
        return "mozilla"
    else:
        return ua.split(" ")[0]

# define (local) folders where files will be found / copied / staged / written
WorkingDirectory = "/home/ryan/w3c"
LogFiles = WorkingDirectory + "/LogFiles/"
StagingArea = WorkingDirectory + "/StagingArea/"
StarSchema = WorkingDirectory + "/StarSchema/"

# Create a String for a BASH command that will extract / sort unique IP 
# addresses from one file, copying them over into another file
uniqIPsCommand = "sort -u " + StagingArea + "RawIPAddresses.txt > " + StagingArea + "UniqueIPAddresses.txt"

# Another BASH command, this time to extract unique Date values from one file into another
uniqDatesCommand = "sort -u " + StagingArea + "RawDates.txt > " + StagingArea + "UniqueDates.txt"

# BASH command for file paths
uniqURICommand = "sort -u " + StagingArea + "RawURIStems.txt > " + StagingArea + "UniqueURIStems.txt"

# BASH command for status of requests
uniqStatusCommand = "sort -u " + StagingArea + "RawStatus.txt > " + StagingArea + "UniqueStatus.txt"

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
        User_agent = Split[9].replace(",","")

        Browser = clean_userAgent(User_agent)

        uriStem = Split[4]

        Status_code = Split[10]

        # create line of text to write to output file, made up of the following: Date,Time,Browser,IP,ResponseTime
        OutputLine = Split[0] + "," + Split[1] + "," + Browser + "," + Split[8] + "," + Split[13] + "," + uriStem + "," + "0," + "0," + Status_code + "\n"

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
        User_agent = Split[9].replace(",","")

        Browser = clean_userAgent(User_agent)

        uriStem = Split[4]

        server_Bytes = Split[15]

        client_Bytes = Split [16]

        Status_code = Split[12]

        # create line of text to write to output file, made up of the following: Date,Time,Browser,IP,ResponseTime
        Out = Split[0] + "," + Split[1] + "," + Browser + "," + Split[8] + "," + Split[16] + "," + uriStem + "," + server_Bytes + "," + client_Bytes + "," + Status_code + "\n"

        # write line of text to output file
        OutFact1.write(Out)

# build the fact table
def BuildFactTable():
    # write header row into the fact table
    with open(StagingArea + 'FactTable.txt', 'w') as file:
        file.write("Date,Time,Browser,IP,ResponseTime,File,client_Bytes,Server_Bytes,StatusCode\n")

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
        if firstLine:
            # ignore this line, but record we have found it now
            firstLine = False
        else:
            # split the line into its parts
            Split = line.strip().split(",")

            # ensure there are enough columns to avoid errors
            if len(Split) > 3:
                # get the IP address within the parts of the line
                IPAddr = Split[3].strip()  # Stripping extra spaces/newlines

                # write IP address to output file
                OutputFile.write(IPAddr + "\n")

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

def getURIStemsFromFactTable():
    with open(StagingArea + 'FactTable.txt', 'r') as infile:
        lines = infile.readlines()
    
    with open(StagingArea + 'RawURIStems.txt', 'w') as outfile:
        first_line = True
        for line in lines:
            if first_line:
                first_line = False
                continue
            # Expected order: Date,Time,Browser,IP,ResponseTime,cs-uri-stem
            parts = line.strip().split(',')
            if len(parts) >= 6:
                uri_stem = parts[5].strip()
                outfile.write(uri_stem + "\n")


def getStatusFromFactTable():
    with open(StagingArea + 'FactTable.txt', 'r') as infile:
        lines = infile.readlines()
    
    with open(StagingArea + 'RawStatus.txt', 'w') as outfile:
        first_line = True
        count = 0
        for line in lines:
            if first_line:
                first_line = False
                continue
            parts = line.strip().split(',')
            # Check the number of columns
            if len(parts) == 7:
                # For 14-col log rows, status code is at index 6
                status = parts[6].strip()
            elif len(parts) == 9:
                # For 18-col log rows, status code is at index 8
                status = parts[8].strip()
            else:
                # If unexpected number of fields, skip or handle accordingly
                continue

            outfile.write(status + "\n")
            count += 1
        print("DEBUG: Number of status entries written:", count)



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




def makeLocationDimension():
    """Fetch geolocation data for unique IPs in parallel and store the results."""
    DimTablename = StarSchema + 'DimIPLoc.txt'
    ip_file_path = StagingArea + 'UniqueIPAddresses.txt'

    logging.info(f"Checking file: {ip_file_path}")

    # Ensure the source file exists
    if not os.path.exists(ip_file_path):
        logging.error(f"File {ip_file_path} not found. Exiting task.")
        return

    # Read unique IPs
    with open(ip_file_path, 'r') as infile:
        ip_addresses = [line.strip() for line in infile if line.strip()]

    if not ip_addresses:
        logging.warning("No IP addresses found to process. Skipping task.")
        return

    logging.info(f"Found {len(ip_addresses)} IP addresses.")

    # Ensure we can write to DimIPLoc.txt
    try:
        with open(DimTablename, 'w') as file:
            file.write("IP, country_code, country_name, city, postal, lat, long\n")
        os.chmod(DimTablename, 0o777)
    except OSError:
        logging.error(f"Cannot write to {DimTablename}. Check file permissions.")
        return

    # Function to process an IP
    def fetch_geolocation(ip):
        """Fetches geolocation data for an IP address."""
        url = f'https://geolocation-db.com/jsonp/{ip}'
        try:
            response = requests.get(url, timeout=5)  
            result = response.content.decode()

            # Parse JSON response
            result = result.split("(")[1].strip(")")
            result = json.loads(result)

            return f"{ip},{result.get('country_code', '')},{result.get('country_name', '')},{result.get('city', '')},{result.get('postal', '')},{result.get('latitude', '')},{result.get('longitude', '')}\n"
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for {ip}: {e}")
            return None

    # Process IPs in parallel using ThreadPoolExecutor
    max_workers = 10  
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ip = {executor.submit(fetch_geolocation, ip): ip for ip in ip_addresses}

        for future in as_completed(future_to_ip):
            data = future.result()
            if data:
                results.append(data)

    # Write results to file
    if results:
        with open(DimTablename, 'a') as file:
            file.writelines(results)
        logging.info(f"Location Dimension Table Created with {len(results)} entries.")
    else:
        logging.warning("No results were written to the file.")

def makeRequestDimension():
    DimTablename = StarSchema + 'DimRequest.txt'
    unique_uris_path = StagingArea + 'UniqueURIStems.txt'
    
    # ensure the filepath of unique URIs
    if not os.path.exists(unique_uris_path):
        print(f"File {unique_uris_path} not found. Cannot build Request dimension.")
        return
    
    # Write header
    with open(DimTablename, 'w') as outfile:
        outfile.write("Request_key,URIStem\n")
    
    # Read each unique URI and assign a key
    with open(unique_uris_path, 'r') as infile, open(DimTablename, 'a') as outfile:
        Request_key = 1
        for line in infile:
            uri = line.strip()
            if uri:
                # Write surrogate key + the URI
                out_line = f"{Request_key},{uri}\n"
                outfile.write(out_line)
                Request_key += 1

    print("Request Dimension Table created.")

def makeStatusDimension():
    DimTablename = StarSchema + 'DimStatusCode.txt'
    unique_status_path = StagingArea + 'UniqueStatus.txt'
    
    if not os.path.exists(unique_status_path):
        print(f"File {unique_status_path} not found. Cannot build Status dimension.")
        return
    
    # Write header row for the dimension table
    with open(DimTablename, 'w') as outfile:
        outfile.write("Status_key,sc-status\n")
    
    # Read each unique status and assign a key
    with open(unique_status_path, 'r') as infile, open(DimTablename, 'a') as outfile:
        Status_key = 1
        for line in infile:
            status = line.strip()
            if status:
                out_line = f"{Status_key},{status}\n"
                outfile.write(out_line)
                Status_key += 1
    print("Status Dimension Table created.")



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

# A python operator to copy / extract filepath information from the Fact table
task_getURIStemsFromFactTable = PythonOperator(
    task_id="task_getURIStemsFromFactTable",
    python_callable=getURIStemsFromFactTable,
    dag=dag,
)

# A python operator to copy / extract status code information from the Fact table
task_getStatusFromFactTable = PythonOperator(
    task_id="task_getStatusFromFactTable",
    python_callable=getStatusFromFactTable,
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

# A python operator to build the Request path Dimension based on filepath information
task_makeRequestDimension = PythonOperator(
    task_id="task_makeRequestDimension",
    python_callable=makeRequestDimension,
    dag=dag,
)

# A python operator to build the Status code Dimension based on status request information
task_makeStatusDimension = PythonOperator(
    task_id="task_makeStatusDimension",
    python_callable=makeStatusDimension,
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

# A bash operator that will transform the complete list of URI Stems into
# a file containing only unique URI Stems
task_makeUniqueURIStems = BashOperator(
    task_id="task_makeUniqueURIStems",
    bash_command=uniqURICommand,
    dag=dag,
)

# A bash operator that will transform the complete list of Status codes into
# a file containing only unique status codes
task_makeUniqueStatus = BashOperator(
    task_id="task_makeUniqueStatus",
    bash_command=uniqStatusCommand,
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
task_copyFactTable.set_upstream(task_makeRequestDimension)
task_copyFactTable.set_upstream(task_makeStatusDimension)

task_makeDateDimension.set_upstream(task_makeUniqueDates)
task_makeLocationDimension.set_upstream(task_makeUniqueIPs)
task_makeRequestDimension.set_upstream(task_makeUniqueURIStems)
task_makeStatusDimension.set_upstream(task_makeUniqueStatus)

task_makeUniqueDates.set_upstream(task_getDatesFromFactTable)
task_makeUniqueIPs.set_upstream(task_getIPsFromFactTable)
task_makeUniqueURIStems.set_upstream(task_getURIStemsFromFactTable)
task_makeUniqueStatus.set_upstream(task_getStatusFromFactTable)

task_getDatesFromFactTable.set_upstream(task_BuildFactTable)
task_getIPsFromFactTable.set_upstream(task_BuildFactTable)
task_getURIStemsFromFactTable.set_upstream(task_BuildFactTable)
task_getStatusFromFactTable.set_upstream(task_BuildFactTable)

task_BuildFactTable.set_upstream(task_CopyLogFilesToStagingArea)
