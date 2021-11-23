# *********************************************************************
# Web scraping bloomberg news
# using Lambda and a Headless Chrome
# in combination with AWS EventBridge 
#
# Deployment through AWS CLI and AWS CloudFormation (for
# serverless solutions)
#
# The results are saved in a JSON file in S3 AND in a DynamoDB table
# *********************************************************************
import json
import os, shutil, uuid
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import boto3
import random
import datetime
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import lxml.html


# ***********************************************************
# Moving chromedriver and headless-chromium into tmp folder
# ***********************************************************
def setup():
    BIN_DIR = "/tmp/bin"
    if not os.path.exists(BIN_DIR):
        print("Creating bin folder")
        os.makedirs(BIN_DIR)

    LIB_DIR = '/tmp/bin/lib'
    if not os.path.exists(LIB_DIR):
        print("Creating lib folder")
        os.makedirs(LIB_DIR)
        
    for filename in ['chromedriver', 'headless-chromium']:
        oldfile = f'/opt/{filename}'
        newfile = f'{BIN_DIR}/{filename}'
        shutil.copy2(oldfile, newfile)
        os.chmod(newfile, 0o775)


# *************************************
# Initiating web driver
# *************************************
def init_web_driver():
    setup()
    #options = webdriver.ChromeOptions()
    #options = Options()
    _tmp_folder = '/tmp/{}'.format(uuid.uuid4())

    if not os.path.exists(_tmp_folder):
        os.makedirs(_tmp_folder)

    if not os.path.exists(_tmp_folder + '/user-data'):
        os.makedirs(_tmp_folder + '/user-data')

    if not os.path.exists(_tmp_folder + '/data-path'):
        os.makedirs(_tmp_folder + '/data-path')

    if not os.path.exists(_tmp_folder + '/cache-dir'):
        os.makedirs(_tmp_folder + '/cache-dir')

    # Configuring Headless Chrome
    chrome_options = webdriver.ChromeOptions()
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument('--incognito')
    chrome_options.add_argument('--user-data-dir={}'.format(_tmp_folder + '/user-data'))  # that row generates an error
    chrome_options.add_argument('--hide-scrollbars')
    chrome_options.add_argument('--enable-logging')
    chrome_options.add_argument('--log-level=0')
    chrome_options.add_argument('--v=99')
    chrome_options.add_argument('--single-process')
    chrome_options.add_argument('--data-path={}'.format(_tmp_folder + '/data-path'))
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--homedir={}'.format(_tmp_folder))
    chrome_options.add_argument('--disk-cache-dir={}'.format(_tmp_folder + '/cache-dir'))
    chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
    chrome_options.add_argument('--disable-dev-shm-usage')  # absolutely necessary to run Chrome on Lambda!
    chrome_options.binary_location = "/tmp/bin/headless-chromium"

    driver = webdriver.Chrome(options=chrome_options, executable_path='/tmp/bin/chromedriver')

    return driver


# ******************************************
# Function writing a news into dynamoDB
# ******************************************
def put_news(id, searched_on, source, content, dynamodb=None):
    ''' writing news details into dynamoDB'''
    if not dynamodb:
        #dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.ap-southeast-2.amazonaws.com")
        dynamodb = boto3.resource('dynamodb')
        
    table = dynamodb.Table('airfares.scraping')
    response = table.put_item(
       Item={
            'id': id,
            'searched_on': searched_on,
            'source': source,
            'content': content
        }
    )
    return response


# *********************************************************************************************
# Handler / main function
# The AWS Lambda function handler is the method in your function code that processes events. 
# When your function is invoked, Lambda runs the handler method. 
# *********************************************************************************************
def lambda_handler(event, context):
    #Calling init_web_driver
    driver = init_web_driver()
    
    # Defining the attributes we are interested in
    departure_airport_name = "Sydney"
    arrival_airport_name = "Auckland"
    departure_airport_code = "SYD"
    arrival_airport_code = "AKL"
    departure_date = "2021-08-02"
    arrival_date = "2021-08-08"
    source = "bloomberg.com/markets"
    
    # Query
    driver.get('https://www.'+source)
    body = f"Headless Chrome Initialized, Bloomberg, Page title: {driver.title}"

    # Wait until the page is fully loaded
    time.sleep(10)


    # **********************************
    # S3 bucket hosting our JSON file
    # **********************************
    bucket_name = "comandante7"
    file_name = "bloomberg.json"
    s3_path = "data/" + file_name
    lambda_path = "/tmp/" + file_name
    fileUpdated = ""

    # Declare an S3 client
    s3_client = boto3.client('s3')

    # Check if already existing JSON file in S3
    try:
         # Upload the already existing JSON file from S3 into Lambda temp
        s3_client.download_file(bucket_name, s3_path, lambda_path)    
        fileUpdated = "JSON S3 file already existing"
    except ClientError as e:
        # define empty JSON
        data = [] 
         # Append - Opens a file for appending, creates the file if it does not exist
        with open(lambda_path, 'a', encoding='utf-8') as outfile:  
	        json.dump(data, outfile)
        fileUpdated = "JSON S3 file was not existing (created in Lambda temp)"

    # Creating a JSON object
    # opening JSON file from temp, loading data into data and temp variable
    with open(lambda_path) as json_file: 
        data = json.load(json_file) 
        temp = data


    # ************************************************
    # Web scraping the content of the page.
    # Here, we are looking for a div class called "multibook-dropdown", which contains
    # various span class called "price-text"
    # ************************************************

    # Retrieving actual datetime/timestamp
    now = datetime.datetime.now()
    dateTime =now.strftime("%Y-%m-%d %H:%M:%S")

    page_source = driver.page_source

    # the randomID will be used in the creation of a primary key for each and every dynamoDB table items
    randomID = 0

    # Beautiful Soup loads the page source
    soup = BeautifulSoup(page_source, 'lxml')

    # Extracting news by iterating through all "H3" tags (with "story-package-module__story__headline" classes)
    news_selector = soup.find_all('h3', class_='story-package-module__story__headline')

    # Looping through all the aforementioned tags
    for news_selector in news_selector:

        # Retrieving the content
        content_span = news_selector.find('a', class_='story-package-module__story__headline-link')
        content = content_span.get_text()"
        
        # python object to be appended
        newObject = {"id": ""+str(dateTime)+"-result"+str(randomID)+"", 
                 "source": ""+source+"",
                 "content": ""+content+"",
                 "searched_on": ""+dateTime+"",
                }  
        
        # appending data to temp JSON variable
        temp.append(newObject)
              
        # calling the put_news method to write a news into dynamoDB
        news_to_be_added = put_news(str(dateTime)+"-result"+str(randomID), dateTime, source, content)
        
        # Increment our randomID
        randomID += 1


    # Saving updated JSON file back into tmp folder
    # 'w' as we want to overwrite any existing content
    with open(lambda_path, 'w', encoding='utf-8') as f:   
        json.dump(temp, f) # indent=4
    try:
        # Saving our updated JSON file back in S3
        response = s3_client.upload_file(lambda_path, bucket_name, s3_path)
    except ClientError as e:
        logging.error(e)
        return False



    driver.close()
    driver.quit()

    response = {
        "statusCode": 200,
        "body": body,
        "JSON file status": fileUpdated
    }

    # Printed output
    return response
