# Base Python Packages
import configparser
import gzip
import time
from io import BytesIO
import json
import logging
import math

# External Packages
import psycopg2
import requests

# Internal Libraries
import db.db_defaults


### Configurations
logging.basicConfig(level = logging.INFO,
                    filename = 'log.log',
                    filemode = 'w',
                    format = '%(asctime)s- %(levelname)s - %(message)s')

def import_configs():
    '''
    Imports all .ini files.
    :return: config parser class containing all .ini data
    '''

    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('config/headers.ini')
    config.read('config/request_filter.ini')
    config.read('config/secrets.ini')

    return config


def gen_base_url(config):
    '''
    Generates a URL based on the request_filter.ini data.
    :param config: Config data from all .ini files
    :return: final_url: String URL
    '''

    base_url = "https://www.brilliantearth.com/lab-diamonds/list/?"
    url_request_filter = dict(config.items('DIAMOND_REQUEST'))

    # Loops through key / pairs specified in config
    url_append = ''
    for key in url_request_filter:
        url_append = url_append + key + "=" + url_request_filter[key] + '&'

    final_url = base_url + url_append

    return final_url


def send_request(config, url: str):
    '''
    Handles all HTTP requests in the script.
    :param config: Config data from all .ini files (used for headers)
    :param url: String containing a URL
    :return: response: request library response item (only returns if response 200; exits otherwise)
    '''

    headers = dict(config.items('HEADERS'))
    req = requests.session()
    response = req.get(url=url, headers=headers)
    response = response_handler(response, url)

    return response


def response_handler(response, url: str):
    '''
    Handles all responses received from requests in the script.
    :param response: Requests library response object
    :param url: String containing a URL
    :return: decoded_response: Python dictionary object containing the JSON response
    '''

    decoded_response = None

    match response.status_code:
        case 200:
            try:
                buffer = BytesIO(response.content)
                with gzip.GzipFile(fileobj=buffer) as f:
                    decompressed_content = f.read()
                    logging.info(f'Response returned in gzip format')
            except:
                # If not gzipped, use the response content as-is
                decompressed_content = response.content
                logging.info(f'Response returned in json format')

            # Decode the JSON content
            decoded_response = json.loads(decompressed_content)

        case 403:
            logging.warning(f"Cloudflare has blocked this request resulting in response code {response.status_code}"
                            f"You must authenticate the cookie used by visiting the link."
                            f"{url}")
            print(f'YOU MUST AUTHENTICATE COOKIE FOR CLOUDFLARE PLEASE VISIT THIS LINK THEN RERUN PROGRAM')
            print(f'{url}')

        case _:
            logging.critical(f"Received a response code that is unable to be handled"
                             f"Response code received = {response.status_code}")
            exit(f'Please check log...')

    return decoded_response


def response_parser(response: dict, db_defaults: dict):
    '''
    Parses the returned json dictionary ensuring all values match with the database table schema.
    If the key is not found in the response it is coerced to a default value.
    :param response: Dictionary containing diamond data from the website
    :param db_defaults: Dictionary outlining the database table schema
    :return: data_tuples: A list of tuples containing parsed / default data about the Diamond
    '''

    # Creating a list to hold all Diamond data (which will be stored in tuples)
    data_tuples = []

    # Formatting the response for the postgres database
    for diamond in response['diamonds']:
        diamond_data_list = []

        for key in db_defaults:
            diamond[key] = diamond.get(key, db_defaults[key])
            diamond_data_list.append(diamond[key])

        # Converting to tuple for ease of db upload
        data_tuples.append(tuple(diamond_data_list))

    return data_tuples


def page_incrementer(url: str, page_number: int):
    '''
    Takes a pre-generated URL and increments the page=X parameter.
    :param url: String containing a URL
    :param page_number: Integer of the current page number. Increments each loop
    :return: url: String containing a URL with an incremented page number
    '''

    # Creates the string we are seeking
    page_num_find = page_number
    str_find = f'&page={page_num_find}&'

    # Creates the string we want to replace (just a page number increment)
    page_num_replace = page_number + 1
    str_repalce = f'&page={page_num_replace}&'

    # Makes replacement
    url = url.replace(str_find, str_repalce)

    return url


def sql_string_generator(db_defaults: dict):
    '''
    Generates a SQL statement using the db_defaults dictionary that matches the table schema.
    :param db_defaults: Dictionary outlining the database table schema
    :return: sql_str: SQL command that will insert data into the database
    '''

    sql_str = 'INSERT INTO be_diamonds '
    sql_str += '('

    for key in db_defaults:
        sql_str += f'{key}, '

    # Trimming the last comma and space
    sql_str = sql_str[:len(sql_str)-2]

    sql_str += ') VALUES ('

    for _ in range(len(db_defaults)):
        sql_str += '%s, '

    # Trimming last comma and space
    sql_str = sql_str[:len(sql_str)-2]

    sql_str += ') '
    sql_str += 'ON CONFLICT DO NOTHING'

    # Example typed-out SQL Statement
    # 'INSERT INTO be_diamonds'
    # '(active, carat, certificate_number, clarity, collection, color, culet, cut, dedicated, depth, '
    # 'diamond_blockchain, diamond_collection, diamond_dor, fluorescence, has_cert, has_v360_video, '
    # 'hearts_and_arrows_diamonds, id, image, index_id, index_name, inventory_location, is_top_sales_price, '
    # 'length, length_width_ratio, measurements, orderby, origin, polish, popular_choice, pre_carat_price, '
    # 'price, product_class, real_diamond_image, receiveby, report, scs_certified, shape, symmetry, title, '
    # 'truly_brilliant, upc, v360_src, valid)'
    # 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '
    # '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    # 'ON CONFLICT DO NOTHING',

    return sql_str


def main():
    # CONSTS
    request_delay = 0


    # Importing configuration parameters
    logging.info("Importing configuration files")
    config = import_configs()
    logging.info("Successfully imported configuration files")

    # Importing table schema and default values
    logging.info("Importing database defaults dictionary")
    db_defaults = db.db_defaults.table_defaults
    logging.info("Successfully imported database defaults")

    # Generate the base URL (page one) of the request
    # This is done to get the "total_count" parameter which will determine
    # how many pages of diamonds must be scraped for completion
    logging.info("Generating starting URL based on parameters in the request_filter.ini file")
    url = gen_base_url(config)
    logging.info("Successfully generated base URL")


    # Requesting the data & saving the total count:
    logging.info("Sending first request to URL")
    response = send_request(config, url)
    logging.info("Successfully received a parsable response from the server")
    total_count = response['total_count']


    # Static number of Diamonds per request
    num_per_page = 50
    num_requests = math.ceil(total_count / num_per_page)
    logging.info(f'Total Diamonds returned under specified parameters: {total_count}')
    logging.info(f'Number of requests to be made during this run: {num_requests}')


    # Setup Database Connection using Psycopg2
    logging.info(f'Setting up database connection using Psycopg2')

    con = psycopg2.connect(
        host=config['DATABASE_CREDS']['host'],
        database=config['DATABASE_CREDS']['database'],
        user=config['DATABASE_CREDS']['user'],
        password=config['DATABASE_CREDS']['password'],
        port=config['DATABASE_CREDS']['port'])

    cur = con.cursor()

    # Setting up the SQL String that will push the data to the PostgreSQL Database
    logging.info(f'Generating SQL Command to push data to PostgreSQL database')
    sql_command = sql_string_generator(db_defaults)
    logging.info(f'SQL generated')

    # Beginning the requesting engine utilizing delay specified above
    logging.info(f'Starting requester with delay of: {request_delay}')


    for page_number in range(num_requests):
        # Sending request to Brilliant Earth & Parsing Data into Tuple for SQL insertion
        response = send_request(config, url)
        data_tuples = response_parser(response, db_defaults)

        for diamond_data in data_tuples:
            cur.execute(f'{sql_command}', diamond_data)

        #TODO Make the Commit Ocurr on Periodic Basis
        # Currently commits 50 Diamonds at a time (once per URL)
        con.commit()

        # Logging & Printing
        logging.info(f'Successfully parsed and uploaded Page {page_number}')
        print(f'Scraping Page {page_number}')

        url = page_incrementer(url, page_number)
        time.sleep(request_delay)

    # Scraping complete, handling end-of-script clean-ups
    logging.info('Closing connection to the database')
    con.close()
    logging.info("Script completed successfully. Script terminating")
    exit("Script completed successfully, log written to main directory")


if __name__ == '__main__':
    main()
