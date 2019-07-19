import csv
import json
import os
import requests
import sys
from io import StringIO
from utils import CacheManager
from pathlib import PurePath
from configparser import ConfigParser, MissingSectionHeaderError, NoSectionError
requests.packages.urllib3.disable_warnings()

class SalesForce(object):

    def __init__(self, params, verify=False):
        """
        """
        self.params = params
        self.params['grant_type'] = 'password'
        token = self.params.pop('token')
        self.params['password'] = self.params['password'] + token
        self.url = 'https://' + params.pop('url')
        
        self.verify = verify
        self._headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    def login(self):
        _auth_url = self.url + '/services/oauth2/token'
        _resp = requests.post(_auth_url, 
                                headers=self._headers, 
                                data=self.params, 
                                verify=self.verify)
        try:
            _access_token = _resp.json()['access_token']
        except KeyError:
            print('Authentication failed: {}'.format(_resp.text))
            sys.exit()
            
        self._headers = {'Content-Type': 'application/json',
                          'Authorization':'Bearer ' + _access_token}
        
    def get_logfiles(self, event_type):
        """
        
        """
        _q_url = self.url + '/services/data/v32.0/query'
        _query = ('?q=SELECT+Id+,+EventType+,+LogFile+,+LogDate+,'
                   '+LogFileLength+FROM+EventLogFile+WHERE+'
                   "+EventType+=+'{}'").format(event_type)
        _log_url = _q_url + _query        
        _resp = requests.get(_log_url,
                                headers=self._headers,
                                verify=self.verify)
        return _resp.json()

    def get(self, uri):
        """
        Send a GET request to SFDC.
        
        Args:
        
        uri(str): full uri to be appended on the GET request
        Example:
            '/services/data/v32.0/sobjects/EventLogFile/0AT4P0000079fTSWAY/LogFile'
        
        Exceptions:
            ValueError: if the URI doesn't start with a slash /.
        """
        
        if not uri.startswith('/'):
            raise ValueError('Full URI prefixed with / as required.')
        _get_url = self.url + uri
        _resp = requests.get(_get_url, 
                              headers=self._headers, 
                              verify=self.verify)
        
        return _resp.text

def write_json(filename, data):
    with open(filename, 'w') as open_data_file:
        for row in data:
            print(json.dumps(row, sort_keys=True), file=open_data_file)

def check_path(path):
    if not os.path.isdir(path):
        os.makedirs(path)

def get_config(filename):        
    config = ConfigParser()

    try:
        config.read(filename)
    except MissingSectionHeaderError:
        print('Section Header [default] required in config file.')
        sys.exit(1)
    
    config = dict(config['default'])

    params = ['url', 'username', 'password', 'token', 
                'client_id', 'client_secret']

    for param in params:
        if param not in config.keys():
            print('Required setting: {} not found in config file'.format(param))
            sys.exit(1)
    
    return config


        
def main():
    
    config_file = 'creds.ini'
    params = get_config(config_file)
        
    try:
        output_path = params['output_path']
    except KeyError:
        output_path = 'logs'
    
    output_path = PurePath(output_path)
    
    check_path(output_path)
    
    global LOG_CACHE
    sf = SalesForce(params)
    print('Logging into Salesforce.')
    sf.login()

    event_types = ['API', 'Login', 'Logout']

    new_log_urls = []
    new_logs = 0
    
    for event_type in event_types:
        logfiles = sf.get_logfiles(event_type)
        print('{} {} log files available.'
                  .format(len(logfiles['records']), event_type))
        for loginfo in logfiles['records']:
            if loginfo['Id'] in LOG_CACHE: continue
            new_logs += 1 
            LOG_CACHE.add(loginfo['Id'])
            log_url = loginfo['LogFile']
            # Store event_type + LogDate for filename.
            log_date = loginfo['LogDate'].replace(':', '-').split('.')[0]
            filename = ''.join(['sf', event_type, '_', log_date, '.json'])
            new_log_urls.append((log_url, filename))

        print('{} {} files are new. Downloading.'.format(new_logs, event_type))
        new_logs = 0
            
    if not new_log_urls:
        print('No new logs found. Exiting.')
        sys.exit()
    
    # Download the log files
    log_blobs = []
    for url, filename in new_log_urls:
        log_blobs.append((sf.get(url), filename))
    
    # Convert the CSV files to JSON
    csvlogs = []
    for blob in log_blobs:
        logfile = StringIO(blob[0])
        filename = PurePath(output_path, blob[1])
        csvlogs.append((csv.DictReader(logfile, delimiter=','), filename))
    
    for file, filename in csvlogs:
        write_json(filename, file)
    print('\n{} new log files written.'.format(len(csvlogs)))

if __name__ == "__main__":
    try:
        LOG_CACHE = CacheManager('logfiles', maxlen=200)
        main()
    except KeyboardInterrupt:
        print("Control-C Pressed, stopping...")
        sys.exit()
    finally:
        LOG_CACHE.write()