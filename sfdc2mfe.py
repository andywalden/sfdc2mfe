import csv
import json
import os
import requests
import sys
from io import StringIO
from utils import CacheManager
from pathlib import PurePath
from configparser import ConfigParser, MissingSectionHeaderError, NoSectionError
from datetime import datetime, timedelta
requests.packages.urllib3.disable_warnings()

class SalesForce(object):

    def __init__(self, params, verify=False):
        """
        """
        self.params = params
        self.params['grant_type'] = 'password'
        token = self.params.pop('token')
        self.params['password'] = self.params['password'] + token
        sf_instance = params.pop('url')
        sf_version = '32.0'
        version_url =  '/services/data/v{}/'.format(sf_version)

        self.base_url = 'https://{}'.format(sf_instance)
        self.auth_url = self.base_url + '/services/oauth2/token'
        self.q_url = self.base_url +  version_url + 'query'
        self.verify = verify
        self.headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    def login(self):
        resp = requests.post(self.auth_url, 
                               headers=self.headers, 
                               data=self.params, 
                               verify=self.verify)
        try:
            access_token = resp.json()['access_token']
        except KeyError:
            print('Authentication failed: {}'.format(resp.text))
            sys.exit()
            
        self.headers = {'Content-Type': 'application/json',
                         'Accept-Encoding': 'gzip',
                         'Authorization':'Bearer ' + access_token}
        
    def get_logfiles(self, event_type):
        """
        
        """
        query = ('?q=SELECT+Id+,+EventType+,+LogFile+,+LogDate+,'
                   '+LogFileLength+FROM+EventLogFile+WHERE+'
                   "+EventType+=+'{}'").format(event_type)
        resp = self.get(uri=query)
        return resp.json()

    def get_audit_trail(self, bookmark):
        """ Query SetupAuditTrail events.

        """
        q = ('?q=SELECT+CreatedBy.userName+,+CreatedBy.name+,+ID+,+Action+,'
             '+CreatedDate+,+Display+,+Section+FROM+SetupAuditTrail'
             '+WHERE+createdDate+>=+{}').format(bookmark)
        resp = self.get(uri=q)
        return resp.json()

    def get(self, uri=None, data=None):
        """
        Send a GET request to SFDC.
        
        Args:
        
        uri(str): full uri to be appended on the GET request
        Example:
            '/services/data/v32.0/sobjects/EventLogFile/0AT4P0000079fTSWAY/LogFile'
        
        Exceptions:
            ValueError: if the URI doesn't start with a slash /.
        """
        
        if uri:
            url = self.q_url + uri
        else:
            url = self.q_url

        return requests.get(url,
                            data=data,
                            headers=self.headers, 
                            verify=self.verify,
                            #proxies={"http": "http://127.0.0.1:8888", "https":"http:127.0.0.1:8888"}
                            )
        

def write_json(filename, data):
    with open(filename, 'a') as open_data_file:
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

def get_bookmark(filename):
    try:
        with open(filename) as f:
            return f.read()
    except FileNotFoundError:
        bm_time = datetime.now() - timedelta(days=2)
        return bm_time.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

def write_bookmark(filename, data):
    with open(filename, 'w') as f:
        f.write(data)

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
            log_date = loginfo['LogDate'].replace(':', '-').split('T')[0]
            filename = ''.join(['sf', event_type, '_', log_date, '.json'])
            new_log_urls.append((log_url, filename))

        print('{} {} files are new. Downloading.'.format(new_logs, event_type))
        new_logs = 0
            
    if new_log_urls:
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

    bm_file = '.sf_bookmark'
    bookmark = get_bookmark(bm_file)
    audit_logs = sf.get_audit_trail(bookmark)
    if audit_logs['records']:
        new_records = []
        for log in audit_logs['records']:
            bookmark = log['CreatedDate'].split('.')[0] + 'Z'
            if log['Id'] in LOG_CACHE: continue
            new_records.append(log)
            LOG_CACHE.add(log['Id'])
        if new_records:
            print('Writing {} new audit log records.'.format(len(new_records)))
            filename = 'sfAudit_' + datetime.now().strftime('%F') + '.json'
            filename = PurePath(output_path, filename)
            write_json(filename, new_records)
            write_bookmark(bm_file, bookmark)

if __name__ == "__main__":
    try:
        LOG_CACHE = CacheManager('logfiles', maxlen=5000)
        main()
    except KeyboardInterrupt:
        print("Control-C Pressed, stopping...")
        sys.exit()
    finally:
        LOG_CACHE.write()