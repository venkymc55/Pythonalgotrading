import os
import configparser

parser = configparser.RawConfigParser()
parser.read(os.path.join(os.getcwd(), 'common_utils', 'config.cfg'))

IP = parser.get('network', 'IP_ADDR')
API_KEY = parser.get('security', 'API_KEY')
API_SECRET = parser.get('security', 'API_SECRET')
