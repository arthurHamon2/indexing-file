"""
Small settings file used to describe the database.
"""
import json
from unipath import Path

BASE_DIR = Path(__file__).ancestor(2)
SETTINGS_DIR = BASE_DIR.child('settings')

# JSON-based secrets module (in order to )
with open(Path(SETTINGS_DIR, 'secrets.json')) as f:
    secrets = json.loads(f.read())


def get_secret(setting, secret_file=secrets):
    """Get the secret variable or return explicit exception"""
    try:
        return secret_file[setting]
    except KeyError:
        error_msg = 'Set the {0} secret variable'.format(setting)
        raise Exception(error_msg)

# # # # # Database parameters # # # # #

DATABASE = "postgres"
USER = "postgres"
PWD = get_secret('BD_PWD')
HOST = "localhost"
PORT = "5432"

DB = {'ENGINE': 'postgresql',
      'DATABASE': DATABASE,
      'USER': USER,
      'PASSWORD': PWD,
      'HOST': HOST,
      'PORT': PORT,
    }
