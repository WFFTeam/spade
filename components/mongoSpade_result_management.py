#mongoSpade_result_management.py


### TEST TEST NETSTABILNO ###

# Ovaj alat ce vrsit laku i jednostavnu pretragu baze za navodom,
# jednostavnu izvedbu operacija nad podacima, njihovu distribuciju i ili
# kreiranje odredista, kao i mogucnost koriscenja svih funkcija u ostatku
# resenja.
# Cilj je pojednostaviti mehanizme u pozadini kao i standardizovati
# njihove operacije i nadzor nad tokom i jednostavna implementacija
# korekcija u bilo kojoj operaciji.

import pymongo
import re
import time
import os
import traceback
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from components.mongoSpade_stdout import *
from components.config_db import *

###  Sve sto moze da se ubaci u centralni element ispod,
###  radi uproscavanja drugih funcija i daljih operacija


def mongodb_core(arg1):
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    collection_name = arg1 
    db_cm = mng_db[collection_name]


###  Pronalazi navode i projektuje(printuje) ih na odgovarajuci nacin,
def mongodb_find(arg1, arg2, arg3):
    collection_name = arg1
    find_query = arg2

    try:
        mongodb_find_results = mongodb_core.db_cm[collection_name].find(find_query)
        print(mongodb_find_results)

    except Exception as error:
        print(red(f'ERROR --- Function mongodb_find failed'))
        print(yellow("Error code: ") + red(error))
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--find", "-f", help="--find [Formatted query to search in DB]")
    args = parser.parse_args()
    if args.find:
        print(args.find) ###DEBUG

### TODO!!!
#def mongodb_transform(arg1, arg2, arg3):

#def mongodb_copy(arg1, arg2, arg3):

#def mongodb_delete(arg1, arg2, arg3):


#def mongodb_export(arg1, arg2, arg3):
#def mongodb_import(arg1, arg2, arg3):