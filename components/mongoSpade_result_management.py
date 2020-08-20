#mongoSpade_result_management.py
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
def mongodb_core(arg1, arg2, arg3):
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]




###  Pronalazi navode i projektuje ih na odgovarajuci nacin,
def mongodb_find(arg1, arg2, arg3):
    search_query = args.query
    collection_name = args.collection
def mongodb_transform(arg1, arg2, arg3):

def mongodb_copy(arg1, arg2, arg3):

def mongodb_delete(arg1, arg2, arg3):


def mongodb_export(arg1, arg2, arg3):
def mongodb_import(arg1, arg2, arg3):