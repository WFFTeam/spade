#mongoSpade_stdout.py

import time
import os
import sys
from termcolor import colored
from datetime import datetime as dt

# TERMCOLOR FUNCTIONS
def yellow(text):
    return colored(text, 'yellow', attrs=['bold'])
def green(text):
    return colored(text, 'green', attrs=['bold'])
def red(text):
    return colored(text, 'red', attrs=['bold'])
def cyan(text):
    return colored(text, 'cyan', attrs=['bold'])

# SCREEN CLEAN FUNCTION
def clear():
    os.system( 'clear')

# CURRENT DATE&TIME FUNCTION
def dt_print():
    now = dt.now()
    dt_string = now.strftime("%H:%M:%S %d/%m/%Y")
    return dt_string


# Nova funkcija za odbrojavanje posto stopwatch() baguje
def countdown(t):
    while t > 0:
        sys.stdout.write(red('\rWaiting for : {}s'.format(t)))
        t -= 1
        sys.stdout.flush()
        time.sleep(1)
    clear()

def stopwatch(sec):
    while sec:
        minn, secc = divmod(sec, 60)
        timeformat = '{:02d}:{:02d}'.format(minn, secc)
        print(timeformat, end='')
        time.sleep(1)
        sec -= 1
    print("Continuing!")

def json_timestamp():
    now = dt.now()
    json_timestamp = now.isoformat()
    return json_timestamp

def QueryProgress(currentLine, numOfLines, queryInput):
    print(" ")
    print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(dt_print()))
    print(green(f"Fetching results for string: {googler_query}"))