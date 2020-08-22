#!/bin/bash

process="python3"
while true
do
        if pgrep "$process" >/dev/null
        then
            echo "$process is running, exiting..."
            exit
        else
            echo "Running SPADE search and scrape"
            python3 mongoSpade_query_google.py
            echo "Finished query, fetching next in 60 seconds."
            sleep 30
        fi
done

