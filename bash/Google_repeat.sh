#!/bin/bash

SSL_CERT_DIR=/etc/ssl/certs
process="python3"
while true
do
        if pgrep "$process" >/dev/null
        then
            echo "$process is running, exiting..."
            exit
        else
            echo "Running SPADE search and scrape"
            python3.8 mongoSpade_query_google.py
            echo "Finished query, fetching next in 60 seconds."
            sleep 30
        fi
done

