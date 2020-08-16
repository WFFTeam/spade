#!/bin/bash

while true
do 
	python3 mongoSpade_query_google.py
	echo "Finished query, fetching next in 60 seconds."
	sleep 30
done
