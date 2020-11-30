#!/bin/bash

mongoexport -d spadeDB --authenticationDatabase=admin -u glavniAdmin -p 'SifraZnjapetMongo' -c beautifulsoup | mongoimport -h 192.168.1.104:27017 --authenticationDatabase=admin -u glavniAdmin -p 'SifraZnjapetMongo' -d spadeDB -c beautifulsoup --drop
