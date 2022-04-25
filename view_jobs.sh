#!/bin/sh

configfile=$1

if [ ! $1 ]; then
        echo "no input config"
        exit
fi

ipython --pprint -i view_jobs.py -- -c $1
