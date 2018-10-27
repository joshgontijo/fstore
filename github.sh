#!/usr/bin/env bash

rm -rf github
mkdir github
for year in {2015..2017}; do
    for month in {01..12}; do
        for day in {01..23}; do
            for hour in {0..23}; do
                echo "Downloading ${year}-${month}-${day}-${hour}"
                wget -P github http://data.gharchive.org/${year}-${month}-${day}-${hour}.json.gz
            done
        done
    done
done
