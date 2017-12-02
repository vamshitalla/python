# Author: Vamshi Talla
# Date  : 12/02/2017

import csv

with open('b.csv') as csvfile:
    readCSV=csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        # print(row)
        print(row[1], row[2])