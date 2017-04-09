import pandas as pd
import numpy as np
import csv, gzip

f = open("../input/page_views.csv","r")
line = f.readline()
total = 0

## Open the file with read only permit
## Read the first line 
## If the file is not empty keep reading line one at a time
## till the file is empty
while line:
    total += 1
    line = f.readline()
f.close()
print "total lines", total
