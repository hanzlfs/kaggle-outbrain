import csv
import os

memory = 30 # stands for 10GB, write your memory here
limit = 114434838 / 10 * memory 

leak = {}
for c,row in enumerate(csv.DictReader(open('../input/promoted_content.csv'))):
    if row['document_id'] != '':
        leak[row['document_id']] = 1 
print(len(leak))
count = 0
filename = '../input/page_views.csv'
for c,row in enumerate(csv.DictReader(open(filename))):
    if count>limit:
	    break
    if c%1000000 == 0:
        print (c,count)
    if row['document_id'] not in leak:
	    continue
    if leak[row['document_id']]==1:
	    leak[row['document_id']] = set()
    lu = len(leak[row['document_id']])
    leak[row['document_id']].add(row['uuid'])
    if lu!=len(leak[row['document_id']]):
	    count+=1
fo = open('../interm/eak.csv','w')
fo.write('document_id,uuid\n')
for i in leak:
    if leak[i]!=1:
	    tmp = list(leak[i])
	    fo.write('%s,%s\n'%(i,' '.join(tmp)))
	    del tmp
fo.close()	
