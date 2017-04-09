file = open('onehotdict.json', 'r')
s = file.readline()
s = s.strip().replace("u'", '"').replace("'", '"').replace('None', '""')
import json
print json.loads(s)
#print s