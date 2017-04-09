import pandas as pd
import numpy as np

hash_length = 6

def get_hash_code(x):
    y = abs(hash(x)%hash_length)
    return int(y)

view = pd.read_csv("../input/page_views.csv", index_col=False)
print "loaded data"
view["uuid_hash_code"] = view["uuid"].apply(get_hash_code)
view["docid_hash_code"] = view["document_id"].apply(get_hash_code)
print "hashed code"

for u in range(hash_length):
    for d in range(hash_length):
        path = "../data/pages_view/uid"+str(u)+"_doc"+str(d)+".csv.gz"
        view[(view["uuid_hash_code"]) == u & ((view["docid_hash_code"]) == d)].to_csv(path, index = False, compression="gzip")
        print path
