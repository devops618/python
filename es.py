from elasticsearch import Elasticsearch
import datetime

es = Elasticsearch("http://ip:9200/", http_auth=("username", "password"))

# print(es.info())

# print(es.search(index="prod-connector-logs-write_20231120", body={"query": {"match_all": {}}}))

date_now = datetime.datetime.now().strftime("%Y.%m.%d")
date_tmp = datetime.datetime.now() - datetime.timedelta(days=30)
date_ago = date_tmp.strftime("%Y.%m.%d")

delete_index_list = []
for index in es.cat.indices(index="k8s-uat-app-*", format='json'):
    index_name = index['index']
    index_time = index_name.split('-')[3]
    if index_time <= date_ago:
        delete_index_list.append(index_name)

print(len(delete_index_list))
for index in delete_index_list:
    es.indices.delete(index)
