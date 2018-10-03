import requests
import pandas
import datetime
from pprint import pprint

pandas.set_option('display.max_columns', 500)

def backupES():
    now = datetime.datetime.now()
    YYYYMM = now.strftime('%Y%m')
    YMD = now.strftime('%Y%m%d')
    HM = now.strftime('%H%M')
    folderBackupname = 'backup_'+YYYYMM
    node = "http://10.235.35.119:9200"
    repo = "/_snapshot/"+folderBackupname
    createRepo = requests.post(node+repo,json={
        "type": "fs",
        "settings": {
            "location": folderBackupname,
            "compress": True
        }
    })

    backupUri = "{repo}/backup_{date}_{hm}".format(repo=repo,date=YMD,hm=HM)
    backup = requests.post(node+backupUri,json={
        "ignore_unavailable": True,
        "include_global_state": False
    })
    res = backup.json()

    if res.has_key('accepted')==True:
        print("Backup complete")
    else:
        pprint(res)


def removeDataESBy_ID(index,_idList):
    body = {
            "query": {
                "ids": {
                    "type": "doc",
                    "values": _idList
                }
            }
    }
    url = "http://10.235.35.119:9200/{index}/_delete_by_query".format(index=index)
    res = requests.post(url,json=body)
    pprint(res.json())

def delDocByAggs(index,uniqueKey='UniqueID'):
    size_bucket = 1000
    stopReq = False

    while stopReq==False:
        _ID = []
        body = {
            "aggs": {
                "group_by_ID": {
                    "terms": {
                        "field": uniqueKey+".keyword",
                        "size": size_bucket,
                        "min_doc_count": 2
                    },
                    "aggs": {
                        "duplicateDocuments": {
                            "top_hits": {"size": 100, "stored_fields": []}
                        }
                    }
                }
            }
        }
        res = requests.post('http://10.235.35.119:9200/{index}/_search?size=0'.format(index=index),json=body)
        buckets = res.json()['aggregations']['group_by_ID']['buckets']
        for items in buckets:
            for hits in items['duplicateDocuments']['hits']['hits'][:-1]:
                _ID.append(str(hits['_id']))

        totalRes = len(res.json()['aggregations']['group_by_ID']['buckets'])
        print("Amount : ",totalRes)
        if totalRes>0:
            stopReq = False
            removeDataESBy_ID(index, _ID)
            print("Remove : ", len(_ID))
        else:
            stopReq = True


if __name__ == '__main__':

    mapping = {
        "tsi7_*_test_result" : "UniqueID",
        "tsi7_*_bclog_result" : "TSI_UID",
        "etsintradayppe_testsummary":"UniqueID",
        "etsinterdaytester_testsummary":"UniqueID",
        "tsi6_test_result": "UniqueID",
        "tsi6_bclog_result": "TSI_UID",
    }
    for index,uid in mapping.iteritems():
        delDocByAggs(index,uid)

    backupES()
