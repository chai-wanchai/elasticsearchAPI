# elasticsearchAPI
BackupES.py
- contain 3 function as 
    - backupES()  ==> take snapshot all index in elasticsearch but it must to config [path.repo] in elasticsearch.yml
    - removeDataESBy_ID(index,_idList)  ==> delete document by list of _id  
    - delDocByAggs(index,uniqueKey='UniqueID') ==> get document by uniqueID of each document for find duplicate documents 
