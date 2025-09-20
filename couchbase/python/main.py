from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import QueryOptions, ClusterOptions
from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException

# 连接集群
auth = PasswordAuthenticator("gzh", "123456")
cluster = Cluster(
    "couchbase://localhost",
    ClusterOptions(auth)
)
cluster.wait_until_ready(timedelta(seconds=10))

# 选择 bucket / scope / collection
bucket = cluster.bucket("bucket1")
scope = bucket.scope("profile")
coll = scope.collection("users")

# # —— N1QL：参数化查询（防注入 & 可走索引）
# # 请先为 WHERE/JOIN/ORDER BY 字段建索引
# q = """
# SELECT u.name, u.lastLogin
# FROM `app`.`profile`.`users` AS u
# WHERE ANY t IN u.tags SATISFIES t = $tag END
# ORDER BY u.lastLogin DESC
# LIMIT 10
# """
# rows = cluster.query(q, QueryOptions(named_parameters={"tag": "vip"}))
# for row in rows:
#     print(row)

def write():
    doc_key = "1001" # 这个 key 必须使用 string
    # value 就是一个 JSON 文档，可以正常使用 string 和 number
    doc_val = {"name": "Lynn", "tags": ["vip"], "credits": 0}

    # 1) insert（幂等性低，适合首写）
    try:
        coll.insert(doc_key, doc_val)
    # 如果这个 key 已经存在的话，
    except DocumentExistsException:
        print("DocumentExistsException")

    # 2) upsert（最常用）
    coll.upsert(doc_key, doc_val)

    # 3) replace（要求已存在；可结合 CAS 乐观并发）
    res = coll.get(doc_key) # 读出 CAS
    doc = res.content_as[dict] # 转为 dict
    doc["credits"] = 10
    coll.replace(doc_key, doc, cas=res.cas) # CAS 保存在 res 里面

def read():
    doc_key = "1001"
    # 读取全文档
    try:
        res = coll.get(doc_key)
        print(res.content_as[dict])   # -> {'name': 'Lynn', 'tags': ['vip'], 'credits': 10}
        print(res.cas)                # 当前 CAS
    except DocumentNotFoundException:
        print("DocumentNotFoundException")

    # 仅检测是否存在（更便宜）
    ex = coll.exists(doc_key)
    print(ex.exists, ex.cas)

def remove():
    try:
        coll.remove("1001")
    except DocumentNotFoundException:
        pass

def query():
    ...

def main():
    write()
    read()
    remove()
    query()

if __name__=="__main__":
    main()
    print("Done")