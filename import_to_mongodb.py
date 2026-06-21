import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["yelp_recommendation"]
collection = db["user_recs"]
business_dict = {}
print("正在加载商铺字典")
with open('businesses.tsv', 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) >= 5:
            b_id = parts[0]
            b_name = parts[3]
            b_stars = parts[4]
            business_dict[b_id] = {'name': b_name, 'stars': b_stars}
records = []
file_path = 'step4_final_recs.txt'
print("正在解析并转换数据结构")
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        parts = line.split('\t')
        if len(parts) < 2: continue
        user_id = parts[0]
        recs_str = parts[1]
        raw_parts = recs_str.split(',')
        recs_list = []
        for i in range(0, len(raw_parts) - 1, 2):
            part1 = raw_parts[i]   # 对应 'store:score:lat'
            part2 = raw_parts[i+1] # 对应 'lon'
            store_id, score_str, lat_str = part1.split(':')
            store_info = business_dict.get(store_id, {'name': '未知餐馆', 'stars': '0'})
            recs_list.append({
                "store_id": store_id,
                "name": store_info['name'],
                "stars": store_info['stars'],
                "score": float(score_str),
                "location": {
                    "type": "Point",
                    "coordinates": [float(part2), float(lat_str)] 
                }
            })
        records.append({
            "user_id": user_id,
            "recommendations": recs_list
        })
print(f"解析完毕，准备将 {len(records)} 位用户的数据写入 MongoDB")
collection.drop()
if records:
    collection.insert_many(records)
    collection.create_index("user_id", unique=True)
    collection.create_index([("recommendations.location", "2dsphere")])
print("数据成功导入")