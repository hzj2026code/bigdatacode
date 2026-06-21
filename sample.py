import json
import time


REVIEW = 'yelp_academic_dataset_review.json'
BUSINESS = 'yelp_academic_dataset_business.json'

#  要提取的评论数量
SAMPLE_SIZE = 250000
# 输出文件路径 (TSV格式)
OUTPUT_REVIEW = 'reviews.tsv'
OUTPUT_BUSINESS = 'businesses.tsv'
print(f"数据预处理开始")
print(f"目标采样数量: {SAMPLE_SIZE} 条")
# 创建一个集合 (Set)，用于存储在评论数据中出现过的商铺ID
store_ids = set()
# 目标：抽取前N条评论，提取关键字段，并记录涉及的商铺ID
print(f"\n[阶段 1/2] 正在处理评论数据 ({REVIEW})...")
try:
    # 使用 'utf-8' 编码打开输入和输出文件
    with open(REVIEW, 'r', encoding='utf-8') as file_in, open(OUTPUT_REVIEW, 'w', encoding='utf-8') as file_out:
        # enumerate 用于同时获取行号(index)和行内容(line)
        for index, line in enumerate(file_in):
            if index >= SAMPLE_SIZE:
                break
            # 解析JSON：将每一行字符串解析为Python字典对象
            review = json.loads(line)
            user_id = review['user_id']
            business_id = review['business_id']
            stars = review['stars']
            # 写入文件：使用制表符(\t)分隔，并在末尾添加换行符(\n)
            # 这种格式是Hadoop MapReduce处理文本文件的标准格式
            file_out.write(f"{user_id}\t{business_id}\t{stars}\n")
            # 将该评论关联的商铺ID加入集合，供第二阶段筛选使用
            store_ids.add(business_id)
            # 进度提示：每处理5万条打印一次
            if (index + 1) % 50000 == 0:
                print(f"已处理 {index + 1} 条评论")
except FileNotFoundError:
    print(f"错误：找不到输入文件 '{REVIEW}'。请确认文件名和路径是否正确。")
    exit(1)
print(f"阶段 1 完成！")
print(f"已生成: {OUTPUT_REVIEW}")
print(f"提取出的独立商铺数量: {len(store_ids)}")
# 目标：遍历所有商铺，只保留在第一阶段中出现过、且具有有效经纬度的商铺
print(f"\n[阶段 2/2] 正在处理商铺数据 ({BUSINESS})...")
try:
    with open(BUSINESS, 'r', encoding='utf-8') as file_in, open(OUTPUT_BUSINESS, 'w', encoding='utf-8') as file_out:
        matched = 0   # 记录成功匹配并写入的商铺数
        scanned = 0   # 记录总共扫描过的商铺数
        for line in file_in:
            scanned += 1
            store = json.loads(line)
            # 获取当前商铺的ID
            id = store['business_id']
            # 只有当这个商铺ID存在于在阶段1建立的集合(store_ids)中时，才处理它
            if id in store_ids:
                # 获取经纬度信息，使用.get()方法防止字段不存在报错
                latitude = store.get('latitude')
                longitude = store.get('longitude')
                # 数据清洗：确保经纬度都不是 None (空值)
                # 如果没有位置信息，就无法计算距离，所以必须过滤掉
                if latitude is not None and longitude is not None:
                    # 写入文件：商铺ID \t 纬度 \t 经度 \t 名字 \t 星级
                    name = store.get('name', 'Unknown').replace('\t', ' ') # 防止名字里自带制表符破坏格式
                    stars = store.get('stars', 0.0)
                    file_out.write(f"{id}\t{latitude}\t{longitude}\t{name}\t{stars}\n")
                    matched += 1
            if scanned % 20000 == 0:
                print(f"  -> 已扫描 {scanned} 家商铺...")
except FileNotFoundError:
    print(f"错误：找不到输入文件 '{BUSINESS}'。")
    exit(1)
print(f"阶段 2 完成！")
print(f"已生成: {OUTPUT_BUSINESS}")
print(f"最终筛选出的有效商铺数: {matched}")