import pymongo
import folium
import webbrowser
import os


try:
    client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
    client.server_info()
    db = client["yelp_recommendation"]
    collection = db["user_recs"]
except Exception as e:
    print("无法连接到 MongoDB")
    exit(1)
print("\n" + "=" * 55)
print("欢迎使用 Yelp 位置感知型智能餐饮推荐系统")
print("=" * 55)
input_id = input("请输入目标用户 ID (可省略前缀符号): ").strip()
print(f"\n正在 MongoDB 中执行智能正则匹配: [*{input_id}]")
user_data = collection.find_one({"user_id": {"$regex": f"{input_id}$"}})
if not user_data:
    print(f"\n抱歉，空间数据库中未找到任何以 [{input_id}] 结尾的推荐记录")
    exit(0)
real_user_id = user_data.get('user_id')
print(f"匹配成功！系统真实的完整 ID 为: {real_user_id}")
recs = user_data.get("recommendations", [])
if not recs:
    print("\n该用户记录存在，但候选推荐列表为空")
    exit(0)
print(f"正在启动空间渲染引擎绘制专属交互地图")
first_lon, first_lat = recs[0]["location"]["coordinates"]
m = folium.Map(
    location=[first_lat, first_lon],
    zoom_start=13,
    tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}',
    attr='Esri'
)
for i, rec in enumerate(recs):
    lon, lat = rec["location"]["coordinates"]
    color = 'red' if i < 3 else 'blue'
    popup_text = f"""
    <div style="font-family: 'Microsoft YaHei', Arial, sans-serif; min-width: 180px;">
        <h4 style="margin-bottom: 5px; color: #d35400;">TOP {i + 1} 推荐</h4>
        <b style="font-size: 14px;">{rec['name']}</b><br>
        <span style="color: #f39c12;">★ Yelp 评分: {rec['stars']} 星</span><br>
        <span style="color: #27ae60;">个性化匹配度: {rec['score']:.4f}</span><br>
        <hr style="margin: 5px 0;">
        <span style="font-size: 0.8em; color: #7f8c8d;">商家 ID: {rec['store_id'][:10]}...</span>
    </div>
    """
    folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(popup_text, max_width=300),
        icon=folium.Icon(color=color, icon='info-sign'),
        tooltip=f"TOP {i + 1}: {rec['name']}"
    ).add_to(m)
clean_filename_id = input_id.replace('-', '').replace('_', '')
html_filename = f'exclusive_map_{clean_filename_id[:8]}.html'
m.save(html_filename)
file_path = "file://" + os.path.abspath(html_filename)
print(f"专属互动推荐地图已生成！即将自动唤起系统浏览器\n")
webbrowser.open(file_path)
