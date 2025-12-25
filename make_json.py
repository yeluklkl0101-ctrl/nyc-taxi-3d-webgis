import pandas as pd
import geopandas as gpd
import json
import os

# ================= 配置区 =================
# 确保文件名和你截图里的一致
PARQUET_FILE = 'fhvhv_tripdata_2025-07.parquet' 
SHP_FILE = 'taxi_zones/taxi_zones.shp'  # 假设你的shp文件在这个子文件夹，如果不是请修改
OUTPUT_FILE = 'trips_data.json'                 
SAMPLE_SIZE = 20000   # 既然你有1900万条数据，我们可以稍微多取一点，设为2万条，效果更壮观！
# ============================================

def generate_final_json():
    print("--- 🚀 开始生成最终版 JSON ---")
    
    # 1. 准备地图数据
    print("1. 读取地图区域数据...")
    # 自动寻找 shapefile，防止路径错误
    if not os.path.exists(SHP_FILE):
        # 尝试常见路径
        if os.path.exists(r'D:\谷歌下载\taxi_zones\taxi_zones.shp'):
             real_shp_path = r'D:\谷歌下载\taxi_zones\taxi_zones.shp'
        else:
             print(f"❌ 找不到 {SHP_FILE}，请修改代码中的 SHP_FILE 路径")
             return
    else:
        real_shp_path = SHP_FILE

    gdf = gpd.read_file(real_shp_path)
    gdf['LocationID'] = gdf['LocationID'].astype(int)
    gdf = gdf.to_crs(epsg=4326) 
    
    # 构建坐标字典
    zone_dict = {}
    for index, row in gdf.iterrows():
        zone_dict[row['LocationID']] = [row.geometry.centroid.x, row.geometry.centroid.y]
    
    print("   ✅ 地图字典准备完毕。")

    # 2. 读取出租车数据
    print("2. 读取 Parquet 数据 (这可能需要几秒钟)...")
    # 根据你的截图，列名确认是 'pickup_datetime'
    try:
        df = pd.read_parquet(PARQUET_FILE, columns=['PULocationID', 'DOLocationID', 'pickup_datetime'])
    except Exception as e:
        # 如果当前目录下找不到，尝试绝对路径 (根据你的截图推测)
        print(f"   ⚠️ 当前目录找不到文件，尝试绝对路径...")
        df = pd.read_parquet(r'D:\谷歌下载\fhvhv_tripdata_2025-07.parquet', columns=['PULocationID', 'DOLocationID', 'pickup_datetime'])

    # 3. 数据清洗与采样
    df = df.dropna()
    
    print(f"   📊 原始数据共 {len(df)} 条，正在随机抽取 {SAMPLE_SIZE} 条...")
    if len(df) > SAMPLE_SIZE:
        df = df.sample(n=SAMPLE_SIZE)
    
    # 4. 时间转换 (关键步骤！)
    print("3. 正在计算时间秒数...")
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    # 将时间转换为 0-86400 的秒数
    df['trip_time'] = df['pickup_datetime'].dt.hour * 3600 + \
                      df['pickup_datetime'].dt.minute * 60 + \
                      df['pickup_datetime'].dt.second
    
    # 按时间排序，这样网页加载时会更顺滑
    df = df.sort_values(by='trip_time')

    # 5. 生成 JSON 结构
    print("4. 正在写入 JSON...")
    export_data = []
    match_count = 0
    
    for index, row in df.iterrows():
        pu = int(row['PULocationID'])
        do = int(row['DOLocationID'])
        time_sec = int(row['trip_time'])
        
        # 只有起点和终点都在地图里，且不是原地打转的订单才保留
        if pu in zone_dict and do in zone_dict and pu != do:
            export_data.append({
                "from": zone_dict[pu],
                "to": zone_dict[do],
                "time": time_sec  # ✅ 这里确保写入了正确的时间
            })
            match_count += 1

    # 6. 保存文件
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(export_data, f)
        
    print("-" * 30)
    print(f"🎉 成功！已生成文件: {OUTPUT_FILE}")
    print(f"📅 包含数据: {match_count} 条")
    print("👉 现在去刷新网页，拖动滑块，你应该能看到完美的动画了！")

if __name__ == "__main__":
    generate_final_json()