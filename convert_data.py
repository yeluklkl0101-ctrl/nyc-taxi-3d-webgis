import pandas as pd
import geopandas as gpd
import json
import os

# ================= 配置区 =================
# ⚠️ 请确保路径正确
PARQUET_FILE = 'D:\\谷歌下载\\fhvhv_tripdata_2025-07.parquet' 
SHP_FILE = 'D:\\谷歌下载\\taxi_zones\\taxi_zones.shp'          
OUTPUT_FILE = 'trips_data.json'                 
SAMPLE_SIZE = 10000   # 稍微增加一点数据量，效果更好                           
# ============================================

def convert():
    print("--- 🚀 升级版：带时间维度的处理 ---")
    
    # 1. 读取地图 Shapefile
    if not os.path.exists(SHP_FILE):
        print(f"❌ 找不到文件: {SHP_FILE}")
        return

    print("1. 读取地图 Shapefile...")
    gdf = gpd.read_file(SHP_FILE)
    
    # 确保 ID 是整数
    gdf['LocationID'] = gdf['LocationID'].astype(int)
    
    # 转换坐标系
    gdf = gdf.to_crs(epsg=4326) 
    gdf['lon'] = gdf.geometry.centroid.x
    gdf['lat'] = gdf.geometry.centroid.y

    # 构建字典: ID -> [lon, lat]
    zone_dict = {}
    for index, row in gdf.iterrows():
        zone_dict[row['LocationID']] = [row['lon'], row['lat']]
    
    print(f"   ✅ 地图字典构建完成。")

    # 2. 读取 Parquet 数据 (关键修改：增加读取 pickup_datetime)
    print("2. 读取 Parquet 数据 (含时间)...")
    
    # 注意：FHV 数据通常叫 'pickup_datetime'，如果是黄色出租车可能是 'tpep_pickup_datetime'
    # 这里我们尝试读取 pickup_datetime
    try:
        df = pd.read_parquet(PARQUET_FILE, columns=['PULocationID', 'DOLocationID', 'pickup_datetime'], engine='pyarrow')
    except Exception as e:
        print(f"❌ 读取列名失败，请检查 Parquet 文件列名。错误: {e}")
        return
    
    df = df.dropna()

    # 强制转换 ID
    df['PULocationID'] = df['PULocationID'].astype(int)
    df['DOLocationID'] = df['DOLocationID'].astype(int)
    
    # 【核心逻辑】：将时间转换为“当天的秒数” (0 - 86400)
    # 这样前端做动画时，只需要从 0 数到 86400 即可
    print("   正在转换时间格式...")
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    # 计算公式：小时*3600 + 分钟*60 + 秒
    df['trip_time'] = df['pickup_datetime'].dt.hour * 3600 + \
                      df['pickup_datetime'].dt.minute * 60 + \
                      df['pickup_datetime'].dt.second
    
    # 按时间排序（为了前端加载更顺滑）
    df = df.sort_values(by='trip_time')

    # 随机抽样
    print(f"   正在抽取 {SAMPLE_SIZE} 条数据...")
    if len(df) > SAMPLE_SIZE:
        df_sample = df.sample(n=SAMPLE_SIZE)
    else:
        df_sample = df

    # 3. 转换
    print("3. 生成 JSON...")
    export_data = []
    
    match_count = 0
    
    for index, row in df_sample.iterrows():
        pu_id = row['PULocationID']
        do_id = row['DOLocationID']
        trip_time = int(row['trip_time']) # 获取秒数
        
        if pu_id in zone_dict and do_id in zone_dict:
            if pu_id != do_id:
                trip = {
                    "from": zone_dict[pu_id],
                    "to": zone_dict[do_id],
                    "time": trip_time  # ✅ 新增：时间字段
                }
                export_data.append(trip)
                match_count += 1

    # 4. 保存
    if match_count > 0:
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(export_data, f)
        print(f"✅ 处理完成！成功转换 {match_count} 条带时间的数据。")
        print(f"✅ 文件已保存: {OUTPUT_FILE}")
    else:
        print("❌ 匹配失败。")

if __name__ == "__main__":
    convert()