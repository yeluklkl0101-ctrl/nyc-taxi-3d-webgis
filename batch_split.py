import pandas as pd
import geopandas as gpd
import json
import os
import numpy as np

# ================= 配置区 =================
PARQUET_FILE = 'fhvhv_tripdata_2025-07.parquet' 
SHP_FILE = 'taxi_zones/taxi_zones.shp'
# 每天抽取的数量 (如果要更密集的效果，可以改成 8000 或 10000)
DAILY_SAMPLE = 5000 
# ============================================

def process_daily_data():
    print("--- 🚀 开始按天切分数据 ---")
    
    # 1. 准备地图字典
    print("1. 读取地图数据...")
    if not os.path.exists(SHP_FILE):
        # 尝试绝对路径容错
        real_shp = r'D:\谷歌下载\taxi_zones\taxi_zones.shp'
    else:
        real_shp = SHP_FILE
        
    gdf = gpd.read_file(real_shp)
    gdf['LocationID'] = gdf['LocationID'].astype(int)
    gdf = gdf.to_crs(epsg=4326) 
    zone_dict = {row['LocationID']: [row.geometry.centroid.x, row.geometry.centroid.y] for i, row in gdf.iterrows()}

    # 2. 读取 Parquet
    print("2. 读取完整 Parquet 文件 (稍安勿躁)...")
    try:
        df = pd.read_parquet(PARQUET_FILE, columns=['PULocationID', 'DOLocationID', 'pickup_datetime'])
    except:
        df = pd.read_parquet(r'D:\谷歌下载\fhvhv_tripdata_2025-07.parquet', columns=['PULocationID', 'DOLocationID', 'pickup_datetime'])

    df = df.dropna()
    
    # 3. 提取日期字符串 (例如 '2025-07-01')
    print("3. 正在分析日期分布...")
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['date_str'] = df['pickup_datetime'].dt.strftime('%Y-%m-%d')
    
    # 获取数据里包含的所有日期
    all_days = df['date_str'].unique()
    all_days.sort()
    
    print(f"   📅 发现数据涵盖: {len(all_days)} 天 (从 {all_days[0]} 到 {all_days[-1]})")

    # 4. 循环处理每一天
    for day in all_days:
        print(f"   👉 正在处理: {day} ...", end="")
        
        # 筛选这一天的数据
        day_df = df[df['date_str'] == day].copy()
        
        # 抽样
        if len(day_df) > DAILY_SAMPLE:
            day_df = day_df.sample(n=DAILY_SAMPLE)
            
        # 计算秒数
        day_df['trip_time'] = day_df['pickup_datetime'].dt.hour * 3600 + \
                              day_df['pickup_datetime'].dt.minute * 60 + \
                              day_df['pickup_datetime'].dt.second
        
        # 修复时间为0的情况 (防止全是00:00)
        if (day_df['trip_time'] == 0).sum() > len(day_df) * 0.9:
            day_df['trip_time'] = np.random.randint(0, 86400, size=len(day_df))
            
        day_df = day_df.sort_values(by='trip_time')

        # 生成 JSON
        export_data = []
        for index, row in day_df.iterrows():
            pu = int(row['PULocationID'])
            do = int(row['DOLocationID'])
            if pu in zone_dict and do in zone_dict and pu != do:
                export_data.append({
                    "from": zone_dict[pu],
                    "to": zone_dict[do],
                    "time": int(row['trip_time'])
                })
        
        # 文件名格式: trips_2025-07-01.json
        filename = f"trips_{day}.json"
        with open(filename, 'w') as f:
            json.dump(export_data, f)
            
        print(f" ✅ 已保存 ({len(export_data)} 条)")

    print("-" * 30)
    print("🎉 全部处理完成！请确保所有 trips_xxxx-xx-xx.json 文件都在文件夹中。")

if __name__ == "__main__":
    process_daily_data()