import pandas as pd
import os

# ⚠️ 确保路径和你的 Parquet 文件一致
PARQUET_FILE = 'D:\\谷歌下载\\fhvhv_tripdata_2025-07.parquet' 

def check():
    if not os.path.exists(PARQUET_FILE):
        print(f"❌ 找不到文件: {PARQUET_FILE}")
        return

    print("正在读取前 20 条数据，请稍候...")
    
    # 尝试读取时间列
    try:
        df = pd.read_parquet(PARQUET_FILE, columns=['pickup_datetime'], engine='pyarrow')
    except:
        try:
            # 如果是黄色出租车数据，列名可能是 tpep_pickup_datetime
            df = pd.read_parquet(PARQUET_FILE, columns=['tpep_pickup_datetime'], engine='pyarrow')
            df.rename(columns={'tpep_pickup_datetime': 'pickup_datetime'}, inplace=True)
        except Exception as e:
            print(f"❌ 读取失败: {e}")
            return

    # 打印原始数据
    print("-" * 30)
    print("🔍 [原始时间数据检查]")
    print(df['pickup_datetime'].head(10))
    print("-" * 30)

    # 模拟转换逻辑
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['hour'] = df['pickup_datetime'].dt.hour
    df['minute'] = df['pickup_datetime'].dt.minute
    df['trip_time'] = df['hour'] * 3600 + df['minute'] * 60 + df['pickup_datetime'].dt.second
    
    print("🧮 [转换后的秒数 (0-86400)]")
    print(df[['pickup_datetime', 'trip_time']].head(10))
    print("-" * 30)
    
    # 诊断
    zero_count = (df['trip_time'] == 0).sum()
    total_count = len(df)
    print(f"📊 诊断报告: 在 {total_count} 条数据中，有 {zero_count} 条的时间是 0 (00:00:00)")
    
    if zero_count > total_count * 0.9:
        print("⚠️ 结论: 你的源数据里**没有具体时间信息**，只有日期！")
        print("💡 建议: 使用下方的【补救方案】，随机生成时间来演示效果。")
    else:
        print("✅ 结论: 数据看起来是正常的，可能是上次转换脚本没跑通。请重新运行 convert_data.py。")

if __name__ == "__main__":
    check()