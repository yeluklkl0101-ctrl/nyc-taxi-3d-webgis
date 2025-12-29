import os
import json
import sqlite3
import pandas as pd
import requests
import random
from datetime import datetime  # ✅ 新增：用于获取真实时间
from flask import Flask, jsonify, request
from flask_cors import CORS
from openai import OpenAI

app = Flask(__name__)
CORS(app)

# ============================================
# 🔑 配置区域
# ============================================
DEEPSEEK_API_KEY = "sk-0ed67650dc3c411e88e35ff4a475aaa2" 
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
WEATHER_API_KEY = "44cd82173350f771d690dc000bb7956d" 

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
# ============================================

def init_db():
    conn = sqlite3.connect('chat_history.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  role TEXT, 
                  content TEXT, 
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

init_db()

# --- 辅助函数 ---
def calculate_daily_stats(date_str):
    file_path = f'trips_{date_str}.json'
    if not os.path.exists(file_path): return "今日暂无历史数据。"
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return f"【历史数据简报】日期：{date_str}，存档订单量：{len(data)} 单。"
    except Exception as e:
        return f"数据分析出错: {str(e)}"

def get_realtime_weather(city="New York"):
    def get_mock_weather():
        temps = [18, 20, 22, 19, 25]
        conds = ["晴朗", "多云", "少云", "有微风"]
        return f"【模拟实时信号】{city} 当前气温 {random.choice(temps)}°C，天气{random.choice(conds)} (数据来自虚拟卫星)。"

    if not WEATHER_API_KEY: return get_mock_weather()
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_API_KEY}&units=metric&lang=zh_cn"
        res = requests.get(url, timeout=3)
        if res.status_code == 200:
            data = res.json()
            return f"【真实API数据】{city} 当前气温 {data['main']['temp']}°C，{data['weather'][0]['description']}。"
        else: return get_mock_weather()
    except: return get_mock_weather()

# --- 路由 ---
@app.route('/api/get_trips', methods=['GET'])
def get_trips():
    date_str = request.args.get('date')
    file_path = f'trips_{date_str}.json'
    if not os.path.exists(file_path): return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    return jsonify(data)

@app.route('/api/get_hotspots', methods=['GET'])
def get_hotspots():
    date_str = request.args.get('date')
    file_path = f'trips_{date_str}.json'
    if not os.path.exists(file_path): return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    df = pd.DataFrame(data)
    if df.empty: return jsonify([])
    return jsonify(df['from'].tolist())

@app.route('/api/chat', methods=['GET'])
def get_chat_history():
    conn = sqlite3.connect('chat_history.db')
    c = conn.cursor()
    c.execute("SELECT role, content FROM messages ORDER BY id ASC")
    rows = c.fetchall()
    conn.close()
    return jsonify([{"role": r[0], "content": r[1]} for r in rows])

@app.route('/api/chat_with_ai', methods=['POST'])
def chat_with_ai():
    data = request.json
    user_message = data.get('message', '')
    context_info = data.get('context', '')
    date_str = data.get('date', '2025-07-01')
    mode = data.get('mode', 'history') 
    
    if not user_message: return jsonify({'error': 'No input'}), 400

    system_instruction = ""
    
    if mode == 'realtime':
        # ✅ 修正点 1：获取真实物理时间
        real_time_str = datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")
        weather_info = get_realtime_weather("New York")
        
        system_instruction = f"""
        你是一个 WebGIS 实时指挥官。当前状态：【🔴 实时实景模式】。
        
        【重要：时间认知】
        请忽略上下文中的历史数据时间。
        现在的真实世界时间是：{real_time_str}。
        如果用户问“现在几点”或“现在的时间”，请回答上述真实时间。
        
        【数据接入】
        系统已连接实时气象网络：{weather_info}。
        
        【地图控制 - 3D沉浸式】
        用户想去某地时，返回 JSON 切换视角。
        必须包含 pitch: 60 (倾斜) 和 bearing (旋转)。
        示例：{{ "action": "flyTo", "center": [-74.0, 40.7], "zoom": 16, "pitch": 60, "bearing": -20, "text": "正在前往..." }}
        """
    else:
        # === 历史模式 ===
        stats = calculate_daily_stats(date_str)
        system_instruction = f"""
        你是一个 WebGIS 数据分析助手。当前状态：【📅 历史分析模式】。
        
        【重要：时间认知】
        你正在回放历史数据。当前回放的日期是：{date_str}。
        如果用户问时间，请明确告知这是“历史回放时间”。
        后端统计数据：{stats}。
        
        【地图控制 - 2D俯视】
        移动地图时保持俯视 (pitch: 0)。
        示例：{{ "action": "flyTo", "center": [-74.0, 40.7], "zoom": 13, "pitch": 0, "bearing": 0, "text": "..." }}
        """

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": f"当前上下文：{context_info}\n用户问题：{user_message}"}
            ],
            stream=False, temperature=0.7 
        )
        ai_reply = response.choices[0].message.content
        
        conn = sqlite3.connect('chat_history.db')
        c = conn.cursor()
        c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", ('user', user_message))
        c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", ('ai', ai_reply))
        conn.commit()
        conn.close()
        return jsonify({'reply': ai_reply})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'reply': f'Error: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)