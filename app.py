import os
import json
import sqlite3
import pandas as pd
import requests
import random
import numpy as np
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from flask import Flask, jsonify, request, session
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from openai import OpenAI
from sklearn.cluster import KMeans

app = Flask(__name__)
# 🔐 配置 Session 密钥 (生产环境请修改)
app.secret_key = 'course_design_super_secret_key' 
# 允许跨域且携带凭证 (Cookie)
CORS(app, supports_credentials=True)

# 配置上传文件夹
UPLOAD_FOLDER = 'uploaded_data'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# ============================================
# 🔑 API 配置
# ============================================
DEEPSEEK_API_KEY = "sk-0ed67650dc3c411e88e35ff4a475aaa2" 
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
WEATHER_API_KEY = "44cd82173350f771d690dc000bb7956d" 

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)

# 数据库路径
DB_PATH = 'chat_history.db'

# ============================================
# 🗄️ 数据库初始化
# ============================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 1. 聊天记录表
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  role TEXT, content TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    
    # 2. 用户表
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  username TEXT UNIQUE, 
                  password_hash TEXT, 
                  role TEXT)''') 
                  
    # 3. 数据文件注册表
    c.execute('''CREATE TABLE IF NOT EXISTS data_files
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  date_str TEXT UNIQUE, 
                  filename TEXT, 
                  description TEXT)''')
    
    # --- 预置账号 ---
    # 管理员: orange / 123456
    try:
        c.execute("SELECT * FROM users WHERE username='orange'")
        if not c.fetchone():
            pwd_hash = generate_password_hash('123456') 
            c.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)", 
                      ('orange', pwd_hash, 'admin'))
            print("✅ 管理员账号已创建: orange")
    except Exception as e: print(e)

    # 普通用户: user01 / 123456
    try:
        target_user = 'user01' 
        c.execute("SELECT * FROM users WHERE username=?", (target_user,))
        if not c.fetchone():
            pwd_hash = generate_password_hash('123456')
            c.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)", 
                      (target_user, pwd_hash, 'user'))
            print(f"✅ 普通用户账号已创建: {target_user}")
    except Exception as e: print(e)
        
    conn.commit()
    conn.close()

init_db()

# ============================================
# 🛠️ 辅助工具函数
# ============================================

def get_file_path(date_str):
    # 优先找上传目录
    path1 = os.path.join(app.config['UPLOAD_FOLDER'], f'trips_{date_str}.json')
    if os.path.exists(path1): return path1
    # 其次找根目录
    path2 = f'trips_{date_str}.json'
    if os.path.exists(path2): return path2
    return None

def haversine(lon1, lat1, lon2, lat2):
    """计算两点间地理距离(米)"""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    return c * 6371 * 1000

def get_realtime_weather(city="New York"):
    if not WEATHER_API_KEY: return "天气数据服务暂不可用(无Key)"
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_API_KEY}&units=metric&lang=zh_cn"
        res = requests.get(url, timeout=3)
        if res.status_code == 200:
            data = res.json()
            return f"【实时气象】{city} 气温 {data['main']['temp']}°C，{data['weather'][0]['description']}。"
    except: pass
    return "【模拟信号】New York 气温 20°C，晴朗 (卫星连接不稳定)。"

def calculate_daily_stats(date_str):
    file_path = get_file_path(date_str)
    if not file_path: return "今日暂无数据。"
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        return f"【历史简报】{date_str} 总计订单 {len(data)} 单。"
    except: return "数据读取错误。"

# ============================================
# 🔐 认证与管理接口
# ============================================

@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, password_hash, role FROM users WHERE username=?", (username,))
    user = c.fetchone()
    conn.close()
    
    if user and check_password_hash(user[1], password):
        session['user_id'] = user[0]
        session['username'] = username
        session['role'] = user[2]
        return jsonify({'status': 'success', 'role': user[2], 'msg': '登录成功'})
    return jsonify({'status': 'fail', 'msg': '用户名或密码错误'}), 401

@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'status': 'success'})

@app.route('/api/check_auth', methods=['GET'])
def check_auth():
    if 'user_id' in session:
        return jsonify({'is_logged_in': True, 'username': session['username'], 'role': session['role']})
    return jsonify({'is_logged_in': False})

@app.route('/api/upload_data', methods=['POST'])
def upload_data():
    if session.get('role') != 'admin': return jsonify({'error': '无权访问'}), 403
    
    file = request.files.get('file')
    date_str = request.form.get('date')
    
    if file and date_str:
        filename = secure_filename(f"trips_{date_str}.json")
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        try:
            c.execute("INSERT OR REPLACE INTO data_files (date_str, filename, description) VALUES (?, ?, ?)",
                      (date_str, filename, "管理员上传"))
            conn.commit()
            return jsonify({'status': 'success', 'msg': '上传成功'})
        except Exception as e: return jsonify({'error': str(e)}), 500
        finally: conn.close()
    return jsonify({'error': '参数缺失'}), 400

@app.route('/api/get_available_dates', methods=['GET'])
def get_available_dates():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT date_str FROM data_files ORDER BY date_str")
    rows = c.fetchall()
    conn.close()
    dates = [r[0] for r in rows]
    if not dates: dates = ['2025-07-01'] 
    return jsonify(dates)

# ============================================
# 🧠 GIS 分析接口
# ============================================

# 1. K-Means 聚类
@app.route('/api/analyze/kmeans', methods=['GET'])
def analyze_kmeans():
    date_str = request.args.get('date')
    k = int(request.args.get('k', 5))
    
    file_path = get_file_path(date_str)
    if not file_path: return jsonify({'error': 'No data'}), 404
    
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    if not data: return jsonify([])

    points = np.array([d['from'] for d in data])
    kmeans = KMeans(n_clusters=k, random_state=0, n_init=10).fit(points)
    return jsonify({'status': 'success', 'centers': kmeans.cluster_centers_.tolist()})

# 2. 缓冲区查询
@app.route('/api/analyze/buffer', methods=['POST'])
def analyze_buffer():
    req = request.json
    date_str, center, radius = req.get('date'), req.get('center'), req.get('radius', 1000)
    
    file_path = get_file_path(date_str)
    if not file_path: return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    
    filtered = [t for t in data if haversine(center[0], center[1], t['from'][0], t['from'][1]) <= radius]
    return jsonify({'status': 'success', 'trips': filtered, 'count': len(filtered)})

# 3. OD流向分析
@app.route('/api/analyze/od', methods=['POST'])
def analyze_od():
    req = request.json
    date_str, center, radius, mode = req.get('date'), req.get('center'), req.get('radius', 1500), req.get('type', 'from')
    
    file_path = get_file_path(date_str)
    if not file_path: return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    
    filtered = []
    for trip in data:
        pt = trip['from'] if mode == 'from' else trip['to']
        if haversine(center[0], center[1], pt[0], pt[1]) <= radius:
            filtered.append(trip)
            
    return jsonify({'status': 'success', 'trips': filtered, 'count': len(filtered)})

# ============================================
# 🤖 AI 与数据获取
# ============================================

@app.route('/api/get_trips', methods=['GET'])
def get_trips():
    date_str = request.args.get('date')
    file_path = get_file_path(date_str)
    if not file_path: return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    return jsonify(data)

@app.route('/api/get_hotspots', methods=['GET'])
def get_hotspots():
    date_str = request.args.get('date')
    file_path = get_file_path(date_str)
    if not file_path: return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
    df = pd.DataFrame(data)
    if df.empty: return jsonify([])
    return jsonify(df['from'].tolist())

@app.route('/api/chat', methods=['GET'])
def get_chat_history():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT role, content FROM messages ORDER BY id ASC")
    rows = c.fetchall()
    conn.close()
    return jsonify([{"role": r[0], "content": r[1]} for r in rows])

@app.route('/api/chat_with_ai', methods=['POST'])
def chat_with_ai():
    data = request.json
    user_msg = data.get('message', '')
    mode = data.get('mode', 'history')
    date_str = data.get('date', '')
    
    if not user_msg: return jsonify({'error': 'No input'}), 400

    # 构建 Prompt
    sys_prompt = f"""你是一个WebGIS智能指挥官。当前模式:{mode}。
    【指令协议】请分析用户意图，返回 JSON 格式指令：
    1. K-Means聚类: {{ "action": "kmeans", "text": "正在进行聚类分析..." }}
    2. 缓冲区分析: {{ "action": "buffer", "center": [经度, 纬度], "radius": 1000, "text": "正在查询周边..." }}
    3. OD分析: {{ "action": "od", "center": [经度, 纬度], "type": "from", "text": "正在分析流向..." }}
    4. 飞行视角: {{ "action": "flyTo", "center": [经度, 纬度], "zoom": 14, "text": "前往目标..." }}
    5. 普通对话: 直接返回文本。
    【参考坐标】纽约: -74.0, 40.7; 时代广场: -73.985, 40.758; 肯尼迪机场: -73.778, 40.641。
    """
    
    if mode == 'realtime':
        weather = get_realtime_weather("New York")
        sys_prompt += f"\n当前是实时模式，真实时间：{datetime.now()}，天气：{weather}。请忽略历史日期。"
    else:
        stats = calculate_daily_stats(date_str)
        sys_prompt += f"\n当前是历史回放模式，日期：{date_str}。统计：{stats}。"

    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_msg}
            ],
            stream=False, temperature=0.7
        )
        ai_reply = resp.choices[0].message.content
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", ('user', user_msg))
        c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", ('ai', ai_reply))
        conn.commit()
        conn.close()
        return jsonify({'reply': ai_reply})
    except Exception as e:
        return jsonify({'reply': f'Error: {str(e)}'}), 500

if __name__ == '__main__':
    print("🚀 WebGIS System Running on http://127.0.0.1:5000")
    app.run(debug=True, port=5000)