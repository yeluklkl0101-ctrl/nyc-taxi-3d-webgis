import os
import json
import sqlite3
import pandas as pd
import requests  # <--- 核心改动：使用 requests
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许跨域

# ============================================
# 🔑 你的新 API KEY (填在这里，不要给别人看)
# ============================================
API_KEY = "AIzaSyCFoT7AohPP-JyEVVE5PINNMoNBgxk1fIg"
# ============================================

# 1. 初始化数据库
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

# 2. 时空数据接口
@app.route('/api/get_trips', methods=['GET'])
def get_trips():
    date_str = request.args.get('date')
    file_path = f'trips_{date_str}.json'
    if not os.path.exists(file_path): return jsonify([])
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return jsonify(data)

# 3. 热点区域接口
@app.route('/api/get_hotspots', methods=['GET'])
def get_hotspots():
    date_str = request.args.get('date')
    file_path = f'trips_{date_str}.json'
    if not os.path.exists(file_path): return jsonify([])
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    if df.empty: return jsonify([])
    
    # 简单提取 'from' 坐标作为热点数据
    points = df['from'].tolist()
    return jsonify(points)

# 4. 获取历史聊天记录
@app.route('/api/chat', methods=['GET'])
def get_chat_history():
    conn = sqlite3.connect('chat_history.db')
    c = conn.cursor()
    c.execute("SELECT role, content FROM messages ORDER BY id ASC")
    rows = c.fetchall()
    conn.close()
    return jsonify([{"role": r[0], "content": r[1]} for r in rows])

# 5. 🔥 AI 对话接口 (Plan B: 纯 HTTP 请求)
@app.route('/api/chat_with_ai', methods=['POST'])
def chat_with_ai():
    data = request.json
    user_message = data.get('message', '')
    context_info = data.get('context', '')
    
    if not user_message:
        return jsonify({'error': 'No input'}), 400

    # 构造发给 Google 的 Prompt
    full_prompt = f"{context_info}\n用户问题：{user_message}"

    # Google Gemini REST API 地址
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    
    # 请求体
    payload = {
        "contents": [{
            "parts": [{"text": full_prompt}]
        }]
    }
    
    try:
        # 直接发送 POST 请求，不依赖任何 SDK
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=30)
        
        if response.status_code != 200:
            print("Google API Error:", response.text)
            return jsonify({'reply': f'AI 响应错误 (Code {response.status_code})'}), 500

        result = response.json()
        
        # 解析返回的 JSON
        if 'candidates' in result and result['candidates']:
            ai_reply = result['candidates'][0]['content']['parts'][0]['text']
        else:
            ai_reply = "AI 暂时无法回答这个问题。"

        # 保存到数据库
        conn = sqlite3.connect('chat_history.db')
        c = conn.cursor()
        c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", ('user', user_message))
        c.execute("INSERT INTO messages (role, content) VALUES (?, ?)", ('ai', ai_reply))
        conn.commit()
        conn.close()

        return jsonify({'reply': ai_reply})

    except Exception as e:
        print(f"Connection Error: {e}")
        return jsonify({'reply': '无法连接到 AI 服务器，请检查网络或 VPN 设置。'}), 500

if __name__ == '__main__':
    print("WebGIS 后端服务 (Plan B) 已启动: http://127.0.0.1:5000")
    app.run(debug=True, port=5000)