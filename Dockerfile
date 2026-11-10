# 使用輕量 Python 3.10 官方基礎映象檔
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 設定環境變數
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

# 安裝基本系統工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 先複製 requirements.txt 利用 Docker 快取層加速建置
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製其餘專案所有檔案
COPY . .

# 預先初始化資料庫結構
RUN python database.py

# 宣告容器對外 Port
EXPOSE 8000

# 啟動命令：自動讀取雲端平台動態注入的 $PORT (如 Render 的 10000 或 Cloud Run 的 8080)
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
