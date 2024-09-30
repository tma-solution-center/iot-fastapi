# Sử dụng image Python chính thức làm base image
FROM python:3.9-slim

# Đặt biến môi trường
ENV PYTHONUNBUFFERED=1
ENV VAULT_TOKEN=hvs.q17GN6j2QvP7sXDny4XOQzGH
ENV VAULT_URL=http://vault-server.vault.svc.cluster.local:8200

# Cài đặt các gói cần thiết cho hệ điều hành
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config libmariadb-dev-compat libmariadb-dev && rm -rf /var/lib/apt/lists/*

# Tạo và sử dụng thư mục làm việc
WORKDIR /app

# Sao chép tệp requirements.txt vào image
COPY requirements.txt .

# Cài đặt các phụ thuộc từ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn ứng dụng vào image
COPY . .

# Lệnh khởi động Uvicorn server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
