FROM python:3.10

WORKDIR /app

COPY requirements.txt .

# pip'ye daha uzun timeout + proxy bypass
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org

COPY main.py .

VOLUME ["/data"]

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

