FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY search_schema.py app.py ./
COPY output/ ./output/

EXPOSE 9234

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=9234"]
