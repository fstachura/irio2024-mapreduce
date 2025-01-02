FROM python:3.13-bookworm

WORKDIR /app/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN make

CMD ["echo", "error: explicit entrypoint for server or node required"]
