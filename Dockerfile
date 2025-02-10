FROM python:3.13-bookworm

WORKDIR /app/
RUN useradd app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN make
RUN chown -R app:app .

USER app
CMD ["echo", "error: explicit entrypoint for server or node required"]
