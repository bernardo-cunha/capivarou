from fastapi import FastAPI

app = FastAPI(title="Capivarou Workers")

@app.get("/health")
def health():
    return {"status": "ok", "service": "capivarou-workers"}
