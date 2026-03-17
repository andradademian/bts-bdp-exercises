from fastapi import FastAPI

app = FastAPI(title="BTS BDP S9 - DevOps Exercises")


@app.get("/")
def root():
    return {"message": "BTS Big Data Platform - Session 9: DevOps & CI/CD"}


@app.get("/hello/{name}")
def hello(name: str):
    return {"message": f"Hello, {name}!"}


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0",
    }

@app.get("/info")
def info():
    return {"session": "S9", "topic": "DevOps & CI/CD"}
