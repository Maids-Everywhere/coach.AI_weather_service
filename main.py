from fastapi import FastAPI
from configuration import OpenMeteoAPI
from models import WeatherRecord
from typing import List

app = FastAPI()


@app.get("/health")
async def health_check() -> dict:
    return {"status": "healthy"}


@app.get("/weather", response_model=List[WeatherRecord])
async def get_weather(
    latitude: float, longitude: float, start_date: str, end_date: str
) -> dict:
    api = OpenMeteoAPI(latitude, longitude, start_date, end_date)
    weather_data = api.fetch_weather_data()
    records = weather_data.to_dict(orient="records")
    for record in records:
        record["date"] = str(record["date"])

    return records
