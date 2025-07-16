from pydantic import BaseModel, field_validator, validator


class WeatherRecord(BaseModel):
    date: str
    temperature_2m: float
    apparent_temperature: float
    weather_code: int
    rain: float
    snowfall: float
    relative_humidity_2m: float
