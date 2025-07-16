# 🌦️ Weather Service

A simple and elegant weather service that provides real-time weather data based on geographic coordinates.

## Features

- 📍 Get weather by latitude & longitude
- ⚡ Fast and lightweight API
- 🌐 Easy integration

## Usage

1. **Send coordinates, start and end date** to the API.
2. **Receive weather data** instantly.

## Example

```bash
curl -X 'GET' \
  'http://127.0.0.1:8000/weather?latitude=11.1111&longitude=22.2222&start_date=2024-12-29&end_date=2024-12-30' \
  -H 'accept: application/json'
```

## License

MIT

