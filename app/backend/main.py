from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import data

app = FastAPI(title="NYC Cab Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Priority: specific routes BEFORE parameterized routes
@app.get("/api/operator/zones")
def operator_zones(
    hour: int = Query(..., ge=0, le=23),
    dow: int = Query(..., ge=0, le=6),
    date: str = Query(...),
    holiday: str = Query("regular"),
):
    """
    Operator dashboard: all zones with demand, forecast, unmet demand, and supply status.
    Used for supply positioning and dynamic pricing decisions.
    """
    return data.get_operator_zones(hour, dow, date, holiday)


@app.get("/api/heatmap")
def heatmap(hour: int = Query(..., ge=0, le=23), dow: int = Query(..., ge=0, le=6), date: str = Query(...), holiday: str = Query("regular")):
    return data.get_heatmap(hour, dow, holiday, date)


@app.get("/api/kpis")
def kpis(hour: int = Query(..., ge=0, le=23), dow: int = Query(..., ge=0, le=6), date: str = Query(...), holiday: str = Query("regular")):
    return data.get_kpis(hour, dow, holiday, date)


@app.get("/api/ranking")
def ranking(
    hour: int = Query(..., ge=0, le=23),
    dow: int = Query(..., ge=0, le=6),
    date: str = Query(...),
    n: int = Query(15, ge=1, le=50),
    holiday: str = Query("regular"),
):
    return data.get_ranking(hour, dow, n, holiday, date)


@app.get("/api/recommendations")
def recommendations(
    zone_id: int = Query(...),
    hour: int = Query(..., ge=0, le=23),
    dow: int = Query(..., ge=0, le=6),
    date: str = Query(...),
    n: int = Query(3, ge=1, le=10),
    holiday: str = Query("regular"),
):
    return data.get_recommendations(zone_id, hour, dow, n, holiday, date)


@app.get("/api/zones/metadata")
def zones_metadata():
    return data.get_zone_metadata()


@app.get("/api/zone/{zone_id}/trend")
def zone_trend(zone_id: int, dow: int = Query(..., ge=0, le=6), date: str = Query(...), holiday: str = Query("regular")):
    return data.get_zone_trend(zone_id, dow, holiday, date)


@app.get("/api/zone/{zone_id}")
def zone_current(
    zone_id: int,
    hour: int = Query(..., ge=0, le=23),
    dow: int = Query(..., ge=0, le=6),
    date: str = Query(...),
    holiday: str = Query("regular"),
):
    return data.get_current_zone(zone_id, hour, dow, holiday, date)


@app.get("/api/holidays")
def holidays_list():
    """Get list of all holidays in the dataset with their names."""
    from data import HOLIDAYS
    return {
        "holidays": [
            {"date": f"{m:02d}-{d:02d}", "name": name}
            for (m, d), name in sorted(HOLIDAYS.items())
        ]
    }


@app.get("/api/synthetic-demand")
def synthetic_demand(
    hour: int = Query(..., ge=0, le=23),
    dow: int = Query(..., ge=0, le=6),
    date: str = Query(...),
):
    """
    Get synthetic 'live' demand for a given hour and day of week.
    Uses historical patterns + intelligent variations (seasonality, noise, rush hours).
    """
    return data.get_synthetic_current_demand(hour, dow, date)


@app.get("/api/forecast")
def forecast(
    zone_id: int = Query(...),
    hour: int = Query(..., ge=0, le=23),
    dow: int = Query(..., ge=0, le=6),
    date: str = Query(...),
    steps: int = Query(16, ge=1, le=96),
):
    """
    Forecast demand for a zone using LightGBM model with synthetic lags.

    Args:
        zone_id: Zone ID to forecast for
        hour: Current hour
        dow: Current day of week
        date: Date in YYYY-MM-DD format
        steps: Number of 15-minute intervals to forecast (default 16 = 4 hours, max 96 = 24 hours)

    Returns:
        List of forecast points with time_bucket and predicted_trips
    """
    return data.forecast_demand(zone_id, hour, dow, steps, date)


@app.get("/api/heatmap-forecast")
def heatmap_forecast(
    hours_ahead: int = Query(0, ge=0, le=4),
):
    """
    Get forecasted heatmap for hours ahead (next 4 hours max).
    Returns predictions for all zones at specified future time using synthetic current demand.
    """
    return data.get_forecast_heatmap(hours_ahead)


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
