import enum


class Table(enum.Enum):
    LINE = "LineDim"
    STOP = "StopDim"
    DELAYS = "DelayDim"
    VEHICLE = "VehicleDim"
    WEATHER = "WeatherDim"
    TIME = "TimeDim"
