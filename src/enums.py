import enum


# mappings for routes.route_type values from gtfs
class LineType(enum.Enum):
    TRAM = 0
    TRAIN = 2
    BUS = 3


class Table(enum.Enum):
    LINE = "LineDim"
    STOP = "StopDim"
    VEHICLE = "VehicleDim"
