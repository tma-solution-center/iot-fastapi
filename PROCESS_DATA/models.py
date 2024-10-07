from datetime import date, time, datetime
from decimal import Decimal

from pydantic import BaseModel, model_validator, Field, ConfigDict
from typing import Optional, List, Literal, Union


class DataMinionPathInfo(BaseModel):
    base_path: str
    year: Optional[str] = Field(default=None)
    month: Optional[Literal["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]] = None
    day: Optional[Literal["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]] = None
    hour: Optional[Literal["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
    "16", "17", "18", "19", "20", "21", "22", "23"]] = None

    @model_validator(mode='before')
    def validate_dates(cls, values):
        year = values.get('year')
        month = values.get('month')
        day = values.get('day')
        hour = values.get('hour')

        # Check if the values are strings and represent numbers
        for field in ['year', 'month', 'day', 'hour']:
            if values.get(field) is not None:
                if not values[field].isdigit():
                    raise ValueError(f"'{field}' must be a string containing numeric characters only.")

        # Custom logic for validating combinations of fields
        if hour:
            if not (year and month and day):
                raise ValueError("When 'hour' is specified, 'year', 'month', and 'day' must also be specified.")
            elif day:
                if not (year and month):
                    raise ValueError("When 'day' is specified, 'month' and 'year' must also be specified.")
            elif month:
                if not year:
                    raise ValueError("When 'month' is specified, 'year' must also be specified.")

        return values


class ColumnInfo(BaseModel):
    column_name: str
    type: Literal["int", "bigint", "string", "float", "decimal", "bool", "date", "time", "timestamp"]


class TableInfoRequest(BaseModel):
    table_name: str
    columns: list[ColumnInfo]


class InsertColumns(BaseModel):
    column_name: str
    column_value: Optional[Union[int, bool, float, Decimal, str, date, time, datetime]] = None
    column_type: Literal["int", "bigint", "string", "float", "decimal", "bool", "date", "time", "timestamp"]

    model_config = ConfigDict(populate_by_name=True)


class InsertRequest(BaseModel):
    table_name: str
    values: List[List[InsertColumns]]

    model_config = ConfigDict(populate_by_name=True)


class DateInfo(BaseModel):
    year: str
    month: Optional[Literal["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]]
    day: Optional[Literal["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]]

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode='before')
    def validate_dates(cls, values):

        # Check if the values are strings and represent numbers
        for field in ['year', 'month', 'day']:
            if values.get(field) is not None:
                if not values[field].isdigit():
                    raise ValueError(f"'{field}' must be a string containing numeric characters only.")
        return values

    def to_date(self) -> Optional[date]:
        """Chuyển đổi DateInfo thành đối tượng datetime.date nếu đủ thông tin."""
        if self.year and self.month and self.day:
            return date(int(self.year), int(self.month), int(self.day))
        return None


class DateRange(BaseModel):
    start_date: DateInfo
    end_date: DateInfo

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode='after')
    def validate_date_range(cls, values):
        start_date = values.start_date.to_date()
        end_date = values.end_date.to_date()

        if start_date and end_date and start_date >= end_date:
            raise ValueError("start_date must be earlier than end_date.")

        return values


class AggregationDataByDateRangeRequest(BaseModel):
    column_name: str
    agg_func: Optional[Literal["sum", "min", "max", "avg", "count",
    "SUM", "MIN", "MAX", "COUNT", "AVG"]]
    date_range: DateRange

    model_config = ConfigDict(populate_by_name=True)
