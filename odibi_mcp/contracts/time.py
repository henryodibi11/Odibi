from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import Optional


class TimeWindow(BaseModel):
    """
    Explicit time semantics for historical catalog queries.
    At least one of (days) or (start AND end) must be provided.
    Matches MCP spec.
    """

    days: Optional[int] = Field(default=None, ge=1, le=365)
    start: Optional[datetime] = None  # Inclusive
    end: Optional[datetime] = None  # Exclusive
    timezone: str = "UTC"

    @model_validator(mode="after")
    def validate_window(self) -> "TimeWindow":
        days = self.days
        start = self.start
        end = self.end
        if days is not None and (start is not None or end is not None):
            raise ValueError("Cannot specify both 'days' and 'start/end'")
        if days is None and start is None and end is None:
            self.days = 7
        if (start is None) != (end is None):
            raise ValueError("Both 'start' and 'end' must be provided together")
        if start is not None and end is not None:
            if start >= end:
                raise ValueError("'start' must be before 'end'")
        return self

    def to_range(self) -> tuple[datetime, datetime]:
        from datetime import timedelta
        import pytz

        tz = pytz.timezone(self.timezone)
        now = datetime.now(tz)
        if self.days is not None:
            end = now
            start = now - timedelta(days=self.days)
        else:
            start = (
                self.start.astimezone(pytz.UTC)
                if self.start.tzinfo
                else tz.localize(self.start).astimezone(pytz.UTC)
            )
            end = (
                self.end.astimezone(pytz.UTC)
                if self.end.tzinfo
                else tz.localize(self.end).astimezone(pytz.UTC)
            )
        return (start, end)
