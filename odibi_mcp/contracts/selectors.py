# odibi_mcp/contracts/selectors.py

from typing import Union, Literal
from pydantic import BaseModel


class RunById(BaseModel):
    """
    Explicit run selector by pipeline run_id.
    """

    run_id: str


RunSelector = Union[
    Literal["latest_successful"],
    Literal["latest_attempt"],
    RunById,
]

DEFAULT_RUN_SELECTOR: RunSelector = "latest_successful"
