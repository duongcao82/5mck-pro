from pydantic import BaseModel
from datetime import datetime
from typing import Optional
class TickerModel(BaseModel):symbol:str;start:str;end:Optional[str]=None;interval:Optional[str]='1D'
class PaginationModel(BaseModel):page:int;size:int;period:int
class FinancialReportModel(BaseModel):type:str;frequency:str