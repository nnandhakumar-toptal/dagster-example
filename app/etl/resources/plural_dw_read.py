import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
from sqlalchemy import create_engine

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class PluralRead(ABC):
    @abstractmethod
    def read_from_dw(self, sql_path: str) -> Optional[pd.DataFrame]:
        pass
