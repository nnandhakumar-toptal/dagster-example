import os

from .plural_dw_io_manager import plural_dw_io_manager
from .plural_dw_io_manager import plural_dw_io_manager

SHARED_DW_CONF = {
    "host": os.getenv("DW_HOST", ""),
    "user": os.getenv("DW_USER", ""),
    "password": os.getenv("DW_PASSWORD", ""),
    "port": os.getenv("DW_PORT", 5432),
}

RESOURCES_LOCAL = {
    "warehouse_io_manager": plural_dw_io_manager.configured(
        dict(dbname="postgres", **SHARED_DW_CONF)
    ),
    "dw_client": PluralDwRead(),
}

RESOURCES_LOCAL = {
    "warehouse_io_manager": plural_dw_io_manager.configured(
        dict(dbname="postgres", **SHARED_DW_CONF)
    ),
    "dw_client": PluralDwRead(subsample_rate=10),
}