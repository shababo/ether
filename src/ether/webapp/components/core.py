from functools import partial

from ether import ether
from ether.webapp.components.data import TableWidgetConfig

def clean_instance_records():
    return [v for k,v in ether.get_active_instances().items()]

active_instance_records_component = TableWidgetConfig(
            id="apparatus-table-button",
            title="Apparatus Table",
            table_df_func=partial(clean_instance_records),
        )