from functools import partial

from ether.liaison import EtherInstanceLiaison
from ether.webapp.components.data import TableWidgetConfig

_liaison = EtherInstanceLiaison()
def clean_instance_records():
    return [v for k,v in _liaison.get_active_instances().items()]

active_instance_records_component = TableWidgetConfig(
            id="apparatus-table-button",
            title="Apparatus Table",
            table_df_func=partial(clean_instance_records),
        )