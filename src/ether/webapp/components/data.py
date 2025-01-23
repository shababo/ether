from pydantic import ConfigDict, Field
from typing import Optional, Callable
import dash_ag_grid as dag
import pandas as pd
from ether.webapp.components.core import WidgetConfig

class TableWidgetConfig(WidgetConfig):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    table_df_func: Callable
    icon: Optional[str] = Field(default="ph:table-duotone")
    

    def _get_custom_component(self):

        table_df = self.table_df_func()
        try:
            if isinstance(table_df, list):
                table_df = pd.DataFrame(table_df)
            if not isinstance(table_df, pd.DataFrame):
                raise ValueError("table_df_func must return or a list of record dicts")
        except Exception as e:
            raise ValueError("table_df_func must return or a list of record dicts")
                
        table_view = dag.AgGrid(
                id=f"{self.id}-table",
                rowData=table_df.to_dict("records"),
                columnDefs=[{'field': column} for column in table_df.columns],
                dashGridOptions={"animateRows": False},
                defaultColDef={"resizable": True, "sortable": True, "filter": True},
            )
        return table_view