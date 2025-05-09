from pydantic import ConfigDict, Field
from typing import Optional, Callable
import dash_ag_grid as dag
import pandas as pd
from ether.webapp.components.base import WidgetConfig

class TableWidgetConfig(WidgetConfig):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    table_df_func: Callable
    icon: Optional[str] = Field(default="ph:table-duotone")
    include_cols: Optional[list] = Field(default_factory=list)
    exclude_cols: Optional[list] = Field(default_factory=list)
    

    def _get_custom_component(self):

        table_df = self.table_df_func()
        try:
            if isinstance(table_df, list):
                table_df = pd.DataFrame(table_df)
            if not isinstance(table_df, pd.DataFrame):
                raise ValueError("table_df_func must return or a list of record dicts")
        except Exception as e:
            raise ValueError("table_df_func must return or a list of record dicts")
        
        display_cols = table_df.columns.tolist()
        if self.include_cols:
            display_cols = [col for col in self.include_cols if col in display_cols]
        if self.exclude_cols:
            display_cols = [col for col in display_cols if col not in self.exclude_cols]
                
        table_view = dag.AgGrid(
                id=f"{self.id}-table",
                rowData=table_df.to_dict("records"),
                columnDefs=[{'field': column} for column in display_cols],
                dashGridOptions={"animateRows": False},
                defaultColDef={"resizable": True, "sortable": True, "filter": True},
            )
        return table_view