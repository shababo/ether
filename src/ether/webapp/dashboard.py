import dash
dash._dash_renderer._set_react_version('18.2.0')

from dash import Input, Output, State
import dash_mantine_components as dmc
import random


from ether import ether
from ether._internal._config import EtherConfig
from ether.webapp.components.core import LayoutConfig

# use font-awesome for icons and boostrap for main style
external_stylesheets = [
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.1.0/css/all.min.css',
    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',
    dmc.styles.ALL,
]


class _EtherDashboard:
    _instance = None
    _app = None
    layout_config = None

    def __new__(self, layout_config: LayoutConfig = None):  
        if self._instance is None:
            self._instance = super().__new__(self)
            self._instance.layout_config = layout_config or LayoutConfig()
        return self._instance
    
    def tap(self, config: EtherConfig = None, layout_config: LayoutConfig = None, **kwargs):
        config = config or EtherConfig()
        self.layout_config = layout_config or self.layout_config

        ether.tap(config=config)
        self._app = self._get_dashboard()
        self._app.run_server(**kwargs)

    def _get_dashboard(self):
        """Get the Dash app"""
        layout_config = self.layout_config

        app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

        # demo_layout = demo_layout_config.get_layout()
        new_layout = dmc.Flex([layout_config.get_navbar_layout(),layout_config.get_core_layout()], direction="column")
        app.layout = dmc.MantineProvider([new_layout])

        widget_button_configs = [button_config for group_config in layout_config.widget_group_configs for button_config in group_config.widget_buttons]

        # a single callback creates different the different plot widgets
        @app.callback(#[
                Output('dock-panel', 'children'),
                # Output('config-json', 'children'),
            # ],
            [
                *[Input(f"{button.id}-button", "n_clicks") for button in widget_button_configs],
                # Input("com:closeAll", "n_called"),
                # Input("com:closeRandom", "n_called"),
                Input('dock-panel', 'widgetEvent')
            ],
            [State('dock-panel', 'children')])
        def handle_widget(*argv):

            # the last argument is the current state of the dock-panel
            widgets_state = argv[-1]
            if not isinstance(widgets_state, list):
                widgets_state = [widgets_state]

            # the second last is the widget event
            event = argv[-2]

            # remove all closed widgets
            live_widgets = [w for w in widgets_state if not(
                "props" in w and "deleted" in w["props"] and w["props"]["deleted"])]

            # get which component made the callback
            ctx = dash.callback_context

            # check which widget needs to be created
            matching_widget_configs = [config for config in widget_button_configs if ctx.triggered[0]["prop_id"].startswith(f"{config.id}")]

            # create the widget 
            if len(matching_widget_configs) > 0:

                new_widget = matching_widget_configs[0].get_widget_component(id_suffix=str(ctx.triggered[0]["value"]))
                live_widgets.append(new_widget)

            # new_widget = dlc.Widget(dlc.BoxPanel([],id=f"new-box-panel-{random.randint(0, 1000000)}"),id=f"new-widget-f{random.randint(0, 1000000)}", title="test")
            # live_widgets.append(new_widget)
            # import time
            # time.sleep(5)
            # close all widgets
            if "prop_id" in ctx.triggered[0] and ctx.triggered[0]["prop_id"] == "com:closeAll.n_called":
                live_widgets = []

            # close a random widget
            if "prop_id" in ctx.triggered[0] and ctx.triggered[0]["prop_id"] == "com:closeRandom.n_called" and len(live_widgets) > 0:
                del_idx = random.randint(0, len(live_widgets)-1)
                del live_widgets[del_idx]

            status = {
                "event": event,
                "open": [
                    {k:v for k,v in w["props"].items() if k not in ["children"]} for w in live_widgets if "props" in w and "id" in w["props"]
                ]
            }

            return live_widgets #, json.dumps(status, indent=2)
        
        return app



# @app.callback(
#     Output("appshell", "navbar"),
#     Input("mobile-burger", "opened"),
#     Input("desktop-burger", "opened"),
#     State("appshell", "navbar"),
# )
# def toggle_navbar(mobile_opened, desktop_opened, navbar):
#     navbar["collapsed"] = {
#         "mobile": not mobile_opened,
#         "desktop": not desktop_opened,
#     }
#     return navbar

