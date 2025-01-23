from typing import Optional
from abc import abstractmethod
from pydantic import BaseModel, Field

from dash import html
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_lumino_components as dlc

class WidgetConfig(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str = Field(default="Widget")
    icon: Optional[str] = Field(default="mdi:question-mark-box")
    width: Optional[int] = Field(default=20)
    props: Optional[dict] = Field(default_factory=dict)

    def get_button_component(self):
        return dmc.ActionIcon(
            DashIconify(
                icon=self.icon,
                width=self.width,
            ),
            id=f"{self.id}-button",
            className="mb-1",
        )
    
    def get_widget_component(self, id_suffix: str = ""):
        return dlc.Widget(
            self._get_custom_component(), 
            id=f"{self.id}-widget-{id_suffix}",
            title=self.title, 
            # icon=DashIconify(
            #     icon=self.icon,
            #     width=self.width,
            # ),
            caption="A Widgie!",
            icon="ph:table-duotone"
        )
    

    def _get_custom_component(self):
        return dlc.BoxPanel(id=f"{self.id}-custom-component")

class WidgetGroupConfig(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    widget_buttons: list[WidgetConfig] = Field(default_factory=list)
    icon: Optional[str] = Field(default="mdi:question-mark-box")
    icon_width: Optional[int] = Field(default=20)
    

    def get_component(self):

        buttons = []
        for widget_button in self.widget_buttons:
            buttons.append(widget_button.get_button_component())

        return dmc.AccordionItem(
                [
                    dmc.AccordionControl(
                        self.id,
                        icon=DashIconify(
                            icon=self.icon,
                            width=self.icon_width,
                        ),
                    ),
                    dmc.AccordionPanel(
                        dmc.Flex(
                            buttons,
                            gap="10px",
                            direction="row",
                            justify="flex-start",
                            align="center",
                        ),
                    ),
                ],
                value=f"{self.id}_value",
            )


class LayoutConfig(BaseModel):
    title: str = Field(default="Ether Dashboard")
    widget_group_configs: list[WidgetGroupConfig] = Field(default_factory=list)

    def get_navbar_layout(self):

        widget_group_components = []
        for widget_group_config in self.widget_group_configs:
            widget_group_components.append(widget_group_config.get_component())

        return dmc.Accordion(
                disableChevronRotation = True,
                multiple = True,
                children = widget_group_components
            )

    
    def get_core_layout(self):

        return  dlc.BoxPanel(
                    [dlc.DockPanel([], id="dock-panel")], 
                    id="box-panel", 
                    addToDom=True,
                )

    
    def get_layout(self):
        return dmc.AppShell(
            [
                dmc.AppShellHeader(
                    dmc.Group(
                        [
                            dmc.Burger(
                                id="mobile-burger",
                                size="sm",
                                hiddenFrom="sm",
                                opened=False,
                            ),
                            dmc.Burger(
                                id="desktop-burger",
                                size="sm",
                                visibleFrom="sm",
                                opened=True,
                            ),
                            dmc.Title("Ether Dashboard", c="blue"),
                        ],
                        h="100%",
                        px="md",
                    )
                ),
                dmc.AppShellNavbar(
                    [
                        self.get_navbar_layout(),
                    ],
                    id="navbar",
                    p="md",
                ),
                dmc.AppShellMain(dmc.AppShellSection([self.get_core_layout()]),style={"display": "flex", "flexDirection": "column"}),
            ],
            header={
                "height": {"base": 60, "md": 70, "lg": 80},
            },
            navbar={
                "width": {"base": 200, "md": 300, "lg": 400},
                "breakpoint": "sm",
                "collapsed": {"mobile": True},
            },
            padding="md",
            id="appshell",
        )