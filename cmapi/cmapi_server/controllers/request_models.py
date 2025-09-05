from typing import Annotated, Optional, Union, Literal

from pydantic import BaseModel, Discriminator, RootModel, Tag

from cmapi_server.constants import DEFAULT_MCS_CONF_PATH, DEFAULT_SM_CONF_PATH
from cmapi_server.managers.application import StatefulConfigModel


class BaseConfigPutRequestModel(BaseModel):
    test: Optional[bool] = False
    timeout: int
    only_stateful_config: Optional[bool] = False
    stateful_config_dict: StatefulConfigModel


class StatefulConfigPutRequestModel(BaseConfigPutRequestModel):
    type: Literal['stateful'] = 'stateful'


class FullConfigPutRequestModel(BaseConfigPutRequestModel):
    type: Literal['full'] = 'full'
    revision: str
    manager: str
    timeout: int
    cluster_mode: Optional[str] = None
    config: Optional[str] = None
    sm_config: Optional[str] = None
    mcs_config_filename: Optional[str] = DEFAULT_MCS_CONF_PATH
    sm_config_filename: Optional[str] = DEFAULT_SM_CONF_PATH
    secrets: Optional[dict] = None


def get_discriminator_value(data: dict) -> str:
    """Custom discriminator function for ConfigRequest union."""
    if data.get('only_stateful_config', False):
        return 'stateful'
    return 'full'


class ConfigPutRequestRootModel(
    RootModel[
        Annotated[
            Union[
                Annotated[StatefulConfigPutRequestModel, Tag('stateful')],
                Annotated[FullConfigPutRequestModel, Tag('full')]
            ],
            Discriminator(get_discriminator_value),
        ]
    ]
):
    pass
