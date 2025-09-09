from typing import Annotated, Optional, Union, Literal

from pydantic import BaseModel, Discriminator, RootModel, Tag

from cmapi_server.constants import (
    DEFAULT_MCS_CONF_PATH, DEFAULT_SM_CONF_PATH, ClusterModeEnum,
)
from cmapi_server.managers.application import StatefulConfigModel


class BaseConfigPutRequestModel(BaseModel):
    timeout: int
    test: Optional[bool] = False


class BaseFullStatefulConfigPutRequestModel(BaseConfigPutRequestModel):
    only_stateful_config: Optional[bool] = False
    stateful_config_dict: StatefulConfigModel


class StatefulConfigPutRequestModel(BaseFullStatefulConfigPutRequestModel):
    type: Literal['stateful'] = 'stateful'


class PutConfigSetModeRequestModel(BaseConfigPutRequestModel):
    type: Literal['set_mode'] = 'set_mode'
    revision: str
    manager: str
    cluster_mode: ClusterModeEnum


class FullConfigPutRequestModel(BaseFullStatefulConfigPutRequestModel):
    type: Literal['full'] = 'full'
    revision: str
    manager: str
    cluster_mode: Optional[str] = None
    config: Optional[str] = None
    sm_config: Optional[str] = None
    mcs_config_filename: Optional[str] = DEFAULT_MCS_CONF_PATH
    sm_config_filename: Optional[str] = DEFAULT_SM_CONF_PATH
    secrets: Optional[dict] = None


def get_discriminator_value(data: dict) -> str:
    """Custom discriminator function for ConfigRequest union."""
    if data.get('cluster_mode') is not None:
        return 'set_mode'
    if data.get('only_stateful_config', False):
        return 'stateful'
    return 'full'


class ConfigPutRequestRootModel(
    RootModel[
        Annotated[
            Union[
                Annotated[StatefulConfigPutRequestModel, Tag('stateful')],
                Annotated[PutConfigSetModeRequestModel, Tag('set_mode')],
                Annotated[FullConfigPutRequestModel, Tag('full')]
            ],
            Discriminator(get_discriminator_value),
        ]
    ]
):
    pass
