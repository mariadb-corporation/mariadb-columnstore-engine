from __future__ import annotations
import logging
from pathlib import Path

from lxml import etree

from cmapi_server.constants import (
    DEFAULT_MCS_CONF_PATH, MCS_DATA_PATH, MCS_MODULE_FILE_PATH,
)


module_logger = logging.getLogger()


def read_module_id():
    """Retrieves module ID from MCS_MODULE_FILE_PATH.

    :rtype: int : seconds
    """
    module_file = Path(MCS_MODULE_FILE_PATH)
    return int(module_file.read_text()[2:])


# TODO: Useless for now, newer called in code
#       Nodeconfig.apply_config doing this.
def set_module_id(module_id: int = 1):
    """Sets current module ID from MCS_MODULE_FILE_PATH.

    :rtype: int : seconds
    """
    module_file = Path(MCS_MODULE_FILE_PATH)
    return module_file.write_text(f'pm{module_id}\n')


def get_dbroots_list(path: str = MCS_DATA_PATH):
    """searches for services

    The method returns numeric ids of dbroots available.

    :rtype: generator of ints
    """
    func_name = 'get_dbroots_list'
    path = Path(path)
    for child in path.glob('data[1-9]*'):
        dir_list = str(child).split('/') # presume Linux only
        dbroot_id = int(''.join(list(filter(str.isdigit, dir_list[-1]))))
        module_logger.debug(f'{func_name} The node has dbroot {dbroot_id}')
        yield dbroot_id


def get_workernodes() -> dict[dict[str, int]]:
    """Get workernodes list.

    Returns a list of network address of all workernodes.
    This is an equivalent of all nodes.

    :return: workernodes dict
    :rtype: dict[dict[str, int]]
    """
    # TODO: fix in MCOL-5147, get xml path from class that will handle xml
    root = current_config_root()
    workernodes = {}
    # searches for all tags starts with DBRM_Worker, eg DBRM_Worker1
    workernodes_elements = root.xpath(
        "//*[starts-with(local-name(), 'DBRM_Worker')]"
    )
    for workernode_el in workernodes_elements:
        workernode_ip = workernode_el.find('./IPAddr').text
        if workernode_ip == '0.0.0.0':
            # skip elements with specific ip
            continue
        try:
            workernode_port = int(workernode_el.find('./Port').text)
        except (AttributeError, ValueError):
            # AttributeError for not found Port tag, so got None.text
            # ValueError for non numeric values in tag text
            module_logger.error(
                'No Port tag found or wrong Port value for tag '
                f'"{workernode_el.tag}".'
            )
            workernode_port = 8700
        workernodes[workernode_el.tag] = {
            'IPAddr': workernode_ip, 'Port': workernode_port
        }
    return workernodes


def get_dbrm_master(config_filename: str = DEFAULT_MCS_CONF_PATH) -> dict:
    """Get DBRM master ip and port.

    :param config_filename: path to xml conf, defaults to DEFAULT_MCS_CONF_PATH
    :type config_filename: str, optional
    :return: ipaddress and port of DBRM master
    :rtype: dict
    """
    # TODO: fix in MCOL-5147, get xml path from class that will handle xml
    #       Use NodeConfig class as a template?
    root = current_config_root(config_filename)
    return {
        'IPAddr': root.find("./DBRM_Controller/IPAddr").text,
        'Port': root.find("./DBRM_Controller/Port").text
    }


def current_config_root(config_filename: str = DEFAULT_MCS_CONF_PATH):
    """Retrievs current configuration

    Read the config and returns Element

    :rtype: lxml.Element
    """
    parser = etree.XMLParser(load_dtd=True)
    tree = etree.parse(config_filename, parser=parser)
    return tree.getroot()
