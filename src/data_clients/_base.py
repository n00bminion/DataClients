from data_clients import logger
import data_clients
from common_utils import config_handler


class BaseDataClient:
    def __init__(self, config_file=None):

        logger.info(f"Initiating {self.__class__.__name__} client")

        if config_file:
            self.config = config_handler.get_config(
                config_file, module_name=data_clients.__name__
            )
            logger.info(f"Using config file {config_file}")
        else:
            self.config = {}
            logger.warning(
                "No config file was passed in, config attribute is set to an empty dictionary"
            )

        # do some more stuff here if needed
