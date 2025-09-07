from data_clients import logger


class BaseDataClient:
    def __init__(self):

        logger.info(f"Initiating {self.__class__.__name__} client")

        # do some more stuff here if needed
