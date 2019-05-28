import logging
import os
import logging.handlers
logger = logging.getLogger()
#logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
loglevel = os.environ.get("LOGLEVEL", "INFO")
numeric_level = getattr(logging, loglevel.upper())
logging.basicConfig(level=numeric_level)
handler = logging.handlers.SysLogHandler(address='/dev/log')
logger.addHandler(handler)
logger.debug("debug testing")


