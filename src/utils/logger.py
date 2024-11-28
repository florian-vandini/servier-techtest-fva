"""Utils.logging module"""

# pylint: disable=C0301
# pylint: disable=W1203

import logging

# Create a logger
logger = logging.getLogger(__name__)

# Set the logging level (can be adjusted to DEBUG, INFO, WARNING, etc.)
logger.setLevel(logging.INFO)

# Create a console handler
console_handler = logging.StreamHandler()

# Create a log formatter
formatter = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s - %(message)s")

# Set the formatter for the handler
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)
