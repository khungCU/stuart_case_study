import logging

#Creating and Configuring logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("ETLlogger")