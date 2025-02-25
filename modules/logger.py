import logging
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file, level=logging.INFO):    
    """
    Sets up a logger with a specified name, log file, and logging level.
    
    Parameters:
    name (str): The name of the logger.
    log_file (str): The path to the log file.
    level (int, optional): The logging level. Defaults to logging.INFO.
    
    Returns:
    Logger: The configured logger instance.
    
    Process:
    1. Create a logger with the specified name.
    2. Check if the logger already has handlers to avoid adding multiple handlers.
    3. If no handlers are present:
       a. Create a RotatingFileHandler that rotates the log after reaching 10 MB and keeps 3 backup versions.
       b. Set the log message format to include the timestamp, log level, and message.
       c. Set the logger level to the specified level.
       d. Add the handler to the logger.
    4. Return the logger instance.
    """    
    logger = logging.getLogger(name)
    # Check if the logger already has handlers
    if not logger.handlers:  
        handler = RotatingFileHandler(log_file, maxBytes=10**6, backupCount=3) # Rotate log after reaching 10 MB, keep 3 backup versions
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

        logger.setLevel(level)
        logger.addHandler(handler)
    return logger

# Setup logger
data_logger = setup_logger('data_process', '\\\\Svrau055csm00.oceania.corp.anz.com\\TEPSS BTMC\\SHARE\\App Support\\Support\\3. Teams\\Technology Business Operations\\4. Practice Management\\Automation Tool\\Security\\Output files\\Security log.log', level=logging.DEBUG)