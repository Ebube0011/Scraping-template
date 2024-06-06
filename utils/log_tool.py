import logging
import logging.config
import logging.handlers

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "%(levelname)s - %(name)s - %(asctime)s  ->  %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "detailed": {
            "format": "[%(levelname)s|%(module)s|%(name)s|L%(lineno)d] - %(asctime)s  ->  %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "Logs/scraping_pipeline.log",
            "encoding": "utf-8",
            "maxBytes": 5000000,
            "backupCount": 3,
        },
    },
    "loggers": {
        "root": {
            "level": "DEBUG",
            "handlers": ["stdout", "file"],
        }
    },
}


def get_logger(phase: str):
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger(phase)
    #logger.setLevel(logging.DEBUG)
    return logger
