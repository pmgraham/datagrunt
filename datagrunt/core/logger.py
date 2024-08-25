import logging

LARGE_FILE_WARNING = "Warning: File is large and may load into memory slowly or exceed memory capacity."

def show_warning(message):
    """Show a warning message.

    Args:
        message (str): The message to show.
    """
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    return logging.warning(message)

def show_large_file_warning():
    show_warning(LARGE_FILE_WARNING)
