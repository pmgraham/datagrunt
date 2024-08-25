import logging

def show_warning(message):
    """Show a warning message.
    
    Args:
        message (str): The message to show.
    """
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s - %(message)s')
    return logging.warning(message)