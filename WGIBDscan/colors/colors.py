class Color:
    """Class to handle changing colors in the terminal output"""
    def __init__(self) -> None:

        self.RED: str   = "\033[1;31m"  
        self.BLUE: str  = "\033[1;34m"
        self.GREEN: str = "\033[0;32m"
        self.RESET: str = "\033[0;0m"
        self.BOLD: str    = "\033[;1m"
        self.REVERSE: str = "\033[;7m"