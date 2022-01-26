import os
from pathlib import Path

class GridFileNotFound(Exception):
    """Excepion that will be raised if the grid file is not found"""

    def __init__(self, filepath: str, message: str) -> None:
        self.filepath = filepath
        self.message = message
        super().__init__(message)

def check_grid_file(filepath: str) -> str:
    """making sure that the grid file is found 
    Parameters
    __________
    filepath : str
        filepath to a text file has a list of grids that we compare ibd sharing for. 
        Each line of the file should have a grid ID.
    
    Returns
    _______
    str
        returns the filepath if it doesn't raise an error
    """
    path = Path(filepath)

    if not path.is_file(): 
        raise GridFileNotFound(
            filepath,
            f"The file {filepath} was not found. Please make sure that you provide the program a text file with a list of individuals"
            )
            
    return filepath