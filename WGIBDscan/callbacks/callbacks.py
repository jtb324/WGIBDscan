import os
from pathlib import Path

class GridFileNotFound(Exception):
    """Excepion that will be raised if the grid file is not found"""

    def __init__(self, filepath: str, message: str) -> None:
        self.filepath = filepath
        self.message = message
        super().__init__(message)

class IncorrectFileType(Exception):
    """Exception that will be raised if the user does not pass a text file for the grid file"""
    def __init__(self, message: str) -> None:
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

    # check to make sure that the file exists
    if not path.is_file(): 
        raise GridFileNotFound(
            filepath,
            f"The file {filepath} was not found. Please make sure that you provide the program a text file with a list of individuals"
            )
    
    # check to make sure that the file is a text file
    if filepath[-3:] != "txt":
        raise IncorrectFileType("The grid file provided was not the appropriate file type. Please provide a text file.")

    return filepath