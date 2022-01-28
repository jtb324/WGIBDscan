import os
from pathlib import Path
from typing import List, Optional

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

class NoIBDFileProvided(Exception):
    """Exception that will be raised if the user does not provide any ibd files"""
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

def check_grid_file(filepath: str) -> str:
    """making sure that the grid file is found 
    
    Parameters

    filepath : str
        filepath to a text file has a list of grids that we compare ibd sharing for. 
        Each line of the file should have a grid ID.
    
    Returns

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

def check_ibd_files(ibd_filepath: Optional[List[str]]) -> List[str]:
    """Callback function to check if the user provided a file path to either a hapibd file or a ilash file
    
    Parameters
    
    ibd_filepath : Optional[List[str]]
        List of file paths to either a hapibd file or a ilash file. This value could be none if the user forgets to provide a value
    
    Returns
    
    List[str]
        returns the list if no errors are raised by the callback function
    """

    if ibd_filepath is None:
        raise NoIBDFileProvided("There was no ibd filepath provided. Please provide an ibd file using the flag '--ibd_files'")
    
    return ibd_filepath
