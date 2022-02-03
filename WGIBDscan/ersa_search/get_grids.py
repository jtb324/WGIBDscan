from typing import List
import os
import colors

color_formatter: colors.Color = colors.Color()

def get_grids(grid_filepath: str) -> List[str]:
    """Function that gathers all of the grid IDs into a list that the user provided and then returns the list
    Parameters
    __________
    grid_filepath : str
        filepath to a text file that has all the grids
    
    Returns
    _______
    List[str]
        returns a list where each element is a GRID ID
    """
    grid_list: List[str] = []

    with open(grid_filepath, 'r') as grid_file:
        for line in grid_file:
            grid_list.append(line.strip())
        
    print(f"identified {len(grid_list)} grids from the file the user provided at {grid_filepath}")

    if os.environ["verbose"] == "True":
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Grid List: {', '.join(grid_list)}")
    return grid_list