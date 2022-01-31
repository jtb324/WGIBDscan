import pandas as pd
import os
from typing import List, Dict, Tuple, Optional
import colors
import sys
from itertools import combinations
from glob import glob
from multiprocessing import Pool, Manager

color_formatter: colors.Color = colors.Color()

def _get_ibd_files(ibd_program: str, ibd_directory: str) ->  Tuple[str, List[str]]:
    """Function that will identify the correct files for each ibd program and return them in a tuple
    
    Parameters
    
    idb_program : str
        name of the ibd program. Should either be hapibd or ilash

    ibd_directory : str
        filepath to the directory with ibd files
    Returns

    Tuple[str, List[str]]
        returns a tuple where the first value is the ibd program and the second value is a list of files
    """
    # get the current directory so we can switch back to it
    cur_dir: str = os.getcwd()

    suffix_dict: Dict[bool, str] = {
        True: "*.match.gz",
        False: "*ibd.gz"
    }

    # change to the directory with the ibd files
    os.chdir(ibd_directory)

    file_list: List[str] = []

    # iterating through the files that match the suffix provided by the suffix dict and appending them to a list
    for file in glob(suffix_dict[ibd_program == "ilash"]):

        file_list.append(os.path.join(ibd_directory, file))

    os.chdir(cur_dir)  

    return ibd_program, file_list


def _get_ibd_directory(ibd_program: str) -> str:
    """Function to return the ibd file directory from the dictionary
    
    Parameters
    
    ibd_program : str
        should be the title of the ibd program. This values is expected to be either 
        hapibd or ilash 
    
    Returns
    
    str
        returns the directory filepath
    """
    ibd_directories: Dict[str, str] = {
        "ilash" : "/data100t1/share/BioVU/shapeit4/Eur_70k/iLash/min100gmap/",
        "hapibd" : "/data100t1/share/BioVU/shapeit4/Eur_70k/hapibd/"
    }

    # getting the correct directory or returning a value of Not Found
    ibd_directory: str = ibd_directories.get(ibd_program, "Not Found")

    if ibd_directory == "Not Found":
        print(color_formatter.RED + "ERROR: " + color_formatter.RESET + "Ibd program not found. Program expecting either the values 'hapibd' or 'ilash'")
        sys.exit(1)
    
    return ibd_directory

def _search_file(ibd_file: str, pair: str, ) -> Dict:
    """Function that will search through the ibd file and return a dictionary that has the results"""
    pass

def _parallelize(pair: Tuple[str, str], ibd_files: List[str], search_space: Optional[List[Tuple]] = None) -> Dict[str, Dict[Tuple[str, str]]]:
    """Function that will parallelize this search for pair's shared ibd segments across the whole genome
    
    Parameters
    
    pair : Tuple[str, str]
        tuple that has two strings with the pairs ids
        
    ibd_files : List[str]
        list of filepaths to the ibd files for hapibd or ilash
        
    search_space : Optional[List] = None
        optional argument that tells all the shared segment sites that are shared between pairs
        
    Returns
    
    Dict[Tuple[str, str], Dict]
        returns a dictionary of dictionaries where the outer key is the chromosome number and the value is a dictionary that has a tuple of pair ids for the keys 
        and the values are the shared segment start and end points
    """

    # now we will parallelize over this process to run multiple things at the same time
    if os.environ["verbose"] == "True":
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Parallelizing to {len(ibd_files)} cpu cores")

    manager = Manager()

    pool = Pool(len(ibd_files))

    # creating a dictionary that will be provided to each pool. This dictionary becomes immutable in each pool 
    # once something is added to it
    pairs_dict: Dict[str, Dict[Tuple[str, str], Tuple[int, int]]] = manager.dict()

    for file in ibd_files:
        pool.apply_async(None, args=(file))

    pool.close()

    pool.join()

    pass 

#Need a function that will take the total number of grids and the most distantly related grids
def search_segments(distant_pairs: Tuple[str, str], grids: List[str], ibd_programs: List[str], output: str) -> None:
    """Function to search the ibd files to find the segments that each pair shares across the whole genome
    
    Parameters
    
    distant_pairs : Tuple[str, str]
        tuple that has the pair that is the most distantly related
    
    grids : List[str]
        list of grid ids
    
    hapibd_programs : List[str]
        list of ibd programs that the user wants to search through
    
    output : str
        directory that the user wants to write the output to
    """

    output_dict: Dict[str, Optional[Dict]] = {program: {} for program in ibd_programs}

    # For loop will iterate over the different ibd programs 
    for program in ibd_programs:
        # get the correct ibd file directory
        ibd_directory: str = _get_ibd_directory(program)

        # Returns a tuple where the first value is the ibd program and the second value is a list of filepaths 
        # to the ibd files
        ibd_info: Tuple[str, List[str]] = _get_ibd_files(program, ibd_directory)

        # if the program is run in verbose mode then tell the user how many ibd files were used
        if os.environ["verbose"] == "True":
            print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"found {len(ibd_info[1])} for the ibd program {program}")
        
        # getting all combinations of the different pairs that are not the distant pairs
        grid_combinations: List[Tuple[str, str]] = [pair for pair in combinations(grids, 2) if pair != distant_pairs or pair != tuple(reversed(distant_pairs))]
        # parallelize the first pair so that it searches for all shared ibd segments across the whole genome. 
        # This will not pass the second argument 'search_space' to the function because no shared segments have
        # been identified yet
        segment_dicts: Dict[Tuple[str,str], Dict] = _parallelize(distant_pairs, ibd_info[1])
        





