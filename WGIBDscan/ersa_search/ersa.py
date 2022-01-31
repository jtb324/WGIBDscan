import pandas as pd
from glob import glob
from typing import List, Generator, Tuple, Dict, Optional
import os
import gzip
from multiprocessing import Pool, Manager
import colors

color_formatter: colors.Color = colors.Color()

def _gather_ersa_grid_files(directory: str) -> Generator:
    """Function that identifies all the different files that have the grid subgroups and then returns them as a list
    
    Parameters
    
    directory : str
        directory to the output of ERSA program

    Returns
    
    Generator
        returns a generator that will have all the filepaths
    """

    for path in glob(os.path.join(directory, "EUR_grid.sub*")):
        yield path


def _check_grids(ersa_file: str, grids: List[str]) -> int:
    """Function that will check if any other the grids are in the specific ersa file
    
    Parameters
    
    ersa_file : str
        filepath to the ersa file that should have a name something like EUR_grid.sub*

    grids : List[str]
        list of grids of interest

    Returns
    
        returns a 1 if any of the grids are in this list or a zero if no grids are found in the list
    """
    ersa_df: pd.DataFrame = pd.read_csv(ersa_file, header=None)

    if any(ersa_df[0].isin(grids)):
        return 1
    else:
        return 0


def _get_subgroup(file: str) -> str:
    """Function that will pull the subgroup substring out of the filepath
    
    Parameters

    file : str
        filepath to the ERSA file

    Returns

        returns a substring that has the subgroup. Ex "sub1"
    """
    return file[-4:]


def _get_relatedness(file: str, grids: List[str], pairs_dict: Dict) -> None:
    """function that will iterate through the ersa file to find the pairs of individuals
    
    Parameters

    file : str
        filepath to the ersa file that has the estimated relateness for each pairs

    grids : List[str]
        list of grids that we want to determine the estimated relatedness for
    """
    if os.environ["verbose"] == "True":
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Searching through the file {file}")
    
    # Open the ersa file using gzip and then read through each line. We need to skip the first lines that
    # have the '#'. Then decode the line and split it into a list so that we can compare the pair_1 and pair_2
    with gzip.open(file, "rb") as ersa_file:
        
        for line in ersa_file:

            if line.decode()[0] != "#" and line.decode().split()[0] != "individual_1":
                
                split_line: List[str] = line.decode().split()

                pair_1, pair_2 = split_line[0], split_line[1]

                if pair_1 in grids and pair_2 in grids:

                    pairs_dict[(pair_1, pair_2)] = split_line[3]

    return pairs_dict


def _search_ersa_files(
    subgroups: List[str], grid_list: List[str], ersa_directory: str, workers: int
) -> Dict[Tuple[str, str], int]:
    """Function to search through the ersa files parallel.

    Parameters

    subgroups : List[str]
        list of subgroup strings such as ex: ["sub2", "sub3"]

    grid_list : List[str]
        list of grid ids that will be compared

    ersa_directory : str
        filepath to the directory that has the ersa files such as "EUR_sub*.ersa.gz

    Returns

    Dict[Tuple[str,str], int]
        returns a dictionary that has a tuple with the pair_1 and pair_2 ids as the key and has the estimated relatedness for each pair as a value.
    """
    # getting a list of files that have the specific subgroup numbers in them
    file_list: List[str] = [
        file
        for file in glob(os.path.join(ersa_directory, "EUR_sub*.ersa.gz"))
        if any(ele in file for ele in subgroups)
    ]

    # now we will parallelize over this process to run multiple things at the same time
    if workers != 1 and os.environ["verbose"] == "True":
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Found all the files that had the format 'EUR_sub*.ersa.gz' ")
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Parallelizing to {workers} cpu cores")

    manager = Manager()

    pool = Pool(workers)

    # creating a dictionary that will be provided to each pool. This dictionary becomes immutable in each pool 
    # once something is added to it
    pairs_dict: Dict[Tuple[str, str], int] = manager.dict()

    for file in file_list:
        pool.apply_async(_get_relatedness, args=(file, grid_list, pairs_dict))

    pool.close()

    pool.join()

    print("Identified all the relatedness of the different pairs of grids. Closing the pools...")

    return dict(pairs_dict)

def _lowest_relatedness(relatedness_dict: Dict[Tuple[str, str], int]) -> Tuple[str, str]:
    """Function that will return the tuple in the dictionary that is the most disztantly relateded, 
    meaning the highest number

    Parameters

    relatedness_dict: Dict[Tuple[str, str], int]

        Dictionary that has tuples for each pair found in the ersa file and then has the 
        estimated distant relatedness as values
    
    Tuple[str, str]
    
    returns the pair that is the most distantly related
    """

    return max(relatedness_dict, key=relatedness_dict.get)

def _record_pairs(grids_dict: Dict[Tuple[str,str], int], output_path: str) -> None:
    """Function to write the pairs relatedness to a file
    
    Parameters
    
    grids_dict : Dict[Tuple[str, str], int]
        dictionary that has the estimated distant relatedness of each pair where the 
        pairs are a tuple and the relatedness is the value
    
    output_path : str
        directory to writesd the output to
    """
    with open(os.path.join(output_path, "pair_relatedness.txt"), "w") as output:
        
        output.write("pair_1\tpair_2\testimated_relatedness\n")

        for key, value in grids_dict.items():

            output.write(f"{key[0]}\t{key[1]}\t{value}\n")

def determine_minimal_relatedness(
    ersa_filepath: str, grid_list: List[str], workers: int, output: str
) -> Tuple[str, str]:
    """Function to determine the pairs with the least relatedness

    Parameters

    ersa_filepath : str
        filepath to the directory with ERSA's output

    grid_list : List[str]
        list of grid ids that the minimal relatedness will be determined for

    workers : int
        number of cpus to be utilized during this step of the program

    output : str
        directory to write the output to. If verbose mode is selected then the 
        relatedness of each pair gets written to a file called pair_relatedness.txt 
        in the output directory

    Returns

    Tuple[str, str]
        returns a tuple where the first string is a grid id and the second string is a grid id
    """
    if os.environ["verbose"] == "True":
        print("Determine the pair from the provided grid list that is the most distantly related")

    # creating a generator that contains all of the filepaths to the ersa pairs files        
    subgroup_gen: Generator = _gather_ersa_grid_files(ersa_filepath)

    # creating a list that we will append the sub group number to if it contains a grid of interest.
    # Ex: ["sub1", "sub2"]
    subgroups_list: List[str] = []

    # iterating through each of the files in the subgroup generator and checking which ones have the grids of
    # interest in them. If these files have the grids then they are added to the sub_groups_list
    # that will be used later
    for file in subgroup_gen:

        grids_found: int = _check_grids(file, grid_list)
        
        # if grids_found == 1 then it means that the file has at least two of the grids of interest in a pair
        if grids_found == 1:

            subgroup: str = _get_subgroup(file)

            subgroups_list.append(subgroup)

    # This dictionary contains all the pairs within the files that have grids of interest and how distantly 
    # related they are
    grids_dict: Dict[Tuple[str,str], int] = _search_ersa_files(subgroups_list, grid_list, ersa_filepath, workers)

    # determining the pair of individuals who are least related
    least_rel_pair: Tuple[str, str] = _lowest_relatedness(grids_dict)

    # If the user chooses verbose mode then the pairs and there relatedness gets written to a file
    if os.environ["verbose"] == "True":

        _record_pairs(grids_dict, output)

    # returns the pair that is the most distantly related
    return least_rel_pair
