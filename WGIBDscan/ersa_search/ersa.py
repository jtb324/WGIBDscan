import pandas as pd
from glob import glob
from typing import List, Generator, Tuple, Dict, Optional
import os
import gzip
from multiprocessing import Pool, Manager


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
    ersa_df: pd.DataFrame = pd.DataFrame(ersa_file, header=None)

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

    # Open the ersa file using gzip and then read through each line. We need to skip the first lines that
    # have the '#'. Then decode the line and split it into a list so that we can compare the pair_1 and pair_2
    with gzip.open(file, "rb") as ersa_file:

        for line in ersa_file:

            if line.decode()[0] == "#":
                continue
            else:
                split_line: List[str] = line.decode().split()

                pair_1, pair_2 = split_line[0], split_line[1]

                if pair_1 in grids and pair_2 in grids:

                    pairs_dict[(pair_1, pair_2)] = split_line[3]


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
    manager = Manager()

    pool = Pool(workers)

    pairs_dict: Dict[Tuple[str, str], int] = manager.dict()

    for file in file_list:
        pool.apply_async(_get_relatedness, args=(file, grid_list, pairs_dict))

    pool.close()

    pool.join()

    return dict(pairs_dict)


def determine_minimal_relatedness(
    ersa_filepath: str, grid_list: List[str], workers: int
) -> Tuple[str, str]:
    """Function to determine the pairs with the least relatedness

    Parameters

    ersa_filepath : str
        filepath to the directory with ERSA's output

    grid_list : List[str]
        list of grid ids that the minimal relatedness will be determined for

    Returns

    Tuple[str, str]
        returns a tuple where the first string is a grid id and the second string is a grid id
    """
    subgroup_gen: Generator = _gather_ersa_grid_files(ersa_filepath)

    # creating a list that we will append the sub group number to if it contains a grid of interest.
    # Ex: ["sub1", "sub2"]
    subgroups_list: List[str] = []

    # iterating through each of the files in the subgroup generator
    for file in subgroup_gen:

        grids_found: int = _check_grids(file, grid_list)

        # if there are grids within the file then it finds the subgroup with the filename and appends it
        # to the sub_groups_list
        if grids_found == 1:

            subgroup: str = _get_subgroup(file)

            subgroups_list.append(subgroup)

    _search_ersa_files(subgroups_list, grids_found, ersa_filepath, workers)
