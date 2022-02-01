import pandas as pd
import os
from typing import List, Dict, Tuple, Optional
import colors
import sys
from itertools import combinations
from glob import glob
from multiprocessing import Pool, Manager
from dataclasses import dataclass
import re

color_formatter: colors.Color = colors.Color()

# create three classes that have the indices for the ibd files
@dataclass
class Ibd_Indices:
    """ base class that will just have the indices that are the same for the columns in the ibd files"""
    id1: int = 0
    phase1: int = 1
    id2: int = 2
    phase2: int = 3
    chr: int = 4
    start: int = 5
    end: int = 6

@dataclass
class Hapibd_Indx(Ibd_Indices):
    """This class has the specific length class for the hapibd file"""
    len: int = 7

@dataclass
class Ilash_Indx(Ibd_Indices):
    """This class has the specific length class for the ilash file"""
    rs1: int = 7
    rs2: int = 8
    len: int = 9
    score: int = 10

@dataclass
class Pair_Segments:
    """class object that will contain information about the object"""
    pair1: str
    pair2: str
    segments: List[Tuple[int, int]] = []
    overlap_others: Optional[Dict[Tuple[int, int], int]] = {}

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

def _get_chr_num(file_name: str) -> str:
    """Function that finds the chromosome number from the file name"""
    
    # finding the chromosome from the filename. This also includes the ending period that it tries to find
    match = re.search("chr\d*.", file_name)

    # return the chromosome number without the trailing period
    return match.group(0)[:-1]

def _search_file(ibd_file: str, pair: Tuple[str, str], return_dict: Dict, ibd_indx: Ibd_Indices, search_space: Optional[List[Tuple[int, int]]] = None) -> Dict:
    """Function that will search through the ibd file and return a dictionary that has the results
    
    Parameters
    
    ibd_file : str
        filepath to the ibd file that the program will search through
        
    pair : Tuple[str, str]
        tuple of ids for the pair of interest
    
    Returns
    
    Dict
    """
    # getting the chromosome number to use a key in the return_dict
    chr_num: str = _get_chr_num(ibd_file)

    # creating the object for the pair
    pair_obj: Pair_Segments = Pair_Segments(pair[0], pair[1])

    # setting a chunksize so we don't have to read the entire file at once
    for chunk in pd.read_csv(ibd_file, sep="\t", header=None, chunksize=100000):

        # filtering the chunk for both pair 1 and pair 2 in the pair tuple
        filter_chunk: pd.DataFrame = chunk[((chunk[0] == pair_obj.pair1) & (chunk[2] == pair_obj.pair2)) | ((chunk[0] == pair_obj.pair2) & (chunk[2] == pair_obj.pair1 ))]
        # at this point the dataframe should only have the information for these two ids. If there is no search space provide then we
        # can just pull out the start and the end position and then append these tuples to the position_list. If there is a search space
        # provided then we need to iterate over each value in the search space and see if the region overlaps this segment.
        if search_space:

            for start, end in search_space:
                loc_filtered_chunk: pd.DataFrame = filter_chunk[((filter_chunk[ibd_indx.start] <= start) & (filter_chunk[ibd_indx.end] > start)) | 
                                                ((filter_chunk[ibd_indx.start] >= start) & (filter_chunk[ibd_indx.end] <= end)) | 
                                                ((filter_chunk[ibd_indx.start] <= end) & (filter_chunk[ibd_indx.end] >= end))]
                # At this point we have filtered the dataframe so that it is only regions that overlap the gene of interest. If the dataframe is 
                # empty than we record that segment in the overlap_others attribute and set it = to zero. Otherwise we set it equal to 1
                if loc_filtered_chunk.empty:
                    pair_obj.overlap_others[(start, end)] = 0
                else:
                    pair_obj.overlap_others[(start, end)] = 1
                
                # now we are going to add the segments to the segments attribute of the pair object
                pair_obj.segments.extend(list(zip(loc_filtered_chunk[ibd_indx.start], loc_filtered_chunk[ibd_indx.end])))
        else:
            pair_obj.segments.extend(list(zip(filter_chunk[ibd_indx.start], filter_chunk[ibd_indx.end])))
    # adding the pair_obj to the return dictionary
    return_dict[chr_num] = pair_obj

    return return_dict

def _parallelize(pair: Tuple[str, str], ibd_files: List[str], ibd_indx: Ibd_Indices, search_space: Optional[List[Tuple]] = None) -> Dict[str, Dict[Tuple[str, str]]]:
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
    pairs_dict: Dict[Tuple[str, str], Dict[str, Tuple[int, int]]] = manager.dict()

    for file in ibd_files:
        pool.apply_async(_search_file, args=(file, pair, pairs_dict, ibd_indx, search_space))

    pool.close()

    pool.join()

    return pair, dict(pairs_dict) 

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
        # You need to get the indices for the ibd file depending on which program is being used
        if program == "hapibd":
            program_indx: Hapibd_Indx = Hapibd_Indx()
        else:
            program_indx: Ilash_Indx = Ilash_Indx()

        # get the correct ibd file directory
        ibd_directory: str = _get_ibd_directory(program)

        # Returns a tuple where the first value is the ibd program and the second value is a list of filepaths 
        # to the ibd files
        ibd_info: Tuple[str, List[str]] = _get_ibd_files(program, ibd_directory)

        # if the program is run in verbose mode then tell the user how many ibd files were used
        if os.environ["verbose"] == "True":
            print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"found {len(ibd_info[1])} files for the ibd program, {program}")
        
        # getting all combinations of the different pairs that are not the distant pairs
        grid_combinations: List[Tuple[str, str]] = [pair for pair in combinations(grids, 2) if pair != distant_pairs or pair != tuple(reversed(distant_pairs))]
        # parallelize the first pair so that it searches for all shared ibd segments across the whole genome. 
        # This will not pass the second argument 'search_space' to the function because no shared segments have
        # been identified yet
        pair_info, segment_dicts = _parallelize(distant_pairs, ibd_info[1], program_indx)

        output_dict["program"][pair_info] = segment_dicts
        





