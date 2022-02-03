import pandas as pd
import os
from typing import List, Dict, Tuple, Optional
import colors
import sys
from itertools import combinations
from glob import glob
from multiprocessing import Pool, Manager
from dataclasses import dataclass, field
import re
import search_ibd_files

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
    segments: List[Tuple[int, int]] = field(default_factory=list)
    overlap_others: Optional[Dict[Tuple[int, int], int]] = field(default_factory=dict)

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

    if os.environ["verbose"] == "True":
        print(color_formatter.BOLD +" INFO: " + color_formatter.RESET + f"Identified {len(file_list)} ibd files in the directory {ibd_directory}")
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

def _search_file(ibd_file: str, pair: Tuple[str, str], return_dict: Dict, ibd_indx: Ibd_Indices, search_space: Optional[Dict[str, Pair_Segments]] = None) -> Dict:
    """Function that will search through the ibd file and return a dictionary that has the results
    
    Parameters
    
    ibd_file : str
        filepath to the ibd file that the program will search through
        
    pair : Tuple[str, str]
        tuple of ids for the pair of interest
    
    return_dict : dict[str, Pair_Segments]
        dictionary that will be returned by the function that has the chromosome number as the key 
        and has the Pair_Segments object as the value

    ibd_indx : Ibd_Indices
        object that has the indices for a specific ibd program so that the correct columns will be accessed

    search_space : Optional[Dict[str, Pair_Segments]] = None
        dictionary that has all the positions of the ibd programs that for the chromosomes of interest. 
        This argument is either a dictionary or the default value of None
    
    Returns
    
    Dict
        returns a dictionary that has the chromosome number as the key and the Pair object as the value
    """
    if os.environ["verbose"] == "True":
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Searching through the file {ibd_file}")
    
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
            # need to pull out the search space list from the search_space. Need to pull out the right list for the appropriate chromosome
            search_space_list: List[Tuple[int, int]] = search_space[chr_num].segments

            for start, end in search_space_list:
                if pair_obj.overlap_others.get((start, end), 0) == 0:
                    loc_filtered_chunk: pd.DataFrame = filter_chunk[((filter_chunk[ibd_indx.start] <= start) & (filter_chunk[ibd_indx.end] > start)) | 
                                                ((filter_chunk[ibd_indx.start] >= start) & (filter_chunk[ibd_indx.end] <= end)) | 
                                                ((filter_chunk[ibd_indx.start] <= end) & (filter_chunk[ibd_indx.end] >= end))]
                # At this point we have filtered the dataframe so that it is only regions that overlap the gene of interest. If the dataframe is 
                # empty than we record that segment in the overlap_others attribute and set it = to zero. Otherwise we set it equal to 1

                    if loc_filtered_chunk.empty:
                        pair_obj.overlap_others[(start, end)] = 0
                    else:
                        pair_obj.overlap_others[(start, end)] = 1
                else:
                    continue
                
                # now we are going to add the segments to the segments attribute of the pair object
                pair_obj.segments.extend(list(zip(loc_filtered_chunk[ibd_indx.start], loc_filtered_chunk[ibd_indx.end])))
        else:
            pair_obj.segments.extend(list(zip(filter_chunk[ibd_indx.start], filter_chunk[ibd_indx.end])))
    # adding the pair_obj to the return dictionary
    return_dict[chr_num] = pair_obj

    return return_dict
def _filter_ibd_files(search_space: List[str], ibd_files: List[str]) -> List[str]:
    """Function that will filter the ibd_files list to include only those files which match the chromosomes in the search space
    
    Parameters
    
    search_space : List[str]
        list of chromosomes to filter the ibd files down to
        
    ibd_files : List[str]
        list of ibd files
        
    Returns
    
    List[str]
        returns a list of the files that include the chromosome numbers
    """
    # add a . to the end of the search space substrings so that the files are correctly identified. Ex:
    # substring will be chr1. which will prevent the function from returning both chr1 and chr10 as a match
    substrings: List[str] = [chr_num + "." for chr_num in search_space]

    # filter the ibd files if any of the substrings match the files
    return [file for file in ibd_files if any(substring in file for substring in substrings)]

def _parallelize(pair: Tuple[str, str], ibd_files: List[str], ibd_indx: Ibd_Indices, search_space: Optional[List[Tuple]] = None) -> Dict[str, Dict[Tuple[str, str], List]]:
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
    if os.cpu_count() < len(ibd_files):
        workers: int = int(os.cpu_count())
    else:
        workers: int = len(ibd_files)    # now we will parallelize over this process to run multiple things at the same time
    if os.environ["verbose"] == "True":
        
        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"Parallelizing to {workers} cpu cores")

        print(color_formatter.BOLD + "INFO: " + color_formatter.RESET + f"search space for {len(search_space.keys())}" if search_space else "no search space provided")

    # creating a manager so that I can keep track of the dictionary throughout the parallelization
    manager = Manager()

    # creating a dictionary that will be provided to each pool. This dictionary becomes immutable in each pool 
    # once something is added to it
    pairs_dict: Dict[Tuple[str, str], Dict[str, Tuple[int, int]]] = manager.dict()
    # if search space is none then we can just parallelize normally. Otherwise we need to filtdr down to the correct ibd file and then 
    if search_space == None:
        pool = Pool(workers)

        for file in ibd_files:    

            pool.apply_async(_search_file, args=(file, pair, pairs_dict, ibd_indx, search_space))

    else:
        filtered_ibd_files: List[str] = _filter_ibd_files(list(search_space.keys()), ibd_files)
        
        pool = Pool(workers)

        for file in filtered_ibd_files:
            pool.apply_async(_search_file, args=(file, pair, pairs_dict, ibd_indx, search_space))

    pool.close()

    pool.join()

    return pair, dict(pairs_dict) 

def find_search_space(segments_dict: Dict[str, Pair_Segments]) -> Dict[str, Pair_Segments]:
    """Function that will identify which chromosomes have idb segments
    
    Parameters
    
    segments_dict : Dict[str, Pair_Segments]
        dictionary that has the chromosome numbers as keys and it has the Pair_Segments as values. 
        These values have attributes pair1, pair2, segments, and overlap_others. If segments is not empty 
        then there were shared segments found for the pair
    
    Returns
    
    Dict[str, Pair_Segments]
        returns a filter version of the input dictionary where there are only chromosomes that have found 
        ibd segments
    """

    return {chr_num: pair_obj for chr_num, pair_obj in segments_dict.items() if len(pair_obj.segments) != 0}

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

        grid_combinations: List[Tuple[str, str]] = [pair for pair in combinations(grids, 2) if pair != distant_pairs and pair != tuple(reversed(distant_pairs))]

        # parallelize the first pair so that it searches for all shared ibd segments across the whole genome. 
        # This will not pass the second argument 'search_space' to the function because no shared segments have
        # been identified yet
        pair_info, segment_dicts = _parallelize(distant_pairs, ibd_info[1], program_indx)

        # adding the pair as a key to the output dictionary for the correct program while the dictionary with the different pair 
        # objects for each chromosome will  be added as values
        output_dict[program][pair_info] = segment_dicts

        # Determining the search space by filtering the input dictionary for only those chromosomes that have the 
        # shared ibd segments
        search_space: Dict[str, Pair_Segments] = find_search_space(output_dict[program][pair_info])

        # need to create the logic for the other pairs so that it reparallelizes out to find the other grid pairs
        for pair in grid_combinations:

            if len(search_space) != 0:
                pair_info, segment_dicts = _parallelize(pair, ibd_info[1], program_indx, search_space)

                output_dict[program][pair_info] = segment_dicts

            else:
                pair_info, segment_dicts = _parallelize(pair, ibd_info[1], program_indx)

                output_dict[program][pair_info] = segment_dicts

        search_ibd_files.write(os.path.join(output, "whole_genome_ibd_sharing.txt"), output_dict)




