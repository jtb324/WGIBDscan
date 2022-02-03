from typing import List, Dict, Tuple, Optional
import search_ibd_files

def _get_chr(chromosome_number: str) -> str:
    """Function to get the actual digits of the chromosome number. Ex: chr19 -> 19
    
    Parameters
    
    chromosome_number : str
        string that has the chromsome number in the form of chr**, where astericks are digits
        
    Returns
    
    str
        returns a string that has only the digits of the chromosome
    """

    # if the chromosome is just chr* then it returns the "0" + the last digit. If it is chr** then 
    # it returns the last 2 characters. If it is anything else then it returns 0 which is an illogical 
    # value
    if len(chromosome_number) == 4:
        return "0" + chromosome_number[-1]
    elif len(chromosome_number) == 5:
        return chromosome_number[-2:]
    else:
        return 0

def _determine_overlap(overlap_dict: Dict[Tuple[str, str], int], partial_output_str: str, chr_num: str, file_handle) -> None:
    """Function that will determine the overlap and will then write the output to a file"""

    if overlap_dict:
        for segment, value in overlap_dict.items():
            file_handle.write("".join([partial_output_str,f"{value}\t{''.join([chr_num, ':',str(segment[0]), '-', str(segment[1])])}\n"]))
    else:
        overlaps: int = 0
        overlapping_segment: Optional[str] = None

        file_handle.write("".join([partial_output_str,f"{overlaps}\t{overlapping_segment}\n"]))

def write(filename: str, output_dict: Dict[str, Dict[Tuple, Dict]]) -> None:
    """Main function for writing the output to a file"""
    with open(filename, "w") as output:

        output.write("pair_1\tpair_2\tibd_program\tchromosome\tsegment_start\tsegment_end\toverlaps\toverlapping_segment\n")
        # this for loop iterates over the program and then the pair dict where the key is a tuple of 
        # pair ids and the values are dictionaries that have objects for each chromosome
        for program, pair_dict in output_dict.items():
            
            # This for loop iterates over the pair ids as keys and the values are dictionaries with 
            # the chromosome numbers as keys and the Pair_Segments object as values
            for pair_tuple, chromo_dict in pair_dict.items():
                
                pair_1: str = pair_tuple[0]

                pair_2: str = pair_tuple[1]

                filtered_chromo: Dict[str, search_ibd_files.Pair_Segments] = search_ibd_files.find_search_space(chromo_dict)

                for chromo, pair_object in filtered_chromo.items():
                    
                    chr_num: str = _get_chr(chromo)

                    if len(pair_object.segments) != 0:
                        for segment in pair_object.segments:
                            segment_start: int = segment[0]

                            segment_end: int = segment[1]

                            _determine_overlap(pair_object.overlap_others, f"{pair_1}\t{pair_2}\t{program}\t{chr_num}\t{segment_start}\t{segment_end}\t", chr_num, output)
                    else:
                        segment_start: Optional[int] = None

                        segment_end: Optional[int] = None

                        _determine_overlap(pair_object.overlap_others, f"{pair_1}\t{pair_2}\t{program}\t{chr_num}\t{segment_start}\t{segment_end}\t", chr_num, output)
                    
                    


                    


