#!/usr/bin/env python 
import typer
from typing import List, Tuple
import callbacks
import ersa_search
import logger
from os import environ
import colors

app = typer.Typer(add_completion=False)

color_formatter: colors.Color = colors.Color()

def display_input(grid_list: str, cores: int, ibd_files: List[str], output: str, ersa: str) -> None: 
    """Function to print all the inputs the user provided if the program is run in verbose mode"""
    print(color_formatter.GREEN + "SUCCESS: Successfully loaded in the user parameters" + color_formatter.RESET)
    print(color_formatter.BOLD + "Provided Input" + color_formatter.RESET)
    print(color_formatter.BOLD + "~"*40 + color_formatter.RESET)
    print(color_formatter.BOLD + "Grids File:" + color_formatter.RESET + grid_list)
    print(color_formatter.BOLD + "CPU cores:" + color_formatter.RESET + str(cores))
    print(color_formatter.BOLD + "IBD File directories:" + color_formatter.RESET + ",.".join(list(ibd_files)))
    print(color_formatter.BOLD + "Output Directory:" + color_formatter.RESET + output)
    print(color_formatter.BOLD + "ERSA File Directory:" + color_formatter.RESET + ersa + "\n")

@app.command()
def main(
    grid_list: str = typer.Option(
        "...",
        help="Filepath to a text file that has a list of grids",
        callback=callbacks.check_grid_file,
    ),
    cores: int = typer.Option(
        1, help="Number of cpu cores to be used during the programs execution"
    ),
    ibd_files: List[str] = typer.Option(
        None,
        help="List of filepaths to the directories with the either ilash or hapibd files. The format of these arguments should be '--ibd_files hapibd --ibd_files ilash'",
        callback=callbacks.check_ibd_files,
    ),
    output: str = typer.Option(
        "./whole_genome_ibdscan.txt",
        help="text file that the output will be written to",
    ),
    ersa: str = typer.Option("...", help="filepath to the relatedness files from ERSA"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Optional Flag to run the program in verbose mode", is_flag=True)
) -> None:
    """CLI tool to determine how many IBD segments a group of grids shares across the entire genome"""
    log_obj = logger.create_logger(log_level="debug")

    # setting an environmental variable for the verbose flag in the program
    environ["verbose"] = str(verbose) 

    if verbose:
        display_input(grid_list, cores, ibd_files, output, ersa)
    
    # log_obj.info(f"ersa filepath: {ersa}")
    # getting a list of grids from the user input file
    grids: List[str] = ersa_search.get_grids(grid_list)

    pairs: Tuple[str, str] = ersa_search.determine_minimal_relatedness(ersa, grids, cores, output)

    print(pairs)

if __name__ == "__main__":
    app()
