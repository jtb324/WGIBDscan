#!/usr/bin/env python 
import typer
from typing import List
import callbacks
import ersa_search

app = typer.Typer(add_completion=False)


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
        callback=callbacks.check_ibd_files
    ),
    output: str = typer.Option(
        "./whole_genome_ibdscan.txt",
        help="text file that the output will be written to",
    ),
    ersa: str = typer.Option("...", help="filepath to the relatedness files from ERSA"),
) -> None:
    """CLI tool to determine how many IBD segments a group of grids shares across the entire genome"""

    # getting a list of grids from the user input file
    grids: List[str] = ersa_search.get_grids(grid_list)

    ersa_search.determine_minimal_relatedness(ersa, grids, cores)

if __name__ == "__main__":
    app()
