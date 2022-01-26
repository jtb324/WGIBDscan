import typer
from typing import List
import callbacks

app= typer.Typer(add_completion=False)

@app.command()
def main(
    grid_list: str = typer.Option("...", help="Filepath to a text file that has a list of grids", callback=callbacks.check_grid_file),
    cores: int = typer.Option(1, help="Number of cpu cores to be used during the programs execution"),
    ibd_files: List[str] = typer.Option("...", help="List of filepaths to the directories with the either ilash or hapibd files. The format of these arguments should be '--ibd_files hapibd --ibd_files ilash'"),
    output: str = typer.Option("./whole_genome_ibdscan.txt", help="text file that the output will be written to")
) -> None:
    """CLI tool to determine how many grids within a """
    pass

if __name__ == '__main__':
    app()