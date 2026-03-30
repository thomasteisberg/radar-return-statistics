import logging

import click

from .runner import run


@click.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--reprocess", is_flag=True, help="Reprocess all frames, ignoring existing results")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def main(config_path: str, reprocess: bool, verbose: bool) -> None:
    """Process radar frames and store return statistics in icechunk."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run(config_path, reprocess=reprocess)


if __name__ == "__main__":
    main()
