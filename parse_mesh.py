#!/usr/bin/env python3

import re

def parse_mesh(descriptor_file):
    desc_uis = []
    desc_names = []
    tree_num_lists = []

    desc_ui = re.compile(r"<DescriptorUI>(D\d+)</DescriptorUI")
    with open(descriptor_file, "r") as handle:
        line = handle.readline()
        while line:
            if line.startswith("<DescriptorRecord "):
                while not line.startswith("</DescriptorRecord"):
                    if line.startswith()
                    line = handle.readline()
            line = handle.readline()

def main():
    # Get command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Pubmed's MeSH descriptor data in XML format", 
                    required=True, type=str)
    parser.add_argument("-o", "--output", help="Output file to write data in a tab-delimited format")
    parser.add_argument("-q", "--quiet", help="Suppress printing of log messages to STDOUT. " \
                    "Warning: exceptions will not be printed to console", action="store_true")
    args = parser.parse_args()

    # Set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler("edge_list_builder.log")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if verbose:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)


if __name__ == "__main__":
    main()