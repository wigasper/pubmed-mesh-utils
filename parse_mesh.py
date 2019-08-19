#!/usr/bin/env python3

import re
import argparse
import logging

def parse_mesh(descriptor_file):
    desc_uis = []
    desc_names = []
    tree_num_lists = []

    desc_name_tag = re.compile(r"\s+</DescriptorName>")
    desc_rec_end_tag = re.compile(r"\s*</DescriptorRecord>")

    desc_ui = re.compile(r"<DescriptorUI>(D\d+)</DescriptorUI")
    desc_name = re.compile(r"<String>(.+)</String>")
    tree_num = re.compile(r"<TreeNumber>(.+)</TreeNumber")

    with open(descriptor_file, "r") as handle:
        line = handle.readline()
        while line:
            if line.startswith("<DescriptorRecord "):
                tree_nums = []
                relevant_lines = []

                while not desc_name_tag.search(line) and not line.startswith("</DescriptorRecord"):
                    relevant_lines.append(line.strip("\n"))
                    line = handle.readline()

                relevant_lines = "".join(relevant_lines)

                ui_match = desc_ui.search(relevant_lines)
                name_match = desc_name.search(relevant_lines)
                
                while not desc_rec_end_tag.search(line):
                    tree_match = tree_num.search(line)
                    if tree_match:
                        tree_nums.append(tree_match.group(1))
                    line = handle.readline()
                
                if ui_match and name_match:
                    desc_uis.append(ui_match.group(1))
                    desc_names.append(name_match.group(1))
                    tree_num_lists.append(",".join(tree_nums))

            line = handle.readline()

    return desc_uis, desc_names, tree_num_lists

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

    uis, names, trees = parse_mesh(args.input)

    if args.output:
        with open(args.output, "w") as out:
            for idx, ui in enumerate(uis):
                out.write("".join([ui, "\t", names[idx], "\t", trees[idx], "\n"]))
    """
    if verbose:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    """

if __name__ == "__main__":
    main()