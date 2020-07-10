#!/usr/bin/env python3

import re
import argparse
import logging

def parse_mesh(descriptor_file):
    allow_permuted_terms = False

    desc_data = {}
    desc_uis = []

    desc_name_tag = re.compile(r"\s+</DescriptorName>")
    desc_rec_end_tag = re.compile(r"\s*</DescriptorRecord>")

    desc_ui = re.compile(r"<DescriptorUI>(D\d+)</DescriptorUI")
    desc_name = re.compile(r"<String>(.+)</String>")
    tree_num = re.compile(r"<TreeNumber>(.+)</TreeNumber")
    
    concept_list_start = re.compile(r"\s*<ConceptList")
    concept_list_end = re.compile(r"\s*</ConceptList")
    
    term_entry_start = re.compile(r"\s*<Term\s+")
    term_entry_end = re.compile(r"\s*</Term>")
    term_string = re.compile(r"\s*<String>(.*)</String")
    term_permute_status = re.compile(r'\s*<Term.*IsPermutedTermYN="([YN])".*')

    with open(descriptor_file, "r") as handle:
        line = handle.readline()
        while line:
            if line.startswith("<DescriptorRecord "):
                tree_nums = []
                entry_terms = []
                relevant_lines = []
                permute_status = None

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
                    
                    if concept_list_start.search(line):
                        while not concept_list_end.search(line):
                            if term_entry_start.search(line):
                                if not permute_status:
                                    permute_status_search = term_permute_status.search(line)
                                    if permute_status_search:
                                        try:
                                            
                                            permute_status = permute_status_search.group(1)
                                        except:
                                            print("bad")
                                            print(line)
                                        if allow_permuted_terms == False:
                                            if permute_status == "Y":
                                                permute_agreement = False
                                        else:
                                            permute_agreement = True

                                while not term_entry_end.search(line):
                                    term_string_match = term_string.search(line)
                                    if term_string_match and permute_agreement:
                                        entry_terms.append(term_string_match.group(1))
                                    line = handle.readline()
                            line = handle.readline()
                    line = handle.readline()
                    
                if ui_match and name_match:
                    desc_data[ui_match.group(1)] = {"name": name_match.group(1),
                                                    "graph_positions": "|".join(tree_nums),
                                                    "entry_terms": "|".join(entry_terms)}
                    desc_uis.append(ui_match.group(1))

            line = handle.readline()

    return (desc_data, desc_uis)

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
    
    fields = ["name", "entry_terms", "graph_positions"]
    
    # desc_uis keeps terms in the same order as the original file but
    # maybe this is not really necessary
    (desc_data, desc_uis) = parse_mesh(args.input)

    if args.output:
        with open(args.output, "w") as out:
            for ui in desc_uis:
                line = [ui]
                for field in fields:
                    line.append(desc_data[ui][field])
                
                out.write("\t".join(line))
                out.write("\n")

if __name__ == "__main__":
    main()
