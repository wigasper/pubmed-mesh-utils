import re
import os
import sys
import logging
import argparse
import traceback
from pathlib import Path

def build_edge_list(file_list, verbose=True):
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

    #pmc_articleset = re.compile(r'<!DOCTYPE pmc-articleset')
    article_pmid = re.compile(r'<front>[\s\S]*<article-id pub-id-type="pmid">(\d+)</article-id>[\s\S]*</front>')
    article_refs_list = re.compile(r'<back>[\s\S]*<ref-list([\s\S]*)</ref-list>[\s\S]*</back>')
    article_ref_pmid = re.compile(r'<pub-id pub-id-type="pmid">(\d+)</pub-id>')
    
    edges = []
    logger.info("Starting edge list build")

    for xml_file in file_list:
        try:
            with open(xml_file, "r") as handle:
                article = handle.readlines()

            article = "".join(article)
            
            #if not pmc_articleset.search(article):
            article_id_match = article_pmid.search(article)
            refs_match = article_refs_list.search(article)

            if article_id_match and refs_match:
                article_id = article_id_match.group(1)
                refs = refs_match.group(1)
            
                refs = refs.split("<ref")
                for ref in refs:
                    ref_match = article_ref_pmid.search(ref)
                    if ref_match:
                        edges.append((article_id, ref_match.group(1)))
            #else:
                
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(repr(e))
            logger.critical(trace)

    logger.info(f"Edge list built - {len(edges)} edges")

    return edges

def write_edge_list(edge_list, out_path="edge_list.csv", delim=","):
    with open(out_path, "w") as out:
        for edge in edge_list:
            out.write("".join([edge[0], delim, edge[1], "\n"]))

def main():
    # Get command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="A directory, can be relative to cwd, containing " \
                    "XMLs to build an edge list from", required=True, type=str)
    parser.add_argument("-o", "--output", help="Output file path to write edge list to, stdout " \
                    "if no path provided")
    parser.add_argument("-n", "--number", help="The number of samples to generate", type=int)
    parser.add_argument("-q", "--quiet", help="Suppress printing of log messages to STDOUT. " \
                    "Warning: exceptions will not be printed to console", action="store_true")
    args = parser.parse_args()

    if args.number:
        xmls = os.listdir(args.input)[:args.number]
    else:
        xmls = os.listdir(args.input)

    # Make filepaths absolute
    xml_containing_dir = Path(args.input).resolve()
    xmls_to_parse = []
    for xml in xmls:
        xmls_to_parse.append(os.path.join(xml_containing_dir, xml))
    
    # Check to make sure paths are files and not dirs
    xmls_to_parse = [path for path in xmls_to_parse if os.path.isfile(path)]

    if args.quiet:
        edge_list = build_edge_list(xmls_to_parse, verbose=False)
    else:
        edge_list = build_edge_list(xmls_to_parse)

    if args.output:
        write_edge_list(edge_list, args.output)
    else:
        for edge in edge_list:
            sys.stdout.write("".join([edge[0], ",", edge[1], "\n"]))

if __name__ == "__main__":
    main()