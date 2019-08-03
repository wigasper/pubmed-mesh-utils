#!/usr/bin/env python3

import re
import os
import sys
import logging
import argparse
import traceback
from pathlib import Path

def edge_generator(file_list, verbose=True):
    ''' Generates the edges from PMC full-text XML files. These edges
        comprise the citation network. Tested on XMLs retrieved from the 
        Pubmed API as well as the bulk files from the Pubmed FTP site.
    params
        file_list - A list of XMLs to parse
        verbose - Boolean, print logging and exceptions to console
    returns
        yields single directed edges as tuples in the format
        (article_PMID, reference_PMID)
        where the article with PMID article_PMID cites the article
        with PMID reference_PMID
    '''
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

    article_pmid = re.compile(r'<front>[\s\S]*<article-id pub-id-type="pmid">(\d+)</article-id>[\s\S]*</front>')
    article_refs_list = re.compile(r'<back>[\s\S]*<ref-list([\s\S]*)</ref-list>[\s\S]*</back>')
    article_ref_pmid = re.compile(r'<pub-id pub-id-type="pmid">(\d+)</pub-id>')
    
    logger.info("Starting edge list generator")

    edge_count = 0
    doc_count = 0

    for xml_file in file_list:
        try:
            with open(xml_file, "r") as handle:
                article = handle.readlines()

            article = "".join(article)
            
            article_id_match = article_pmid.search(article)
            refs_match = article_refs_list.search(article)

            if article_id_match and refs_match:
                article_id = article_id_match.group(1)
                refs = refs_match.group(1)
                refs = refs.split("<ref")
                doc_count += 1

                for ref in refs:
                    ref_match = article_ref_pmid.search(ref)
                    if ref_match:
                        edge_count += 1
                        yield (article_id, ref_match.group(1))
                
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(repr(e))
            logger.critical(trace)

    logger.info(f"Generated {edge_count} edges from {doc_count} documents")

def build_edge_list(file_list, verbose=True):
    ''' A wrapper for the generator in cases where a list is needed
    params
        file_list - A list of XMLs to parse
        verbose - Boolean, print logging and exceptions to console
    returns
        returns a list of directed edges as tuples in the format
        (article_PMID, reference_PMID)
        where the article with PMID article_PMID cites the article
        with PMID reference_PMID
    '''
    gen = edge_generator(file_list, verbose)

    edge_list = []
    for edge in gen:
        edge_list.append(edge)

    return edge_list

def write_edge_list(file_list, out_path, delim=",", verbose=True):
    ''' A wrapper for the generator that writes to an output file
    params
        file_list - A list of XMLs to parse
        out_path - Output path to write edge list to
        delim - delimiter to separate nodes for each edge
        verbose - Boolean, print logging and exceptions to console
    '''

    gen = edge_generator(file_list, verbose)
    with open(out_path, "w") as out:
        for edge in gen:
            out.write("".join([edge[0], delim, edge[1], "\n"]))

def main():
    # Get command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="A directory, can be relative to cwd, containing " \
                    "XMLs to build an edge list from. Does not recursively search child directories, " \
                    "all XMLs should be at the top level of the directory", 
                    required=True, type=str)
    parser.add_argument("-o", "--output", help="Output file path to write edge list to, stdout " \
                    "if no path provided")
    parser.add_argument("-n", "--number", help="The number of documents to parse", type=int)
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

    if args.output:
        if args.quiet:
            write_edge_list(xmls_to_parse, args.output, verbose=False)
        else:
            write_edge_list(xmls_to_parse, args.output, verbose=True)
    else:
        # Verbose set to false here, as this is intended primarily for piping output
        gen = edge_generator(xmls_to_parse, verbose=False)
        for edge in gen:
            sys.stdout.write("".join([edge[0], ",", edge[1], "\n"]))

if __name__ == "__main__":
    main()