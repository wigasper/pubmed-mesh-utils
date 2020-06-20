#!/usr/bin/env python3
import os
import re
import sys
import html
import argparse
import logging
import unicodedata
import traceback
from pathlib import Path

'''
text_elements is a tuple of (title, abstract, body)
'''
def write_xml(fp, text_elements):
    with open(fp, "w") as out:
        out.write("<title>\n")
        out.write(text_elements["title"])
        out.write("\n</title>\n")

        out.write("<abstract>\n")
        out.write(text_elements["abstract"])
        out.write("\n</abstract>\n")

        out.write("<body>\n")
        out.write(text_elements["body"])
        out.write("\n</body>\n")

def write_json(fp, text_elements):
    pass

def write_plain_text(fp, text_elements):
    pass

'''
Takes an HTML entity and attempts to parse it into a UTF-8 
character
'''
def parse_entity(match_obj):
    logger = logging.getLogger(__name__)
    symbol = ""
    
    if match_obj.group(0):
        # try to convert
        try:
            symbol = html.unescape(match_obj.group(0)).encode("utf-8").decode()
            symbol = unicodedata.normalize("NFKC", symbol)
        
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(repr(e))
            logger.critical(trace)
            symbol = ""
    
    return symbol

'''
Deal with HTML entity codes
'''
def remove_codes(string):
    entity_regex = re.compile("&[^;\s]*;")

    string = entity_regex.sub(parse_entity, string)

    return string

'''
Removes tags from a string
'''
def remove_tags(string):
    tag_regex = re.compile("<[^>]+>")

    return tag_regex.sub("", string)

'''
Removes empty lines. Currently also has logic to remove non-sentence 
lines (lines with no whitespace or with less than 8 
whitespace-separated-elements)

Note: this is all quite arbitrary
'''
def remove_empty_lines(string):
    string = string.split("\n")
    string = [line for line in string if re.search("\S", line)]
    
    for index, line in enumerate(string):
        line = line.split()
        if len(line) > 7:
            string[index] = " ".join(line)
        else:
            string[index] = ""

    string = [line for line in string if line]

    return "\n".join(string)


'''
Parses the abstract section of the XML
abstract should be a string, can contain tags and HTML entities
returns a string without tags and HTML entities
'''
def parse_abstract(abstract):
    # remove title
    abstract = re.sub("\s*<title>[^<]*</title>", "", abstract)
    
    # remove tags
    abstract = remove_tags(abstract)

    # remove hexadecimal unicode
    abstract = remove_codes(abstract)

    abstract = remove_empty_lines(abstract)

    return abstract


'''
Parses the body section of the XML
'''
def parse_body(body):
    # make one big block of text to make everything easier
    body = "|$|$|".join(body.split("\n"))
    
    # remove text that is in tables
    body = re.sub("<sup[^<]*</sup>", "", body)
    body = re.sub("<td.*</td>", "", body)
    body = re.sub("<th.*</th>", "", body)

    # remove LaTeX
    body = re.sub("\\\documentclass\[.*\\\end\{document\}", "", body)
    # remove titles
    body = re.sub("\s*<title>[^<]*</title>", "", body)

    # remove figure labels
    body = re.sub("\s*<label>[^<]*</label>", "", body)
    
    body = "\n".join(body.split("|$|$|"))

    # remove tags
    body = remove_tags(body)

    # remove unicode
    body = remove_codes(body)

    body = remove_empty_lines(body)

    return body

'''
Parses PMC full text XMLs
'''
def parse_xml(fp):
    logger = logging.getLogger(__name__)

    title = ""
    clean_abstract = ""
    clean_body = ""

    title_group_start = re.compile("^\s*<title-group")
    title_group_stop = re.compile("^\s*</title-group")
    title_regex = re.compile("\s*<article-title>(.*)</article-title>")

    abstract_start = re.compile("\s*<abstract")
    abstract_stop = re.compile("\s*</abstract>")

    body_start = re.compile("\s*<body")
    body_stop = re.compile("\s*</body>")

    abstract = []
    body = []

    try:
        with open(fp, "r") as handle:
            line = handle.readline()
            logger.debug("starting line loop")
            while line:
                
                if title_group_start.search(line):
                    logger.debug("found title group start tag")
                    while not title_group_stop.search(line):
                        if title_regex.search(line):
                            title = title_regex.search(line).group(1)
                        line = handle.readline()
                
                if abstract_start.search(line):
                    logger.debug("found abstract start tag")
                    while not abstract_stop.search(line):
                        abstract.append(line)
                        line = handle.readline()

                if body_start.search(line):
                    logger.debug("found body start tag")
                    while not body_stop.search(line):
                        body.append(line)
                        line = handle.readline()

                line = handle.readline()
            logger.debug("end line loop")
        clean_abstract = parse_abstract("".join(abstract))
        clean_body = parse_body("".join(body))

    except Exception as e:
        trace = traceback.format_exc()
        logger.error(repr(e))
        logger.critical(trace)

    title = remove_codes(title)
    title = remove_tags(title)

    return {"title": title, "abstract": clean_abstract, "body": clean_body}


def get_file_list(directory):
    absolute_path = Path(directory).resolve()
    files = os.listdir(directory)
    
    return [os.path.join(absolute_path, f) for f in files]

def initialize_logger(debug=False, quiet=False):
    level = logging.INFO
    if debug:
        level = logging.DEBUG

    # Set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    handler = logging.FileHandler("pmc-parser.log")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if not quiet:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def output_function_map():
    function_map = {"xml": write_xml,
                    "json": write_json,
                    "text": write_plain_text}

    return function_map

def parse_xmls(input_dir, output_dir, output_format, quiet, debug):
    logger = initialize_logger(args.debug, args.quiet)

    logger.info(f"Starting parser, input dir: {args.input}, output dir: {args.output}")
    logger.debug("Getting file list")
    
    input_files = get_file_list(args.input)

    output_function = output_function_map()[output_format]

    logger.debug("Starting parse loop")
    for input_file in input_files:
        # clean_text is a tuple (title, abs, body)
        clean_text = parse_xml(input_file)
        pmc_id = input_file.split("/")[-1].split(".")[0]
            
        output_function(f"{args.output}/{pmc_id}.xml", clean_text)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Directory containing PMC XML files",
                        required=True)
    parser.add_argument("-o", "--output", help="Directory to write output files to",
                        required=True)
    parser.add_argument("-f", "--output-format", help="Output format, currently " \
                        "XML, JSON, and plain text are supported and can be specified " \
                        "using the strings 'xml', 'json', or 'text'", default="xml")
    parser.add_argument("-q", "--quiet", help="Suppress printing of log messages to STDOUT. " \
                        "Warning: exceptions will not be printed to console", 
                        action="store_true", default=False)
    parser.add_argument("-d", "--debug", help="Set log level to DEBUG", action="store_true", 
                        default=False)

    args = parser.parse_args()

    parse_xmls(args.input, args.output, args.output_format, args.quiet, args.debug)   
