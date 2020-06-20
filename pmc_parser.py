#!/usr/bin/env python3
import os
import re
import sys
import html
import json
import argparse
import logging
import unicodedata
import traceback
from pathlib import Path

'''
text_elements is a dict containing text elements like
title, abstract, and body. Writes output in an XML-like format
'''
def write_xml(fp, text_elements):
    with open(f"{fp}.xml", "w") as out:
        for element in text_elements.keys():
            out.write(f"<{element}>\n")
            out.write(text_elements[element])
            out.write(f"\n</{element}>\n")

'''
text_elements is a dict containing text elements like 
title, abstract, and body. Writes output in JSON
'''
def write_json(fp, text_elements):
    with open(f"{fp}.json", "w") as out:
        json.dump(text_elements, out)

'''
text_elements is a dict containing text elements like
title, abstract, and body. Writes output in a plain text
format
'''
def write_plain_text(fp, text_elements):
    with open(f"{fp}.txt", "w") as out:
        for element in text_elements.keys():
            out.write(f"{element.upper()}: {text_elements[element]}\n")

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
body should be a string, can contain tags and HTML entities
returns a string
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
Parses a single PMC full text XML
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

'''
returns a list of absolute filepaths for every file in a directory
'''
def get_file_list(directory):
    absolute_path = Path(directory).resolve()
    files = os.listdir(directory)
    absolute_fps = [os.path.join(absolute_path, f) for f in files]

    return [fp for fp in absolute_fps if os.path.isfile(fp)]

'''
returns a logger
'''
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

'''
Validates the section names requested in the input
'''
def validate_sections(sections_in):
    logger = logging.getLogger(__name__)

    valid_sections = ["title", "abstract", "body"]
    sections = [sec for sec in sections_in if sec in valid_sections]
    
    if len(sections) == 0:
        raise ValueError("No valid text sections were passed")

    if len(sections_in) > len(sections):
        excluded = [sec for sec in sections_in if sec not in sections]
        logger.info(f"The following sections are not valid: {excluded}")

    return sections

'''
Maps the output format string to a function
'''
def get_output_function(output_format):
    logger = logging.getLogger(__name__)

    function_map = {"xml": write_xml,
                    "json": write_json,
                    "text": write_plain_text}

    if output_format not in function_map.keys():
        logger.warning("Requested output format not supported, defaulting to XML")
        output_format = "xml"

    return function_map[output_format]

'''
Main driver function
'''
def parse_xmls(input_dir, output_dir, output_format="xml", 
        sections=["title", "abstract", "body"], quiet=False, debug=False):
    logger = initialize_logger(debug, quiet)

    sections = validate_sections(sections)
    

    logger.info(f"Starting parser, input dir: {input_dir}, output dir: {output_dir}")
    logger.debug("Getting file list")
    
    input_files = get_file_list(input_dir)

    output_function = get_output_function(output_format)
    
    logger.debug("Starting parse loop")
    for input_file in input_files:
        clean_text_temp = parse_xml(input_file)
        
        clean_text = {}
        for section in sections:
            clean_text[section] = clean_text_temp[section]

        pmc_id = input_file.split("/")[-1].split(".")[0]    
        output_function(f"{output_dir}/{pmc_id}", clean_text)
        
'''
For command line usage
'''
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Directory containing PMC XML files",
                        required=True)
    parser.add_argument("-o", "--output", help="Directory to write output files to",
                        required=True)
    parser.add_argument("-f", "--output-format", help="Output format, currently " \
                        "XML, JSON, and plain text are supported and can be specified " \
                        "using the strings 'xml', 'json', or 'text'", default="xml")
    parser.add_argument("-s", "--sections", help="Specify the desired sections for " \
                        "output. Sections should be follow the '-s' or '--sections' " \
                        "argument name and be space delimited, for example: " \
                        "'-s title abstract'. Sensitive to order. By default " \
                        "parses titles, abstracts, and body text", nargs="*", 
                        default=["title", "abstract", "body"])
    parser.add_argument("-q", "--quiet", help="Suppress printing of log messages to STDOUT. " \
                        "Warning: exceptions will not be printed to console", 
                        action="store_true", default=False)
    parser.add_argument("-d", "--debug", help="Set log level to DEBUG", action="store_true", 
                        default=False)

    args = parser.parse_args()
    
    parse_xmls(args.input, args.output, args.output_format, args.sections, 
                args.quiet, args.debug)   
