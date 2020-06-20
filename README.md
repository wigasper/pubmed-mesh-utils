# pubmed-mesh-utils
NLP and graph utilities for mining Pubmed/MeSH data

These are tools I created for a project that generated features from the biomedical literature citation network in order to predict MeSH term indexing.

## Tools
**citation_network.py** - This parses PMC full-text XML files and generates a directed edge list describing the relationships between nodes in the network. It is currently written to parse full-text XMLs from either the PubMed API or the PubMed FTP site - which have slightly different formats. It currently takes a directory path as input and will parse all files in the directory, additional input validation and input from stdin are planned for the future.

Full-text articles in XML format that are part of PMC's open access subset can be obtained from [NCBI's FTP site](https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/) or by using the [NCBI API EFetch utility](https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.EFetch)

citation_network.py can be used from the command line like so:
```
$ python3 citation_network.py -i ./directory_containing_xmls -o edge_list
```
There are also functions intended for use by import in other Python scripts.

**parse_mesh.py** - This parses the MeSH vocabulary in XML format, available from [NCBI's FTP site](ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/), to Python list data structures (for usage by other Python-based utilities) or writes to output in a tab-delimited format. Currently only extracts UIDs, names, and tree numbers for MeSH terms, because that is all my tools require, but it could easily be expanded to extract more information for each term.

For command line usage, it can be used like so:
```
$ python3 parse_mesh.py -i ./desc2019.xml -o mesh_data.tab
```
And, in this use case, will produce an output file with each MeSH term on a newline, with fields separated by tabs, and with distinct graph positions separated by commas:
```
D000001	Calcimycin	D03.633.100.221.173
D000002	Temefos	D02.705.400.625.800,D02.705.539.345.800,D02.886.300.692.800
...
```

**pmc_parser.py** - This parses PMC full text XMLs in order to extract natural language. Currently it has only been tested and used with XMLs retrieved via the NCBI API. The primary purpose is to prep texts for natural language processing by removing all tags, HTML entities, and extraneous information.

Usage:
```bash
usage: pmc_parser.py [-h] -i INPUT -o OUTPUT [-f OUTPUT_FORMAT]
                     [-s [SECTIONS [SECTIONS ...]]] [-q] [-d]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Directory containing PMC XML files
  -o OUTPUT, --output OUTPUT
                        Directory to write output files to
  -f OUTPUT_FORMAT, --output-format OUTPUT_FORMAT
                        Output format, currently XML, JSON, and plain text are
                        supported and can be specified using the strings
                        'xml', 'json', or 'text'
  -s [SECTIONS [SECTIONS ...]], --sections [SECTIONS [SECTIONS ...]]
                        Specify the desired sections for output. Sections
                        should be follow the '-s' or '--sections' argument
                        name and be space delimited, for example: '-s title
                        abstract'. Sensitive to order. By default parses
                        titles, abstracts, and body text
  -q, --quiet           Suppress printing of log messages to STDOUT. Warning:
                        exceptions will not be printed to console
  -d, --debug           Set log level to DEBUG
```


**semantic_similarity.py** - Computes the semantic similarity of all MeSH terms by Song, Li, Srimani, Yu, and Wang's aggregate information content method detailed in the article [Measure the Semantic Similarity of GO Terms Using Aggregate Information Content](https://www.ncbi.nlm.nih.gov/pubmed/26356015). Requires the parse_mesh module, the MeSH vocabulary available from [NCBI's FTP site](ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/), and documents containing PubMed citations in XML format, available from [NCBI's FTP site](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/) or by way of the [NCBI API EFetch utility](https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.EFetch). This is quite a time and memory consuming process - it computes the semantic similarity of all pair-combinations of all MeSH terms (currently 29,351) using a simple multiprocessing architecture. Currently intended to be used only from the command line. May be ported to Rust in the future.

It can be used from the command line like so:
```bash
$ python3 semantic_similariy.py -i ./pubmed_xmls -m ./desc2019.xml -o semantic_similarities.csv
```

**term_co-occurrence.py** - This is still under review and being refactored
