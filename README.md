# pubmed-mesh-utils
NLP and graph utilities for mining Pubmed/MeSH data

These are tools I created for a project that used features generated from a biomedical literature citation network in order to predict term indexing using MeSH terms.

## Tools
**citation_network.py** - This parses PMC full-text XML files and generates a directed edge list describing the relationships between nodes in the network. It is currently written to parse full-text XMLs from either the Pubmed API or the Pubmed FTP site - which have slightly different formats. It currently takes a directory path as input and will parse all files in the directory, additional input validation and input from stdin are planned for the future.
It can be used from the command line like so:
```
$ python3 citation_network.py -i ./directory_containing_xmls -o edge_list
```
There are also functions intended for use by import in other Python scripts.