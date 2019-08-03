# pubmed-mesh-utils
NLP and graph utilities for mining Pubmed/MeSH data

These are tools I created for a project that used features generated from a biomedical literature citation network in order to predict term indexing using MeSH terms.

## Tools
**citation_network.py** - This parses PMC full-text XML files and generates a directed edge list describing the relationships between nodes in the network. It is currently written to parse full-text XMLs from either the Pubmed API or the Pubmed FTP site - which have slightly different formats. It currently takes a directory path as input and will parse all files in the directory, additional input validation and input from stdin are planned for the future.

Full-text articles in XML format that are part of PMC's open access subset can be obtained from (NCBI's FTP site)[ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/] or by using the (PMC API)[https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.EFetch]

citation_network.py can be used from the command line like so:
```
$ python3 citation_network.py -i ./directory_containing_xmls -o edge_list
```
There are also functions intended for use by import in other Python scripts.

**parse_mesh.py** - This parses the MeSH vocabulary in XML format, available from (NCBI's FTP site)[ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/], to Python list data structures (for usage by other Python-based utilities) or writes to output in a tab-delimited format. Currently only extracts UIDs, names, and tree numbers for MeSH terms, because that is all my tools require, but it could easily be expanded to extract more information for each term.

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
