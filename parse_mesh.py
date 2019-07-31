#!/usr/bin/env python3

from bs4 import BeautifulSoup
from tqdm import tqdm
records = []

with open("../data/desc2019", "r") as handle:
    record = ""
    for line in handle:
        if line.startswith("<DescriptorRecord"):
            if record:
                records.append(record)
                record = ""
            record = "".join([record, line])
        else:
            record = "".join([record, line])

# Discard first 2 records which have document info
records = records[2:]

desc_records = []
desc_uis = []
desc_names = []
tree_num_lists = []
min_depths = []
distinct_tree_posits = []

# Extract data. tree_num_lists is currently commented out but this might be
# useful to keep in the future
for rec in tqdm(records):
    soup = BeautifulSoup(rec)
    
    tree_nums = []

    desc_uis.append(soup.descriptorui.string)
    desc_names.append(soup.descriptorname.find('string').string)
    if soup.treenumberlist is not None:
        for tree_num in soup.treenumberlist.find_all('treenumber'):
            tree_nums.append(tree_num.string)
        min_depths.append(len(min([t.split(".") for t in tree_nums], key=len)))
    else:
        min_depths.append(0)
    distinct_tree_posits.append(len(tree_nums))
    tree_num_lists.append(tree_nums)

with open("../data/mesh_data.tab", "w") as out:
    for index in range(len(records)):
        out.write("".join([desc_uis[index], "\t", desc_names[index], "\t"]))
        out.write("".join([str(min_depths[index]), "\t"]))
        out.write("".join([str(distinct_tree_posits[index]), "\t"]))
        out.write(",".join(tree_num_lists[index]))
        out.write("\n")
