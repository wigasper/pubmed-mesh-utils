#!/usr/bin/env python3
import sys
import logging
import argparse

from parse_mesh import parse_mesh

def write_edge_list(edge_list, desc_data, fp, uids=False):
    with open(fp, "w") as out:
        for edge in edge_list:
            if not uids:
                out.write(f"{desc_data[edge[0]]['name']}\t{desc_data[edge[1]]['name']}\n")
            else:
                out.write(f"{edge[0]}\t{edge[1]}\n")

def to_edge_list(adj_list):
    edge_list = set()
    list_out = []

    for key_node in adj_list:
        for neighbor in adj_list[key_node]:
            item_0 = f"{key_node}{neighbor}"
            item_1 = f"{neighbor}{key_node}"

            if item_0 not in edge_list and item_1 not in edge_list:
                edge_list.add(item_0)
                edge_list.add(item_1)
                
                list_out.append([key_node, neighbor])

    return list_out

def get_children(uid, term_trees, term_trees_lookup):
    if len(term_trees[uid][0]) == 0:
        return []

    children = []

    uid_positions = term_trees[uid]

    for num in range(1000):
        num_str = str(num).zfill(3)

        for position in uid_positions:
            putative_posit = f"{position}.{num_str}"

            if putative_posit in term_trees_lookup.keys():
                children.append(term_trees_lookup[putative_posit])
    
    return children

def get_mesh_graph(desc_data, directed=False):
    trees = {}
    trees_lookup = {}

    for uid in desc_data:
        tree_locs = desc_data[uid]["graph_positions"].split("|")
        trees[uid] = tree_locs
        
        for posit in tree_locs:
            trees_lookup[posit] = uid
    
    adj_list = {uid: [] for uid in desc_data}

    for uid in desc_data.keys():
        children = get_children(uid, trees, trees_lookup)
        
        adj_list[uid].extend(children)

        if not directed:
            for child in children:
                adj_list[child].append(uid)
        
    return adj_list

def initialize_logger(debug=False, quiet=False):
    level = logging.INFO
    if debug:
        level = logging.DEBUG

    # Set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    handler = logging.FileHandler("mesh_to_edge_list.log")
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

def get_args():
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mesh", help="Path to MeSH descriptor file", required=True)
    parser.add_argument("-o", "--out", help="Path to write edge list to")

    args = parser.parse_args()

    # log delimiter
    logger.info("###############################")

    return parser.parse_args()

if __name__ == "__main__":
    args = get_args()
    logger = initialize_logger()
    
    (desc_data, desc_uis) = parse_mesh(args.mesh)
    adj_list = get_mesh_graph(desc_data)


    if args.out:
        edge_list = to_edge_list(adj_list)
        write_edge_list(edge_list, desc_data, args.out)
