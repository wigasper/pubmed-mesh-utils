#!/usr/bin/env python3

import os
import re
import math
import json
import time
import logging
import argparse
import traceback
from multiprocessing import Process, Queue
from copy import deepcopy
from itertools import combinations
from subprocess import Popen, PIPE

import numpy as np
from parse_mesh import parse_mesh

# Gets a list of children for a term. Because we we don't actually have a graph
# to traverse, it is done by searching according to position on the graph
def get_children(uid, term_trees):
    # Return empty list for terms (like 'D005260' - 'Female') that aren't
    # actually part of any trees
    if len(term_trees[uid][0]) == 0:
        return []
    
    children = []

    for tree in term_trees[uid]:
        parent_depth = len(tree.split("."))
        for key, vals in term_trees.items():
            for val in vals:
                child_depth = len(val.split("."))
                if tree in val and uid != key and child_depth == parent_depth + 1:
                    children.append(key)
    
    return list(dict.fromkeys(children))

# Recursively computes the frequency according to Song et al by adding
# the term's count to sum of the frequencies of all its children
def freq(uid, term_counts, term_freqs, term_trees):
    total = term_counts[uid]
    if term_freqs[uid] != -1:
        return term_freqs[uid]
    if len(get_children(uid, term_trees)) == 0:
        return total
    else:
        for child in get_children(uid, term_trees):
            total += freq(child, term_counts, term_freqs, term_trees)
        return total

# Get all ancestors of a term
def get_ancestors(uid, term_trees, term_trees_rev):
    ancestors = [tree for tree in term_trees[uid]]
    # Remove empty strings if they exist
    ancestors = [ancestor for ancestor in ancestors if ancestor]
    idx = 0
    while idx < len(ancestors):
        ancestors.extend([".".join(tree.split(".")[:-1]) for tree in term_trees[term_trees_rev[ancestors[idx]]]])
        ancestors = [ancestor for ancestor in ancestors if ancestor]
        ancestors = list(dict.fromkeys(ancestors))
        idx += 1
    ancestors = [term_trees_rev[ancestor] for ancestor in ancestors]
    ancestors = list(dict.fromkeys(ancestors))
    return ancestors

# Compute semantic similarity for 2 terms
def semantic_similarity(uid1, uid2, sws, svs, term_trees, term_trees_rev):
    uid1_ancs = get_ancestors(uid1, term_trees, term_trees_rev)
    uid2_ancs = get_ancestors(uid2, term_trees, term_trees_rev)
    intersection = [anc for anc in uid1_ancs if anc in uid2_ancs]
    num = sum([(2 * sws[term]) for term in intersection])
    denom = svs[uid1] + svs[uid2]
    
    return 0 if num is np.NaN or denom is 0 else num / denom

# Get MeSH term counts
def count_mesh_terms(doc_list, uids, logger):
    print("Starting MeSH term counting...")
    # Compile regexes for counting MeSH terms
    mesh_list_start = re.compile(r"\s*<MeshHeadingList>")
    mesh_list_stop = re.compile(r"\s*</MeshHeadingList>")
    mesh_term_id = re.compile(r'\s*<DescriptorName UI="(D\d+)".*>')

    term_counts = {uid:0 for uid in uids}
    # Count MeSH terms
    for doc in doc_list:
        try:
            with open("./pubmed_bulk/{}".format(doc), "r") as handle:
                start_time = time.perf_counter()

                line = handle.readline()
                while line:
                    if mesh_list_start.search(line):
                        while not mesh_list_stop.search(line):
                            if mesh_term_id.search(line):
                                term_id = mesh_term_id.search(line).group(1)
                                term_counts[term_id] += 1
                            line = handle.readline()
                    line = handle.readline()

                # Get elapsed time and truncate for log
                elapsed_time = int((time.perf_counter() - start_time) * 10) / 10.0
                logger.info(f"{doc} MeSH term counts completed in {elapsed_time} seconds")
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(repr(e))
            logger.critical(trace)
    """
    if save_flag:
        with open("./data/pm_bulk_term_counts.json", "w") as out:
            json.dump(term_counts, out)
    """
    return term_counts

# Get term frequencies by counting (according to Song et al.'s recursive definition)
# or by loading
def get_term_freqs(term_counts, term_trees, uids, logger):
    term_freqs = {uid:-1 for uid in uids}
    
    # Get term frequencies (counts) recursively as described by
    # Song et al
    start_time = time.perf_counter()

    # Sort terms so that we hit leaf nodes first and work up from there
    # - this takes a little longer upfront but reduces computation
    # time greatly by limiting the number of recursive calls
    
    # Get the max depth
    max_depth = 0
    for term in term_trees:
        for tree in term_trees[term]:
            if len(tree.split(".")) > max_depth:
                max_depth = len(tree.split("."))
    
    sorted_terms = []
    for depth in range(max_depth, 0, -1):
        for term in term_trees:
            for tree in term_trees[term]:
                if len(tree.split(".")) == depth and term not in sorted_terms:
                    sorted_terms.append(term)
                    break

    print("Computing term frequencies...")
    for term in sorted_terms:
        term_freqs[term] = freq(term, term_counts, term_freqs, term_trees)
    
    # Get elapsed time and truncate for log
    elapsed_time = int((time.perf_counter() - start_time) * 10) / 10.0
    logger.info(f"Term freqs calculated in {elapsed_time} seconds")

    """
    if save_flag:
        with open("./data/mesh_term_freq_vals.csv", "w") as out:
            for term in term_freqs:
                out.write(",".join([term, str(term_freqs[term])]))
                out.write("\n")
    """
    return term_freqs

# A function for multiprocessing, pulls from the queue and writes
def output_writer(write_queue, out_path):
    with open(out_path, "w") as out:
        while True:
            result = write_queue.get()
            if result is None:
                break
            out.write(result)

# A function for multiprocessing, the worker grabs a pair of terms from the queue
# and then computes the semantic similarity for the pair
def mp_worker(work_queue, write_queue, id_num, sws, svs, term_trees, term_trees_rev):
    # Set up logging - I do actually want a logger for each worker to catch any exceptions
    # this is easier than sharing the original logger - but this may be implemented
    # in the future
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(f"./logs/compute_semantic_similarity_worker{id_num}.log")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    try:
        while True:
            pair = work_queue.get()
            if pair is None:
                break
            sem_sim = semantic_similarity(pair[0], pair[1], sws, svs, term_trees, term_trees_rev)
            write_queue.put(("".join([pair[0], ",", pair[1], ",", str(sem_sim), "\n"])))
    except Exception as e:
        trace = traceback.format_exc()
        logger.error(repr(e))
        logger.critical(trace)

def main():
    # Get command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mesh", help="Pubmed's MeSH descriptor data in XML format", 
                    required=True, type=str)
    parser.add_argument("-i", "--input", help="A directory containing Pubmed citation XMLs",
                    required=True, type=str)
    parser.add_argument("-o", "--output", help="Output file to write data in a comma-delimited format")
    #parser.add_argument("-q", "--quiet", help="Suppress printing of log messages to STDOUT. " \
    #                "Warning: exceptions will not be printed to console", action="store_true")
    args = parser.parse_args()

    # Set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler("compute_semantic_similarity.log")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    uids = []
    names = []
    trees = []

    uids, names, trees = parse_mesh(args.mesh)

    docs = os.listdir(args.input)

    # Create term_trees dict and reverse for quick and easy lookup later
    term_trees = {uids[idx]:trees[idx] for idx in range(len(uids))}
    term_trees_rev = {tree:uids[idx] for idx in range(len(uids)) for tree in trees[idx]}

    # Get term counts. If recounting terms change the flags
    term_counts = count_mesh_terms(docs, uids, logger)

    # Computing aggregate information content is done in a step-by-step
    # process here to make it easy to follow along. I used Song, Li, Srimani,
    # Yu, and Wang's paper, "Measure the Semantic Similarity of GO Terms Using
    # Aggregate Information Content" as a guide
    
    # Get term counts. If recounting terms change the flags
    term_freqs = get_term_freqs(term_counts, term_trees, uids, logger)

    root_freq = sum(term_freqs.values())
                
    # Get term probs
    term_probs = {uid:np.NaN for uid in uids}
    for term in term_probs:
        term_probs[term] = term_freqs[term] / root_freq

    # Compute IC values
    ics = {uid:np.NaN for uid in uids}
    for term in ics:
        try:
            ics[term] = -1 * math.log(term_probs[term])
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(repr(e))
            logger.error(f"Term: {term}")
            logger.critical(trace)

    # Compute knowledge for each term
    knowledge = {uid:np.NaN for uid in uids}
    for term in knowledge:
        knowledge[term] = 1 / ics[term]
            
    # Compute semantic weight for each term
    sws = {uid:np.NaN for uid in uids}
    for term in sws:
        sws[term] = 1 / (1 + math.exp(-1 * knowledge[term]))
        
    # Compute semantic value for each term by adding the semantic weights
    # of all its ancestors
    svs = {uid:np.NaN for uid in uids}
    for term in svs:
        sv = 0
        ancestors = get_ancestors(term, term_trees, term_trees_rev)
        for ancestor in ancestors:
            sv += sws[ancestor]
        svs[term] = sv

    # Compute semantic similarity for each pair utilizing multiprocessing
    print("Computing semantic similarities...")
    start_time = time.perf_counter()

    # TODO: use os.cpu_count() here to figure out how to distribute worker roles
    num_workers = 3
    num_writers = 2
    write_queue = Queue(maxsize=100)
    work_queue = Queue(maxsize=100)

    writers = [Process(target=output_writer, args=(write_queue, 
                f"{args.output}.{num}.csv")) for num in range(num_writers)]

    for writer in writers:
        writer.daemon = True
        writer.start()

    processes = [Process(target=mp_worker, args=(work_queue, write_queue, num, deepcopy(sws), 
                deepcopy(svs), deepcopy(term_trees), deepcopy(term_trees_rev))) for num in range(num_workers)]
    
    for process in processes:
        process.start()
    
    for pair in combinations(uids, 2):
        work_queue.put(pair)
    
    while True:
        if work_queue.empty():
            for _ in range(num_workers):
                work_queue.put(None)
            break

    for process in processes:
        process.join()
    
    for writer in writers:
        write_queue.put(None)
        writer.join()

    # Get elapsed time and truncate for log
    elapsed_time = int((time.perf_counter() - start_time) * 10) / 10.0
    logger.info(f"Semantic similarities calculated in {elapsed_time} seconds")

    # Cleanup
    cat = f"cat {args.output}.*.csv > {args.output}.csv"
    with Popen(cat, stdout=PIPE, stderr=PIPE, shell=True) as proc:
        results, errs = proc.communicate()
    with Popen(f"rm {args.output}.*.csv", stdout=PIPE, 
                stderr=PIPE, shell=True) as proc:
        results, errs = proc.communicate()

if __name__ == "__main__":
    main()
