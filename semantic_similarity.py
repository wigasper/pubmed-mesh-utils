#!/usr/bin/env python3
import os
import re
import sys
import math
import json
import time
import logging
import argparse
import traceback
from multiprocessing import Process, Queue
from pathlib import Path
from copy import deepcopy
from itertools import combinations
from subprocess import Popen, PIPE

import numpy as np
from parse_mesh import parse_mesh

def get_children(uid, term_trees):
    ''' Gets a list of children for a term. Because there isn't actually a graph
        to traverse, it's done by searching according to position (described by a string)
        on the graph
    params
        uid - the UID of the term
        term_trees - a dict giving the position(s) on the graph for each UID
    returns
        a list of the children of the UID
    '''
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

def freq(uid, term_counts, term_freqs, term_trees):
    ''' Recursively computes the frequency according to Song et al by adding
        the term's count to the sum of the frequencies of all its children
    params
        uid - the UID of the term to get the frequency for
        term_counts - a dict giving the counts for each term
        term_freqs - a dict containing the currently known frequencies for each
            term
        term_trees - a dict giving the position(s) on the graph for each UID to
            pass to get_children()
    returns
        the frequency of the term after adding the frequencies of all its children
    '''
    total = term_counts[uid]
    
    # Check to see if frequency has already been computed - prevents
    # recomputation if this is a recursive call
    if term_freqs[uid] != -1:
        return term_freqs[uid]

    # Return the count if we hit a leaf node
    if len(get_children(uid, term_trees)) == 0:
        return total
    # Recurse if freq has not already been computed and if not at a leaf node
    else:
        for child in get_children(uid, term_trees):
            total += freq(child, term_counts, term_freqs, term_trees)
        return total

def get_ancestors(uid, term_trees, term_trees_rev):
    ''' Gets all the ancestors of a term
    params
        uid - the UID of the term to get ancestors for
        term_trees - a dict giving the position(s) on the graph for each UID
        term_trees_rev - a dict giving the UID for each position on the graph
    returns
        a list of all the ancestors of the passed term
    '''
    # Get the graph positions for the term, put in a list
    # The current term is included in the list, even though it's not an 'ancestor'
    # because of how the function is used by main
    ancestors = [tree for tree in term_trees[uid]]
    # Remove empty strings if they exist
    ancestors = [ancestor for ancestor in ancestors if ancestor]
    idx = 0
    while idx < len(ancestors):
        # Add every ancestor of the current ancestors by cutting the position string
        # down and adding to teh list
        ancestors.extend([".".join(tree.split(".")[:-1]) for tree in term_trees[term_trees_rev[ancestors[idx]]]])
        # Ensure no empty strings
        ancestors = [ancestor for ancestor in ancestors if ancestor]
        # Remove duplicates
        ancestors = list(dict.fromkeys(ancestors))
        idx += 1
    ancestors = [term_trees_rev[ancestor] for ancestor in ancestors]
    ancestors = list(dict.fromkeys(ancestors))
    return ancestors

# Compute semantic similarity for 2 terms
def semantic_similarity(uid1, uid2, sws, svs, term_trees, term_trees_rev):
    ''' Computes the semantic similarity for 2 terms
    params
        uid1 - the UID of a term
        uid2 - the UID of a term for which to compute sem. sim. with uid1
        sws - a dict containing the semantic weights for each term
        svs - a dict containing the semantic values for each term
        term_trees - a dict containing the graph position(s) for each term
        term_trees_rev - a dict containing the term at each graph position
    returns
        the semantic similarity of the provided UIDs
    '''
    logger = logging.getLogger(__name__)

    uid1_ancs = get_ancestors(uid1, term_trees, term_trees_rev)
    uid2_ancs = get_ancestors(uid2, term_trees, term_trees_rev)
    intersection = [anc for anc in uid1_ancs if anc in uid2_ancs]
    num = sum([(2 * sws[term]) for term in intersection])
    denom = svs[uid1] + svs[uid2]
    
    return 0 if num is np.NaN or denom is 0 else num / denom

# Get MeSH term counts
def count_mesh_terms(doc_list, uids):
    ''' Counts the number of times each term is indexed to a Pubmed citation
        for a set of Pubmed documents
    params
        doc_list - A list of file paths to Pubmed citation documents in XML format
        uids - a list of all MeSH UIDs
    returns
        a dict containing the count of each term
    '''
    logger = logging.getLogger(__name__)
    
    logger.info("Starting MeSH term counting...")
    # Compile regexes for counting MeSH terms
    mesh_list_start = re.compile(r"\s*<MeshHeadingList>")
    mesh_list_stop = re.compile(r"\s*</MeshHeadingList>")
    mesh_term_id = re.compile(r'\s*<DescriptorName UI="(D\d+)".*>')

    term_counts = {uid:0 for uid in uids}
    # Count MeSH terms
    for doc in doc_list:
        try:
            with open(f"{doc}", "r") as handle:
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
                # The only reason this is currently here is because it helped me
                # find a serious issue with a package I was previously using
                elapsed_time = int((time.perf_counter() - start_time) * 10) / 10.0
                logger.info(f"{doc} MeSH term counts completed in {elapsed_time} seconds")
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(repr(e))
            logger.critical(trace)
    
    return term_counts

def get_term_freqs(term_counts, term_trees, uids):
    ''' Get the term frequencies by Song et al.'s recursive definition. A
        term's frequency is the count of that term plus the count of all
        its children (and their children, and so on, until leaf nodes).
    params
        term_counts - a dict giving the count for each term
        term_trees - a dict giving the position(s) of each term on the graph
        uids - a list of MeSH term UIDs
    returns
        a dict containing the frequency for each term according
    '''
    logger = logging.getLogger(__name__)

    term_freqs = {uid:-1 for uid in uids}
    start_time = time.perf_counter()

    # Sort terms so that we hit leaf nodes first and work up from there
    # - this takes a little longer (seconds) up front but reduces computation
    # time greatly by limiting the number of recursive calls
    
    # Get the max depth
    max_depth = 0
    for term in term_trees:
        for tree in term_trees[term]:
            if len(tree.split(".")) > max_depth:
                max_depth = len(tree.split("."))
    
    # Sort terms so by level on the graph, leaf nodes first, roots last
    sorted_terms = []
    for depth in range(max_depth, 0, -1):
        for term in term_trees:
            for tree in term_trees[term]:
                if len(tree.split(".")) == depth and term not in sorted_terms:
                    sorted_terms.append(term)
                    break

    logger.info("Computing term frequencies...")
    for term in sorted_terms:
        term_freqs[term] = freq(term, term_counts, term_freqs, term_trees)
    
    # Get elapsed time and truncate for log
    elapsed_time = int((time.perf_counter() - start_time) * 10) / 10.0
    logger.info(f"Term freqs calculated in {elapsed_time} seconds")

    return term_freqs

def output_writer(write_queue, out_path):
    ''' A multiprocessing worker, pulls from the queue and writes. The worker
        is killed if it pulls None from the queue
    params
        write_queue - the queue to pull data from to write
        out_path - the output file path to write to
    '''
    logger = logging.getLogger(__name__)
    with open(out_path, "w") as out:
        while True:
            result = write_queue.get()
            if result is None:
                break
            out.write(result)

# A function for multiprocessing, the worker grabs a pair of terms from the queue
# and then computes the semantic similarity for the pair
def mp_worker(work_queue, write_queue, sws, svs, term_trees, term_trees_rev):
    ''' A multiprocessing worker. The worker grabs a pair of terms from the queue
        and then computes the semantic similarity for the pair. Worker then adds
        the pair and the semantic similarity value to the write queue. Worker
        is killed if it pulls None from the queue
    params
        work_queue - a queue from which to pull UIDs to calculate semantic similarity for
        write_queue - a queue to put semantic similarity calculation results in
        sws - a dict containing the semantic weight for each term
        svs - a dict containing the semantic value for each term
        term_trees - a dict containing the position(s) on the graph for each term
        term_trees_rev - a dict containing the term for each position on the graph
    '''
    logger = logging.getLogger(__name__)
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
    parser.add_argument("-q", "--quiet", help="Suppress printing of log messages to STDOUT. " \
                    "Warning: exceptions will not be printed to console", action="store_true")
    args = parser.parse_args()

    # Set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler("compute_semantic_similarity.log")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if not args.quiet:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # Make filepaths absolute:
    mesh_dir = Paths(args.mesh).resolve()
    docs_temp = os.listdir(args.input)
    docs_dir = Path(args.input).resolve()
    docs = [os.path.join(docs_dir, doc) for doc in docs_temp]

    # Get required MeSH data
    uids = []
    names = []
    trees = []

    uids, names, trees = parse_mesh(mesh_dir)

    # Create term_trees dict and reverse for quick and easy lookup later
    term_trees = {uids[idx]:trees[idx] for idx in range(len(uids))}
    term_trees_rev = {tree:uids[idx] for idx in range(len(uids)) for tree in trees[idx]}

    # Get term counts. If recounting terms change the flags
    term_counts = count_mesh_terms(docs, uids)

    # Computing aggregate information content is done in a step-by-step
    # process here to make it easy to follow along. I used Song, Li, Srimani,
    # Yu, and Wang's paper, "Measure the Semantic Similarity of GO Terms Using
    # Aggregate Information Content" as a guide
    
    # Get term counts. If recounting terms change the flags
    term_freqs = get_term_freqs(term_counts, term_trees, uids)

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
    logger.info("Computing semantic similarities...")
    start_time = time.perf_counter()

    num_workers = os.cpu_count() - 3
    num_writers = 2
    write_queue = Queue(maxsize=100)
    work_queue = Queue(maxsize=100)

    writers = [Process(target=output_writer, args=(write_queue, 
                f"{args.output}.{num}.csv")) for num in range(num_writers)]

    for writer in writers:
        writer.daemon = True
        writer.start()

    processes = [Process(target=mp_worker, args=(work_queue, write_queue, deepcopy(sws), 
                deepcopy(svs), deepcopy(term_trees), deepcopy(term_trees_rev))) for _ in range(num_workers)]
    
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
