# -*- coding: utf-8 -*-
#
# Copyright 2013 James Thornton (http://jamesthornton.com)
# BSD License
#
"""
GitHub ETL: Extract & Transform GitHub Egocentric Data.
Load via Gremlin

Requires the `requests` Python library to be installed:

$ sudo easy_install requests

"""

import os
import sys
import json
import string
import codecs
import random
import argparse
import datetime
import dateutil.parser
from itertools import count
from functools import partial
from collections import OrderedDict, defaultdict
from commands import getoutput as cmd
from threading import Thread, Lock
from Queue import Queue

import requests


#
# Extract
#


# Web Client

MAX_PER_PAGE = 100

def get_token(username, password, token=None):
    # TODO: implement oauth2 logic
    #if username is not None and password is not None:
    #    token = oauth2
    if token is None:    
        # TODO: error if token not found in .gitconfig
     token = cmd('git config github.token')
    return token

def create_client(token, per_page=MAX_PER_PAGE):
    client = requests.session()
    client.params = dict(access_token=token, per_page=per_page)
    return client

def get_page(client, url):
    resp = client.get(url)
    assert (resp.status_code == requests.codes.ok)
    return resp

def get_page_data(client, url):
    data = []

    resp = get_page(client, url)
    data = data + resp.json()

    while resp.links.get('next'):
        url = resp.links.get('next').get('url')
        resp = get_page(client, url)
        data = data + resp.json()

    return data

            
# File Funcs

def ensure_dir(filename):
    directory = os.path.dirname(filename)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    
def dump_json(data, filename, mode):
    ensure_dir(filename)
    with open(filename, mode) as outfile: 
        json.dump(data, outfile)
    return data

def read_json_file(filename):
    with codecs.open(filename, "r", "utf-8") as fin:
        data = json.load(fin)
    return data


# Misc Utils

def ordered_subset(desired_keys, bigdict):
    subset = OrderedDict([(i, bigdict[i]) for i in desired_keys if i in bigdict])
    return subset

def partial_args(*part_args):
    def wrapper(func, extra_args):
        args = list(part_args)
        args.extend(extra_args)
        return func(*args)
    return wrapper

def dictlist_to_dict(data, key):
    # eliminates dupes and returns a dict of uniques
    return dict(map(lambda item: (item[key], item), data))        


# Queue


def create_task_with_tries(func, args, tries):
    return (func, args, tries)

def create_task(func, *args):
    return create_task_with_tries(func, args, tries=0)

def requeue(queue, func, args, tries):
    task = create_task_with_tries(func, args, tries)
    queue.put(task)

def display_progress(lock):
    global counter
    with lock: 
        counter += 1
        print counter if (counter % 10 == 0) else "."
    
def worker_wrapper(worker_func, queue):
    max_tries = 3
    lock = Lock()
    while True:
        task = queue.get()
        if task is None: break
        func, args, tries = task
        try:
            worker_func(func, args)
            display_progress(lock)
        except:
            print "ERROR: ", sys.exc_info()[0]
            if tries < max_tries:
                tries += 1
                requeue(queue, func, args, tries)
        finally:
            queue.task_done()

def start_threadpool(workers, worker_func, queue):
    pool = []
    for i in range(workers):
        t = Thread(target=worker_wrapper, args=(worker_func, queue))
        t.daemon = True
        pool.append(t)
        t.start()
    return pool

def threaded_queue(client_func, worker_func, workers=5):
    global counter
    queue = Queue(workers)
    try:
        pool = start_threadpool(workers, worker_func, queue)
        client_func(queue)
        print counter          # final count
    except (KeyboardInterrupt, SystemExit):
        print "Killed by keyboard..."
        raise
    finally:
        # Reset counter
        counter = 0

def signal_workers(queue, n_workers, signal=None):
    # Send None (Done) Signal to Workers
    for i in range(n_workers):
        queue.put(signal)


#
# Get, Dump, Save, Read
#

#data_dir = "data"  # override via command line arg

def users_dir():
    global data_dir
    # should be raw users, not users
    # data dir set globally via command line
    return "%s/raw/users" % data_dir

def repos_dir():
    global data_dir
    # should be raw repos, not repos
    # data dir set globally via command line
    return "%s/raw/repos" % data_dir
    

# User

def user_file(username):
    return "%s/raw/users/%s/profile.json" % (data_dir, username)    

def get_user_data(client, username):
    url = "https://api.github.com/users/%s" % username
    return get_page(client, url).json()

def dump_user_data(username, user_data):
    filename = user_file(username)
    return dump_json(user_data, filename, "w")

def save_user_data(client, username):
    print "Saving user data for %s..." % username
    data = get_user_data(client, username)
    return dump_user_data(username, data)
    
def read_user_data(username):
    filename = user_file(username)
    return read_json_file(filename)


# Following

def following_file(username):
    return "%s/raw/users/%s/following.json" % (data_dir, username)

def get_following(client, username):
    url = "https://api.github.com/users/%s/following" % username
    return get_page_data(client, url)

def dump_following(username, following_data):
    filename = following_file(username)
    return dump_json(following_data, filename, "w")

def save_following(client, username):
    print "Saving following data for %s..." % username
    data = get_following(client, username)
    return dump_following(username, data)

def read_following(username):
    filename = following_file(username)
    return read_json_file(filename)


# Starred Repos

def starred_repos_file(username):
    return "%s/raw/users/%s/starred.json" % (data_dir, username)    

def get_starred_repos(client, username):
    url = "https://api.github.com/users/%s/starred" % username
    return get_page_data(client, url)

def dump_starred_repos(username, starred_repos_data):
    filename = starred_repos_file(username)
    return dump_json(starred_repos_data, filename, "w")
    
def save_starred_repos(client, username):
    print "Saving starred repos for %s..." % username
    data = get_starred_repos(client, username)
    return dump_starred_repos(username, data)

def read_starred_repos(username):
    filename = starred_repos_file(username)
    return read_json_file(filename)

def starred_repos_client(username, queue):
    for starred_repo in read_starred_repos(username):
        #print starred_repo
        task = create_task(save_stargazers, starred_repo)
        queue.put(task)

def crawl_starred_repos_stargazers(client, username, workers=5):
    print "Saving all of %s's starred repos stargazers..." % username
    client_func = partial(starred_repos_client, username)
    #worker_func = lambda func, *args: func(client, *args)  # execute(client)
    worker_func = partial_args(client)
    threaded_queue(client_func, worker_func, workers=5)



# Stargazers

def stargazers_file(repo):
    return "%s/raw/repos/%s/stargazers.json" % (data_dir, repo["full_name"])    

def get_stargazers(client, starred_repo):
    stargazers_url = starred_repo['stargazers_url']
    return get_page_data(client, stargazers_url)

def dump_stargazers(repo, stargazers_data):
    filename = stargazers_file(repo)
    return dump_json(stargazers_data, filename, "w")

def save_stargazers(client, starred_repo):
    data = get_stargazers(client, starred_repo)
    return dump_stargazers(starred_repo, data)

def read_stargazers(repo):
    filename = stargazers_file(repo)
    return read_json_file(filename)


def filter_stargazers(username, data):
    max_stargazers = 500
    following_data = read_following(username)
    following_usernames = (map(lambda user: user['login'], following_data))
    n_following = len(following_usernames)

    uniques = dictlist_to_dict(data, "login")
    unique_keys = uniques.keys()
    
    population =  set(unique_keys) - set(following_usernames) # some following may not be in set
    found_usernames = len(unique_keys) - len(population) 
    random_sample = random.sample(population, max_stargazers - found_usernames)
    return random_sample + following_usernames


def stargazers_client(username, queue):
    # could return stargazers that repeat the most
    data = []
    for starred_repo in read_starred_repos(username):
        print starred_repo['full_name']
        try:
            data += read_stargazers(starred_repo)
        except:
            data += save_stargazers(client, starred_repo)
    for stargazer in filter_stargazers(username, data):
        task = create_task(save_starred_repos, stargazer)
        queue.put(task)

def crawl_stargazers_starred_repos(client, username, workers):
    print "Saving all of the stargazers' starred repos..."
    client_func = partial(stargazers_client, username)
    worker_func = partial_args(client)
    threaded_queue(client_func, worker_func, workers=5)

def extract(client, username, workers):

    global data_dir
    
    # 1. Get and Save User Data
    save_user_data(client, username) 

    # 2. Get and Save Who User Is Following
    save_following(client, username)

    # 3. Get and Save the User's Starred Repos
    save_starred_repos(client, username)

    # 4. Get and Save Starred Repos' List of Stargazers 
    crawl_starred_repos_stargazers(client, username, workers)

    # 5. Get and Save Stargazers' List of Starred Repos
    crawl_stargazers_starred_repos(client, username, workers)


#
# Transform
#

def walk_dir(start_dir, target_file):
    paths = []
    for dirname, dirnames, filenames in os.walk(start_dir):
        for filename in filenames:
            path = os.path.join(dirname, filename)
            basename = os.path.basename(filename)
            if (basename == target_file):
                print path
                paths.append(path)
    return paths


def filter_repo_data(raw_data):
    # Returns and OrderedDict
    desired_keys = ["id",
                    "name",
                    "full_name",
                    "owner",
                    "fork",
                    "created_at",
                    "updated_at",
                    "pushed_at",
                    "homepage",
                    "size",
                    "watchers_count",
                    "language",
                    "forks_count",
                    "forks",
                    "watchers"]
    data = ordered_subset(desired_keys, raw_data)
    data['owner'] = data['owner']['login']
    return data


def filter_stargazer_data(raw_data):
    # Returns and OrderedDict
    desired_keys = ["id",
                    "login",
                    "avatar_url",
                    "gravatar_id",
                    "html_url",
                    "type"]
    data = ordered_subset(desired_keys, raw_data)
    return data


def transform_starred_repo_data(vertex_file, edge_file):
    # have asides explaining the python code    
    #starred_repos_files = glob.glob('data/raw/starred-repos-*')
    start_dir = users_dir()
    ensure_dir(vertex_file)
    ensure_dir(edge_file)
    seen_repos = set([])
    starred_repos_files = walk_dir(start_dir, "starred.json")
    with codecs.open(vertex_file, "a", "utf-8") as vout:
        with codecs.open(edge_file, "a", "utf-8") as eout:
            for filename in starred_repos_files:
                username = filename.split(os.sep)[-2]
                starred_repos_data = read_json_file(filename)
                for raw_data in starred_repos_data:
                    data = filter_repo_data(raw_data)  
                    repo_full_name = data['full_name']
                    if repo_full_name not in seen_repos:
                        seen_repos.add(repo_full_name)
                        repo_line_template = ["%s"] * len(data.keys())
                        repo_line = ("::").join(repo_line_template) % tuple(data.values())
                        vout.write(repo_line + u"\n")
                    edge_line = "%s::starred::%s" % (username, repo_full_name)
                    eout.write(edge_line + u"\n")

def transform_stargazers_data(vertex_file, edge_file):
    # have asides explaining the python code    
    #starred_repos_files = glob.glob('data/raw/starred-repos-*')
    start_dir = repos_dir()
    ensure_dir(vertex_file)
    ensure_dir(edge_file)
    seen_users = set([])
    starred_repos_files = walk_dir(start_dir, "stargazers.json")
    with codecs.open(vertex_file, "a", "utf-8") as vout:
        with codecs.open(edge_file, "a", "utf-8") as eout:
            for filename in starred_repos_files:
                username = filename.split(os.sep)[-3]
                reponame = filename.split(os.sep)[-2]
                fullname = "%s/%s" % (username, reponame)
                stargazers_data = read_json_file(filename)
                for raw_data in stargazers_data:
                    data = filter_stargazer_data(raw_data)
                    stargazer = data['login']
                    if stargazer not in seen_users:  # prevent dupes
                        seen_users.add(stargazer)
                        user_line_template = ["%s"] * len(data.keys())
                        user_line = ("::").join(user_line_template) % tuple(data.values())
                        vout.write(user_line + u"\n")
                    edge_line = "%s::starred::%s" % (data['login'], fullname)
                    eout.write(edge_line + u"\n")


    
def transform(username):
    global data_dir

    repos_file = "%s/transformed/repos.dat" % data_dir
    users_file = "%s/transformed/users.dat" % data_dir      # dedupe the users file
    starred_file = "%s/transformed/starred.dat" % data_dir
    
    transform_starred_repo_data(repos_file, starred_file)
    transform_stargazers_data(users_file, starred_file)


def extract_and_transform(client, username, workers):
    global data_dir
    extract(client, username, workers)
    transform(username)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # Positional args
    parser.add_argument('ego',
                        help="The egocentric GitHub username to explore.")
    parser.add_argument('command_name', default="extract_and_transform")
    #parser.add_argument('command_args', nargs='*')
    
    # Optional args
    parser.add_argument("-u", "--username", default=None,
                        help="Your GitHub Account's Username")
    parser.add_argument("-p", "--password", default=None,
                        help="Your GitHub Account's Password")
    #parser.add_argument("-t", "--token", default=None,
    #                    help="Your GitHub Account's Auth Token")
    parser.add_argument("-d", "--data-dir", default="data",
                        help="Directory where to save data.")
    parser.add_argument("-w", "--workers", default=5, 
                        help="Number of worker threads to use.")

    args = parser.parse_args()

    username = args.ego
    data_dir = args.data_dir

    command_name = args.command_name
    #command_args = args.command_args

    token = get_token(args.username, args.password)
    client = create_client(token)    # client is thread-safe
    workers = args.workers
    counter = 0
    
    if command_name == "extract" or command_name == "e":
        extract(client, username, workers)

    if command_name == "transform" or command_name == "t":
        transform(username)

    if command_name == "extract_and_transform" or command_name == "et":
        extract_and_transform(client, username, workers)

    print username
    print data_dir
    print token
    print command_name
