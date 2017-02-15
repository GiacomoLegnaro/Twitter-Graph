#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Short summary that describe what the algorithm does
"""

import networkx as nx
# import pymongo
import tweepy
import time
import numpy as np
import os.path
# import threading
import matplotlib.pyplot as plt
import random
import json
from tweepy import OAuthHandler
from pymongo import MongoClient
# from random import choice
# from Tkinter import *
from tqdm import tqdm
# from multiprocessing import Pool, freeze_support
# import itertools
import os
os.chdir('/Users/giacomolegnaro/Documents/'
         'Data Science/1st Year/1st Semester/'
         'Algorithmic Methods of Data Mining and Laboratory/'
         'Project 2')
# os.chdir('C:\Giacomo\Project 2')
# os.chdir('C:\Giacomo\ADM\Project 2')
from tokens import *


__author__ = 'Giacomo Legnaro'
__version__ = '0.0'
__email__ = 'g.legnaro@gmail.com'
__status__ = 'Production'
__date__ = 'September 1st, 2016'


# OAuth(0)
# MONGODB_URI = \
#    'mongodb://macaroniglegoo:adm2016@ds035846.mlab.com:35846/adm2016'
# client = MongoClient('localhost', 27017)
database = 'adm2016'
api = None

# Function
def getOAuth(key_id):
    """
    Return the API Instance
    """
    try:
        cfg = OAuth(key_id)
        auth = OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
        auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
        api = tweepy.API(auth)
    except KeyError:
        api = None
    return api



def limitHandled(cursor,user,fun):
    global ct
    global time_limit_reached
    global api
    limit_reached = False
    while True:
        try:
            if limit_reached is True:
                print 'Continue from user: ', user
                limit_reached = False
            yield cursor.next()
        except tweepy.RateLimitError:
            if ct == 0:
                time_limit_reached = time.time()
            print 'Limit reached - change api #', ct, '...'
            ct += 1
            time.sleep(5)
            if getOAuth(ct) is None:
                t_wait = max(((15*60+15)-(time.time()-time_limit_reached)), 0)
                print 'All key tokens have been used'
                print 'Waiting time: ', t_wait
                time.sleep(t_wait)
                ct = 0
            limit_reached = True
            cursor_id = cursor.next_cursor
            api = getOAuth(ct)
            if fun == 'fwr':
                cursor = tweepy.Cursor(
                    api.followers_ids, id=user, cursor=cursor_id,
                    count=5000).pages()
            elif fun == 'search':
                cursor = tweepy.Cursor(
                    api.search, q=topic, cursor=cursor_id,
                    count=100).pages()
        except tweepy.TweepError as warning:
            if warning.reason == 'Not authorized.':
                print 'User follower_s list is not public'
                break
            if warning.message[0]['message'] == 'Sorry, that page does not exist.':
                print 'User does not exist'
                break
            time.sleep(5)


def getMongoCollection(collection):
    # client = pymongo.MongoClient(MONGODB_URI)
    client = MongoClient('localhost', 27017)
    db = client[database]
    return db[collection]


def storeTweets(topic):
    # global ct
    topic_dir = makeDirectory(os.path.join(os.getcwd(), 'output'), topic)
    api = getOAuth(0)
    print 'Connected to Twitter'
    tweets_collection = getMongoCollection(topic+'_tweets')
    print 'New collection', topic + '_tweets created'
    for tweetsPag in limitHandled(tweepy.Cursor(
            api.search, q=topic, count=100).pages(100), 0, 'search'):
        tweets_collection.insert_many(
            [{str((tweet.user).id): tweet.text} for tweet in tweetsPag])
    print 'Tweets correctly uploaded to mongoDB'


def getIDsMongo(topic):
    tweets_collection = getMongoCollection(topic+'_tweets')
    ids = []
    for document in tweets_collection.find():
        ids.append(sorted(document.keys())[0])
    return [list(set(ids)), list(ids)]


def saveHandledUsers(handled_users):
    global topic
    global flag
    if flag is 1:
        flag = 0
        np.array(handled_users).dump(topic+'_handledUsers_data')
        flag = 1


def getStoreFollowers(user):
    global topic
    global handled_users
    global ct
    global api
    api = getOAuth(ct)
    edges_collection = getMongoCollection(topic+"_edges")
    for fwrs_page in limitHandled(
        tweepy.Cursor(
            api.followers_ids, id=user, count=5000).pages(), user, 'fwr'):
        add_fwr = [{user: str(fwr)} for fwr in fwrs_page]
        edges_collection.insert_many(add_fwr)
    handled_users.append(int(user))
    saveHandledUsers(handled_users)
    print 'User ', user, ' handled, no. users: ', len(handled_users)


def getFollowers(ids):
    for user in ids:
        print 'Processing user:', user, 'WIP:', round(float(ids.index(user))/len(ids), 2)
        # WIP: Work In Progress
        getStoreFollowers(user)
        time.sleep(5)
        print 'User again to download:', len(ids)-ids.index(user)
    print "Followers Collected"


def createGraph(topic):
    topic_dir = makeDirectory(os.path.join(os.getcwd(), 'output'), topic)
    try:
        print 'Find out if the social graph is already store...'
        G = nx.read_edgelist(
            open(topic_dir+'/'+topic+'_socialGraph_edgelist', 'rb'),
            nodetype=int, create_using=nx.DiGraph())
        print 'Graph reloaded'
    except:
        # G = nx.DiGraph()
        # edges_collection = getMongoCollection(topic + '_edges')
        # print 'No. We must build a new ones...'
        # for document in tqdm(edges_collection.find()):
        #     fwd = sorted(document.keys())[0]
        #     fwr = document[fwd]
        #     G.add_edge(int(fwr), int(fwd))
        # print 'Graph built!'
        # nx.write_edgelist(G, open(
        #     topic_dir+'/'+topic+'_socialGraph_edgelist', 'wb'))

        edges_collection = getMongoCollection(topic + '_edges')
        print 'No. We must build a new ones...'
        with open(topic_dir+'/'+topic+'_socialGraph_edgelist', 'w') as f:
            for document in tqdm(edges_collection.find()):
                fwd = sorted(document.keys())[0]
                fwr = document[fwd]
                f.write(str(fwr)+'\t'+str(fwd)+'\n')
        print 'Graph saved!'
        # G = nx.read_edgelist(topic_dir+'/'+topic+'_socialGraph_edgelist',
        #                     nodetype=int, create_using=nx.DiGraph())
        # print 'Graph loaded'
    return G


def makeDirectory(path, directory):
    """
    Return the directory path where save the documents

    :param path: path where create the new folder
    :param directory: name of the new folder
    """
    try:
        os.makedirs(os.path.join(path, directory))
    except OSError:
        if not os.path.isdir(os.path.join(path, directory)):
            raise
    return os.path.join(path, directory)


def graphAnalysis(G, topic):
    print 'Create the folder to save the', topic, 'plot'
    topic_dir = makeDirectory(os.path.join(os.getcwd(), 'output'), topic)

    # plot Graph Directed
    plt.clf()
    nx.spring_layout(G)
    color = [G.degree(n) for n in G]
    size = [10 * (G.degree(n) + 1.0) for n in G]
    nx.draw(G, node_color=color, node_size=size,
            cmap=plt.cm.Blues, arrows=False)
    plt.savefig(topic_dir+'/Directed_Graph.svg', format='svg', dpi=1200)
    plt.savefig(topic_dir+'/Directed_Graph.png')
    plt.close()

    # Plot graph UnDirected
    print 'Create an undirected copy of the graph \n'
    G_ud = G.to_undirected()
    plt.clf()
    nx.spring_layout(G_ud)
    color = [G_ud.degree(n) for n in G_ud]
    size = [10 * (G_ud.degree(n) + 1.0) for n in G]
    nx.draw(G, node_color=color, node_size=size,
            cmap=plt.cm.Blues, arrows=False)
    plt.savefig(topic_dir+'/UnDirected_Graph.svg', format='svg', dpi=1200)
    plt.savefig(topic_dir+'/UnDirected_Graph.png')
    plt.close()

    # (a) Compute and plot the degree distribution
    # In-Degree
    print 'Compute the degree distribution \n'
    print 'Compute the number of edges pointing in to the node \n'
    in_degree = G.in_degree()
    print topic_dir+'/in_degree.json'
    print
    with open(topic_dir+'/in_degree.json', 'w') as fp:
        json.dump(in_degree, fp)
    in_values = sorted(set(in_degree.values()), reverse=True)
    in_hist = [in_degree.values().count(x) for x in in_values]
    plt.figure()
    plt.loglog(in_values, in_hist, 'ro', ls='None')

    # Out-Degree
    print 'Compute the number of edges pointing out of the node \n'
    out_degree = G.out_degree()
    with open(topic_dir+'/out_degree.json', 'w') as fp:
        json.dump(in_degree, fp)
    out_values = sorted(set(out_degree.values()), reverse=True)
    out_hist = [out_degree.values().count(x) for x in out_values]
    plt.plot(out_values, out_hist, 'bv', ls='None')
    plt.legend(['In-degree', 'Out-degree'])
    plt.xlabel('Degree')
    plt.ylabel('Number of nodes')
    plt.title('Degree Distribution - In vs Out')
    plt.savefig(topic_dir+'/degree_distribution_in_out.svg', format='svg', dpi=1200)
    plt.savefig(topic_dir+'/degree_distribution_in_out.png')
    plt.close()

    # Degree
    print 'Compute the degree distribution \n'
    degree = G.degree()
    with open(topic_dir+'/in_degree.json', 'w') as fp:
        json.dump(degree, fp)
    d_values = sorted(set(degree.values()), reverse=True)
    hist = [degree.values().count(x) for x in d_values]
    plt.figure()
    plt.loglog(d_values, in_hist, 'ro', ls='None')
    plt.legend('Degree')
    plt.xlabel('Degree')
    plt.ylabel('Number of nodes')
    plt.title('Degree distribution')
    plt.savefig(topic_dir+'/degree_distribution.svg', format='svg', dpi=1200)
    plt.savefig(topic_dir+'/degree_distribution.png')
    plt.close()

    # (b)
    # √
    # (b.1-4) Compute and plot the degree centralities
    print 'Compute degree centrality for nodes \n'
    dc = nx.degree_centrality(G)
    with open(topic_dir+'/degree_centrality.json', 'w') as fp:
        json.dump(dc, fp)
    dc_sort = sorted(dc.values(), reverse=True)
    plt.loglog(dc_sort, 'b-', marker='x', label='Degree')
    # (b.2-4) Compute and plot the closeness centralities
    print 'Compute closeness centrality\n'
    closeness = nx.closeness_centrality(G)
    with open(topic_dir+'/closeness_centrality.json', 'w') as fp:
        json.dump(closeness, fp)
    closeness_sort = sorted(closeness.values(), reverse=True)
    plt.loglog(closeness_sort, 'g-', marker='o', label='Closeness')
    # (b.3-4) Compute and plot the betweeness centralities
    print 'Compute betweenness centrality\n'
    betweenness = nx.betweenness_centrality(G)
    with open(topic_dir+'/betweenness_centrality.json', 'w') as fp:
        json.dump(betweenness, fp)
    betweenness_sort = sorted(betweenness.values(), reverse=True)
    plt.loglog(betweenness_sort, 'r-', marker='8', label='Betweenness')
    # (b.4-4) Compute and plot the PageRank centralities
    print 'Compute the PageRank of the nodes in the graph \n'
    pagerank = nx.pagerank(G, alpha=0.85)
    with open(topic_dir+'/pagerank.json', 'w') as fp:
        json.dump(pagerank, fp)
    pagerank_sort = sorted(pagerank.values(), reverse=True)
    plt.loglog(pagerank_sort, 'c-', marker='*', label='Betweenness')
    plt.title('Measures of node centrality')
    plt.ylabel('Value - log scale')
    plt.xlabel('No of nodes - log scale')
    plt.legend()
    plt.savefig(topic_dir+'/measures_centrality.svg', format='svg', dpi=1200)
    plt.close()
    # plt.show()

    # (c) Compute the clustering coefficient of some of the nodes,
    # and the clustering coefficient of the graph.
    # √
    print 'Compute the clustering coefficient of some nodes \n'
    no_nodes = int(raw_input('How many nodes do you want to consider? '))
    id_nodes = sorted(random.sample(G_ud.nodes(), no_nodes))
    cc_nodes = nx.clustering(G_ud, id_nodes)
    cc_sub = {}
    for i in range(no_nodes):
        print 'The clustering coefficient of node', id_nodes[i], \
              'is', cc_nodes.values()[i]
        cc_sub[id_nodes[i]] = cc_nodes.values()[i]
    with open(topic_dir+'/clustering.json', 'w') as fp:
        json.dump(cc_sub, fp)
    print 'Compute the clustering coefficient average of the graph \n'
    cc_graph = nx.average_clustering(G_ud)
    with open(topic_dir+'/clustering_avg.json', 'w') as fp:
        json.dump(cc_graph, fp)
    print 'The clustering coefficient average of the graph is', cc_graph
    with open(topic_dir+'/output.txt', 'a') as f:
        f.write('The clustering coefficient average of the graph is ' +
                cc_graph+'\n')

    # (d) Compute the connected components of the graph
    print 'Compute the connected components of the graph'
    no_comp = sum(1 for comp in nx.connected_components(G_ud))
    print 'The number of connected components is:', no_comp
    with open(topic_dir+'/output.txt', 'a') as f:
        f.write('The number of connected components is: '+no_comp+'\n')


    cc_sizes = [len(w) for w in nx.connected_components(G_ud)]
    with open(topic_dir+'/connected_components.json', 'w') as fp:
        json.dump(no_comp, fp)
    # len(cc_sizes)
    # max(cc_sizes)
    plt.hist(cc_sizes, bins=range(1, 20))
    plt.title('Sizes of connected components')
    plt.xlabel('Size')
    plt.ylabel('Frequency')
    plt.savefig(topic_dir+'/hist_connected_components.svg', format='svg', dpi=1200)
    plt.close()

    # (e) Perform the k-core decomposition of the largest component
    # Choose the largest component
    print 'Perform the k-core decomposition of the largest component \n'
    G_ccs = max(nx.connected_component_subgraphs(G_ud), key=len)
    core_nodes = nx.k_core(G_ccs).nodes()
    with open(topic_dir+'/k_core.json', 'w') as fp:
        json.dump(core_nodes, fp)
    print 'k-degree of the graph core:', len(G_ccs)
    print 'The nodes of the graph core are:', core_nodes
    with open(topic_dir+'/output.txt', 'a') as f:
        f.write('k-degree of the graph core: '+len(G_ccs)+'\n')
        f.write('The nodes of the graph core are:'+core_nodes+'\n')
    plt.clf()
    nx.spring_layout(G_ccs)
    color = [G_ccs.degree(n) for n in G_ud]
    size = [10 * (G_ccs.degree(n) + 1.0) for n in G]
    nx.draw(G, node_color=color, node_size=size,
            cmap=plt.cm.Blues, arrows=False)
    plt.savefig(topic_dir+'/Connected_Component_Subgraphs.svg', format='svg', dpi=1200)
    plt.savefig(topic_dir+'/Connected_Component_Subgraphs.png')
    plt.close()

    # Compute the k-core decomposition
    size = len(G_ccs)
    while size != 0:
        k = min(G_ccs.degree().values()) + 1
        G_ccs_dec = nx.k_core(G_ccs, k)
        size = len(G_ccs)

# def main():
sel = raw_input('Topic (DEFAULT: 0 Enron Graph - 1 Our Own Graph) \n')
if sel == '0':
    topic = 'enron'
    try:
        G = nx.read_edgelist(
                'Email-Enron.txt', nodetype=int, create_using=nx.DiGraph())
        print 'It is ok'
        graphAnalysis(G, topic)
    except:
        print 'Some issue with the graph'
elif sel == '1':
    topic = 'tesla'
    G = createGraph(topic)
    graphAnalysis(G, topic)
else:
    print 'Your choice: ', sel
    topic = sel.replace(' ', '')
    topic_dir = makeDirectory(os.path.join(os.getcwd(), 'output'), topic)
    if not os.path.exists(topic_dir+'/'+topic+'_handledUsers_data'):
        storeTweets(topic)
        handled_users = []
    else:
        print 'You have already look up this topic'
        handled_users = np.load(
            topic_dir+'/'+topic+'_handledUsers_data').tolist()
    print 'Getting IDs from MongoLab '
    ids = getIDsMongo(topic)
    print 'IDs Acquired:', len(ids[1]), ' --  IDs unique:', len(ids[0])
    temp = map(str, handled_users)
    ids = list(set(ids[1])-set(temp))
    print 'IDs to be acquired:', len(ids), ' --  IDs acquired:',
    flag = 1
    ct = 0
    getFollowers(ids)
    gext = createGraph(topic)
    #graphAnalysis(gext, topic)


# if __name__ == "__main__":
#     main()
