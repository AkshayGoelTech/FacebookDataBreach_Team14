import matplotlib
matplotlib.use("agg")
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import pickle
from sklearn import manifold
from sklearn.preprocessing import normalize
import json
import random

NOISE_THRESH = 10

clusterout = "big/lsa_clusters.pickle"
lsaresults = "big/lsa.csv"
dataset = "big/cleaned.json"


def create_matrix():
    print "Reading input data from CSV File..."
    mat = np.genfromtxt(lsaresults, delimiter=',').T
    print "Size: ", mat.shape
    return mat


def get_headlines(fname):
    with open(fname) as f:
        content = f.readlines()
    headlines = [(json.loads(x.lower()))['title'] for x in content]
    return headlines


def get_input(fname):
    print "Reading Input JSON..."
    with open(fname) as f:
        content = f.readlines()
    content = [json.loads(x.lower()) for x in content]
    print "Done loading json!"
    return content


def cat_documents(indices, cluster):
    text = [cleaned[i]['text'] for i in indices]
    url = [cleaned[i]['originalurl'] for i in indices]
    title = [cleaned[i]['title'] for i in indices]
    catdoc = {'text': text, 'originalurl': url, 'title': title, 'clusterid': int(cluster)}
    return catdoc


def create_json(points_dict, outfile):
    print 'Creating json output'
    for key in points_dict.keys():
        indices = points_dict[key]
        if len(indices) <= NOISE_THRESH:
            continue
        catdoc = cat_documents(indices, key)
        outfile.write(json.dumps(catdoc) + "\n")
    outfile.close()


headlines = np.array(get_headlines(dataset))
mat = create_matrix()
normal_mat = normalize(mat, axis=1)
cleaned = get_input(dataset)

#embedding = manifold.MDS(n_components=2)
#mat_mds = embedding.fit_transform(normal_mat)


def legend_rm_duplicates():
    handles, labels = plt.gca().get_legend_handles_labels()
    newLabels, newHandles = [], []
    for handle, label in zip(handles, labels):
        if label not in newLabels:
            newLabels.append(label)
            newHandles.append(handle)
    plt.gca().legend(newHandles, newLabels)


def plot_clusters(points_dict):
    fig = plt.figure()
    ax = fig.add_subplot(111)#, projection='3d')
    lengths = np.array([len(points_dict[key]) for key in points_dict.keys()])
    nclusters = np.sum(lengths > NOISE_THRESH)
    colors = plt.cm.jet(np.linspace(0, 1, nclusters+2)) #max(points_dict.keys())+2)) # +2 to account for 0-index/1-index & -1
    colors[-1,:] = [0, 0, 0, 1]  # black for noise
    i = 0
    for key in points_dict.keys():
        indices = points_dict[key]
        #X = mat_mds[indices, 0]
        #Y = mat_mds[indices, 1]
        #Z = mat_mds[indices, 2]
        X = mat[indices, 0]
        Y = mat[indices, 1]
        #Z = mat[indices, 2]
        if len(indices) <= NOISE_THRESH:
            c = -1    # plot as noise
        else:
            c = i
            i += 1
        label = key if (c != -1) else c
        ax.scatter(X, Y, color=colors[c], label=label)
        ax.set_xlabel('topic1')
        ax.set_ylabel('topic2')
        ax.set_title('Clustered Documents')
    legend_rm_duplicates()


def print_headlines(points_dict, outfile):
    for key in points_dict.keys():
        indices = points_dict[key]
        if len(indices) <= NOISE_THRESH:
            continue
        cluster_headlines = headlines[indices]
        i = random.sample(range(len(cluster_headlines)), min(40, len(cluster_headlines)))
        outfile.write('~~~~~~~~~~ Cluster %d ~~~~~~~~~~~~\n' % key)
        for ch in cluster_headlines[i]:
            outfile.write(ch + '\n')
    outfile.write('\n')
    outfile.close()



def expand_node(node, params=[]):
    # base case - we got data
    #print "Expanding new plot ", str(params)
    if node == {}:
        return
    if type(node[node.keys()[0]]) != dict:
        # Save headlines for each cluster to a text file
        #print_headlines(node, outfile)
        # Save output data
        if set(['reweighted', 'kmeans', '40-topics', '24-clusters']) == set(params):
            create_json(node, open('_'.join(params) + '.json', 'w'))
            #outfile = open('final_' + str(params) + '.txt', 'w')
            #print_headlines(node, outfile)
        if set(['original_weight', 'kmeans', '40-topics', '16-clusters']) == set(params):
            create_json(node, open('_'.join(params) + '.json', 'w'))
            #outfile = open('final_' + str(params) + '.txt', 'w')
            #print_headlines(node, outfile)
        # Create a plot
        #plot_clusters(node)
        #plt.suptitle(str(params))
        #plt.savefig(str(params) + '.png')
        #plt.show()
        #plt.close()
    else:
        for key in node.keys():
            new_params = params + [key]
            new_node = node[key]
            #if key == 'agglomerative':
            #    continue
            expand_node(new_node, new_params)


def plot_dataset(filename):
    data = pickle.load(open(filename, 'r'))
    expand_node(data)


plot_dataset(clusterout)
