#!/bin/python3

# Measures the distribution of word vectors in the vector space

# add folder of utils module to python path
import sys
sys.path.append('../index_creation/')

import random
import time
import faiss
import numpy as np
import plotly
import plotly.graph_objs as go

import index_utils as utils

SAMPLE_SIZE = 100
STEPS_K = 6

def updateTopK(topk, id, sim, negative=True):
    if (sim < topk[STEPS_K-1][1]) == negative:
        i = 0
        tmp_id = -1
        tmp_sim = 0
        while (i < STEPS_K):
            if (sim < topk[i][1]) == negative:
                tmp_id = topk[i][0]
                tmp_sim = topk[i][1]
                topk[i] = (id, sim)
                break
            i += 1
        for j in range(1, STEPS_K-i):
            tmp_id_new = topk[i+j][0]
            tmp_sim_new = topk[i+j][1]
            topk[i+j] = (tmp_id, tmp_sim)
            tmp_id = tmp_id_new
            tmp_sim = tmp_sim_new
    return

def calculate_similarity_values(words, vecs, size):
    sampleIds = [random.randint(0, size) for x in range(SAMPLE_SIZE)]
    # init faiss
    index = faiss.IndexFlatL2(len(vecs[0]))
    index.add(vecs[:size])

    # calculate TopK
    similarities = []
    dissimilarities = []
    for i in range(STEPS_K):
        similarities.append([])
    for i, sampleId in enumerate(sampleIds):
        D, I = index.search(np.array([vecs[sampleId]]),STEPS_K)
        for j in range(STEPS_K):
            # measure similarity
            cos_sim = np.dot(vecs[sampleId], vecs[I[0][j]])
            similarities[j].append(cos_sim)
    # calculate farest neighbors
    for i in range(STEPS_K):
        dissimilarities.append([])
    for i, sampleId in enumerate(sampleIds):
        topk = [(-1,1) for x in range(STEPS_K)]
        for j, vec in enumerate(vecs[:size]):
            cos_sim = np.dot(vecs[sampleId], vecs[j])
            updateTopK(topk, j, cos_sim)
        for j in range(STEPS_K):
            dissimilarities[j].append(topk[j][1])
    return similarities, dissimilarities

def plot_graph(sim_values, dis_sim_values):
    plot_data = [
        go.Scatter(
            x=[n+1 for n in range(len(sim_values))],
            y=[np.arccos(np.mean(x))*180/np.pi for x in sim_values],
            error_y=dict(
                type='data',
                visible=True,
                array=[np.mean([abs(np.mean(x) - y) for y in x])*180/np.pi for x in sim_values]
            ),
            name='Nearest Neighbors'
        ),
        go.Scatter(
            x=[n+1 for n in range(len(dis_sim_values))],
            y=[np.arccos(np.mean(x))*180/np.pi for x in dis_sim_values],
            error_y=dict(
                type='data',
                visible=True,
                array=[np.mean([abs(np.mean(x) - y) for y in x])*180/np.pi for x in dis_sim_values]
            ),
            name='Farest Neighbors'

        )
    ]
    print([np.arccos(np.mean(x))*180/np.pi for x in sim_values])
    print([np.arccos(np.mean(x))*180/np.pi for x in dis_sim_values])
    layout = go.Layout(xaxis=dict(nticks=STEPS_K, tickfont=dict(size=20)), yaxis= dict(range=[0,180], title='angle in degree', titlefont=dict(size=20), tickfont=dict(size=20)))
    fig = go.Figure(data=plot_data, layout=layout)
    plotly.offline.plot(fig, filename="tmp.html", auto_open=True)
    return None

def main(argc, argv):
    filename = '../vectors/google_vecs.txt'
    if argc > 1:
        filename = argv[1]
    words, vecs, size = utils.get_vectors(filename)
    print(size)
    sim_values, dis_sim_values = calculate_similarity_values(words, vecs, size)
    print(sim_values)
    print(dis_sim_values)
    plot_graph(sim_values[1:], dis_sim_values[:-1])


if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
