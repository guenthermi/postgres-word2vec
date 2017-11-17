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

def calculate_similarity_values(words, vecs, size):
    sampleIds = [random.randint(0, size) for x in range(SAMPLE_SIZE)]
    # init faiss
    index = faiss.IndexFlatL2(len(vecs[0]))
    index.add(vecs[:size])

    # calculate TopK
    similarities = []
    for i in range(STEPS_K):
        similarities.append([])
    for i, sampleId in enumerate(sampleIds):
        D, I = index.search(np.array([vecs[sampleId]]),STEPS_K)
        for j in range(STEPS_K):
            # measure similarity
            cos_sim = np.dot(vecs[sampleId], vecs[I[0][j]])
            similarities[j].append(cos_sim)
    return similarities

def plot_graph(sim_values):
    plot_data = [
        go.Scatter(
            x=[n+1 for n in range(len(sim_values))],
            y=[np.arccos(np.mean(x))*180/np.pi for x in sim_values],
            error_y=dict(
                type='data',
                visible=True,
                array=[np.mean([abs(np.mean(x) - y) for y in x])*180/np.pi for x in sim_values]
            )
        )
    ]
    layout = go.Layout(xaxis=dict(nticks=STEPS_K, tickfont=dict(size=20)), yaxis= dict(range=[0,90], title='angle in degree', titlefont=dict(size=20), tickfont=dict(size=20)))
    fig = go.Figure(data=plot_data, layout=layout)
    plotly.offline.plot(fig, filename="tmp.html", auto_open=True)
    return None

def main(argc, argv):
    words, vecs, size = utils.get_vectors('../vectors/google_vecs.txt')
    sim_values = calculate_similarity_values(words, vecs, size)
    print(sim_values)
    plot_graph(sim_values[1:])


if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
