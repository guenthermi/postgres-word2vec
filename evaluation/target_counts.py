import psycopg2
import evaluation_utils as ev
import ivpq_evaluation as ivpqEv
from tracking import Tracker
import plotly.graph_objs as go
import plotly.offline as py
import json
import sys
import numpy as np

filename =  sys.argv[1];

f = open(filename, 'r')
config = json.loads(f.read())

con, cur = ev.connect()

number_of_query_samples = config['number_of_query_samples']
number_of_target_samples = config['number_of_target_samples']
num_iters = config['num_iters']

k = config['k']
alpha = config['alpha_start']
method_flag = config['method']
step_size = config['step_size']

query_template = 'SELECT query, target FROM knn_in_ivpq_batch(''{!s}'', {:d}, ''{!s}'');'

ev.set_vec_table_name(config['vecs_table_name'])

search_params = {'pvf':1, 'alpha':alpha, 'method':method_flag}

ivpqEv.set_search_params(con, cur, search_params)


number_of_vectors = ev.get_vector_dataset_size(cur)

query_samples = ev.get_samples(con, cur, number_of_query_samples, number_of_vectors)
target_samples = ev.get_samples(con, cur, number_of_target_samples, number_of_vectors)


prediction = []
real = []
divergence = []
divergence_relative = []
for i in range(num_iters):
    params = [(ivpqEv.add_escapes([x]), k, ivpqEv.add_escapes(target_samples)) for x in query_samples]
    trackings, _ = ev.create_track_statistics(cur, con, query_template, params)
    prediction += ([float(t['target_count'][0][0]) for t in trackings])
    real += ([float(t['retrieved'][0][0]) for t in trackings])
    print([float(t['target_count'][0][0]) for t in trackings])
    # divergence +=
    alpha += step_size
    print("alpha:", alpha)
    search_params = {'pvf':1, 'alpha':alpha, 'method':method_flag}
    ivpqEv.set_search_params(con, cur, search_params)
    print('Iteration', i)
for i in range(len(prediction)):
    divergence.append(abs(prediction[i] - real[i]))
    divergence_relative.append(2* (abs(prediction[i]- real[i])/ (prediction[i] + real[i])))


py.init_notebook_mode(connected=True)

trace = go.Scatter(
    x = prediction,
    y = real,
    mode = 'markers',
    name = 'target_counts',
    marker = {
        'size': 4
    }
)

regression = np.polyfit(prediction, divergence, 4)
regression_func = np.poly1d(regression)
trace_divergence = go.Scatter(
    x = prediction,
    y = divergence,
    mode = 'markers',
    name = 'divergence',
    marker = {
        'size': 4
    }
)
trace_divergence_regression = go.Scatter(
    x = [5*i for i in range(int(number_of_target_samples/5))],
    y = [regression_func(5*i) for i in range(int(number_of_target_samples/5))],
    mode = 'lines',
    line = {'width':4},
    name = 'divergence'
)

layout = go.Layout(
    margin = {
        'b': 140,
    },
    height = 750,
    showlegend = False,
    xaxis=dict(
        title=('Prediction'),
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0, 0.45],
        ),
    yaxis=dict(
        title='Real Value',
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0.5, 0.95]))
        ))

data = [trace]
py.offline.plot({'data': data, 'layout': layout}, filename="target_counts.html", auto_open=True)

layout = go.Layout(
    margin = {
        'b': 140,
    },
    height = 750,
    showlegend = False,
    xaxis=dict(
        title=('Prediction'),
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0, 0.45],
        ),
    yaxis=dict(
        title='Divergence Absolute',
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0.5, 0.95]))
        ))

data = [trace_divergence, trace_divergence_regression]
py.offline.plot({'data': data, 'layout': layout}, filename="target_counts_divergence.html", auto_open=True)
