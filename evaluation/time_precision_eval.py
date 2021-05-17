import psycopg2
import plotly.graph_objs as go

import evaluation_utils as ev
import ivpq_evaluation as ivpqEv

import json
import sys

MARKER_COLORS = ['rgba(255, 0, 0, 1)', 'rgba(0, 150, 0, 1)', 'rgba(0, 0, 200, 1)', 'rgb(204,204,0, 1)']

def set_markers(markers, names, color, alphas, i):

    for n, name in enumerate(names):
        if n == 0:
            markers[name] = dict(color= color, symbol='square-open', size=10)
        elif n == 1:
            markers[name] = dict(color= color, symbol='circle-open', size=10)
        elif n == 2:
            markers[name] = dict(color= color, symbol='triangle-open', size=10)

filename =  sys.argv[1];

f = open(filename, 'r')
config = json.loads(f.read())

con, cur = ev.connect()

number_of_query_samples = config['number_of_query_samples']
number_of_target_samples = config['number_of_target_samples']
num_iters = config['num_iters']
steps = config['steps']
step_size = config['step_size']
small_sample_size = config['small_sample_size'] # for bootstrap sample to calculate precision

k = config['k']
pvf = config['pvf']
alphas = config['alphas']

pvf_values = [0+ i*step_size for i in range(1, steps+1)] # parameter_variables
pvf_set_query = 'SELECT set_pvf({:d});' # param_query

# TODO this is not used at the moment
alpha_values = [0+ i*5 for i in range(1, 10)] # parameter_variables
apha_set_query = 'SELECT set_alpha({:d});' # param_query

ev.set_vec_table_name(config['vecs_table_name'])

search_params = [{'pvf':pvf, 'alpha':alphas[0], 'method':0}, {'pvf':pvf, 'alpha':alphas[0], 'method':0}, {'pvf':pvf, 'alpha':alphas[0], 'method':1}, {'pvf':pvf, 'alpha':alphas[0], 'method':2}]
names = ['Baseline','PQ a='+ str(alphas[0]), 'Exact a=' + str(alphas[0]), 'PQ+PV a=' + str(alphas[0])]
query = 'SELECT query, target FROM knn_in_{!s}pq_batch(''{!s}'', {:d}, ''{!s}'');'
param_query = pvf_set_query
parameter_variables =  [[0], [0], [0], pvf_values]

markers = dict()
markers['Baseline'] = dict(color= 'rgba(0, 0, 0, 1)', symbol='circle-dot', size=10)
set_markers(markers, names[1:], MARKER_COLORS[0], alphas, 1)

ivpqEv.set_num_iterations(num_iters)
execution_times, inner_times, precision_values, _ = ivpqEv.time_and_precision_measurement_for_ivpq_batch(con, cur, search_params, names, query, k, param_query, parameter_variables, number_of_query_samples, number_of_target_samples, small_sample_size)
for i in range(1, len(alphas)):
    alpha = alphas[i]
    search_params = [{'pvf':pvf, 'alpha':alpha, 'method':0}, {'pvf':pvf, 'alpha':alpha, 'method':1}, {'pvf':pvf, 'alpha':alpha, 'method':2}]
    names_new = ['PQ a='+ str(alpha), 'Exact a=' + str(alpha), 'PQ+PV a=' + str(alpha)]
    parameter_variables =  [[0], [0], pvf_values]
    _, inner_times_new, precision_values_new, _ = ivpqEv.time_and_precision_measurement_for_ivpq_batch(con, cur, search_params, names_new, query, k, param_query, parameter_variables, number_of_query_samples, number_of_target_samples, small_sample_size)
    inner_times.update(inner_times_new)
    precision_values.update(precision_values_new)
    names += names_new
    print(names_new, inner_times_new)
    print('i', i )
    print('MARKER_COLORS[i]', MARKER_COLORS[i])
    set_markers(markers, names_new, MARKER_COLORS[i], alphas, i+1)

layout = go.Layout(
    margin = {
        'b': 140,
    },
    height = 750,
    showlegend = True,
    legend=dict(x=.95, y=0.95, font=dict(size=20)),
    xaxis=dict(
        title=('Response Time'),
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0, 0.45],
        ),
    yaxis=dict(
        title='Precision',
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        range= [0, 1]
        ))

ivpqEv.plot_time_precision_graphs(inner_times, precision_values, names, make_iplot=False, layout=layout, markers=markers)
