import evaluation_utils as ev
import plotly.offline as py
import plotly.graph_objs as go
import numpy as np
import psycopg2
from tracking import Tracker
from collections import defaultdict
from numpy import mean
import json
import sys



filename =  sys.argv[1];

f = open(filename, 'r')
config = json.loads(f.read())

number_of_query_samples = config['number_of_query_samples'] # 100000
number_of_target_samples = config['number_of_target_samples'] # 100000
variable_parameter = config['variable_parameter'] # 'target_size'

num_iters = config['num_iters'] # 1

k = config['k'] # 5
alpha = config['alpha'] # 10
pvf = config['pvf'] # 10
method = config['method'] # 0 # 0: PQ / 1: EXACT / 2: PQ+PostVerification
use_target_list = config['use_target_list'] # 'true'

step_size = config['step_size']

# ev.set_vec_table_name('twitter25_vecs_norm')

con, cur = ev.connect()

# ev.set_vec_table_name('twitter25_vecs_norm')
ev.set_vec_table_name(config['vecs_table_name'])

data_size = ev.get_vector_dataset_size(cur)
print(data_size)

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'
STD_DB_NAME = 'imdb'

con = psycopg2.connect("dbname='" + STD_DB_NAME + "' user='" + STD_USER + "' host='" + STD_HOST + "' password='" + STD_PASSWORD + "'")
cur = con.cursor()


# set parameters
cur.execute('SELECT set_pvf({:d});'.format(pvf))
cur.execute('SELECT set_alpha({:d});'.format(alpha))
cur.execute('SELECT set_method_flag({:d});'.format(method))
cur.execute("SELECT set_use_targetlist('{!s}');".format(use_target_list))
con.commit()

dynamic_sizes = list([i*step_size for i in range(1,11)])

query = 'SELECT * FROM knn_in_iv_batch(''{!s}'', {:d}, ''{!s}'', \'ivpq_search_in\') LIMIT 1;'
convert = lambda x: "'{" + ",".join([s.replace("'", "''").replace("\"", "\\\"").replace("{", "\}").replace("{", "\}").replace(",", "\,") for s in x]) + "}'"

get_samples = lambda x: ev.get_samples(con, cur, x, data_size)

all_trackings = [defaultdict(list) for i in dynamic_sizes]
for iteration in range(num_iters):
    if variable_parameter == 'query_size':
        params = [(convert(get_samples(i)), k, convert(get_samples(number_of_target_samples))) for i in dynamic_sizes]
    if variable_parameter == 'target_size':
        params = [(convert(get_samples(number_of_query_samples)), k, convert(get_samples(i))) for i in dynamic_sizes]
    trackings, execution_times = ev.create_track_statistics(cur, con, query, params)
    for i in range(len(trackings)):
        for key in trackings[i].keys():
            all_trackings[i][key].append(trackings[i][key])


trackings = all_trackings
calculate_time = lambda x: mean([sum([float(elem[0]) for elem in ar]) for ar in x])

# create diagram
trace0 = go.Scatter(
    x = dynamic_sizes,
    y = [calculate_time(t['precomputation_time']) for t in trackings],
    mode = 'lines+markers',
    name = 'precomputation',
    marker = {
        'size': 10,
        'symbol': 'square-open'
    }
)
trace1 = go.Scatter(
    x = dynamic_sizes,
    y = [calculate_time(t['query_construction_time']) for t in trackings],
    mode = 'lines+markers',
    name = 'query construction',
    marker = {
        'size': 10,
        'symbol': 'circle-open'
    }
)
trace2 = go.Scatter(
    x = dynamic_sizes,
    y = [calculate_time(t['data_retrieval_time']) for t in trackings],
    mode = 'lines+markers',
    name = 'data retrieval',
    marker = {
        'size': 10,
        'symbol': 'triangle-open'
    }
)
trace3 = go.Scatter(
    x = dynamic_sizes,
    y = [calculate_time(t['computation_time']) for t in trackings],
    mode = 'lines+markers',
    name = 'distance computation',
    marker = {
        'size': 10,
        'symbol': 'asterisk-open'
    }
)
trace4 = go.Scatter(
    x = dynamic_sizes,
    y = [mean([float(x[0][0]) for x in t['total_time']]) for t in trackings],
    mode = 'lines+markers',
    name = 'inner execution time',
    marker = {
        'size': 10,
        'symbol': 'triangle-open'
    }
)
trace5 = go.Scatter(
    x = dynamic_sizes,
    y = [sum([calculate_time(t[k]) for k in t.keys()])/(s/10) for (t,s) in zip(trackings,dynamic_sizes)],
    mode = 'lines+markers',
    name = 'relative*10^-100',
    marker = {
        'size': 10,
        'symbol': 'square-open'
    }
)
trace6 = go.Scatter(
    x = dynamic_sizes,
    y = execution_times,
    mode = 'lines+markers',
    name = 'complete_time',
    marker = {
        'size': 10,
        'symbol': 'circle-open'
    }
)

layout = go.Layout(
    margin = {
        'b': 140,
    },
    height = 750,
    showlegend = True,
    legend=dict(x=.05, y=0.95, font=dict(size=20)),
    xaxis=dict(
        title=('Target Set Size' if variable_parameter == 'target_size' else 'Query Set Size'),
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0, 0.45],
        ),
    yaxis=dict(
        title='Time in Seconds',
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        # domain= [0.5, 0.95]))
        ))

print(trackings[0].keys())

print([t['query_construction_time'] for t in trackings])

data = [trace0, trace1, trace2, trace3]
py.offline.plot({'data': data, 'layout': layout}, filename="inner_times.html", auto_open=True)

data = [trace4,trace6]
py.offline.plot({'data': data, 'layout': layout}, filename="interface_overhead.html", auto_open=True)
