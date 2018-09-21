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

num_iters = config['num_iters'] # 1

k = config['k'] # 5
alpha = config['alpha'] # 10
pvf = config['pvf'] # 10
method = config['method'] # 0 # 0: PQ / 1: EXACT / 2: PQ+PostVerification
use_target_list = config['use_target_list'] # 'true'

steps = config['steps']
step_size = config['step_size']

con, cur = ev.connect()

ev.set_vec_table_name(config['vecs_table_name'])

data_size = ev.get_vector_dataset_size(cur)
print(data_size)


# set parameters
cur.execute('SELECT set_pvf({:d});'.format(pvf))
cur.execute('SELECT set_alpha({:d});'.format(alpha))
cur.execute('SELECT set_method_flag({:d});'.format(method))
cur.execute("SELECT set_use_targetlist('{!s}');".format(use_target_list))
con.commit()

dynamic_sizes = list([i*step_size for i in range(1,steps+1)]) # target size values

query = 'SELECT * FROM knn_in_iv_batch(''{!s}'', {:d}, ''{!s}'', \'ivpq_search_in\') LIMIT 1;'
convert = lambda x: "'{" + ",".join([s.replace("'", "''").replace("\"", "\\\"").replace("{", "\}").replace("{", "\}").replace(",", "\,") for s in x]) + "}'"

get_samples = lambda x: ev.get_samples(con, cur, x, data_size)

init_trackings = lambda: {
    'total_time': [list() for i in dynamic_sizes],
    'precomputation_time': [list() for i in dynamic_sizes],
    'computation_time': [list() for i in dynamic_sizes]}

trackings_short_codes = init_trackings()
trackings_long_codes = init_trackings()

set_short_codes = 'SELECT set_long_codes_threshold(0)'
set_long_codes = 'SELECT set_long_codes_threshold(10000000)' # set threshold to a sufficient high value
for (result, param_query) in [(trackings_short_codes, set_short_codes), (trackings_long_codes, set_long_codes)]:
    cur.execute(param_query)
    con.commit()
    for i, size_value in enumerate(dynamic_sizes):
        for count, iteration in enumerate(range(num_iters)):
            print('Iteration:', count)
            cur.execute('SELECT set_alpha({:d});'.format(int((size_value)/(2*k))))
            con.commit()
            cur.execute('SELECT get_alpha()')
            print(cur.fetchall())
            params = [(convert(get_samples(number_of_query_samples)), k, convert(get_samples(size_value)))]
            trackings, execution_times = ev.create_track_statistics(cur, con, query, params)
            result['total_time'][i].append(trackings[0]['total_time'][0][0])
            result['precomputation_time'][i].append(trackings[0]['precomputation_time'][0][0])
            result['computation_time'][i].append(trackings[0]['computation_time'][0][0])
print(trackings_long_codes)
print(trackings_short_codes)
calculate_time = lambda x: [mean([float(elem) for elem in ar]) for ar in x]
short_codes_times = dict()
long_codes_times = dict()
for key in trackings_short_codes:
    short_codes_times[key] = calculate_time(trackings_short_codes[key])
    long_codes_times[key] = calculate_time(trackings_long_codes[key])

trace_total0 = go.Scatter(
    x = dynamic_sizes,
    y = short_codes_times['total_time'],
    mode = 'lines+markers',
    name = 'Short Codes',
    marker = {
        'color':'rgba(0, 0, 200, 1)',
        'symbol': 'circle-open',
        'size': 10
    }
)
trace_total1 = go.Scatter(
    x = dynamic_sizes,
    y = long_codes_times['total_time'],
    mode = 'lines+markers',
    name = 'Long Codes',
    marker = {
        'color':'rgba(200, 0, 0, 1)',
        'symbol': 'circle-open',
        'size': 10
    }
)

trace_parts0 = go.Scatter(
    x = dynamic_sizes,
    y = short_codes_times['precomputation_time'],
    mode = 'lines+markers',
    name = 'Short Codes Precomputation',
    marker = {
        'color':'rgba(0, 0, 200, 1)',
        'symbol': 'circle-open',
        'size': 10
    }
)
trace_parts1 = go.Scatter(
    x = dynamic_sizes,
    y = short_codes_times['computation_time'],
    mode = 'lines+markers',
    name = 'Short Codes Distance Computation',
    marker = {
        'color':'rgba(0, 0, 200, 1)',
        'symbol':'square-open',
        'size': 10
    }
)
trace_parts2 = go.Scatter(
    x = dynamic_sizes,
    y = long_codes_times['precomputation_time'],
    mode = 'lines+markers',
    name = 'Long Codes Precomputation',
    marker = {
        'color':'rgba(200, 0, 0, 1)',
        'symbol':'circle-open',
        'size': 10
    }
)
trace_parts3 = go.Scatter(
    x = dynamic_sizes,
    y = long_codes_times['computation_time'],
    mode = 'lines+markers',
    name = 'Long Codes Distance Computation',
    marker = {
        'color':'rgba(200, 0, 0, 1)',
        'symbol':'square-open',
        'size': 10
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
        title='Target Set Size',
        titlefont=dict(size=30),
        tickfont=dict(size=20)
        ),
    yaxis=dict(
        title='Time in Seconds',
        titlefont=dict(size=30),
        tickfont=dict(size=20)
        ))

data = [trace_total0,trace_total1]
py.offline.plot({'data': data, 'layout': layout}, filename="flexible_pq_eval_total_time.html", auto_open=True)

data = [trace_parts0,trace_parts1,trace_parts2,trace_parts3]
py.offline.plot({'data': data, 'layout': layout}, filename="flexible_pq_eval_parts.html", auto_open=True)
