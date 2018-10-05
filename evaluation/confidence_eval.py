import psycopg2
import evaluation_utils as ev
import ivpq_evaluation as ivpqEv
from tracking import Tracker
import plotly.graph_objs as go
import plotly.offline as py
import json
import sys

filename =  sys.argv[1];

f = open(filename, 'r')
config = json.loads(f.read())

con, cur = ev.connect()

number_of_query_samples = config['number_of_query_samples']
number_of_target_samples = config['number_of_target_samples']
num_iters = config['num_iters']

k = config['k']
alpha = config['alpha']
method_flag = config['method']
step_size = config['step_size']

query_template = 'SELECT query, target FROM knn_in_ivpq_batch(''{!s}'', {:d}, ''{!s}'');'

ev.set_vec_table_name(config['vecs_table_name'])

search_params = {'pvf':1, 'alpha':alpha, 'method':method_flag}

ivpqEv.set_search_params(con, cur, search_params)

number_of_vectors = ev.get_vector_dataset_size(cur)

query_samples = ev.get_samples(con, cur, number_of_query_samples, number_of_vectors)
target_samples = ev.get_samples(con, cur, number_of_target_samples, number_of_vectors)

quotes = []
x_axis = []
confidence = 0.0
for i in range(num_iters):
    confidence += step_size
    x_axis.append(confidence)
    cur.execute('SELECT set_confidence_value({:f});'.format(confidence))
    con.commit()
    params = [(ivpqEv.add_escapes([x]), k, ivpqEv.add_escapes(target_samples)) for x in query_samples]
    trackings, _ = ev.create_track_statistics(cur, con, query_template, params)
    quotes.append(len([1 for i in range(len(trackings)) if int(trackings[i]['retrieved'][0][0]) > k*alpha])*100/float(number_of_query_samples))
    print('Iteration', i)
print(quotes)
py.init_notebook_mode(connected=True)

trace = go.Scatter(
    x = x_axis,
    y = quotes,
    mode = 'markers+lines',
    name = 'target_counts',
    marker = {
        'size': 10
    }
)

layout = go.Layout(
    margin = {
        'b': 140,
    },
    height = 750,
    showlegend = False,
    xaxis=dict(
        title=('Confidence'),
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        ),
    yaxis=dict(
        title='Condition Valid in %',
        titlefont=dict(size=30),
        tickfont=dict(size=20),
        ))

data = [trace]
py.offline.plot({'data': data, 'layout': layout}, filename="confidence_eval.html", auto_open=True)
