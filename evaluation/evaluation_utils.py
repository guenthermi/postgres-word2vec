#!/usr/bin/python3

import psycopg2
import sys
import random
import time

import plotly
import plotly.graph_objs as go
import numpy as np
from random import shuffle
from random import sample

from tracking import Tracker

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'
STD_DB_NAME = 'imdb'

STD_VEC_TABLE_NAME = 'google_vecs_norm' # TODO add argument to configure this

vec_table_name = STD_VEC_TABLE_NAME


def set_vec_table_name(table_name):
    global vec_table_name
    vec_table_name = table_name

def get_vec_table_name():
    return vec_table_name

def get_query_set_full():
    return [
        ('brute-force', 'SELECT word FROM k_nearest_neighbour({!s}, {:d});'),
        ('pq search', 'SELECT word FROM k_nearest_neighbour_pq({!s}, {:d});'),
        ('ivfadc search', 'SELECT word FROM k_nearest_neighbour_ivfadc({!s}, {:d});')]

def get_query_set_full_pv(factor):
    return [('brute-force', 'SELECT v2.word FROM '+ vec_table_name + ' AS v2 ORDER BY cosine_similarity({!s}, v2.vector) DESC FETCH FIRST {:d} ROWS ONLY'),
        ('pq search', 'SELECT word FROM k_nearest_neighbour_pq_pv({!s}, {:d}, ' + str(factor) + ');'),
        ('ivfadc search', 'SELECT word FROM k_nearest_neighbour_ivfadc_pv({!s}, {:d}, ' + str(factor) + ');')]

def get_only_exact_query():
    return [('brute-force', 'SELECT v2.word FROM '+ vec_table_name + ' AS v2 ORDER BY cosine_similarity_norm({!s}, v2.vector) DESC FETCH FIRST {:d} ROWS ONLY')]

def get_query_set_pq_pv(factors):
    return [(('pq search', factor), 'SELECT word FROM k_nearest_neighbour_pq_pv({!s}, {:d}, ' + str(factor) + ');') for factor in factors]

def get_query_set_ivfadc_pv(factors):
    return [(('ivfadc search', factor), 'SELECT word FROM k_nearest_neighbour_ivfadc_pv({!s}, {:d}, ' + str(factor) + ');') for factor in factors]

def get_query_set_ivfadc_batch(size_values, dataset_size):
    return [(('ivfadc batch search', size), 'SELECT * FROM ivfadc_batch_search(' + serialize_ids(sample(range(1, dataset_size+1), size)) + ', {:d}) AS (id integer, target integer, squaredistance float4);') for size in size_values]

def get_query_simple_ivfadc_batch():
    return [('ivfadc batch search', 'SELECT * FROM knn_batch(''{!s}'', {:d});')]

def get_query_set_ivfadc_batch_precision(size_values, dataset_size):
    ids = dict()
    for size in size_values:
        ids[size] = sample(range(1, dataset_size+1), size)
    return [(('ivfadc batch search', size), 'SELECT * FROM ivfadc_batch_search(' + serialize_ids(ids[size]) + ', {:d}) AS (id integer, target integer, squaredistance float4);') for size in ids] + [(('exact', size), 'SELECT gv.id, gv2.id FROM '+ vec_table_name + ' as gv, '+ vec_table_name + ' AS gv2, k_nearest_neighbour(gv.vector, {:d}) as n WHERE (gv.id = ANY (' + serialize_ids(ids[size]) + ')) AND (gv2.word = n.word);') for size in ids]


def get_exact_query_topkin(size_values, ids):
    return [(('brute-force', size), 'SELECT word FROM  knn_in({!s}, {:d}, ' + serialize_ids(ids[:size]) + ' );') for size in size_values]

def get_query_set_topkin_pq(size_values, ids):
    return [(('pq search', size), 'SELECT word FROM  knn_in_pq({!s}, {:d}, ' + serialize_ids(ids[:size]) + ' );') for size in size_values]

def serialize_ids(ids):
    result = ''
    for elem in ids:
        result += (str(elem) + ',')
    return '\'{{{{{!s}}}}}\'::int[]'.format(result[:-1])

def serialize_vector(vector):
    result = ''
    for elem in vector:
        result += (str(elem) + ',')
    return '\'{{{!s}}}\'::float4[]'.format(result[:-1])

def create_track_statistics(cur, con, query, params, log=True):
    tracker = Tracker(con)
    trackings = []
    time_values = []
    for i, param_set in enumerate(params):
        tracker.clear_track()
        start = time.time()
        cur.execute(query.format(*param_set))
        _ = cur.fetchone()
        end = time.time()
        time_values.append((end-start))
        trackings.append(tracker.get_tracking())
        if log:
            print(str(round((i*100) / len(params),2))+'%', end='\r')
    return trackings, time_values

def get_vector_dataset_size(cur):
    cur.execute('SELECT count(*) FROM {!s};'.format(vec_table_name))
    return cur.fetchone()[0]
def get_samples(con, cur, number, size):
    rnds = ''
    blacklist = set()
    for i in range(number):
        r = random.randint(1, size)
        while (r in blacklist):
            r = random.randint(1, size)
        blacklist.add(r)
        rnds += (str(r) + ',')
    rnds = rnds[:-1]
    query = 'SELECT word from {!s} WHERE id in ({!s})'.format(vec_table_name, rnds)
    cur.execute(query)
    results = [x[0] for x in cur.fetchall()]
    return results

def measurement(cur, con, query_set, k, samples):
    time_values = {}
    responses = {}
    count = 0
    for (name, query) in query_set:
        time_values[name] = []
        responses[name] = {}
        print('Start Test for', name)
        for i, sample in enumerate(samples):
            rendered_query = query.format("'" + samples[i].replace("'", "''") + "'", k)
            start = time.time()
            cur.execute(rendered_query)
            result = cur.fetchall()
            end = time.time()
            responses[name][i] = result
            time_values[name].append((end-start))
            count += 1
            print('Iteration', count, 'completed')
    return time_values, responses

def batch_measurement_simple(cur, con, query_set, k, samples):
    time_values = {}
    responses = {}
    count = 0
    for (name, query) in query_set:
        time_values[name] = []
        responses[name] = {}
        sample_array = "'{" + ','.join([samples[i].replace("'", "''") for i in range(len(samples))]) + "}'"
        # print('sample array:',sample_array)
        rendered_query = query.format(sample_array, k)
        start = time.time()
        cur.execute(rendered_query)
        result = cur.fetchall()
        end = time.time()
        time_values[name].append((end-start)/len(samples))
        for i, key in enumerate(samples):
            responses[name][i] = [(t,) for (q, t, dist) in result if q == key]

    return time_values, responses

def batch_measurement_simple_targets(cur, con, query_set, k, samples, targets):
    time_values = {}
    responses = {}
    count = 0
    for (name, query) in query_set:
        time_values[name] = []
        responses[name] = {}
        sample_array = "'{" + ','.join([samples[i].replace("'", "''") for i in range(len(samples))]) + "}'"
        target_array = "'{" + ','.join([targets[i].replace("'", "''") for i in range(len(targets))]) + "}'"
        rendered_query = query.format(sample_array, k, target_array)
        start = time.time()
        cur.execute(rendered_query)
        result = cur.fetchall()
        end = time.time()
        time_values[name].append((end-start)/len(samples))
        for i, key in enumerate(samples):
            responses[name][i] = [(t,) for (q, t, dist) in result if q == key]

    return time_values, responses



def measurement_simple(cur, con, size_values, k, number, dataset_size):
    time_values = {}
    count = 0
    for i in range(number):
        for (name, query) in get_query_set_ivfadc_batch(size_values, dataset_size):
            if not name in time_values:
                time_values[name] = []
            rendered_query = query.format(k)
            start = time.time()
            cur.execute(rendered_query)
            result = cur.fetchall()
            end = time.time()
            time_values[name].append((end-start))
            count += 1
            print('Iteration', count, 'completed')
    return time_values

def measurement_batch_precision(cur, con, size_values, k, number, dataset_size):
    time_values = {}
    precisions = {}
    count = 0
    for i in range(number):
        exact_results = {}
        ivfadc_results = {}
        for (name, query) in get_query_set_ivfadc_batch_precision(size_values, dataset_size):
            if name[0] == 'exact':
                rendered_query = query.format(k)
                print('perform exact query')
                cur.execute(rendered_query)
                exact_results[name[1]] = cur.fetchall()
                print('finished exact query')
            else:
                if not name in time_values:
                    time_values[name] = []
                rendered_query = query.format(k)
                start = time.time()
                cur.execute(rendered_query)
                result = cur.fetchall()
                end = time.time()
                time_values[name].append((end-start))
                count += 1
                ivfadc_results[name[1]] = result
                print('Iteration', count, 'completed')
        for key in ivfadc_results:
            ivfadc = set([(elem[0], elem[1]) for elem in ivfadc_results[key]])
            exact = set(exact_results[key])
            precisions[('ivfadc batch search', key)] = (len(ivfadc.intersection(exact)) / len(ivfadc))
            # print(len(set([(ivfadc_results[key][0], elem[key][1] for elem in ])))
    return time_values, precisions

def calculate_precision(responses, exact, threshold=5):
    result = dict()
    for name in responses.keys():
        response = responses[name]
        precs = []
        for key in response.keys():
            precs.append(len(set.intersection(set(response[key][:threshold]), set(exact[key])))/threshold)
        result[name] = np.mean(precs)
    return result

def plot_bars(measured_data, iplot=False, layout=None):
    data = []
    data = [go.Bar(
            x=list(measured_data.keys()),
            y=[np.mean(measured_data[x]) for x in measured_data.keys()],
            text=[np.mean(measured_data[x]) for x in measured_data.keys()],
            textposition = 'outside',
            textfont=dict(family='Arial', size=20),
    )]
    if layout == None:
        layout = go.Layout(yaxis= dict(title='time in seconds', titlefont=dict(size=30), tickfont=dict(size=30)), xaxis=dict(titlefont=dict(size=30), tickfont=dict(size=30)))
    fig = go.Figure(data=data, layout=layout)
    if iplot:
        plotly.offline.iplot(fig, filename="tmp.html")
    else:
        plotly.offline.plot(fig, filename="tmp.html", auto_open=True)
    return None

def plot_scatter_graph(time_data_pq, precision_data_pq, time_data_ivfadc, precision_data_ivfadc, number):
    plot_data = []
    keys_pq = sorted(time_data_pq.keys(), key=lambda x: np.mean(time_data_pq[x]))
    keys_ivfadc = sorted(time_data_ivfadc.keys(), key=lambda x: np.mean(time_data_ivfadc[x]))
    sc_pq = go.Scatter(
        x=[np.mean(time_data_pq[key]) for key in keys_pq],
        y=[precision_data_pq[key] for key in keys_pq],
        mode = 'lines+markers',
        name='Product Quantization'

    )
    plot_data.append(sc_pq)
    sc_ivfadc = go.Scatter(
        x=[np.mean(time_data_ivfadc[key]) for key in keys_ivfadc],
        y=[precision_data_ivfadc[key] for key in keys_ivfadc],
        mode = 'lines+markers',
        name='IVFADC'

    )
    plot_data.append(sc_ivfadc)

    layout = go.Layout(xaxis= dict(title='Time in Seconds', titlefont=dict(size=20), tickfont=dict(size=20)), yaxis=dict(title='Precision',tickfont=dict(size=20)), )
    fig = go.Figure(data=plot_data, layout=layout)
    plotly.offline.plot(fig, filename="tmp.html", auto_open=True)
    return None

def plot_scatter_graphs_size_dep(time_data_exact, time_data_pq, precision_data_pq):
    plot_data_time = []
    plot_data_precision = []
    keys_exact = sorted(time_data_exact.keys(), key=lambda x: x[1])
    keys_pq = sorted(time_data_pq.keys(), key=lambda x: x[1])
    sc_pq_time = go.Scatter(
        x=[key[1] for key in keys_pq],
        y=[np.mean(time_data_pq[key]) for key in keys_pq],
        mode = 'lines+markers',
        name='Product Quantization'

    )
    plot_data_time.append(sc_pq_time)
    sc_exact_time = go.Scatter(
        x=[key[1] for key in keys_pq],
        y=[np.mean(time_data_exact[key]) for key in keys_exact],
        mode = 'lines+markers',
        name='Exact Search'

    )
    plot_data_time.append(sc_exact_time)
    sc_precision = go.Scatter(
        x=[key[1] for key in keys_pq],
        y=[precision_data_pq[key][key] for key in keys_pq],
        mode = 'lines+markers',
        name='Product Quantization'

    )
    plot_data_precision.append(sc_precision)

    layout = go.Layout(xaxis= dict(title='size of output set', titlefont=dict(size=20), tickfont=dict(size=20)), yaxis=dict(title='time in seconds',tickfont=dict(size=20)), )
    fig = go.Figure(data=plot_data_time, layout=layout)
    plotly.offline.plot(fig, filename="tmp_size_dep_time.html", auto_open=True)

    layout = go.Layout(xaxis= dict(title='size of ouput set', titlefont=dict(size=20), tickfont=dict(size=20)), yaxis=dict(title='precision',tickfont=dict(size=20)), )
    fig = go.Figure(data=plot_data_precision, layout=layout)
    plotly.offline.plot(fig, filename="tmp_size_dep_prec.html", auto_open=True)
    return None

def plot_scatter_graph_batch(time_data):
    keys = sorted(time_data.keys(), key=lambda x: x[1])
    sc_time = go.Scatter(
        x=[key[1] for key in keys],
        y=[np.mean(time_data[key]) for key in keys],
        mode = 'lines+markers',
        name='Product Quantization'
    )
    sc_quotient = go.Scatter(
        x=[key[1] for key in keys],
        y=[np.mean(time_data[key]) / int(key[1]) for key in keys],
        mode = 'lines+markers',
        name='Product Quantization'
    )

    layout = go.Layout(xaxis= dict(title='size of batch', titlefont=dict(size=20), tickfont=dict(size=20)), yaxis=dict(title='time in seconds',tickfont=dict(size=20)), )
    fig_time = go.Figure(data=[sc_time], layout=layout)
    plotly.offline.plot(fig_time, filename="tmp_batch_time_absolut.html", auto_open=True)

    fig_quotient = go.Figure(data=[sc_quotient], layout=layout)
    plotly.offline.plot(fig_quotient, filename="tmp_batch_time_relative.html", auto_open=True)



def post_verif_measurement(con, cur, k, samples, resolution, basis):
    factors = [basis*n + k for n in range(resolution)]

    _, responses_exact = measurement(cur, con, get_only_exact_query(), k, samples)
    time_values_pq, responses_pq = measurement(cur, con, get_query_set_pq_pv(factors),k,samples)
    precisions_pq = calculate_precision(responses_pq, responses_exact['brute-force'])
    time_values_ivfadc, responses_ivfadc = measurement(cur, con, get_query_set_ivfadc_pv(factors),k,samples)
    precisions_ivfadc = calculate_precision(responses_ivfadc, responses_exact['brute-force'])
    return time_values_pq, precisions_pq, time_values_ivfadc, precisions_ivfadc

def size_dependend_measurement(con, cur, k, samples, resolution, basis, dataset_size):
    size_values = [basis*n + k for n in range(resolution)]
    ids = list(range(1, dataset_size))
    random.seed()
    shuffle(ids)
    time_values_exact, responses_exact = measurement(cur, con, get_exact_query_topkin(size_values, ids), k, samples)
    time_values_pq, responses_pq = measurement(cur, con, get_query_set_topkin_pq(size_values, ids),k,samples)
    precisions = dict()
    for key in responses_pq.keys():
        precision = calculate_precision({key: responses_pq[key]}, responses_exact[('brute-force', key[1])])
        precisions[key] = precision
    return time_values_pq, time_values_exact, precisions

def batch_measurement(con, cur, k, resolution, basis, dataset_size, number):
    size_values = [basis*n + 1 for n in range(resolution)]
    time_values = measurement_simple(cur, con, size_values, k, number, dataset_size)
    return time_values

def batch_measurement_precision(con, cur, k, resolution, basis, dataset_size, number):
    size_values = [basis*n + 1 for n in range(resolution)]
    time_values = measurement_batch_precision(cur, con, size_values, k, number, dataset_size)
    return time_values

def connect(db_name=STD_DB_NAME,user=STD_USER,password=STD_PASSWORD,host=STD_HOST):
    con = None
    try:
        con = psycopg2.connect("dbname='" + db_name + "' user='" + user + "' host='" + host + "' password='" + password + "'")
    except:
        print('Can not connect to database')
        return
    cur = con.cursor()
    return con, cur

def main(argc, argv):
    global vec_table_name
    k = 5
    number = 100
    m_type = ''
    resolution = 10
    basis = 100
    time_values = dict()
    precisions = dict()
    samples = None
    HELP_TEXT = '\033[1mtime_measurement.py\033[0m method table_name [k] [sample_size] [resolution] [basis]'
    if argc < 2:
        print('Too few arguments!')
        print(HELP_TEXT)
        return
    method = argv[1]
    vec_table_name = argv[2]
    if argc > 4:
        k = int(argv[3])
        number = int(argv[4])
    if argc > 5:
        resolution = int(argv[5])
    if argc > 6:
        basis = int(argv[6])

    con, cur = connect()

    data_size = get_vector_dataset_size(cur)
    if method != 'batch':
        samples = get_samples(con, cur, number, data_size)

    if method == 'default':
        time_values, responses = measurement(cur, con, get_query_set_full(), k, samples)
        precisions = calculate_precision(responses, responses['brute-force'])
        plot_bars(time_values)

    if method == 'defaultpv':
        time_values, responses = measurement(cur, con, get_query_set_full_pv(basis), k, samples)
        precisions = calculate_precision(responses, responses['brute-force'])
        plot_bars(time_values)

    if method == 'sizedependend':
        time_values_pq, time_values_exact, precisions = size_dependend_measurement(con, cur, k, samples, resolution, basis, data_size)
        plot_scatter_graphs_size_dep(time_values_exact, time_values_pq, precisions)
        time_values = time_values_pq

    if method == 'postverification':
        time_values_pq, precisions_pq, time_values_ivfadc, precisions_ivfadc  = post_verif_measurement(con, cur, k, samples, resolution, basis)
        plot_scatter_graph(time_values_pq, precisions_pq, time_values_ivfadc, precisions_ivfadc, number)
        time_values = time_values_pq
        precisions = precisions_pq

    if method == 'batch':
        time_values = batch_measurement(con, cur, k, resolution, basis, data_size, number)
        plot_scatter_graph_batch(time_values)
        for key in time_values.keys():
            precisions[key] = None
    if method == 'batch-precision':
        time_values, precisions = batch_measurement_precision(con, cur, k, resolution, basis, data_size, number)


    print('Parameters k:', k, 'Number of Queries:', number)
    for test in time_values.keys():
        print('TEST', test, 'TIME_SUM:', sum(time_values[test]), 'TIME_SINGLE:', sum(time_values[test])/number, 'Precision', precisions[test])

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
