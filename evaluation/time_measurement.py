#!/bin/python3

import psycopg2
import sys
import random
import time

import plotly
import plotly.graph_objs as go
import numpy as np

STD_USER = 'postgres'
STD_PASSWORD = 'postgres'
STD_HOST = 'localhost'
STD_DB_NAME = 'imdb'

VEC_TABLE_NAME = 'google_vecs_norm'

def get_query_set_full():
    return [('brute-force', 'SELECT v2.word FROM '+ VEC_TABLE_NAME + ' AS v2 ORDER BY cosine_similarity_norm({!s}, v2.vector) DESC FETCH FIRST {:d} ROWS ONLY'),
        ('pq search', 'SELECT word FROM k_nearest_neighbour_pq({!s}, {:d});'),
        ('ivfadc search', 'SELECT word FROM k_nearest_neighbour_ivfadc({!s}, {:d});')]

def get_query_set_test():
    return [
        ('pq_search', 'SELECT * FROM pq_search({!s}, {:d}) AS (id integer, distance float4);'),
        ('ivfadc_search', 'SELECT * FROM ivfadc_search({!s}, {:d}) AS (id integer, distance float4);')]


def get_only_exact_query():
    return [('brute-force', 'SELECT v2.word FROM '+ VEC_TABLE_NAME + ' AS v2 ORDER BY cosine_similarity_norm({!s}, v2.vector) DESC FETCH FIRST {:d} ROWS ONLY')]

def get_query_set_pq_pv(factors):
    return [(('pq search', factor), 'SELECT word FROM k_nearest_neighbour_pq_pv({!s}, {:d}, ' + str(factor) + ');') for factor in factors]

def get_query_set_ivfadc_pv(factors):
    return [(('ivfadc search', factor), 'SELECT word FROM k_nearest_neighbour_ivfadc_pv({!s}, {:d}, ' + str(factor) + ');') for factor in factors]

def serialize_vector(vector):
    result = ''
    for elem in vector:
        result += (str(elem) + ',')
    return '\'{{{!s}}}\'::float4[]'.format(result[:-1])

def get_vector_dataset_size(cur):
    cur.execute('SELECT count(*) FROM {!s};'.format(VEC_TABLE_NAME))
    return cur.fetchone()[0]
def get_samples(con, cur, number, size):
    rnds = ''
    for i in range(number):
        rnds += (str(random.randint(1, size)) + ',')
    rnds = rnds[:-1]
    query = 'SELECT vector from {!s} WHERE id in ({!s})'.format(VEC_TABLE_NAME, rnds)
    cur.execute(query)
    return [x[0] for x in cur.fetchall()]


def measurement(cur, con, query_set, k, samples):
    time_values = {}
    responses = {}
    count = 0
    for (name, query) in query_set:
        time_values[name] = []
        responses[name] = {}
        print('Start Test for', name)
        for i, sample in enumerate(samples):
            vector = serialize_vector(sample)
            rendered_query = query.format(vector, k)
            start = time.time()
            cur.execute(rendered_query)
            result = cur.fetchall()
            end = time.time()
            responses[name][i] = result
            time_values[name].append((end-start))
            count += 1
            print('Iteration', count, 'completed')
    return time_values, responses

def calculate_precision(responses, exact, threshold=5):
    result = dict()
    for name in responses.keys():
        response = responses[name]
        precs = []
        for key in response.keys():
            print(response[key], "vs.", exact[key])
            print('len', len(response[key]))
            print(set(response[key]), set(exact[key]), set.intersection(set(response[key]), set(exact[key])))
            precs.append(len(set.intersection(set(response[key][:threshold]), set(exact[key])))/threshold)
        result[name] = np.mean(precs)
    return result

def plot_bars(measured_data):
    data = []
    for i, key in enumerate(measured_data.keys()):
        trace = go.Scatter(
            name=key,
            y=measured_data[key],
            x=[i]*len(measured_data[key])
        )
    data = [go.Bar(
            x=list(measured_data.keys()),
            y=[np.mean(measured_data[x]) for x in measured_data.keys()]
    )]
    layout = go.Layout(yaxis= dict(title='time in seconds', titlefont=dict(size=20), tickfont=dict(size=20)))
    fig = go.Figure(data=data, layout=layout)
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
    # fig = go.Figure(data=plot_data)
    plotly.offline.plot(fig, filename="tmp.html", auto_open=True)
    return None

def post_verif_measurement(con, cur, k, samples, resolution):
    BASIS = 1000
    factors = [BASIS*n + k for n in range(resolution)]

    _, responses_exact = measurement(cur, con, get_only_exact_query(), k, samples)
    time_values_pq, responses_pq = measurement(cur, con, get_query_set_pq_pv(factors),k,samples)
    precisions_pq = calculate_precision(responses_pq, responses_exact['brute-force'])
    time_values_ivfadc, responses_ivfadc = measurement(cur, con, get_query_set_ivfadc_pv(factors),k,samples)
    precisions_ivfadc = calculate_precision(responses_ivfadc, responses_exact['brute-force'])
    return time_values_pq, precisions_pq, time_values_ivfadc, precisions_ivfadc

def main(argc, argv):
    global VEC_TABLE_NAME
    k = 5
    number = 100
    m_type = ''
    resolution = 10
    time_values = []
    precisions = []
    if argc < 2:
        print('Too few arguments!')
        return
    method = argv[1]
    if argc > 3:
        k = int(argv[2])
        number = int(argv[3])
    if argc > 4:
        resolution = int(argv[4])

    try:
        con = psycopg2.connect("dbname='" + STD_DB_NAME + "' user='" + STD_USER + "' host='" + STD_HOST + "' password='" + STD_PASSWORD + "'")
    except:
        print('Can not connect to database')
        return
    cur = con.cursor()

    data_size = get_vector_dataset_size(cur)
    samples = get_samples(con, cur, number, data_size)

    if method == 'default':
        time_values, responses = measurement(cur, con, get_query_set_full(), k, samples)
        precisions = calculate_precision(responses, responses['brute-force'])
        plot_bars(time_values)
    if method == 'postverification':
        time_values_pq, precisions_pq, time_values_ivfadc, precisions_ivfadc  = post_verif_measurement(con, cur, k, samples, resolution)
        plot_scatter_graph(time_values_pq, precisions_pq, time_values_ivfadc, precisions_ivfadc, number)
        time_values = time_values_pq
        precisions = precisions_pq


    print('Parameters k:', k, 'Number of Queries:', number)
    for test in time_values.keys():
        print('TEST', test, 'TIME_SUM:', sum(time_values[test]), 'TIME_SINGLE:', sum(time_values[test])/number, 'Precision', precisions[test])

if __name__ == "__main__":
	main(len(sys.argv), sys.argv)
