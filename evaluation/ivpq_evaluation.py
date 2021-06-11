from numpy import mean
from numpy import median
from numpy import percentile
import time
import random
import copy
from collections import defaultdict
import plotly.offline as py
import plotly.graph_objs as go

import evaluation_utils as ev

NUM_ITERATIONS = 1

def set_num_iterations(iterations):
    global NUM_ITERATIONS
    NUM_ITERATIONS = iterations

def set_search_params(con, cur, search_params):
    cur.execute('SELECT set_pvf({:d});'.format(search_params['pvf']))
    cur.execute('SELECT set_alpha({:d});'.format(search_params['alpha']))
    cur.execute('SELECT set_method_flag({:d});'.format(search_params['method']))
    con.commit()

add_escapes = lambda x: "'{" + ",".join([s.replace("\\", "\\\\").replace("'", "''").replace("\"", "\\\"").replace("{", "\{").replace("}", "\}").replace(",", "\,") for s in x]) + "}'"

def is_outlier(value, ar):
    if (value > percentile(ar, 20)) and (value < percentile(ar, 80)):
        return False
    else:
        return True

def get_exact_results(cur, con, samples, k, targets):
    set_search_params(con, cur, {'pvf':1,'alpha':1000000, 'method':1}) # sufficient high alpha value
    exact_query2 = 'SELECT query, target, similarity FROM knn_in_ivpq_batch({!s}::varchar[], {:d}, {!s}::varchar[]);'
    exact_results = defaultdict(list)
    sample_string = '\'{'
    query = exact_query2.format(samples, k, targets)
    cur.execute(query)
    query_results = cur.fetchall();
    for entry in query_results:
        exact_results[entry[0]].append(entry[1])
    return exact_results

def calculate_precision(exact_results, approximated_results, k):
    summation = 0
    for key in exact_results.keys():
        summation += len([y for y in exact_results[key] if y in approximated_results[key]]) / k
    return float(summation)/float(len(exact_results.keys()))

def precision_measurement_for_ivpq_batch(con, cur, k, search_parameters, approximated_query, params, param_query, parameter_variables, exact_results):
    results = []
    for search_params in search_parameters:
        approximated_results = list()
        precision_values = []
        set_search_params(con, cur, search_params)
        for j, i in enumerate(parameter_variables):
            approximated_results.append(dict())
            cur.execute(param_query.format(parameter_variables[j]))
            con.commit()
            cur.execute(approximated_query.format(*params))
            for res in cur.fetchall():
                if res[0] in  approximated_results[j]:
                    approximated_results[j][res[0]].append(res[1])
                else:
                    approximated_results[j][res[0]] = [res[1]]
            summation = 0
            for key in exact_results.keys():
                summation += len([y for y in exact_results[key] if y in approximated_results[j][key]]) / k
            precision_values.append(float(summation)/float(len(exact_results.keys())))
            print('Iteration', str(len(results)+1) + '/' + str(len(search_params)), 'Done:', str(j)+'/'+str(len(parameter_variables)), end='\r')
        results.append(precision_values)
        print(results)
    return results

def time_measurement_for_ivpq_batch(con, cur, search_parameters, names, query, k, param_query, parameter_variables, num_queries, num_targets):
    all_execution_times = defaultdict(list)
    all_inner_times = defaultdict(list)
    count = 0
    data_size = ev.get_vector_dataset_size(cur)
    for i, search_params in enumerate(search_parameters):
        set_search_params(con, cur, search_params)
        for elem in parameter_variables:
            # set parameter variable
            cur.execute(param_query.format(elem))
            con.commit()
            times = []
            inner_times = []
            for iteration in range(NUM_ITERATIONS):
                samples = ev.get_samples(con, cur, num_queries, data_size)
                target_samples = ev.get_samples(con, cur, num_targets, data_size)
                params = [(add_escapes(samples), k, add_escapes(target_samples))]
                trackings, execution_times = ev.create_track_statistics(cur, con, query, params, log=False)
                print(param_query.format(elem), search_params, execution_times)
                times.append(execution_times)
                inner_times.append(float(trackings[0]['total_time'][0][0]))
                count+= 1;
                print(str(round((count*100) / (NUM_ITERATIONS*len(parameter_variables)*len(search_parameters)),2))+'%', end='\r')
            all_execution_times[names[i]].append(mean(times))
            all_inner_times[names[i]].append(mean(inner_times))
    return all_execution_times, all_inner_times

def time_and_precision_measurement_for_ivpq_batch(con, cur, search_parameters, names, query, k, param_query, parameter_variables, num_queries, num_targets, small_sample_size, outlier_detect=0):
    USE_MEDIAN = True
    all_execution_times = defaultdict(list)
    all_inner_times = defaultdict(list)
    all_precision_values = defaultdict(list)
    count = 0
    data_size = data_size = ev.get_vector_dataset_size(cur)

    # init phase
    for i, search_params in enumerate(search_parameters):
        all_execution_times[names[i]] = [[] for j in range(len(parameter_variables[i]))]
        all_inner_times[names[i]] = [[] for j in range(len(parameter_variables[i]))]
        all_precision_values[names[i]] = [[] for j in range(len(parameter_variables[i]))]
    # measurement phase
    for iteration in range(NUM_ITERATIONS):
        print('Start Iteration', iteration)
        for i, search_params in enumerate(search_parameters):
            for j, elem in enumerate(parameter_variables[i]):
                # TODO set parameter variable
                cur.execute(param_query.format(elem))
                con.commit()
                times = all_execution_times[names[i]][j]
                inner_times = all_inner_times[names[i]][j]
                precision_values = all_precision_values[names[i]][j]

                # big sample set
                samples = ev.get_samples(con, cur, num_queries, data_size)

                # create smaller sample set (bootstraping)
                small_samples = [samples[random.randint(0,num_queries-1)] for i in range(small_sample_size)]
                target_samples = ev.get_samples(con, cur, num_targets, data_size)
                # calculate exact results
                start_time = time.time()
                exact_results = get_exact_results(cur, con, add_escapes(small_samples), k, add_escapes(target_samples))
                print("--- %s seconds ---" % (time.time() - start_time))
                set_search_params(con, cur, search_params)
                cur.execute(param_query.format(elem))
                con.commit()
                params = [('iv' if names[i] != 'Baseline' else '', add_escapes(samples), k, add_escapes(target_samples))]

                trackings, execution_times = ev.create_track_statistics(cur, con, query, params, log=False)

                times.append(execution_times)
                print(names[i],search_params, elem, "arguments:", len(samples), params[0][2], len(target_samples), float(trackings[0]['total_time'][0][0]))
                inner_times.append(float(trackings[0]['total_time'][0][0]))

                # execute approximated query to obtain results
                cur.execute(query.format(*(params[0])))
                approximated_results = defaultdict(list)
                for res in cur.fetchall():
                    approximated_results[res[0]].append(res[1])
                precision_values.append(calculate_precision(exact_results, approximated_results, k))
                count+= 1;
                print(str(round((count*100) / (NUM_ITERATIONS*sum([len(p) for p in parameter_variables])),2))+'%', end='\r')
    # evaluation phase
    raw_data = {
        'execution_times': copy.deepcopy(all_execution_times),
        'inner_times': copy.deepcopy(all_inner_times),
        'precision_values': copy.deepcopy(all_precision_values)
    }
    for i, search_params in enumerate(search_parameters):
        for j, elem in enumerate(parameter_variables[i]):
            if outlier_detect:
                all_execution_times[names[i]][j] = mean([v for v in all_execution_times[names[i]][j] if not is_outlier(v, all_execution_times[names[i]][j])])
                all_inner_times[names[i]][j] = mean([v for v in all_inner_times[names[i]][j] if not is_outlier(v, all_inner_times[names[i]][j])])
            else:
                if USE_MEDIAN:
                    all_execution_times[names[i]][j] = median(all_execution_times[names[i]][j])
                    all_inner_times[names[i]][j] = median(all_inner_times[names[i]][j])
                else:
                    all_execution_times[names[i]][j] = mean(all_execution_times[names[i]][j])
                    all_inner_times[names[i]][j] = mean(all_inner_times[names[i]][j])
            all_precision_values[names[i]][j] = mean(all_precision_values[names[i]][j])
    return all_execution_times, all_inner_times, all_precision_values, raw_data

def plot_precision_graphs(parameter_variables, precision_values, names):
    data = []
    for i in range(len(names)):
        trace = go.Scatter(
        x = parameter_variables,
        y = precision_values[i],
        mode = 'lines+markers',
        name = names[i]
        )
        data.append(trace)
    py.iplot(data, filename='precision_measurement.html')

def plot_time_precision_graphs(time_values, precision_values, names, make_iplot=True, layout=None, markers=None):
    data = []
    for i in range(len(names)):
        trace = go.Scatter(
        x = time_values[names[i]],
        y = precision_values[names[i]],
        mode = ('lines+markers' if len(time_values[names[i]]) > 1 else 'markers'),
        name = names[i],
        marker = markers[names[i]] if markers else {}
        )
        data.append(trace)
    if make_iplot:
        py.iplot(data, filename='/tmp/precision_measurement.html')
    else:
        py.offline.plot({'data': data, 'layout': layout}, filename="time_precision_eval.html", auto_open=True)
