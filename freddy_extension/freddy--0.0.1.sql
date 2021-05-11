-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION freddy" to load this file. \quit

-- TODO line breaks
CREATE OR REPLACE FUNCTION init(original regclass, normalized regclass, pq_quantization regclass, codebook regclass, residual_quantization regclass, coarse_quantization regclass, residual_codebook regclass, ivpq_quantization regclass, ivpq_codebook regclass, coarse_quantization_multi regclass) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_original() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', original);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', normalized);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_pq_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', pq_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_codebook() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', codebook);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_residual_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', residual_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_coarse_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', coarse_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_coarse_quantization_multi() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', coarse_quantization_multi);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_residual_codebook() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', residual_codebook);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_ivpq_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', ivpq_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_ivpq_codebook() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', ivpq_codebook);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_pvf(f integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_pvf() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', f);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_w(w integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_w() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', w);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_alpha(alpha integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_alpha() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', alpha);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_method_flag(flag integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_method_flag() RETURNS integer AS ''SELECT %s'' LANGUAGE sql IMMUTABLE', flag);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_use_targetlist(flag boolean) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_use_targetlist() RETURNS boolean AS ''SELECT ''''%s''''::boolean'' LANGUAGE sql IMMUTABLE', flag);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION  set_confidence_value(confidence float4) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_confidence_value() RETURNS float4 AS ''SELECT ''''%s''''::float4'' LANGUAGE sql IMMUTABLE', confidence);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION  set_long_codes_threshold(threshold integer) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_long_codes_threshold() RETURNS integer AS ''SELECT ''''%s''''::int'' LANGUAGE sql IMMUTABLE', threshold);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_statistics_table(table_name regclass) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_statistics_table() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', table_name);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION set_knn_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_knn_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_knn_in_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_knn_in_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_knn_batch_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_knn_batch_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_knn_join_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_knn_join_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_analogy_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_analogy_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_analogy_in_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_analogy_in_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_groups_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_groups_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION set_cluster_function(name varchar) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_cluster_function_name() RETURNS varchar AS ''SELECT varchar ''''%s'''''' LANGUAGE sql IMMUTABLE', name);
END
$$
LANGUAGE plpgsql;

DO $$
DECLARE
init_done int;
number_tables int;
BEGIN
EXECUTE 'SELECT count(proname) FROM pg_proc WHERE proname=''get_vecs_name''' INTO init_done;
EXECUTE 'SELECT count(*) FROM information_schema.tables WHERE table_schema=''public'' AND table_type=''BASE TABLE'' AND table_name in (''google_vecs'', ''google_vecs_norm'', ''pq_quantization'', ''pq_codebook'', ''fine_quantization'', ''coarse_quantization'', ''residual_codebook'', ''fine_quantization_ivpq'', ''codebook_ivpq'', ''coarse_quantization_ivpq'')' INTO number_tables;
RAISE INFO '% out of 10 default vector and index tables are availabe.', number_tables;
IF init_done = 0 AND number_tables = 10 THEN
  EXECUTE 'SELECT init(''google_vecs'', ''google_vecs_norm'', ''pq_quantization'', ''pq_codebook'', ''fine_quantization'', ''coarse_quantization'', ''residual_codebook'', ''fine_quantization_ivpq'', ''codebook_ivpq'', ''coarse_quantization_ivpq'')';
ELSE
  RAISE INFO 'Execute the init function to declear vector and index tables (use ''-'' for missing tables)';
  EXECUTE 'SELECT init(''-'', ''-'', ''-'', ''-'', ''-'', ''-'', ''-'', ''-'', ''-'', ''-'')';
END IF;
END$$;

CREATE OR REPLACE FUNCTION create_statistics(table_name varchar, column_name varchar, coarse_table_name varchar)  RETURNS void AS $$
DECLARE
ivpq_quantization varchar;
number_of_coarse_ids int;
stats_table_name varchar;
vec_table_name varchar;
total_amount float;
BEGIN
stats_table_name = format('stat_%s_%s', table_name, column_name);
EXECUTE 'SELECT get_vecs_name_ivpq_quantization()' INTO ivpq_quantization;
EXECUTE 'SELECT get_vecs_name()' INTO vec_table_name;
EXECUTE format('DROP TABLE IF EXISTS %s', stats_table_name);
EXECUTE format('CREATE TABLE %s (coarse_id int, coarse_freq float4)', stats_table_name);
EXECUTE format('SELECT count(*) FROM %s_counts', coarse_table_name) INTO number_of_coarse_ids;
EXECUTE format('SELECT count(*) FROM %s AS t INNER JOIN %s AS v ON t.%s = v.word', table_name, vec_table_name, column_name) INTO total_amount;
FOR I IN 0 .. (number_of_coarse_ids-1) LOOP
  EXECUTE format('INSERT INTO %s (coarse_id, coarse_freq) VALUES (%s, (SELECT count(*) FROM %s AS iv INNER JOIN %s AS vtm ON iv.id = vtm.id INNER JOIN %s AS tn ON tn.%s = vtm.word WHERE coarse_id = %s)::float / %s)', stats_table_name,I, ivpq_quantization, vec_table_name, table_name, column_name, I, total_amount);
END LOOP;
  EXECUTE format('INSERT INTO %s (coarse_id, coarse_freq) VALUES (%s, (SELECT count(*) FROM %s AS tn INNER JOIN %s AS vtn ON tn.%s = vtn.word))', stats_table_name, number_of_coarse_ids, table_name, vec_table_name, column_name);
END
$$
LANGUAGE plpgsql;

DO $$
DECLARE
stat_table_present int;
vec_table_name varchar;
ivpq_quantization varchar;
BEGIN
EXECUTE 'SELECT count(*) FROM information_schema.tables WHERE table_schema=''public'' AND table_type=''BASE TABLE'' AND table_name in (''stat_google_vecs_norm_word'')' INTO stat_table_present;
EXECUTE 'SELECT get_vecs_name()' INTO vec_table_name;
EXECUTE 'SELECT get_vecs_name_ivpq_quantization()' INTO ivpq_quantization;
IF stat_table_present = 0 AND ivpq_quantization != '-' AND vec_table_name != '-' THEN
  EXECUTE 'SELECT create_statistics(''google_vecs_norm'', ''word'', ''coarse_quantization_ivpq'')';
  EXECUTE 'SELECT set_statistics_table(''stat_google_vecs_norm_word'')';
END IF;
END $$;

SELECT set_pvf(20);
SELECT set_w(3);
SELECT set_alpha(3);
SELECT set_confidence_value(0.8);
SELECT set_long_codes_threshold(10000000);
SELECT set_method_flag(0);
SELECT set_use_targetlist('true');
SELECT set_knn_function('k_nearest_neighbour');
SELECT set_knn_in_function('knn_in_exact');
SELECT set_knn_batch_function('k_nearest_neighbour_ivfadc_batch');
SELECT set_analogy_function('analogy_3cosadd');
SELECT set_analogy_in_function('analogy_3cosadd_in');
SELECT set_groups_function('grouping_func');
SELECT set_knn_join_function('knn_search_in_batch');
SELECT set_cluster_function('cluster_exact');

CREATE OR REPLACE FUNCTION knn(query varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
function_name varchar;
BEGIN
EXECUTE 'SELECT get_knn_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', %s)
', function_name,  replace(query, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in(query varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
function_name varchar;
formated varchar[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_knn_in_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', %s, ''%s''::varchar(100)[])
', function_name,  replace(query, '''', ''''''), k, formated);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_batch(query_set varchar(100)[], k integer) RETURNS TABLE (query varchar(100), target varchar(100), similarity float4) AS $$
DECLARE
function_name varchar;
formated varchar[];
BEGIN
FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  formated[I] = replace(query_set[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_knn_batch_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', %s)
', function_name, formated, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_join(query_set varchar(100)[], k integer, target_set varchar(100)[]) RETURNS TABLE (query varchar(100), target varchar(100), similarity float4) AS $$
DECLARE
function_name varchar;
formated varchar[];
formated_targets varchar[];
BEGIN
FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  formated[I] = replace(query_set[I], '''', '''''');
END LOOP;
FOR I IN array_lower(target_set, 1)..array_upper(target_set, 1) LOOP
  formated_targets[I] = replace(target_set[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_knn_join_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s''::varchar(100)[], %s,  ''%s''::varchar(100)[])
', function_name, formated, k, formated_targets);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION analogy(a varchar(100),b varchar(100),c varchar(100), OUT result varchar(100)) AS $$
DECLARE
function_name varchar;
BEGIN
EXECUTE 'SELECT get_analogy_function_name()' INTO function_name;
EXECUTE format('
SELECT * FROM %s(''%s'', ''%s'',''%s'')
', function_name, replace(a, '''', ''''''), replace(b, '''', ''''''), replace(c, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_in(w1 varchar(100), w2 varchar(100), w3 varchar(100), input_set varchar(100)[], OUT result varchar(100)) AS $$
DECLARE
function_name varchar;
formated varchar[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
-- replace(token, '''', '''''')
EXECUTE 'SELECT get_analogy_in_function_name()' INTO function_name;
EXECUTE format('
SELECT * FROM %s(''%s'', ''%s'',''%s'', ''%s''::varchar(100)[])
', function_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', ''''''), formated) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION groups(tokens varchar(100)[], groups varchar(100)[]) RETURNS TABLE (token varchar(100), grouptoken varchar(100)) AS $$
DECLARE
function_name varchar;
formated_tokens varchar[];
formated_groups varchar[];
BEGIN
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated_tokens[I] = replace(tokens[I], '''', '''''');
END LOOP;
FOR I IN array_lower(groups, 1)..array_upper(groups, 1) LOOP
  formated_groups[I] = replace(groups[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_groups_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s''::varchar(100)[], ''%s''::varchar(100)[])
', function_name, formated_tokens, formated_groups);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster(tokens varchar(100)[], k integer) RETURNS  TABLE (word varchar(100), cluster integer) AS $$
DECLARE
function_name varchar;
formated_tokens varchar[];
BEGIN
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated_tokens[I] = replace(tokens[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_cluster_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', %s)
', function_name, formated_tokens, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cosine_similarity(float4[], float4[]) RETURNS float8
AS '$libdir/freddy', 'cosine_similarity'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cosine_similarity_norm(float4[], float4[]) RETURNS float8
AS '$libdir/freddy', 'cosine_similarity_norm'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cosine_similarity_bytea(bytea, bytea) RETURNS float4
AS '$libdir/freddy', 'cosine_similarity_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_minus(float[], float[]) RETURNS float[]
AS '$libdir/freddy', 'vec_minus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_plus(float[], float[]) RETURNS float[]
AS '$libdir/freddy', 'vec_plus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_normalize(float[]) RETURNS float[]
AS '$libdir/freddy', 'vec_normalize'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_minus_bytea(bytea, bytea) RETURNS bytea
AS '$libdir/freddy', 'vec_minus_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_plus_bytea(bytea, bytea) RETURNS bytea
AS '$libdir/freddy', 'vec_plus_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_normalize_bytea(bytea) RETURNS bytea
AS '$libdir/freddy', 'vec_normalize_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search(bytea, integer) RETURNS SETOF record
AS '$libdir/freddy', 'pq_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION ivfadc_search(bytea, integer) RETURNS SETOF record
AS '$libdir/freddy', 'ivfadc_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search_in(bytea, integer, integer[]) RETURNS SETOF record
AS '$libdir/freddy', 'pq_search_in'
LANGUAGE C IMMUTABLE STRICT;

  CREATE OR REPLACE FUNCTION ivpq_search_in(bytea[], integer[], integer, integer[], integer, integer, integer, boolean, float4, integer) RETURNS SETOF record
AS '$libdir/freddy', 'ivpq_search_in'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search_in_batch(bytea[], integer[], integer, integer[], boolean) RETURNS SETOF record
AS '$libdir/freddy', 'pq_search_in_batch'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION ivfadc_batch_search(integer[], integer) RETURNS SETOF record
AS '$libdir/freddy', 'ivfadc_batch_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION grouping_pq(integer[], integer[]) RETURNS SETOF record
AS '$libdir/freddy', 'grouping_pq'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION insert_batch(varchar(100)[]) RETURNS integer
AS '$libdir/freddy', 'insert_batch'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION centroid(anyarray) RETURNS anyarray
AS '$libdir/freddy', 'centroid'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION centroid_bytea(bytea[]) RETURNS bytea
AS '$libdir/freddy', 'centroid_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION read_bytea(bytea) RETURNS integer[]
AS '$libdir/freddy', 'read_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION read_bytea_int16(bytea) RETURNS smallint[]
AS '$libdir/freddy', 'read_bytea_int16'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION read_bytea_float(bytea) RETURNS float4[]
AS '$libdir/freddy', 'read_bytea_float'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_to_bytea(anyarray) RETURNS bytea
AS '$libdir/freddy', 'vec_to_bytea'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION k_nearest_neighbour(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(''%s'', v2.vector)
FROM %s AS v2
ORDER BY cosine_similarity_bytea(''%s'', v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, table_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour(token varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
ORDER BY cosine_similarity_bytea(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, replace(token, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_search_in_batch(query_set varchar(100)[], k integer, input_set varchar[]) RETURNS TABLE (query varchar, target varchar, similarity float4) AS $$
DECLARE
formated varchar[];
formated_query varchar;
rec RECORD;
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  formated_query = replace(query_set[I], '''', '''''');
  FOR rec IN EXECUTE format('SELECT word, similarity FROM knn_in_exact(''%s''::varchar, ''%s''::integer, ''%s''::varchar[])', formated_query, k, formated) LOOP
    query := query_set[I];
    target := rec.word;
    similarity := rec.similarity;
    RETURN NEXT;
  END LOOP;
END LOOP;
RETURN;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_search_in_batch(query_set bytea[], k integer, input_set varchar[]) RETURNS TABLE (query integer, target varchar, similarity float4) AS $$
DECLARE
formated varchar[];
formated_query varchar;
rec RECORD;
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  FOR rec IN EXECUTE format('SELECT word, similarity FROM knn_in_exact(''%s''::bytea, ''%s''::integer, ''%s''::varchar[])', query_set[I], k, formated) LOOP
    query := I;
    target := rec.word;
    similarity := rec.similarity;
    RETURN NEXT;
  END LOOP;
END LOOP;
RETURN;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(token varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
fine_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, (1.0 - (distance / 2.0))::float4
FROM %s AS gv, ivfadc_search(gv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
WHERE gv.word = ''%s''
', table_name, k, fine_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
fine_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, (1.0 - (distance / 2.0))::float4
FROM ivfadc_search(''%s'', %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
', input_vector, k, fine_quantization_name);
END
$$
LANGUAGE plpgsql;

-- ivfadc_batch_search(
CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_batch(input_set varchar(100)[], k integer) RETURNS TABLE (query varchar(100), target varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
fine_quantization_name varchar;
formated varchar[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, fq2.word, (1.0 - (distance / 2.0))::float4
FROM ivfadc_batch_search(ARRAY(SELECT id FROM %s WHERE word = ANY (''%s'')), %s) AS (idx integer, idxx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
INNER JOIN %s AS fq2 ON idxx = fq2.id
', table_name, formated, k, fine_quantization_name, fine_quantization_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_pv(token varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(v1.vector, v2.vector)
FROM %s AS v1, ivfadc_search(v1.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS v2 ON idx = v2.id
WHERE v1.word = ''%s''
ORDER BY cosine_similarity_bytea(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, post_verif*k, table_name, replace(token, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_pv(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT v.word, cosine_similarity_bytea(''%s''::bytea, v.vector)
FROM ivfadc_search(''%s''::bytea, %s) AS (idx integer, distance float4)
INNER JOIN %s AS v ON idx = v.id
ORDER BY cosine_similarity_bytea(''%s''::bytea, v.vector) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, input_vector, post_verif*k, table_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(token varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, (1.0 - (distance / 2.0))::float4 AS similarity
FROM %s AS gv, pq_search(gv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
WHERE gv.word = ''%s''
', table_name, k, pq_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, (1.0 - (distance / 2.0))::float4 AS similarity
FROM pq_search(''%s'', %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
', input_vector, k, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

-- with postverification
CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq_pv(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
pq_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, cosine_similarity_bytea(''%s'', pqs.word) AS similarity
FROM pq_search(''%s'', %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
ORDER BY cosine_similarity_bytea(''%s'', pqs.word) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, input_vector, post_verif*k, pq_quantization_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq_pv(token varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word,  cosine_similarity_bytea(wv.vector, pqs.word) AS similarity
FROM %s as wv, pq_search(wv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
WHERE wv.word = ''%s''
ORDER BY cosine_similarity_bytea(wv.vector, pqs.word) DESC
FETCH FIRST %s ROWS ONLY
', table_name, post_verif*k, pq_quantization_name, replace(token, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(token varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;

RETURN QUERY EXECUTE format('
SELECT pqs.word, (1.0 - (distance / 2.0))::float4
FROM %s AS gv, pq_search_in(gv.vector, %s, ''%s''::int[]) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
WHERE gv.word = ''%s''
', table_name, k, input_set, pq_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_ivpq(token varchar(100), k integer, input_set varchar[]) RETURNS TABLE (word varchar, similarity float4) AS $$
DECLARE
table_name varchar;
formated varchar[];
post_verif integer;
alpha integer;
method_flag integer;
use_targetlist boolean;
confidence real;
double_threshold integer;

BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
EXECUTE 'SELECT get_alpha()' INTO alpha;
EXECUTE 'SELECT get_method_flag()' INTO method_flag;
EXECUTE 'SELECT get_use_targetlist()' INTO use_targetlist;
EXECUTE 'SELECT get_confidence_value()' INTO confidence;
EXECUTE 'SELECT get_long_codes_threshold()' INTO double_threshold;

FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT f.word, (1.0 - (distance / 2.0))::float4 as similarity
FROM ivpq_search_in(
  ARRAY(SELECT vector FROM %s WHERE word = ''%s''),
  ''{0}''::int[],
  ''%s''::int,
  ARRAY(SELECT id FROM %s WHERE word = ANY(''%s''::varchar(100)[])),
  %s, %s, %s, ''%s''::boolean, ''%s''::float4, ''%s''::int)
  AS (qid integer, tid integer, distance float4) INNER JOIN %s AS f ON tid = f.id;
', table_name, replace(token, '''', ''''''), k, table_name, formated, alpha, post_verif, method_flag, use_targetlist, confidence, double_threshold, table_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_iv_batch(query_set varchar(100)[], k integer, input_set varchar[], function_name varchar) RETURNS TABLE (query varchar, target varchar, similarity float4) AS $$
DECLARE
table_name varchar;
post_verif integer;
alpha integer;
method_flag integer;
use_targetlist boolean;
confidence float4;
long_codes_threshold integer;
formated varchar[];
formated_queries varchar[];
ids integer[];
vectors bytea[];
words varchar[];
rec RECORD;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
EXECUTE 'SELECT get_alpha()' INTO alpha;
EXECUTE 'SELECT get_method_flag()' INTO method_flag;
EXECUTE 'SELECT get_use_targetlist()' INTO use_targetlist;
EXECUTE 'SELECT get_confidence_value()' INTO confidence;
EXECUTE 'SELECT get_long_codes_threshold()' INTO long_codes_threshold;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  formated_queries[I] = replace(query_set[I], '''', '''''');
END LOOP;
-- create lookup id -> query_word
FOR rec IN EXECUTE format('SELECT word, vector, id FROM %s WHERE word = ANY(''%s'')', table_name, formated_queries) LOOP
  vectors := vectors || rec.vector;
  ids := ids || rec.id;
END LOOP;
RETURN QUERY EXECUTE format('
SELECT f.word, g.word, (1.0 - (distance / 2.0))::float4 as similarity
FROM %s(''%s''::bytea[], ''%s''::integer[], ''%s''::int, ARRAY(SELECT id FROM %s WHERE word = ANY(''%s''::varchar(100)[])), %s, %s, %s, ''%s'', %s, %s) AS (qid integer, tid integer, distance float4) INNER JOIN %s AS f ON qid = f.id INNER JOIN %s AS g ON tid = g.id;
', function_name, vectors, ids, k, table_name, formated, alpha, post_verif, method_flag, use_targetlist, confidence, long_codes_threshold, table_name, table_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_iv_batch(query_set bytea[], k integer, input_set varchar[], function_name varchar) RETURNS TABLE (query integer, target varchar, similarity float4) AS $$
DECLARE
table_name varchar;
post_verif integer;
alpha integer;
method_flag integer;
use_targetlist boolean;
confidence float4;
long_codes_threshold integer;
formated varchar[];
ids integer[];
rec RECORD;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
EXECUTE 'SELECT get_alpha()' INTO alpha;
EXECUTE 'SELECT get_method_flag()' INTO method_flag;
EXECUTE 'SELECT get_use_targetlist()' INTO use_targetlist;
EXECUTE 'SELECT get_confidence_value()' INTO confidence;
EXECUTE 'SELECT get_long_codes_threshold()' INTO long_codes_threshold;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

EXECUTE format('SELECT array_agg(x) FROM generate_series(1,%s) x',array_upper(query_set, 1)) INTO ids;

RETURN QUERY EXECUTE format('
SELECT qid, g.word, (1.0 - (distance / 2.0))::float4 as similarity
FROM %s(''%s''::bytea[], ''%s''::integer[], ''%s''::int, ARRAY(SELECT id FROM %s WHERE word = ANY(''%s''::varchar(100)[])), %s, %s, %s, ''%s'', %s, %s) AS (qid integer, tid integer, distance float4) INNER JOIN %s AS g ON tid = g.id;
', function_name, query_set, ids, k, table_name, formated, alpha, post_verif, method_flag, use_targetlist, confidence, long_codes_threshold, table_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_ivpq_batch(query_set varchar(100)[], k integer, input_set varchar[]) RETURNS TABLE (query varchar, target varchar, similarity float4) AS $$
DECLARE
formated varchar[];
formated_queries varchar[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  formated_queries[I] = replace(query_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT * FROM knn_in_iv_batch(''%s''::varchar[], %s, ''%s'', ''%s'')', formated_queries, k, formated, 'ivpq_search_in');
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_ivpq_batch(query_set bytea[], k integer, input_set varchar[]) RETURNS TABLE (query int, target varchar, similarity float4) AS $$
DECLARE
formated varchar[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT * FROM knn_in_iv_batch(''%s''::bytea[], %s, ''%s'', ''%s'')', query_set, k, formated, 'ivpq_search_in');
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(query_vector anyarray, k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;

RETURN QUERY EXECUTE format('
SELECT pqs.word, (1.0 - (distance / 2.0))::float4
FROM pq_search_in(''%s''::float4[], %s, ''%s''::int[]) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
', query_vector, k, input_set, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq_batch(query_set bytea[], k integer, input_set varchar[]) RETURNS TABLE (query integer, target varchar, similarity float4) AS $$
DECLARE
table_name varchar;
use_targetlist boolean;
formated varchar[];
ids integer[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_use_targetlist()' INTO use_targetlist;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

EXECUTE format('SELECT array_agg(x) FROM generate_series(1,%s) x',array_upper(query_set, 1)) INTO ids;

RETURN QUERY EXECUTE format('
SELECT qid, g.word, (1.0 - (distance / 2.0))::float4 as similarity
FROM pq_search_in_batch(''%s''::bytea[], ''%s''::integer[], ''%s''::int, ARRAY(SELECT id FROM %s WHERE word = ANY(''%s''::varchar(100)[])), ''%s'') AS (qid integer, tid integer, distance float4) INNER JOIN %s AS g ON tid = g.id;
', query_set, ids, k, table_name, formated, use_targetlist, table_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq_batch(query_set varchar(100)[], k integer, input_set varchar[]) RETURNS TABLE (query varchar, target varchar, similarity float4) AS $$
DECLARE
table_name varchar;
use_targetlist boolean;
formated varchar[];
formated_queries varchar[];
ids integer[];
vectors bytea[];
words varchar[];
rec RECORD;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_use_targetlist()' INTO use_targetlist;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

FOR I IN array_lower(query_set, 1)..array_upper(query_set, 1) LOOP
  formated_queries[I] = replace(query_set[I], '''', '''''');
END LOOP;
-- create lookup id -> query_word
FOR rec IN EXECUTE format('SELECT word, vector, id FROM %s WHERE word = ANY(''%s'')', table_name, formated_queries) LOOP
  words := words || rec.word;
  vectors := vectors || rec.vector;
  ids := ids || rec.id;
END LOOP;
RETURN QUERY EXECUTE format('
SELECT f.word, g.word, (1.0 - (distance / 2.0))::float4 as similarity
FROM pq_search_in_batch(''%s''::bytea[], ''%s''::integer[], ''%s''::int, ARRAY(SELECT id FROM %s WHERE word = ANY(''%s''::varchar(100)[])), ''%s'') AS (qid integer, tid integer, distance float4) INNER JOIN %s AS f ON qid = f.id INNER JOIN %s AS g ON tid = g.id;
', vectors, ids, k, table_name, formated, use_targetlist, table_name, table_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(token varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT pqs.word, (1.0 - (distance / 2.0))::float4
FROM %s AS gv, pq_search_in(gv.vector, %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[]))) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
WHERE gv.word = ''%s''
', table_name, k, table_name, formated, pq_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(query_vector bytea, k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT pqs.word, (1.0 - (distance / 2.0))::float4
FROM pq_search_in(''%s'', %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[]))) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
', query_vector, k, table_name, formated, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cosine_similarity(token1 varchar(100), token2 varchar(100), OUT result float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;

EXECUTE format('
SELECT cosine_similarity_bytea(t1.vector, t2.vector)
FROM %s AS t1, %s AS t2
WHERE (t1.word = ''%s'') AND (t2.word = ''%s'')
', table_name, table_name, replace(token1, '''', ''''''), replace(token2, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cosine_similarity_bytea(vector bytea, token2 varchar(100), OUT result float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;

EXECUTE format('
SELECT cosine_similarity_bytea(''%s'', t2.vector)
FROM %s AS t2
WHERE t2.word = ''%s''
', vector, table_name, replace(token2, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cosine_similarity_norm(vector float4[], token2 varchar(100), OUT result float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;

EXECUTE format('
SELECT cosine_similarity_norm(''%s''::float4[], t2.vector)
FROM %s AS t2
WHERE t2.word = ''%s''
', vector, table_name, replace(token2, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_exact(token varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
WHERE v2.id = ANY (''%s''::integer[])
ORDER BY cosine_similarity_bytea(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, replace(token, '''', ''''''), input_set, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_exact(query_vector bytea, k integer, input_set bytea[]) RETURNS TABLE (id integer, vec bytea, similarity float4) AS $$
DECLARE
id_array integer[];
rec RECORD;
BEGIN
-- create array of ids
EXECUTE format('SELECT array_agg(generate_series) FROM generate_series(1,%s)',
  array_upper(input_set, 1)) INTO id_array;
FOR rec in EXECUTE format('
  SELECT id, vec, cosine_similarity_bytea(''%s''::bytea, vec) as sim
  FROM unnest(''%s''::int[], ''%s''::bytea[]) x(id,vec)
  ORDER BY cosine_similarity_bytea(''%s''::bytea, vec) DESC
  FETCH FIRST %s ROWS ONLY', query_vector, id_array, input_set, query_vector, k) LOOP
  id := rec.id;
  vec := rec.vec;
  similarity := rec.sim;
  RETURN NEXT;
END LOOP;
RETURN;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_exact(query_vector bytea, k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(''%s'', v2.vector) FROM %s AS v2
WHERE v2.id = ANY (''%s''::integer[])
ORDER BY cosine_similarity_bytea(''%s'', v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', query_vector, table_name, input_set, query_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_exact(token varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
-- execute replace on every element of input set
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
WHERE v2.word = ANY (''%s''::varchar(100)[])
ORDER BY cosine_similarity_bytea(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, replace(token, '''', ''''''), formated, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_exact(query_vector bytea, k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
-- execute replace on every element of input set
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_bytea(''%s'', v2.vector) FROM %s AS v2
WHERE v2.word = ANY (''%s''::varchar(100)[])
ORDER BY cosine_similarity_bytea(''%s'', v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', query_vector, table_name, formated, query_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generic_cluster(tokens varchar(100)[], k integer, function_name varchar) RETURNS  TABLE (word varchar(100), cluster integer) AS $$
DECLARE
table_name varchar;
formated varchar[];
token_id int;
token_ids int[];
centroids bytea[];
centroid bytea;
clusters int[];
cluster_lens int[];
processed boolean[];
samples bytea[];
rec RECORD;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
-- get vectors of tokens
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated[I] = replace(tokens[I], '''', '''''');
  processed[I] = 'f';
  token_ids[I] = I;
END LOOP;
-- select k random tokens
FOR I IN 1..k LOOP
  EXECUTE format('SELECT round(random()*%s+0.5)', array_upper(formated, 1)) INTO token_id;
  EXECUTE format('SELECT vector FROM %s WHERE word = ''%s''', table_name, formated[token_id]) INTO centroid;
  centroids[I] := centroid;
  cluster_lens[I] := 0;
END LOOP;
-- knn search
FOR J IN 1..10 LOOP
  FOR rec IN EXECUTE format('
    SELECT query as qid, x.tid, similarity
    FROM %s(''%s''::bytea[], ''%s''::integer, ''%s''::varchar[])
    INNER JOIN unnest(''%s''::int[], ''%s''::varchar[]) as x(tid, token) ON token = target
    ORDER BY similarity DESC
    ', function_name, centroids, array_upper(tokens, 1), formated, token_ids, formated) LOOP
    IF processed[rec.tid] = 'f' THEN
      clusters[rec.tid] := rec.qid;
      cluster_lens[rec.qid] := cluster_lens[rec.qid] + 1;
      processed[rec.tid] := 't';
    END IF;
  END LOOP;
  -- recalculate centroids
  IF J < 10 THEN
    FOR I IN 1..k LOOP
      -- sample vectors from clusters[I]
      IF cluster_lens[I] = 0 THEN
        EXECUTE format('
          SELECT array_agg(vec)
          FROM (SELECT round(random()*%s+0.5)
            FROM generate_series(1,10) gs1(x)) r(x)
          INNER JOIN unnest((SELECT array_agg(gs2.val)
            FROM generate_series(1,%s) gs2(val))::int[],(ARRAY(SELECT vector FROM %s INNER JOIN unnest(''%s''::int[], ''%s''::varchar[]) x(i,t) ON t = word)::bytea[])) x(id, vec)
          ON r.x = x.id
          ', array_upper(tokens, 1), array_upper(tokens, 1), table_name, token_ids, formated) INTO samples;
      ELSE
        EXECUTE format('
          SELECT array_agg(vec)
          FROM (SELECT round(random()*%s+0.5)
            FROM generate_series(1,10) gs1(x)) r(x)
          INNER JOIN unnest((SELECT array_agg(gs2.val)
            FROM generate_series(1,%s) gs2(val))::int[],(ARRAY(SELECT vector FROM %s INNER JOIN unnest(''%s''::int[], ''%s''::varchar[]) x(i,t) ON t = word WHERE i = %s)::bytea[])) x(id, vec)
          ON r.x = x.id
          ', cluster_lens[I], cluster_lens[I], table_name, clusters, formated, I) INTO samples;
          -- apply centroid function on samples
          centroids[I] := centroid_bytea(samples);
          cluster_lens[I] := 0;
      END IF;
    END LOOP;
    -- reset processed
    FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
      processed[I] = 'f';
    END LOOP;
  END IF;
END LOOP;
-- output
FOR I IN 1..array_upper(clusters, 1) LOOP
  word := formated[I];
  cluster := clusters[I];
  RETURN NEXT;
END LOOP;
RETURN;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_exact(tokens varchar(100)[], k integer) RETURNS  TABLE (word varchar(100), cluster integer) AS $$
DECLARE
formated varchar[];
BEGIN
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated[I] = replace(tokens[I], '''', '''''');
END LOOP;
RETURN QUERY EXECUTE format('
  SELECT word, cluster FROM generic_cluster(''%s''::varchar[], %s, ''knn_search_in_batch'')', formated, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_ivpq(tokens varchar(100)[], k integer) RETURNS  TABLE (word varchar(100), cluster integer) AS $$
DECLARE
formated varchar[];
BEGIN
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated[I] = replace(tokens[I], '''', '''''');
END LOOP;
RETURN QUERY EXECUTE format('
  SELECT word, cluster FROM generic_cluster(''%s''::varchar[], %s, ''knn_in_ivpq_batch'')', formated, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_pq(tokens varchar(100)[], k integer) RETURNS  TABLE (word varchar(100), cluster integer) AS $$
DECLARE
formated varchar[];
BEGIN
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated[I] = replace(tokens[I], '''', '''''');
END LOOP;
RETURN QUERY EXECUTE format('
  SELECT word, cluster FROM generic_cluster(''%s''::varchar[], %s, ''knn_in_pq_batch'')', formated, k);
END
$$
LANGUAGE plpgsql;

-- note: maybe different variances of analogy possible
CREATE OR REPLACE FUNCTION analogy_pair_direction(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_original()' INTO table_name;
EXECUTE format('
SELECT v4.word FROM %s AS v4
INNER JOIN %s AS v1 ON v1.word = ''%s''
INNER JOIN %s AS v2 ON v2.word = ''%s''
INNER JOIN %s AS v3 ON v3.word = ''%s''
WHERE v4.word NOT IN (''%s'', ''%s'', ''%s'')
ORDER BY cosine_similarity_bytea(vec_normalize_bytea(vec_minus_bytea(v1.vector, v2.vector)), vec_normalize_bytea(vec_minus_bytea(v3.vector, v4.vector))) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, replace(w1, '''', ''''''), table_name, replace(w2, '''', ''''''), table_name, replace(w3, '''', ''''''), replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_3cosmul(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE format('
SELECT v4.word FROM %s AS v4
INNER JOIN %s AS v1 ON v1.word = ''%s''
INNER JOIN %s AS v2 ON v2.word = ''%s''
INNER JOIN %s AS v3 ON v3.word = ''%s''
WHERE v4.word NOT IN (''%s'', ''%s'', ''%s'')
ORDER BY (((cosine_similarity_bytea(v4.vector, v3.vector) + 1)/2) * ((cosine_similarity_bytea(v4.vector, v2.vector) + 1.0)/2.0)) /  (((cosine_similarity_bytea(v4.vector, v1.vector) + 1.0)/2.0)+0.001) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, replace(w1, '''', ''''''), table_name, replace(w2, '''', ''''''), table_name, replace(w3, '''', ''''''), replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- -- works only with normalized vectors
-- CREATE OR REPLACE FUNCTION analogy2(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
-- AS  $$
-- DECLARE
-- table_name varchar;
-- BEGIN
-- EXECUTE 'SELECT get_vecs_name()' INTO table_name;
-- EXECUTE format('
-- SELECT v4.word FROM %s AS v4
-- INNER JOIN %s AS v1 ON v1.word = ''%s''
-- INNER JOIN %s AS v2 ON v2.word = ''%s''
-- INNER JOIN %s AS v3 ON v3.word = ''%s''
-- ORDER BY cosine_similarity(vec_plus(vec_minus(v1.vector, v2.vector), v4.vector), v3.vector) DESC
-- FETCH FIRST 1 ROWS ONLY
-- ', table_name, table_name, replace(w1, '''', ''''''), table_name, replace(w2, '''', ''''''), table_name, replace(w3, '''', '''''')) INTO result;
-- END
-- $$
-- LANGUAGE plpgsql;

-- w3 - w1 + w2
CREATE OR REPLACE FUNCTION analogy_3cosadd(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE format('
SELECT v4.word
FROM %s AS v4
INNER JOIN %s AS v1 ON v1.word = ''%s''
INNER JOIN %s AS v2 ON v2.word = ''%s''
INNER JOIN %s AS v3 ON v3.word = ''%s''
WHERE v4.word NOT IN (''%s'', ''%s'', ''%s'')
ORDER BY cosine_similarity_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, replace(w1, '''', ''''''), table_name, replace(w2, '''', ''''''), table_name, replace(w3, '''', ''''''), replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_3cosadd_in(w1 varchar(100), w2 varchar(100), w3 varchar(100), input_set varchar(100)[], OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
formated varchar[];
command varchar;
test varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE format('
SELECT v4.word
FROM %s AS v4
INNER JOIN %s AS v1 ON v1.word = ''%s''
INNER JOIN %s AS v2 ON v2.word = ''%s''
INNER JOIN %s AS v3 ON v3.word = ''%s''
WHERE (v4.word NOT IN (''%s'', ''%s'', ''%s'')) AND (v4.word = ANY (''%s''::varchar(100)[]))
ORDER BY cosine_similarity_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, replace(w1, '''', ''''''), table_name, replace(w2, '''', ''''''), table_name, replace(w3, '''', ''''''), replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', ''''''), formated) INTO result;
END
$$
LANGUAGE plpgsql;

-- with postverification
CREATE OR REPLACE FUNCTION analogy_3cosadd_pq(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE format('
SELECT pqs.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
pq_search(vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector)), %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
INNER JOIN %s AS v4 ON v4.word = pqs.word
WHERE (v1.word = ''%s'')
AND (v2.word = ''%s'')
AND (v3.word = ''%s'')
AND (pqs.word != v1.word)
AND (pqs.word != v2.word)
AND (pqs.word != v3.word)
ORDER BY cosine_similarity_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, table_name, post_verif+3, pq_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_3cosadd_in_pq(w1 varchar(100), w2 varchar(100), w3 varchar(100), input_set varchar(100)[], OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
formated varchar[];
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE format('
SELECT pqs.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
pq_search_in(vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector)), %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[]))) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
INNER JOIN %s AS v4 ON v4.word = pqs.word
WHERE (v1.word = ''%s'')
AND (v2.word = ''%s'')
AND (v3.word = ''%s'')
AND (pqs.word != v1.word)
AND (pqs.word != v2.word)
AND (pqs.word != v3.word)
ORDER BY cosine_similarity_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, table_name, post_verif+3, pq_quantization_name, formated, pq_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', ''''''), formated) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_3cosadd_in_ivpq(w1 varchar(100), w2 varchar(100), w3 varchar(100), input_set varchar(100)[], OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
formated varchar[];
post_verif integer;
alpha integer;
method_flag integer;
use_targetlist boolean;
confidence real;
double_threshold integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
EXECUTE 'SELECT get_alpha()' INTO alpha;
EXECUTE 'SELECT get_method_flag()' INTO method_flag;
EXECUTE 'SELECT get_use_targetlist()' INTO use_targetlist;
EXECUTE 'SELECT get_confidence_value()' INTO confidence;
EXECUTE 'SELECT get_long_codes_threshold()' INTO double_threshold;

FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE format('
SELECT pqs.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
ivpq_search_in(ARRAY(SELECT vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector))), ''{0}''::int[], %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[])), %s, %s, %s, ''%s''::boolean, ''%s''::float4, ''%s''::int) AS (qid integer, idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
INNER JOIN %s AS v4 ON v4.word = pqs.word
WHERE (v1.word = ''%s'')
AND (v2.word = ''%s'')
AND (v3.word = ''%s'')
AND (pqs.word != v1.word)
AND (pqs.word != v2.word)
AND (pqs.word != v3.word)
ORDER BY cosine_similarity_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, table_name, 4, table_name, formated, alpha, post_verif, method_flag, use_targetlist, confidence, double_threshold, table_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- with postverification
CREATE OR REPLACE FUNCTION analogy_3cosadd_ivfadc(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
fine_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
EXECUTE format('
SELECT fq.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
ivfadc_search(vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector)), %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
INNER JOIN %s AS v4 ON v4.word = fq.word
WHERE (v1.word = ''%s'')
AND (v2.word = ''%s'')
AND (v3.word = ''%s'')
AND (fq.word != v1.word)
AND (fq.word != v2.word)
AND (fq.word != v3.word)
ORDER BY cosine_similarity_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, table_name, post_verif+3, fine_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- without pq-index
CREATE OR REPLACE FUNCTION grouping_func(tokens varchar(100)[], groups varchar(100)[]) RETURNS TABLE (token varchar(100), grouptoken varchar(100)) AS $$
DECLARE
table_name varchar;
groups_formated varchar[];
tokens_formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(groups, 1)..array_upper(groups, 1) LOOP
  groups_formated[I] = replace(groups[I], '''', '''''''''');
END LOOP;
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  tokens_formated[I] = replace(tokens[I], '''', '''''''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT v1.word, gt.word
FROM %s AS v1,
knn_in(v1.word, 1, ''%s''::varchar(100)[]) AS gt
WHERE v1.word = ANY(''%s''::varchar(100)[])
', table_name, groups_formated, tokens_formated);
END
$$
LANGUAGE plpgsql;

-- with PQ-Index
CREATE OR REPLACE FUNCTION grouping_func_pq(tokens varchar(100)[], groups varchar(100)[]) RETURNS TABLE (token varchar(100), grouptoken varchar(100)) AS $$
DECLARE
table_name varchar;
groups_formated varchar[];
tokens_formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(groups, 1)..array_upper(groups, 1) LOOP
  groups_formated[I] = replace(groups[I], '''', '''''');
END LOOP;
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  tokens_formated[I] = replace(tokens[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT v1.word, v2.word
FROM grouping_pq(ARRAY(SELECT id FROM %s WHERE word = ANY (''%s'')),ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''))) AS (token_id integer, group_id integer)
INNER JOIN %s AS v1 ON v1.id = token_id
INNER JOIN %s AS v2 ON v2.id = group_id
', table_name, tokens_formated, table_name, groups_formated, table_name, table_name);
END
$$
LANGUAGE plpgsql;


-- Tokenization + Normalization
CREATE OR REPLACE FUNCTION tokenize(input text, OUT result bytea) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE format('
SELECT vec_normalize_bytea(centroid_bytea(ARRAY(SELECT vector FROM %s WHERE word = ANY(regexp_split_to_array(''%s'', '' '')))))
', table_name, replace(input, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- Tokenization without normalization
CREATE OR REPLACE FUNCTION tokenize_raw(input text, OUT result bytea) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_original()' INTO table_name;
EXECUTE format('
SELECT centroid_bytea(ARRAY(SELECT vector FROM %s WHERE word = ANY(regexp_split_to_array(''%s'', '' ''))))
', table_name, replace(input, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION cosine_similarity(varchar(100), varchar(100), )
