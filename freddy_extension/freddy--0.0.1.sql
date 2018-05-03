-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION freddy" to load this file. \quit

CREATE OR REPLACE FUNCTION init(original regclass, normalized regclass, pq_quantization regclass, codebook regclass, residual_quantization regclass, coarse_quantization regclass, residual_codebook regclass) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_original() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', original);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', normalized);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_pq_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', pq_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_codebook() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', codebook);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_residual_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', residual_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_coarse_quantization() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', coarse_quantization);
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name_residual_codebook() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', residual_codebook);
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

DO $$
DECLARE
init_done int;
number_tables int;
BEGIN
EXECUTE 'SELECT count(proname) FROM pg_proc WHERE proname=''get_vecs_name''' INTO init_done;
EXECUTE 'SELECT count(*) FROM information_schema.tables WHERE table_schema=''public'' AND table_type=''BASE TABLE'' AND table_name in (''google_vecs'', ''google_vecs_norm'', ''pq_quantization'', ''pq_codebook'', ''fine_quantization'', ''coarse_quantization'', ''residual_codebook'')' INTO number_tables;
IF init_done = 0 AND number_tables = 7 THEN
  EXECUTE 'SELECT init(''google_vecs'', ''google_vecs_norm'', ''pq_quantization'', ''pq_codebook'', ''fine_quantization'', ''coarse_quantization'', ''residual_codebook'')';
END IF;
END$$;

SELECT set_pvf(1000);
SELECT set_w(3);
SELECT set_knn_function('k_nearest_neighbour');
SELECT set_knn_in_function('knn_in_exact');
SELECT set_knn_batch_function('k_nearest_neighbour_ivfadc_batch');
SELECT set_analogy_function('analogy_3cosadd');
SELECT set_analogy_in_function('analogy_3cosadd_in');
SELECT set_groups_function('grouping_func');



CREATE OR REPLACE FUNCTION knn(query varchar, k integer) RETURNS TABLE (word varchar(100), similarity float4) AS $$
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
formated varchar(100)[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_knn_in_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', %s, ''%s''::varchar[])
', function_name,  replace(query, '''', ''''''), k, formated);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_batch(query_set varchar(100)[], k integer) RETURNS TABLE (query varchar(100), target varchar(100), squareDistance float4) AS $$
DECLARE
function_name varchar;
formated varchar(100)[];
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

CREATE OR REPLACE FUNCTION analogy(a varchar(100),b varchar(100),c varchar(100)) RETURNS TABLE (target varchar(100)) AS $$
DECLARE
function_name varchar;
BEGIN
EXECUTE 'SELECT get_analogy_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', ''%s'',''%s'')
', function_name, replace(a, '''', ''''''), replace(b, '''', ''''''), replace(c, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_in(w1 varchar(100), w2 varchar(100), w3 varchar(100), input_set varchar(100)[]) RETURNS TABLE (target varchar(100)) AS $$
DECLARE
function_name varchar;
formated varchar(100)[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
-- replace(token, '''', '''''')
EXECUTE 'SELECT get_analogy_in_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s'', ''%s'',''%s'', ''%s''::varchar[])
', function_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', ''''''), formated);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION groups(tokens varchar[], groups varchar[]) RETURNS TABLE (token varchar(100), grouptoken varchar(100)) AS $$
DECLARE
function_name varchar;
formated_tokens varchar(100)[];
formated_groups varchar(100)[];
BEGIN
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated_tokens[I] = replace(tokens[I], '''', '''''');
END LOOP;
FOR I IN array_lower(groups, 1)..array_upper(groups, 1) LOOP
  formated_groups[I] = replace(groups[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_groups_function_name()' INTO function_name;
RETURN QUERY EXECUTE format('
SELECT * FROM %s(''%s''::varchar[], ''%s''::varchar[])
', function_name, formated_tokens, formated_groups);
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

CREATE OR REPLACE FUNCTION ivfadc_batch_search(integer[], integer) RETURNS SETOF record
AS '$libdir/freddy', 'ivfadc_batch_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cluster_pq_to_id(integer[], integer) RETURNS SETOF record
AS '$libdir/freddy', 'cluster_pq'
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

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(token varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
fine_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, distance
FROM %s AS gv, ivfadc_search(gv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
WHERE gv.word = ''%s''
', table_name, k, fine_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
fine_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, distance
FROM ivfadc_search(''%s'', %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
', input_vector, k, fine_quantization_name);
END
$$
LANGUAGE plpgsql;

-- ivfadc_batch_search(
CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_batch(input_set varchar(100)[], k integer) RETURNS TABLE (query varchar(100), target varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
fine_quantization_name varchar;
formated varchar(100)[];
BEGIN
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, fq2.word, distance
FROM ivfadc_batch_search(ARRAY(SELECT id FROM %s WHERE word = ANY (''%s'')), %s) AS (idx integer, idxx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
INNER JOIN %s AS fq2 ON idxx = fq2.id
', table_name, formated, k, fine_quantization_name, fine_quantization_name);
END
$$
LANGUAGE plpgsql;

-- TODO ADAPT
CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_pv(token varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT v2.word, distance
FROM %s AS v1, ivfadc_search(v1.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS v2 ON idx = v2.id
WHERE v1.word = ''%s''
ORDER BY cosine_similarity_bytea(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, post_verif, table_name, replace(token, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

-- TODO ADAPT
CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_pv(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
fine_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT fq.word, distance
FROM ivfadc_search(''%s''::float4[], %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
ORDER BY cosine_similarity_bytea(''%s''::float4[], fq.word) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, post_verif, fine_quantization_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(token varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, distance AS distance
FROM %s AS gv, pq_search(gv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
WHERE gv.word = ''%s''
', table_name, k, pq_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(input_vector bytea, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, distance AS distance
FROM pq_search(''%s'', %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
', input_vector, k, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

-- TODO ADAPT
-- with postverification
CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq_pv(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
pq_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, distance AS distance
FROM pq_search(''%s''::float4[], %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
ORDER BY cosine_similarity_bytea(''%s''::float4[], pqs.word) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, post_verif, pq_quantization_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

-- TODO ADAPT
CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq_pv(token varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
post_verif integer;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_pvf()' INTO post_verif;
RETURN QUERY EXECUTE format('
SELECT pqs.word AS word, distance AS distance
FROM %s as wv, pq_search(wv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqs ON idx = pqs.id
WHERE wv.word = ''%s''
ORDER BY cosine_similarity_bytea(wv.vector, pqs.word) DESC
FETCH FIRST %s ROWS ONLY
', table_name, post_verif, pq_quantization_name, replace(token, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(token varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;

RETURN QUERY EXECUTE format('
SELECT pqs.word, distance
FROM %s AS gv, pq_search_in(gv.vector, %s, ''%s''::int[]) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
WHERE gv.word = ''%s''
', table_name, k, input_set, pq_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(query_vector anyarray, k integer, input_set integer[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;

RETURN QUERY EXECUTE format('
SELECT pqs.word, distance
FROM pq_search_in(''%s''::float4[], %s, ''%s''::int[]) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
', query_vector, k, input_set, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(token varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
formated varchar(100)[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT pqs.word, distance
FROM %s AS gv, pq_search_in(gv.vector, %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[]))) AS (result_id integer, distance float4)
INNER JOIN %s AS pqs ON result_id = pqs.id
WHERE gv.word = ''%s''
', table_name, k, table_name, formated, pq_quantization_name, replace(token, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(query_vector bytea, k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
formated varchar(100)[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT pqs.word, distance
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

-- TODO implement  cosine_similarity_bytea(vector float4[], token2 varchar(100), OUT result float8)

-- CREATE OR REPLACE FUNCTION cosine_similarity_norm(anyarray, anyarray) RETURNS float8
-- AS '$libdir/freddy', 'cosine_similarity_norm'
-- LANGUAGE C IMMUTABLE STRICT;

-- TODO ADAPT
-- TopK_In Exakt
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

-- TODO ADAPT
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

-- TODO ADAPT
CREATE OR REPLACE FUNCTION knn_in_exact(token varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
formated varchar(100)[];
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

-- TODO ADAPT
CREATE OR REPLACE FUNCTION knn_in_exact(query_vector bytea, k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float4) AS $$
DECLARE
table_name varchar;
formated varchar(100)[];
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

-- TODO ADAPT
CREATE OR REPLACE FUNCTION cluster_pq(tokens varchar(100)[], k integer) RETURNS  TABLE (words varchar(100)[]) AS $$
DECLARE
table_name varchar;
formated varchar(100)[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(tokens, 1)..array_upper(tokens, 1) LOOP
  formated[I] = replace(tokens[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT array_agg(word)
FROM %s, cluster_pq_to_id(ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar[])), %s) AS (centroid float4[], ids int[])
WHERE id = ANY ((ids)::integer[])
GROUP BY centroid;
', table_name, table_name, formated, k);
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
formated varchar(100)[];
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
WHERE (v4.word NOT IN (''%s'', ''%s'', ''%s'')) AND (v4.word = ANY (''%s''::varchar[]))
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
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE format('
SELECT pqs.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
pq_search(vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector)), 100) AS (idx integer, distance float4)
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
', table_name, table_name, table_name, pq_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analogy_3cosadd_in_pq(w1 varchar(100), w2 varchar(100), w3 varchar(100), input_set varchar(100)[], OUT result varchar(100))
AS  $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
formated varchar(100)[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
FOR I IN array_lower(input_set, 1)..array_upper(input_set, 1) LOOP
  formated[I] = replace(input_set[I], '''', '''''');
END LOOP;
EXECUTE format('
SELECT pqs.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
pq_search_in(vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector)), 100, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar[]))) AS (idx integer, distance float4)
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
', table_name, table_name, table_name, pq_quantization_name, formated, pq_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', ''''''), formated) INTO result;
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
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
EXECUTE format('
SELECT fq.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
ivfadc_search(vec_normalize_bytea(vec_plus_bytea(vec_minus_bytea(v3.vector, v1.vector), v2.vector)), 100) AS (idx integer, distance float4)
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
', table_name, table_name, table_name, fine_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
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
knn_in(v1.word, 1, ''%s''::varchar[]) AS gt
WHERE v1.word = ANY(''%s''::varchar[])
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
