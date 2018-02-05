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

SELECT CASE (SELECT count(proname) FROM pg_proc WHERE proname='get_vecs_name')
  WHEN 0 THEN init('google_vecs', 'google_vecs_norm', 'pq_quantization', 'pq_codebook', 'fine_quantization', 'coarse_quantization', 'residual_codebook')
  END;


CREATE OR REPLACE FUNCTION cosine_similarity(float4[], float4[]) RETURNS float8
AS '$libdir/freddy', 'cosine_similarity'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cosine_similarity_norm(float4[], float4[]) RETURNS float8
AS '$libdir/freddy', 'cosine_similarity_norm'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_minus(anyarray, anyarray) RETURNS anyarray
AS '$libdir/freddy', 'vec_minus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_plus(anyarray, anyarray) RETURNS anyarray
AS '$libdir/freddy', 'vec_plus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_normalize(anyarray) RETURNS anyarray
AS '$libdir/freddy', 'vec_normalize'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search(anyarray, integer) RETURNS SETOF record
AS '$libdir/freddy', 'pq_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION ivfadc_search(anyarray, integer) RETURNS SETOF record
AS '$libdir/freddy', 'ivfadc_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search_in(anyarray, integer, integer[]) RETURNS SETOF record
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

CREATE OR REPLACE FUNCTION pq_search_in_cplx(anyarray, integer, varchar(100)[]) RETURNS SETOF record
AS '$libdir/freddy', 'pq_search_in_cplx'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION insert_batch(varchar(100)[]) RETURNS integer
AS '$libdir/freddy', 'insert_batch'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION centroid(anyarray) RETURNS anyarray
AS '$libdir/freddy', 'centroid'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION k_nearest_neighbour(term varchar(100), k integer) RETURNS TABLE (word varchar(100), similarity float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_norm(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
ORDER BY cosine_similarity_norm(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, replace(term, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), similarity float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_norm(''%s''::float4[], v2.vector)
FROM %s AS v2
ORDER BY cosine_similarity_norm(''%s''::float4[], v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, table_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(term varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
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
', table_name, k, fine_quantization_name, replace(term, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
fine_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, distance
FROM ivfadc_search(''%s''::float4[], %s) AS (idx integer, distance float4)
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


CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_pv(term varchar(100), k integer, post_verif integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, distance
FROM %s AS v1, ivfadc_search(v1.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS v2 ON idx = v2.id
WHERE v1.word = ''%s''
ORDER BY cosine_similarity(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, post_verif, table_name, replace(term, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc_pv(input_vector anyarray, k integer, post_verif integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
fine_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_residual_quantization()' INTO fine_quantization_name;
RETURN QUERY EXECUTE format('
SELECT fq.word, distance
FROM ivfadc_search(''%s''::float4[], %s) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
ORDER BY cosine_similarity(''%s''::float4[], fq.word) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, post_verif, fine_quantization_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(term varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqq.word AS word, distance AS distance
FROM %s AS gv, pq_search(gv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqq ON idx = pqq.id
WHERE gv.word = ''%s''
', table_name, k, pq_quantization_name, replace(term, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqq.word AS word, distance AS distance
FROM pq_search(''%s''::float4[], %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqq ON idx = pqq.id
', input_vector, k, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

-- with postverification
CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq_pv(input_vector anyarray, k integer, post_verif integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqq.word AS word, distance AS distance
FROM pq_search(''%s''::float4[], %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqq ON idx = pqq.id
ORDER BY cosine_similarity(''%s''::float4[], pqq.word) DESC
FETCH FIRST %s ROWS ONLY
', input_vector, post_verif, pq_quantization_name, input_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq_pv(term varchar(100), k integer, post_verif integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;
RETURN QUERY EXECUTE format('
SELECT pqq.word AS word, distance AS distance
FROM %s as wv, pq_search(wv.vector, %s) AS (idx integer, distance float4)
INNER JOIN %s AS pqq ON idx = pqq.id
WHERE wv.word = ''%s''
ORDER BY cosine_similarity(wv.vector, pqq.word) DESC
FETCH FIRST %s ROWS ONLY
', table_name, post_verif, pq_quantization_name, replace(term, '''', ''''''), k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(term varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
DECLARE
table_name varchar;
pq_quantization_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE 'SELECT get_vecs_name_pq_quantization()' INTO pq_quantization_name;

RETURN QUERY EXECUTE format('
SELECT pqq.word, distance
FROM %s AS gv, pq_search_in(gv.vector, %s, ''%s''::int[]) AS (result_id integer, distance float4)
INNER JOIN %s AS pqq ON result_id = pqq.id
WHERE gv.word = ''%s''
', table_name, k, input_set, pq_quantization_name, replace(term, '''', ''''''));
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
SELECT pqq.word, distance
FROM pq_search_in(''%s''::float4[], %s, ''%s''::int[]) AS (result_id integer, distance float4)
INNER JOIN %s AS pqq ON result_id = pqq.id
', query_vector, k, input_set, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(term varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
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
SELECT pqq.word, distance
FROM %s AS gv, pq_search_in(gv.vector, %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[]))) AS (result_id integer, distance float4)
INNER JOIN %s AS pqq ON result_id = pqq.id
WHERE gv.word = ''%s''
', table_name, k, table_name, formated, pq_quantization_name, replace(term, '''', ''''''));
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in_pq(query_vector anyarray, k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS $$
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
SELECT pqq.word, distance
FROM pq_search_in(''%s''::float4[], %s, ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''::varchar(100)[]))) AS (result_id integer, distance float4)
INNER JOIN %s AS pqq ON result_id = pqq.id
', query_vector, k, table_name, formated, pq_quantization_name);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cosine_similarity(term1 varchar(100), term2 varchar(100), OUT result float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;

EXECUTE format('
SELECT cosine_similarity(t1.vector, t2.vector)
FROM %s AS t1, %s AS t2
WHERE (t1.word = ''%s'') AND (t2.word = ''%s'')
', table_name, table_name, replace(term1, '''', ''''''), replace(term2, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cosine_similarity(vector float4[], term2 varchar(100), OUT result float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;

EXECUTE format('
SELECT cosine_similarity(''%s''::float4[], t2.vector)
FROM %s AS t2
WHERE t2.word = ''%s''
', vector, table_name, replace(term2, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION cosine_similarity_norm(anyarray, anyarray) RETURNS float8
-- AS '$libdir/freddy', 'cosine_similarity_norm'
-- LANGUAGE C IMMUTABLE STRICT;

-- TopK_In Exakt
CREATE OR REPLACE FUNCTION knn_in(term varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_norm(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
WHERE v2.id = ANY (''%s''::integer[])
ORDER BY cosine_similarity_norm(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, replace(term, '''', ''''''), input_set, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in(query_vector float4[], k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_norm(''%s''::float4[], v2.vector) FROM %s AS v2
WHERE v2.id = ANY (''%s''::integer[])
ORDER BY cosine_similarity_norm(''%s''::float4[], v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', query_vector, table_name, input_set, query_vector, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION knn_in(term varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float8) AS $$
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
SELECT v2.word, cosine_similarity_norm(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
WHERE v2.word = ANY (''%s''::varchar(100)[])
ORDER BY cosine_similarity_norm(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, replace(term, '''', ''''''), formated, k);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cluster_pq(terms varchar(100)[], k integer) RETURNS  TABLE (words varchar(100)[]) AS $$
DECLARE
table_name varchar;
formated varchar(100)[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(terms, 1)..array_upper(terms, 1) LOOP
  formated[I] = replace(terms[I], '''', '''''');
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
ORDER BY cosine_similarity(vec_minus(v1.vector, v2.vector), vec_minus(v3.vector, v4.vector)) DESC
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
ORDER BY cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, replace(w1, '''', ''''''), table_name, replace(w2, '''', ''''''), table_name, replace(w3, '''', ''''''), replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
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
SELECT pqq.word FROM
%s AS v1,
%s AS v2,
%s AS v3,
pq_search(vec_normalize(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector)), 100) AS (idx integer, distance float4)
INNER JOIN %s AS pqq ON idx = pqq.id
INNER JOIN %s AS v4 ON v4.word = pqq.word
WHERE (v1.word = ''%s'')
AND (v2.word = ''%s'')
AND (v3.word = ''%s'')
AND (pqq.word != v1.word)
AND (pqq.word != v2.word)
AND (pqq.word != v3.word)
ORDER BY cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, table_name, pq_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
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
ivfadc_search(vec_normalize(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector)), 100) AS (idx integer, distance float4)
INNER JOIN %s AS fq ON idx = fq.id
INNER JOIN %s AS v4 ON v4.word = fq.word
WHERE (v1.word = ''%s'')
AND (v2.word = ''%s'')
AND (v3.word = ''%s'')
AND (fq.word != v1.word)
AND (fq.word != v2.word)
AND (fq.word != v3.word)
ORDER BY cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, table_name, fine_quantization_name, table_name, replace(w1, '''', ''''''), replace(w2, '''', ''''''), replace(w3, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- without pq-index
CREATE OR REPLACE FUNCTION grouping_func(terms varchar(100)[], groups varchar(100)[]) RETURNS TABLE (term varchar(100), groupterm varchar(100)) AS $$
DECLARE
table_name varchar;
groups_formated varchar[];
terms_formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(groups, 1)..array_upper(groups, 1) LOOP
  groups_formated[I] = replace(groups[I], '''', '''''');
END LOOP;
FOR I IN array_lower(terms, 1)..array_upper(terms, 1) LOOP
  terms_formated[I] = replace(terms[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT v1.word, gt.word
FROM %s AS v1,
top_k_in(v1.word, 1, ''%s''::varchar[]) AS gt
WHERE v1.word = ANY(''%s''::varchar[])
', table_name, groups_formated, terms_formated);
END
$$
LANGUAGE plpgsql;

-- with PQ-Index
CREATE OR REPLACE FUNCTION grouping_func_pq(terms varchar(100)[], groups varchar(100)[]) RETURNS TABLE (term varchar(100), groupterm varchar(100)) AS $$
DECLARE
table_name varchar;
groups_formated varchar[];
terms_formated varchar[];
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
FOR I IN array_lower(groups, 1)..array_upper(groups, 1) LOOP
  groups_formated[I] = replace(groups[I], '''', '''''');
END LOOP;
FOR I IN array_lower(terms, 1)..array_upper(terms, 1) LOOP
  terms_formated[I] = replace(terms[I], '''', '''''');
END LOOP;

RETURN QUERY EXECUTE format('
SELECT v1.word, v2.word
FROM grouping_pq(ARRAY(SELECT id FROM %s WHERE word = ANY (''%s'')),ARRAY(SELECT id FROM %s WHERE word = ANY (''%s''))) AS (token_id integer, group_id integer)
INNER JOIN %s AS v1 ON v1.id = token_id
INNER JOIN %s AS v2 ON v2.id = group_id
', table_name, terms_formated, table_name, groups_formated, table_name, table_name);
END
$$
LANGUAGE plpgsql;


-- Tokenization + Normalization
CREATE OR REPLACE FUNCTION tokenize(input text, OUT result float4[]) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE format('
SELECT vec_normalize(centroid(ARRAY(SELECT vector FROM %s WHERE word = ANY(regexp_split_to_array(''%s'', '' '')))::float4[]))
', table_name, replace(input, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- Tokenization without normalization
CREATE OR REPLACE FUNCTION tokenize_raw(input text, OUT result float4[]) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
EXECUTE format('
SELECT centroid(ARRAY(SELECT vector FROM %s WHERE word = ANY(regexp_split_to_array(''%s'', '' '')))::float4[])
', table_name, replace(input, '''', '''''')) INTO result;
END
$$
LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION cosine_similarity(varchar(100), varchar(100), )
