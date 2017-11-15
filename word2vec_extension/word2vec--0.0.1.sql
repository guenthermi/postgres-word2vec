-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION word2vec" to load this file. \quit

CREATE OR REPLACE FUNCTION init(_tbl regclass) RETURNS void AS $$
BEGIN
EXECUTE format('CREATE OR REPLACE FUNCTION get_vecs_name() RETURNS regclass AS ''SELECT regclass ''''%s'''''' LANGUAGE sql IMMUTABLE', _tbl);
END
$$
LANGUAGE plpgsql;

SELECT CASE (SELECT count(proname) FROM pg_proc WHERE proname='get_vecs_name')
  WHEN 0 THEN init('google_vecs')
  END;


CREATE OR REPLACE FUNCTION cosine_similarity(anyarray, anyarray) RETURNS float8
AS '$libdir/word2vec', 'cosine_similarity'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cosine_similarity_norm(anyarray, anyarray) RETURNS float8
AS '$libdir/word2vec', 'cosine_similarity_norm'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_minus(anyarray, anyarray) RETURNS anyarray
AS '$libdir/word2vec', 'vec_minus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_plus(anyarray, anyarray) RETURNS anyarray
AS '$libdir/word2vec', 'vec_plus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search(anyarray, integer) RETURNS SETOF record
AS '$libdir/word2vec', 'pq_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION ivfadc_search(anyarray, integer) RETURNS SETOF record
AS '$libdir/word2vec', 'ivfadc_search'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search_in(anyarray, integer, integer[]) RETURNS SETOF record
AS '$libdir/word2vec', 'pq_search_in'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cluster_pq_to_id(integer[], integer) RETURNS SETOF record
AS '$libdir/word2vec', 'cluster_pq'
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
', table_name, table_name, term, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(term varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT fine_quantization.word, distance FROM google_vecs_norm AS gv, ivfadc_search(gv.vector, k) AS (idx integer, distance float4) INNER JOIN fine_quantization ON idx = fine_quantization.id WHERE gv.word = term;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_ivfadc(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT fine_quantization.word, distance FROM ivfadc_search(input_vector, k) AS (idx integer, distance float4) INNER JOIN fine_quantization ON idx = fine_quantization.id;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(term varchar(100), k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT pq_quantization.word AS word, distance AS distance FROM google_vecs_norm AS gv, pq_search(gv.vector, k) AS (idx integer, distance float4) INNER JOIN pq_quantization ON idx = pq_quantization.id WHERE gv.word = term;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION k_nearest_neighbour_pq(input_vector anyarray, k integer) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT pq_quantization.word AS word, distance AS distance FROM pq_search(input_vector, k) AS (idx integer, distance float4) INNER JOIN pq_quantization ON idx = pq_quantization.id;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION top_k_in_pq(term varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT pq_quantization.word, distance FROM google_vecs_norm AS gv, pq_search_in(gv.vector, k, input_set)
AS (result_id integer, distance float4) INNER JOIN pq_quantization ON result_id = pq_quantization.id
WHERE gv.word = term;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION top_k_in_pq(query_vector anyarray, k integer, input_set integer[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT pq_quantization.word, distance FROM pq_search_in(query_vector, k, input_set)
AS (result_id integer, distance float4) INNER JOIN pq_quantization ON result_id = pq_quantization.id;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION top_k_in_pq(term varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT pq_quantization.word, distance FROM google_vecs_norm AS gv, pq_search_in(gv.vector, k, ARRAY(SELECT id FROM google_vecs_norm WHERE word = ANY (input_set)))
AS (result_id integer, distance float4) INNER JOIN pq_quantization ON result_id = pq_quantization.id
WHERE gv.word = term;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION top_k_in_pq(query_vector anyarray, k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), squareDistance float4) AS
$$ SELECT pq_quantization.word, distance FROM pq_search_in(query_vector, k, ARRAY(SELECT id FROM google_vecs_norm WHERE word = ANY (input_set)))
AS (result_id integer, distance float4) INNER JOIN pq_quantization ON result_id = pq_quantization.id;
$$
LANGUAGE sql IMMUTABLE;

-- TopK_In Exakt
CREATE OR REPLACE FUNCTION top_k_in(term varchar(100), k integer, input_set integer[]) RETURNS TABLE (word varchar(100), similarity float8) AS $$
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
', table_name, table_name, term, input_set, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_k_in(term varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float8) AS $$
DECLARE
table_name varchar;
BEGIN
EXECUTE 'SELECT get_vecs_name()' INTO table_name;
RETURN QUERY EXECUTE format('
SELECT v2.word, cosine_similarity_norm(v1.vector, v2.vector) FROM %s AS v2
INNER JOIN %s AS v1 ON v1.word = ''%s''
WHERE v2.word = ANY (''%s''::varchar(100)[])
ORDER BY cosine_similarity_norm(v1.vector, v2.vector) DESC
FETCH FIRST %s ROWS ONLY
', table_name, table_name, term, input_set, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_pq(terms varchar(100)[], k integer) RETURNS  TABLE (words varchar(100)[]) AS
$$ SELECT array_agg(word) FROM google_vecs_norm, cluster_pq_to_id(ARRAY(SELECT id FROM google_vecs_norm WHERE word = ANY (terms)), k) AS (centroid float4[], ids int[]) WHERE id = ANY ((ids)::integer[]) GROUP BY centroid;
$$
LANGUAGE sql IMMUTABLE;

-- note: maybe different variances of analogy possible
CREATE OR REPLACE FUNCTION analogy(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
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
ORDER BY cosine_similarity(vec_minus(v1.vector, v2.vector), vec_minus(v3.vector, v4.vector)) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, w1, table_name, w2, table_name, w3) INTO result;
END
$$
LANGUAGE plpgsql;

-- works only with normalized vectors
CREATE OR REPLACE FUNCTION analogy2(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
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
ORDER BY cosine_similarity(vec_plus(vec_minus(v1.vector, v2.vector), v4.vector), v3.vector) DESC
FETCH FIRST 1 ROWS ONLY
', table_name, table_name, w1, table_name, w2, table_name, w3) INTO result;
END
$$
LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION cosine_similarity(varchar(100), varchar(100), )
