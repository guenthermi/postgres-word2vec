-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION word2vec" to load this file. \quit

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
AS '$libdir/word2vec', 'cosine_similarity'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cosine_similarity_norm(float4[], float4[]) RETURNS float8
AS '$libdir/word2vec', 'cosine_similarity_norm'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_minus(anyarray, anyarray) RETURNS anyarray
AS '$libdir/word2vec', 'vec_minus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_plus(anyarray, anyarray) RETURNS anyarray
AS '$libdir/word2vec', 'vec_plus'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION vec_normalize(anyarray) RETURNS anyarray
AS '$libdir/word2vec', 'vec_normalize'
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

CREATE OR REPLACE FUNCTION grouping_pq_to_id(integer[], anyarray) RETURNS SETOF record
AS '$libdir/word2vec', 'grouping_pq_to_id'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pq_search_in_cplx(anyarray, integer, varchar(100)[]) RETURNS SETOF record
AS '$libdir/word2vec', 'pq_search_in_cplx'
LANGUAGE C IMMUTABLE STRICT;



CREATE OR REPLACE FUNCTION centroid(anyarray) RETURNS anyarray
AS '$libdir/word2vec', 'centroid'
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

CREATE OR REPLACE FUNCTION cosine_similarity(term1 varchar(100), term2 varchar(100)) RETURNS float8 AS
$$ SELECT cosine_similarity(t1.vector, t2.vector) FROM google_vecs_norm as t1, google_vecs_norm AS t2 WHERE (t1.word = term1) AND (t2.word = term2);
$$
LANGUAGE sql IMMUTABLE;

-- CREATE OR REPLACE FUNCTION cosine_similarity_norm(anyarray, anyarray) RETURNS float8
-- AS '$libdir/word2vec', 'cosine_similarity_norm'
-- LANGUAGE C IMMUTABLE STRICT;

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
', table_name, table_name, replace(term, '''', ''''''), input_set, k);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_k_in(term varchar(100), k integer, input_set varchar(100)[]) RETURNS TABLE (word varchar(100), similarity float8) AS $$
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

-- w3 - w1 + w2
CREATE OR REPLACE FUNCTION CosAdd(w1 varchar(100), w2 varchar(100), w3 varchar(100)) RETURNS TABLE (word varchar(100), similarity float8)
AS  $$
SELECT v4.word, cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) FROM google_vecs AS v4
INNER JOIN google_vecs AS v1 ON v1.word = w1
INNER JOIN google_vecs AS v2 ON v2.word = w2
INNER JOIN google_vecs AS v3 ON v3.word = w3
ORDER BY cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 5 ROWS ONLY;
$$
LANGUAGE sql IMMUTABLE;

-- with postverification
CREATE OR REPLACE FUNCTION CosAdd_pq(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
SELECT pq_quantization.word FROM
google_vecs AS v1,
google_vecs AS v2,
google_vecs AS v3,
pq_search(vec_normalize(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector)), 100) AS (idx integer, distance float4)
INNER JOIN pq_quantization ON idx = pq_quantization.id
INNER JOIN google_vecs AS v4 ON v4.word = pq_quantization.word
WHERE (v1.word = w1)
AND (v2.word = w2)
AND (v3.word = w3)
AND (pq_quantization.word != v1.word)
AND (pq_quantization.word != v2.word)
AND (pq_quantization.word != v3.word)
ORDER BY cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY;
$$
LANGUAGE sql IMMUTABLE;

-- with postverification
CREATE OR REPLACE FUNCTION CosAdd_ivfadc(w1 varchar(100), w2 varchar(100), w3 varchar(100), OUT result varchar(100))
AS  $$
SELECT fine_quantization.word FROM
google_vecs AS v1,
google_vecs AS v2,
google_vecs AS v3,
ivfadc_search(vec_normalize(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector)), 1000) AS (idx integer, distance float4)
INNER JOIN  fine_quantization ON idx = fine_quantization.id
INNER JOIN google_vecs AS v4 ON v4.word = fine_quantization.word
WHERE (v1.word = w1)
AND (v2.word = w2)
AND (v3.word = w3)
AND (fine_quantization.word != v1.word)
AND (fine_quantization.word != v2.word)
AND (fine_quantization.word != v3.word)
ORDER BY cosine_similarity(vec_plus(vec_minus(v3.vector, v1.vector), v2.vector), v4.vector) DESC
FETCH FIRST 1 ROWS ONLY;
$$
LANGUAGE sql IMMUTABLE;

-- index makes no sense
CREATE OR REPLACE FUNCTION grouping_func(terms varchar(100)[], groups varchar(100)[]) RETURNS TABLE (term varchar(100), groupterm varchar(100)) AS
$$
SELECT v1.word, gt.word
FROM google_vecs_norm AS v1,
top_k_in(v1.word, 1, groups) AS gt
WHERE v1.word = ANY(terms);
$$
LANGUAGE sql IMMUTABLE;


-- Tokenization + Normalization
CREATE OR REPLACE FUNCTION tokenize(input text) RETURNS float4[] AS
$$ SELECT vec_normalize(centroid(ARRAY(SELECT vector FROM google_vecs_norm WHERE word = ANY(regexp_split_to_array(input, ' ')))::float4[]));
$$
LANGUAGE sql IMMUTABLE;

-- Tokenization without normalization
CREATE OR REPLACE FUNCTION tokenize_raw(input text) RETURNS float4[] AS
$$ SELECT centroid(ARRAY(SELECT vector FROM google_vecs_norm WHERE word = ANY(regexp_split_to_array(input, ' ')))::float4[]);
$$
LANGUAGE sql IMMUTABLE;


-- CREATE OR REPLACE FUNCTION cosine_similarity(varchar(100), varchar(100), )
