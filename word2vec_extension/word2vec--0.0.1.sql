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
