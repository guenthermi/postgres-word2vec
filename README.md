#  FREDDY: Fast Word Embeddings in Database Systems

FREDDY is a system based on Postgres which is able to use word embeddings exhibit the rich information encoded in textual values. Database systems often contain a lot of textual values which express a lot of latent semantic information which can not be exploited by standard SQL queries. We developed a Postgres extension which provides UDFs for word embedding operations to compare textual values according to there syntactic and semantic meaning.      

## Word Embedding operations

### Similarity Queries
```
SELECT cosine_similarity('king', 'queen');

```
### Analogy Queries based on 3CosAdd
```
SELECT *
FROM cosadd_pq('Francis_Ford_Coppola', 'Godfather', 'Christopher_Nolan');

```
### K Nearest Neighbour Queries

```
SELECT *
FROM k_nearest_neighbour_ivfadc('birch', 5);
```

### K Nearest Neighbour Queries with Specific Output Set

```
SELECT * FROM
top_k_in_pq('Godfather', 5, ARRAY(SELECT title FROM movies));
```

### Grouping

```
SELECT term, groupterm
FROM grouping_func(ARRAY(SELECT title FROM movies), '{Europe,America}');
```

## Indexes

We implemented two types of index structures to accelerate word embedding operations. One index is based on [product quantization](http://ieeexplore.ieee.org/abstract/document/5432202/) and one on IVFADC (inverted file system with asymmetric distance calculation). Product quantization provides a fast approximated distance calculation. IVFADC is even faster and provides a non-exhaustive approach which also uses product quantization.

| Method                | Response Time | Precision     |
| --------------------- | ------------- | ------------- |
| Exact Search          | 8.69s         | 1.0           |
| Product Quantization  | 0.93s         | 0.30          |
| IVFADC                | 0.27s         | 0.31          |

**Parameters:**
* Number of subvectors per vector: 12
* Number of centroids for fine quantization (PQ and IVFADC): 256
* Number of centroids for coarse quantization: 1000

![time measurement](evaluation/time_measurment.png)

## Installation
At first, you need to setup a [Postgres server](https://www.postgresql.org/). You have to install [faiss](https://github.com/facebookresearch/faiss) and a few other python libraries to run the import scripts.

To build the extension you have to switch to the "word2vec_extension" folder. Here you can run `sudo make install` to build the shared library and install the extension into the Postgres server. Hereafter you can add the extension in PSQL by running `CREATE EXTENSION word2vec;`
To use the extension you have to provide word embeddings. The recommendation here is the [word2vec dataset from google news](https://drive.google.com/file/d/0B7XkCwpI5KDYNlNUTTlSS21pQmM/edit?usp=sharing). You have to download the dataset and put it into the "vectors" folder. After that, you can transform it into a text format py running the "transform_vecs.py" script in the "index_creation" folder. Then you can fill the database with the vectors by running vec2database.py [name of your database]. You need to configure the script by changing username, password and host of your database in the constants.
