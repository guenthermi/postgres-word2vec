#  Postgres Extension for Word Embeddings

## Requirements
At first you need to setup a [Postgres server](https://www.postgresql.org/). You have to install [faiss](https://github.com/facebookresearch/faiss) and a few other python libraries to run the import scripts.
## Setup
To build the extension you have to switch to the "word2vec_extension" folder. Here you can run `sudo make install` to build the shared libary and install the extension into the postgres server. Hereafter you can add the extension in psql by running `CREATE EXTENSION word2vec;`
To use the extension you have to provide word embeddings. The recommendation here is the [word2vec dataset from google news](https://drive.google.com/file/d/0B7XkCwpI5KDYNlNUTTlSS21pQmM/edit?usp=sharing). You have to download the dataset and put it into the "vectors" folder. After that you can transform it into a text format py running the "transform_vecs.py" script in the "index_creation" folder. Then you can fill the database with the vectors by running vec2database.py [name of your database]. You need to configure the script by changing username, password and host of your database in the constants.
