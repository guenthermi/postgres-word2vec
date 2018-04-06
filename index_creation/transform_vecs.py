#!/usr/bin/python3

from gensim.models.keyedvectors import KeyedVectors

VECTORS_LOCATION = '../vectors/GoogleNews-vectors-negative300.bin'
OUTPUT_LOCATION = '../vectors/google_vecs.txt'

word_vectors = KeyedVectors.load_word2vec_format(VECTORS_LOCATION, binary=True)
word_vectors.save_word2vec_format(OUTPUT_LOCATION, binary=False)
