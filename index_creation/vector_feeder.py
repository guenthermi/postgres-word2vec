#!/usr/bin/python3

class VectorFeeder:
    def __init__(self, vectors, words, cursor=0):
        self.data = vectors
        self.cursor = cursor
        self.words = words
        print('len words', len(self.words), 'len data', len(self.data))
    def get_next_batch(self, size):
        batch = self.data[self.cursor:(self.cursor+size)]
        word_batch = self.words[self.cursor:(self.cursor+size)]
        self.cursor += size
        return batch, word_batch
    def has_next(self):
        return self.cursor < len(self.data)
    def get_cursor(self):
        return self.cursor
