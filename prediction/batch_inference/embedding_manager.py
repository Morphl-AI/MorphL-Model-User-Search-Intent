import torchtext.vocab as vocab
import numpy as np
import torch as tr


class EmbeddingManager:

    def __init__(self):
        self.embedding = vocab.GloVe(name="6B", dim=100)

        self.device = tr.device(
            "cuda") if tr.cuda.is_available() else tr.device("cpu")

    def get_word_vector(self, word):
        index = self.embedding.stoi[word]
        return self.embedding.vectors[index]

    def get_words_embeddings(self, query):
        result = []

        for word in query.split(" "):
            word_vector = self.get_word_vector(word)
            if type(word_vector) is np.ndarray:
                word_vector = tr.from_numpy(word_vector)

            if len(word_vector.shape) == 1:
                word_vector = word_vector.unsqueeze(0)

            result.append(word_vector)

        result = tr.cat(result)
        result = result.unsqueeze(0).type(tr.FloatTensor).to(self.device)
        return result
