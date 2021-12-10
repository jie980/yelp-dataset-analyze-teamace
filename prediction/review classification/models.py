from typing_extensions import final
import torch.nn as nn
import torch

class FastText(nn.Module):
    def __init__(self, vocab_size, embedding_dim, num_classes, word_embeddings=None):
        """
        :param vocab_size: the number of different embeddings to make (need one embedding for every unique word).
        :param embedding_dim: the dimension of each embedding vector.
        :param num_classes: the number of target classes.
        :param word_embeddings: optional pre-trained word embeddings. If not given word embeddings are trained from
        random initialization. If given then provided word_embeddings are used and the embeddings are not trained.
        """
        super().__init__()
        self.embeddings = nn.Embedding(vocab_size,embedding_dim, padding_idx=0)
        if word_embeddings is not None:
            self.embeddings = self.embeddings.from_pretrained(word_embeddings, freeze=True, padding_idx=0)
        self.W = nn.Linear(embedding_dim, 512)
        self.relu = nn.ReLU()
        self.W1 = nn.Linear(512,128)
        self.W2 = nn.Linear(128, num_classes)
        self.lstm = nn.LSTM(embedding_dim,embedding_dim,batch_first=True)
    def forward(self, x):
        """
        :param x: a LongTensor of shape [batch_size, max_sequence_length]. Each row is one sequence (movie review),
        the i'th element in a row is the (integer) ID of the i'th token in the original text.
        :return: a FloatTensor of shape [batch_size, num_classes]. Predicted class probabilities for every sequence
        in the batch.
        """
        result = self.embeddings(x)
        sequence = torch.sum(result!=0,1)
        pack = torch.nn.utils.rnn.pack_padded_sequence(result, sequence[:,0], batch_first=True,enforce_sorted= False)
        aggr_result,state = self.lstm(pack)
        x = self.relu(self.W1(state[0][-1,:,:]))
        final_result = self.relu(self.W2(x))
        return final_result;        

class LSTM_multilayer(nn.Module):
    def __init__(self, vocab_size, embedding_dim, num_classes, word_embeddings=None):
        """
        :param vocab_size: the number of different embeddings to make (need one embedding for every unique word).
        :param embedding_dim: the dimension of each embedding vector.
        :param num_classes: the number of target classes.
        :param word_embeddings: optional pre-trained word embeddings. If not given word embeddings are trained from
        random initialization. If given then provided word_embeddings are used and the embeddings are not trained.
        """
        super().__init__()
        self.embeddings = nn.Embedding(vocab_size,embedding_dim, padding_idx=0)
        if word_embeddings is not None:
            self.embeddings = self.embeddings.from_pretrained(word_embeddings, freeze=True, padding_idx=0)
        self.W = nn.Linear(embedding_dim, embedding_dim)
        self.W1 = nn.Linear(embedding_dim, num_classes)

        self.lstm = nn.LSTM(embedding_dim,embedding_dim,batch_first=True)
    def forward(self, x):
        """
        :param x: a LongTensor of shape [batch_size, max_sequence_length]. Each row is one sequence (movie review),
        the i'th element in a row is the (integer) ID of the i'th token in the original text.
        :return: a FloatTensor of shape [batch_size, num_classes]. Predicted class probabilities for every sequence
        in the batch.
        """
        result = self.embeddings(x)
        sequence = torch.sum(result!=0,1)
        pack = torch.nn.utils.rnn.pack_padded_sequence(result, sequence[:,0], batch_first=True,enforce_sorted= False)
        aggr_result,state = self.lstm(pack)

        final_result = self.W(state[0][-1,:,:])
        final_result = self.W1(state[0][-1,:,:])
        return final_result;  

        

        
