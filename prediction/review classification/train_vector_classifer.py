import os

from sklearn.linear_model import LogisticRegression
from data_loader import LabelledTextDS
from plotting import *

# dataset = LabelledTextDS(os.path.join('data', 'labelled_movie_reviews.csv'))
dataset = LabelledTextDS('mycsv.csv')
print(dataset)
train, valid, test = dataset.get_vector_representation()
#C has to keep a proper value
model = LogisticRegression(C = 3.75, max_iter= 100,class_weight='balanced')  # You can change the hyper-parameters of the model by passing args here
#change the dataloader, preprocessor, model above
model.fit(train[0], train[1])
train_accuracy = (model.predict(train[0]) == train[1]).astype(float).mean()
valid_accuracy = (model.predict(valid[0]) == valid[1]).astype(float).mean()
test_accuracy = (model.predict(test[0]) == test[1]).astype(float).mean()
print_accuracies((train_accuracy, valid_accuracy, test_accuracy))
