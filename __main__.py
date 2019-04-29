###########################################################
# Pre-requisites
# Python 3.7
# pip install -U nltk
# pip install -U gensim 
###########################################################
import collections
import csv
import gensim
import logging
import nltk
import pickle
import sys
import yaml
# from gensim.utils import simple_preprocess
from nltk.metrics.scores import precision, recall
from nltk.stem import WordNetLemmatizer, SnowballStemmer
#from nltk.stem.porter import *
from pathlib import Path

with open(Path('./pullrequestcommentanalyzer/config.yaml'), 'r') as config_file:
    cfg = yaml.safe_load(config_file)

###########################################################
# Processing
###########################################################

# Methods Declaration #####################################
# Define a simple feature extractor that checks what words the post contains.
def dialogue_act_features(post):
    features = {}
    for word in nltk.word_tokenize(post):
        features['contains({})'.format(word.lower())] = True
    return features

def lemmatize_stemming(text):
    stemmer = SnowballStemmer('english')
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

def topic_modelling_preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        # All stopwords are removed, and words that have fewer than 3 characters are removed.
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

# Setting up logging.
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s, %(levelname)s, %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S %z',
    handlers=[logging.StreamHandler(), logging.FileHandler(filename=Path(cfg['log_file']), mode='a')])
logging.info('Program started.')

trained_dialogue_act_classifier_file = Path(cfg['dialogue_act_classification']['trained_dialogue_act_classifier'])

if trained_dialogue_act_classifier_file.is_file():
    with open(trained_dialogue_act_classifier_file, mode="rb") as f:
        dialogue_act_classifier = pickle.load(f)
        logging.info('Loaded trained dialogue act classifier.')
else:
    # Extract the labeled basic messaging data.
    #posts = nltk.corpus.nps_chat.xml_posts()[:10000]
    posts = nltk.corpus.nps_chat.xml_posts()
    logging.info(f'Loaded {len(posts)} posts from nps_chat corpus.')

    # Construct the training and test data by applying the feature extractor to each post, and create a new classifier.
    featuresets = [(dialogue_act_features(post.text), post.get('class'))
                    for post in posts]
    size = int(len(featuresets) * 0.1) # 10% to use as Training Set, 90% to use Test Set.
    train_set, test_set = featuresets[size:], featuresets[:size]
    logging.info('Size of feature set: %d, train on %d instances, test on %d instances.' % (len(featuresets), len(train_set), len(test_set)))

    # Train the dialogue act classifier.
    dialogue_act_classifier = nltk.NaiveBayesClassifier.train(train_set)
    logging.info('Accuracy: {}%.'.format(round(nltk.classify.accuracy(dialogue_act_classifier, test_set) * 100, 4)))

    # Getting the Precision (% of prediction that are correct), and Recall (% of that identifies the class accurately).
    refsets = collections.defaultdict(set)
    testsets = collections.defaultdict(set)

    for i, (features, class_label) in enumerate(test_set):
        refsets[class_label].add(i)
        observed = dialogue_act_classifier.classify(features)
        testsets[observed].add(i)

    for class_label in refsets:
        precision_rate = precision(refsets[class_label], testsets[class_label])
        recall_rate = recall(refsets[class_label], testsets[class_label])
        if not isinstance(precision_rate, float):
            precision_rate = 0
        if not isinstance(recall_rate, float):
            recall_rate = 0
        logging.info(f'Precision ({class_label}): {round(precision_rate * 100, 4)}%.')
        logging.info(f'Recall ({class_label}): {round(recall_rate * 100, 4)}%.')

    with open(trained_dialogue_act_classifier_file, mode="wb") as f:
        pickle.dump(dialogue_act_classifier, f)
        logging.info('Saved trained dialogue act classifier.')

# Use the model to classify unlabeled data (BigQuery results from the CSV file).
comments = collections.defaultdict(set)
with open(Path(cfg['dialogue_act_classification']['pull_request_comments_csv_file']), mode='r', encoding='utf-8') as input_csvfile:
    with open(Path(cfg['dialogue_act_classification']['classified_output_csv_file']), mode='w', newline='', encoding='utf-8') as output_csvfile:
        dict_reader = csv.DictReader(input_csvfile, delimiter=',')
                
        # Add a new column of the NLP classification
        field_names = dict_reader.fieldnames + ['dialogue_act_classification']
        csv_writer = csv.DictWriter(output_csvfile, field_names, delimiter=',')
        csv_writer.writeheader()

        for row in dict_reader:
            comment = row['body']
            comments[row['comment_id']] = comment

            if cfg['perform_classify'] == True:
                unlabeled_data_features = dialogue_act_features(comment)
                dialogue_act_classification = dialogue_act_classifier.classify(unlabeled_data_features)
                row['dialogue_act_classification'] = dialogue_act_classification
                csv_writer.writerow(row)

# Topic Modelling with Latent Dirichlet Allocation.
# Step 1. Tokenization: Split the text into sentences and the sentences into workds. Lowercase the words and remove punctuation.
# Step 2. Words that have fewer than 3 characters are removed.
# Step 3. All stopwords are removed.
# Step 4. Words are lemmatized - words in third person are changed to first person and verbs in past and future tenses are changed into present.
# Step 5. Words are stemmed - words are reduced to their root form.

# Bag of words (BoW)
processed_comments = []
for comment_id, comment in comments.items():
    processed_comments.append(topic_modelling_preprocess(comment))
bow_dict = gensim.corpora.Dictionary(processed_comments)

# Filter out tokens that appear in less than 15 documents, or more than 0.5 documents (fraction of total corpus size, not absolute number), 
# and then only keep the first 100,000 most frequent tokens.
bow_dict.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)

# For each document to report how many words and how many times those words appear.
bow_corpus = [bow_dict.doc2bow(doc) for doc in processed_comments]



# # Use Term Frequency-Inverse Document Frequency (TD-IDEF) to measure topic relevance.
# tfidf = gensim.models.TfidfModel()

logging.info('Program ended.')