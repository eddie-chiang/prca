import collections
import csv
import gensim
import logging
import nltk
import pickle
import sys
import yaml
from dialogueactclassification import Classifier
from manuallabeling import FileGenerator
# from gensim.utils import simple_preprocess
from nltk.metrics.scores import precision, recall
from nltk.stem import WordNetLemmatizer, SnowballStemmer
#from nltk.stem.porter import *
from pathlib import Path

def main():
    with open(Path('./pullrequestcommentanalyzer/config.yaml'), 'r') as config_file:
        cfg = yaml.safe_load(config_file)

    # Setting up logging.
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s, %(levelname)s, %(name)s, %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S %z',
        handlers=[logging.StreamHandler(), logging.FileHandler(filename=Path(cfg['log_file']), mode='a')])
    logger = logging.getLogger('pullrequestcommentanalyzer')
    logger.info('Program started.')
   
    pull_request_comments_csv_file = Path(cfg['dialogue_act_classification']['pull_request_comments_csv_file'])

    if cfg['dialogue_act_classification']['manual_labeling']['generate_csv_file'] == True:
        manual_label_file_generator = FileGenerator(pull_request_comments_csv_file, 
                                        Path(cfg['dialogue_act_classification']['manual_labeling']['csv_file']),
                                        Path(cfg['dialogue_act_classification']['manual_labeling']['random_sample_line_numbers_csv_file']))
        manual_label_file_generator.generate()    

    # Use the model to classify unlabeled data (BigQuery results from the CSV file).
    comments = collections.defaultdict(set)
    with open(pull_request_comments_csv_file, mode='r', encoding='utf-8') as input_csvfile:
        dict_reader = csv.DictReader(input_csvfile, delimiter=',')

        if cfg['perform_dialogue_act_classification'] == True:
            dac_classifier = Classifier(Path(cfg['dialogue_act_classification']['trained_classifier_file']), 
                                cfg['dialogue_act_classification']['train_classifier'],
                                cfg['dialogue_act_classification']['test_set_percentage'])

            classified_output_csv_file = Path(cfg['dialogue_act_classification']['classified_output_csv_file'])
            logger.info(f'Performing Dialogue Act Classification and exporting to {classified_output_csv_file}')
                                
            with open(classified_output_csv_file, mode='w', newline='', encoding='utf-8') as output_csvfile:      
                # Add a new column of the NLP classification
                field_names = dict_reader.fieldnames + ['dialogue_act_classification']
                csv_writer = csv.DictWriter(output_csvfile, field_names, delimiter=',')
                csv_writer.writeheader()                               

                for row in dict_reader:
                    dialogue_act_classification = dac_classifier.classify(row['body'])
                    row['dialogue_act_classification'] = dialogue_act_classification
                    csv_writer.writerow(row)

        input_csvfile.seek(0) # Seek the file back to the start in order to use dict_reader again.
        next(dict_reader) # Skip header row.                
        
        for row in dict_reader:
            comments[row['comment_id']] = row['body']

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

    logger.info('Program ended.')

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
    
# Execute only if run as a script
if __name__ == "__main__":
    main()