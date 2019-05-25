import collections
import confuse
import csv
import gensim
import logging
import nltk
import pickle
import sys
from commentexpansion import CommentLoader
from dialogueactclassification import Classifier
from manuallabeling import FileGenerator
# from gensim.utils import simple_preprocess
from nltk.metrics.scores import precision, recall
from nltk.stem import WordNetLemmatizer, SnowballStemmer
#from nltk.stem.porter import *


def main():
    cfg = confuse.LazyConfig('pullrequestcommentanalyzer', __name__)
    cfg.set_file('./pullrequestcommentanalyzer/config.yaml')
    # Override config for the workspace.
    cfg.set_file('./pullrequestcommentanalyzer/config.workspace.yaml')

    # Setting up logging.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s, %(levelname)s, %(name)s, %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z',
        handlers=[logging.StreamHandler(), logging.FileHandler(filename=cfg['log_file'].as_filename(), mode='a')])
    logger = logging.getLogger('pullrequestcommentanalyzer')
    logger.info('Program started.')

    pull_request_comments_csv_file = cfg['bigquery']['pull_request_comments_results_csv_file'].as_filename(
    )

    commentLoader = CommentLoader(cfg['ghtorrent_mongodb']['ssh_tunnel_host'].get(),
                                  cfg['ghtorrent_mongodb']['ssh_tunnel_port'].get(
                                      int),
                                  cfg['ghtorrent_mongodb']['ssh_username'].get(),
                                  cfg['ghtorrent_mongodb']['ssh_private_key'].get(),
                                  cfg['ghtorrent_mongodb']['ssh_private_key_password'].get(),
                                  cfg['ghtorrent_mongodb']['host'].get(),
                                  cfg['ghtorrent_mongodb']['port'].get(int),
                                  cfg['ghtorrent_mongodb']['username'].get(),
                                  cfg['ghtorrent_mongodb']['password'].get(),
                                  cfg['ghtorrent_mongodb']['database'].get())
    # TODO remove this.
    commentLoader.Load('opendatakit', 'collect', 279, 90929163)

    if cfg['dialogue_act_classification']['manual_labeling']['generate_csv_file'].get(bool) == True:
        manual_label_file_generator = FileGenerator(pull_request_comments_csv_file,
                                                    cfg['dialogue_act_classification']['manual_labeling']['csv_file'].as_filename(
                                                    ),
                                                    cfg['dialogue_act_classification']['manual_labeling']['random_sample_line_numbers_csv_file'].as_filename())
        sys.exit()
        manual_label_file_generator.generate()

    # Use the model to classify unlabeled data (BigQuery results from the CSV file).
    comments = collections.defaultdict(set)
    with open(pull_request_comments_csv_file, mode='r', encoding='utf-8') as input_csvfile:
        dict_reader = csv.DictReader(input_csvfile, delimiter=',')

        if cfg['perform_dialogue_act_classification'].get(bool) == True:
            dac_classifier = Classifier(cfg['dialogue_act_classification']['trained_classifier_file'].as_filename(),
                                        cfg['dialogue_act_classification']['train_classifier'].get(
                                            bool),
                                        cfg['dialogue_act_classification']['test_set_percentage'].as_number())

            classified_output_csv_file = cfg['dialogue_act_classification']['classified_output_csv_file'].as_filename(
            )
            logger.info(
                f'Performing Dialogue Act Classification and exporting to {classified_output_csv_file}')

            with open(classified_output_csv_file, mode='w', newline='', encoding='utf-8') as output_csvfile:
                # Add a new column of the NLP classification
                field_names = dict_reader.fieldnames + \
                    ['dialogue_act_classification']
                csv_writer = csv.DictWriter(
                    output_csvfile, field_names, delimiter=',')
                csv_writer.writeheader()

                for row in dict_reader:
                    dialogue_act_classification = dac_classifier.classify(
                        row['body'])
                    row['dialogue_act_classification'] = dialogue_act_classification
                    csv_writer.writerow(row)

        # Seek the file back to the start in order to use dict_reader again.
        input_csvfile.seek(0)
        next(dict_reader)  # Skip header row.

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
