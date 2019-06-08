import logging
import math
from commentexpansion import CommentLoader
from csv import DictReader, DictWriter
from dialogueactclassification import Classifier
from nltk.classify import textcat


class BigQueryCsvFileProcessor:
    """A file processor that iterate through all rows in the given BigQuery result .csv file and determine whether a comment is truncated.
    If so, will load using :class:`CommentLoader`.
    Also, it determines the language of a comment, and skip rows that are not in English.

    Args:
        csv_file (Path): A Path object that points to the .csv file to be processed.
        comment_loader: An instance of comment loader.
        dac_classifier: An instance of Dialogue Act Classification classifier.
    """

    def __init__(self, csv_file, comment_loader: CommentLoader, dac_classifier: Classifier):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.csv_file = csv_file
        self.comment_loader = comment_loader
        self.dac_classifier = dac_classifier

    def process(self):
        dst_csv_file = self.csv_file.replace('.csv', '_cleaned_classified.csv')
        with open(self.csv_file, mode='r', encoding='utf-8') as input, open(dst_csv_file, mode='w', encoding='utf-8') as output:
            dict_reader = DictReader(input, delimiter=',')

            total_rows = sum(1 for row in dict_reader)
            ctr = 0
            truncated_ctr = 0
            skip_ctr = 0
            progress_pct = 0

            # Seek the file back to the start in order to use dict_reader again.
            input.seek(0)
            next(dict_reader)  # Skip header row.

            self.logger.info(
                f'Number of rows in {self.csv_file}: {total_rows}')
            self.logger.info(f'Start generating {dst_csv_file}...')

            writer = DictWriter(output, self.__get_header_fields(
                dict_reader.fieldnames), delimiter=',')
            writer.writeheader()

            for row in dict_reader:
                row, is_truncated = self.__process_row(row)

                if row is not None:
                    writer.writerow(row)
                else:
                    skip_ctr += 1

                if is_truncated:
                    truncated_ctr += 1

                ctr += 1

                progress_pct_floor = math.floor(ctr / total_rows * 10000)
                if progress_pct_floor != progress_pct:
                    progress_pct = progress_pct_floor
                    self.logger.info(
                        f'Progress: {progress_pct / 100}%, row processed: {ctr}, comment truncated: {truncated_ctr}, rows skipped: {skip_ctr}')

            self.logger.info(
                f'Processing completed, output file: {dst_csv_file}')

    def __get_header_fields(self, field_names: list):
        field_names = field_names + ['comment_from_mongodb'] + ['pr_url'] + ['dialogue_act_classification_ml'] + [
            'dialogue_act_classification_manual'] + ['dialogue_act_classification_manual_flag']

        # Remove unused columns.
        del field_names[field_names.index('description')]
        del field_names[field_names.index('latest_commit_date')]
        del field_names[field_names.index('medium_term_commit_count')]
        del field_names[field_names.index('medium_term_distinct_author_count')]
        del field_names[field_names.index(
            'medium_term_distinct_committer_count')]
        del field_names[field_names.index('recent_commit_count')]
        del field_names[field_names.index('recent_distinct_author_count')]
        del field_names[field_names.index('recent_distinct_committer_count')]
        del field_names[field_names.index('latest_pull_request_history_date')]
        del field_names[field_names.index('medium_term_pull_request_count')]
        del field_names[field_names.index('recent_pull_request_count')]
        del field_names[field_names.index('project_language')]
        del field_names[field_names.index('project_language_details_language')]
        del field_names[field_names.index('project_language_bytes')]
        del field_names[field_names.index('language_percentage')]
        del field_names[field_names.index('project_language_created_at')]
        del field_names[field_names.index('forked_from')]
        del field_names[field_names.index('intra_branch')]

        return field_names

    def __process_row(self, row):
        is_truncated = False

        if len(row['body']) == 255:
            # Likely to be a truncated comment, load using CommentLoader.
            is_truncated = True
            row['comment_from_mongodb'] = True
            owner = row['project_url'].replace(
                'https://api.github.com/repos/', '')
            owner = owner[0:owner.index('/')]
            repo = row['project_url'][row['project_url'].rfind('/') + 1:]
            loaded_comment = self.comment_loader.load(
                owner, repo, int(row['pullreq_id']), int(row['comment_id']))
            if loaded_comment is not None:
                row['body'] = loaded_comment
            else:
                # Comment may have been deleted from GitHub, skip the row for further processing.
                return None, is_truncated

        if self.__detect_language(row['body']) != "eng":
            # Comment detected as not in English, skip the row for further processing.
            return None, is_truncated

        row['dialogue_act_classification_ml'] = self.dac_classifier.classify(
            row['body'])

        # Remove unused columns.
        del row['description']
        del row['latest_commit_date']
        del row['medium_term_commit_count']
        del row['medium_term_distinct_author_count']
        del row['medium_term_distinct_committer_count']
        del row['recent_commit_count']
        del row['recent_distinct_author_count']
        del row['recent_distinct_committer_count']
        del row['latest_pull_request_history_date']
        del row['medium_term_pull_request_count']
        del row['recent_pull_request_count']
        del row['project_language']
        del row['project_language_details_language']
        del row['project_language_bytes']
        del row['language_percentage']
        del row['project_language_created_at']
        del row['forked_from']
        del row['intra_branch']

        return row, is_truncated

    def __detect_language(self, comment):
        text_cat = textcat.TextCat()
        language = text_cat.guess_language(comment)
        return language
