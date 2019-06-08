import cld2
import logging
import math
from commentexpansion import CommentLoader
from csv import DictReader, DictWriter
from dialogueactclassification import Classifier
from playsound import playsound
from pathlib import Path


class BigQueryCsvFileProcessor:
    """A file processor that iterate through all rows in the given BigQuery result .csv file and determine whether a comment is truncated.
    If so, will load using :class:`CommentLoader`.
    Also, it determines the language of a comment, and skip rows that are not in English.

    Args:
        comment_loader (CommentLoader): An instance of comment loader.
        dac_classifier (Classifier): An instance of Dialogue Act Classification classifier.
        error_alert_sound (str): A path pointing to the error alert sound.
    """

    def __init__(self, comment_loader: CommentLoader, dac_classifier: Classifier, error_alert_sound: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.comment_loader = comment_loader
        self.dac_classifier = dac_classifier
        self.error_alert_sound = error_alert_sound

    def process(self, csv_file):
        """Process the given BigQuery result .csv file.

        Args:
            csv_file: File path that points to the .csv file to be processed.
        """
        final_file = Path(csv_file.replace('.csv', '_cleaned_classified.csv'))

        if final_file.exists():
            self.logger.info(
                f'Processed file already exists, stop further processing: {final_file}')
            return

        self.logger.info(f'Start processing {csv_file}...')
        dst_csv_file = Path(csv_file.replace('.csv', '_processing.csv'))
        with open(csv_file, mode='r', encoding='utf-8') as input, open(dst_csv_file, mode='w', encoding='utf-8') as output:
            dict_reader = DictReader(input, delimiter=',')

            total_rows = sum(1 for row in dict_reader)
            ctr = 0
            truncated_ctr = 0
            non_eng_ctr = 0
            deleted_ctr = 0
            skip_ctr = 0
            progress_pct = 0

            # Seek the file back to the start in order to use dict_reader again.
            input.seek(0)
            next(dict_reader)  # Skip header row.

            self.logger.info(f'Number of rows in {csv_file}: {total_rows}')
            self.logger.info(f'Start generating {dst_csv_file}...')

            writer = DictWriter(output, self.__get_header_fields(
                dict_reader.fieldnames), delimiter=',')
            writer.writeheader()

            for row in dict_reader:
                row, is_eng, is_truncated, is_deleted = self.__process_row(row)

                if row is not None:
                    writer.writerow(row)
                else:
                    skip_ctr += 1

                if is_truncated:
                    truncated_ctr += 1

                if is_eng is not True:
                    non_eng_ctr += 1

                if is_deleted:
                    deleted_ctr += 1

                ctr += 1

                progress_pct_floor = math.floor(ctr / total_rows * 10000)
                if progress_pct_floor != progress_pct:
                    progress_pct = progress_pct_floor
                    self.logger.info(
                        f'Progress: {progress_pct / 100}%, row processed: {ctr}, comment truncated: {truncated_ctr}, deleted: {deleted_ctr}, non English: {non_eng_ctr}, skipped: {skip_ctr}')

        dst_csv_file.rename(final_file)
        self.logger.info(f'Processing completed, output file: {final_file}')

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
        is_eng = True
        is_deleted = False

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

        if self.__is_english(row['body']) is not True:
            # Comment detected as not in English, skip the row for further processing.
            return None, False, is_truncated, is_deleted

        if len(row['body']) == 255:
            # Likely to be a truncated comment, load using CommentLoader.
            is_truncated = True
            row['comment_from_mongodb'] = True
            owner = row['project_url'].replace(
                'https://api.github.com/repos/', '')
            owner = owner[0:owner.index('/')]
            repo = row['project_url'][row['project_url'].rfind('/') + 1:]
            pullreq_id = int(row['pullreq_id'])
            comment_id = int(row['comment_id'])

            try:              
                loaded_comment = self.comment_loader.load(
                    owner, repo, pullreq_id, comment_id)

                if loaded_comment is not None:
                    row['body'] = loaded_comment
                else:
                    # Comment may have been deleted from GitHub, skip the row for further processing.
                    is_deleted = True
                    return None, is_eng, is_truncated, is_deleted
            except Exception as e:
                playsound(self.error_alert_sound, False)
                self.logger.error(
                    f'Failed to load comment, owner: {owner}, repo: {repo}, pullreq_id: {pullreq_id}, comment_id: {comment_id}, error: {e}')
                # Return the row without dialogue act classification.
                return row, is_eng, is_truncated, is_deleted

        row['dialogue_act_classification_ml'] = self.dac_classifier.classify(
            row['body'])

        return row, is_eng, is_truncated, is_deleted

    def __is_english(self, comment):
        is_reliable, _, details = cld2.detect(comment)

        i = 0
        for detail in details:
            if i == 0 and is_reliable:
                # Top language is much better than the 2nd best language, so just rely on the first result.
                return True if detail.language_name == 'ENGLISH' else False
            elif detail.language_name == 'ENGLISH':
                # English being one of the top 3 probable language.
                return True
            i += 1

        return False
