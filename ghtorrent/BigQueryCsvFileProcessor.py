import cld2
import logging
import math
import pandas
from commentexpansion import CommentLoader
from csv import DictReader, DictWriter
from dialogueactclassification import Classifier
from pathlib import Path


class BigQueryCsvFileProcessor:
    """A file processor that iterate through all rows in the given BigQuery result .csv file and determine whether a comment is truncated.
    If so, will load using :class:`CommentLoader`.
    Also, it determines the language of a comment, and skip rows that are not in English.

    Args:
        comment_loader (CommentLoader): An instance of comment loader.
        dac_classifier (Classifier): An instance of Dialogue Act Classification classifier.
    """

    def __init__(self, comment_loader: CommentLoader, dac_classifier: Classifier):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.comment_loader = comment_loader
        self.dac_classifier = dac_classifier

    def process(self, csv_file):
        """Process the given BigQuery result .csv file.

        Args:
            csv_file: File path that points to the .csv file to be processed.
        """
        final_csv = Path(csv_file.replace('.csv', '_cleaned_classified.csv'))
        final_stats_csv = Path(csv_file.replace(
            '.csv', '_cleaned_classified_stats.csv'))

        if final_csv.exists():
            self.logger.info(
                f'Processed file already exists, stop further processing: {final_csv}')
            return

        self.logger.info(f'Start processing {csv_file}...')

        tmp_csv = Path(csv_file.replace('.csv', '_processing.csv'))
        tmp_stats_csv = Path(
            csv_file.replace('.csv', '_processing_stats.csv'))

        data_frame = pandas.read_csv(csv_file)
        total_rows = data_frame.shape[0]
        self.logger.info(f'Number of rows in {csv_file}: {total_rows}')

        ctr = 0
        truncated_ctr = 0
        deleted_ctr = 0
        non_eng_ctr = 0
        skip_ctr = 0
        progress_pct = 0
        temp_stats_df = None

        if tmp_csv.exists():
            temp_total_rows = pandas.read_csv(tmp_csv).shape[0]
            temp_stats_df = pandas.read_csv(tmp_stats_csv)
            ctr = temp_stats_df['rows_processed'].iat[0]
            truncated_ctr = temp_stats_df['comments_truncated'].iat[0]
            deleted_ctr = temp_stats_df['deleted'].iat[0]
            non_eng_ctr = temp_stats_df['non_english'].iat[0]
            skip_ctr = temp_stats_df['skipped'].iat[0]

            self.logger.warn(
                f'The file {tmp_csv.name} already exists, no. of rows in the file: {temp_total_rows}, no. of rows processed: {ctr}. Resuming processing...')
        else:
            stats = {'rows_processed': [0], 'comments_truncated': [
                0], 'deleted': [0], 'non_english': [0], 'skipped': [0]}
            temp_stats_df = pandas.DataFrame(data=stats)

        # Skip any previously processed rows, but do not skip the header.
        data_frame = pandas.read_csv(
            csv_file, chunksize=100, converters={'body': str}, skiprows=range(1, ctr + 1))
        for chunk in data_frame:
            # Get chunk size first before any filtering.
            chunk_size = chunk.shape[0]

            # Add new columns
            chunk = self.__get_header_fields(chunk)

            # Process comments.
            chunk[['body', 'is_eng', 'comment_from_mongodb', 'is_deleted']] = chunk.apply(
                lambda row: self.__load_comment(
                    row['body'],
                    row['project_url'],
                    int(row['pullreq_id']),
                    int(row['comment_id'])),
                axis=1)

            truncated_ctr += len(chunk[chunk['comment_from_mongodb']
                                       == True].index)
            non_eng_ctr += len(chunk[chunk['is_eng'] == False].index)
            deleted_ctr += len(chunk[chunk['is_deleted'] == True].index)
            skip_ctr += len(chunk[chunk['body'].isnull()].index)

            # Remove unused columns, and filter rows.
            chunk.drop(columns='is_eng', inplace=True)
            chunk.drop(columns='is_deleted', inplace=True)
            chunk = chunk[chunk['body'].notnull()]

            if chunk.shape[0] > 0:
                chunk['dialogue_act_classification_ml'] = chunk.apply(
                    lambda row: self.dac_classifier.classify(row['body']), axis=1)

                chunk.to_csv(tmp_csv,
                            index=False,
                            header=False if ctr > 0 else True,
                            mode='w' if ctr == 0 else 'a')

            ctr += chunk_size

            # Save the counters for resume purpose.
            temp_stats_df['rows_processed'].iat[0] = ctr
            temp_stats_df['comments_truncated'].iat[0] = truncated_ctr
            temp_stats_df['deleted'].iat[0] = deleted_ctr
            temp_stats_df['non_english'].iat[0] = non_eng_ctr
            temp_stats_df['skipped'].iat[0] = skip_ctr
            temp_stats_df.to_csv(tmp_stats_csv,
                                 index=False, header=True, mode='w')

            # Progress precision: 0.01%.
            progress_pct_floor = math.floor(ctr / total_rows * 10000)
            if progress_pct_floor != progress_pct:
                progress_pct = progress_pct_floor
                self.logger.info(
                    f'Progress: {progress_pct / 100}%, row processed: {ctr}, comment truncated: {truncated_ctr}, deleted: {deleted_ctr}, non English: {non_eng_ctr}, skipped: {skip_ctr}')

        tmp_csv.rename(final_csv)
        tmp_stats_csv.rename(final_stats_csv)
        self.logger.info(f'Processing completed, output file: {final_csv}')

    def __get_header_fields(self, df: pandas.DataFrame):
        # Remove unused columns.
        df.drop(columns='description', inplace=True)
        df.drop(columns='latest_commit_date', inplace=True)
        df.drop(columns='medium_term_commit_count', inplace=True)
        df.drop(columns='medium_term_distinct_author_count', inplace=True)
        df.drop(columns='medium_term_distinct_committer_count', inplace=True)
        df.drop(columns='recent_commit_count', inplace=True)
        df.drop(columns='recent_distinct_author_count', inplace=True)
        df.drop(columns='recent_distinct_committer_count', inplace=True)
        df.drop(columns='latest_pull_request_history_date', inplace=True)
        df.drop(columns='medium_term_pull_request_count', inplace=True)
        df.drop(columns='recent_pull_request_count', inplace=True)
        df.drop(columns='project_language', inplace=True)
        df.drop(columns='project_language_details_language', inplace=True)
        df.drop(columns='project_language_bytes', inplace=True)
        df.drop(columns='language_percentage', inplace=True)
        df.drop(columns='project_language_created_at', inplace=True)
        df.drop(columns='forked_from', inplace=True)
        df.drop(columns='intra_branch', inplace=True)

        # Add new columns.
        df['comment_from_mongodb'] = ""
        df['pr_url'] = ""
        df['dialogue_act_classification_ml'] = ""
        df['dialogue_act_classification_manual_flag'] = ""
        df['dialogue_act_classification_manual'] = ""

        return df

    def __load_comment(self, comment: str, project_url: str, pullreq_id: int, comment_id: int):
        is_truncated = False
        is_eng = True
        is_deleted = False

        if self.__is_english(comment) is not True:
            # Comment detected as not in English, skip the row for further processing.
            is_eng = False
            comment = None
        elif len(comment) == 255:
            # Likely to be a truncated comment, load using CommentLoader.
            is_truncated = True
            owner = project_url.replace('https://api.github.com/repos/', '')
            owner = owner[0:owner.index('/')]
            repo = project_url[project_url.rfind('/') + 1:]

            comment = self.comment_loader.load(
                owner, repo, pullreq_id, comment_id)

            # Comment may have been deleted from GitHub, skip the row for further processing.
            is_deleted = False if comment is not None else True

        return pandas.Series([comment, is_eng, is_truncated, is_deleted])

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
