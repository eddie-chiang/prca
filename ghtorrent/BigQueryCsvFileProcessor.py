
import logging
import math
import pandas
import time
from commentprocessing import CommentLoader, LanguageDetector, GitHubPullRequestHelper
from csv import DictReader, DictWriter
from dialogueactclassification import Classifier
from pathlib import Path
from tqdm import tqdm


class BigQueryCsvFileProcessor:
    """A file processor that iterate through all rows in the given BigQuery result .csv file and determine whether a comment is truncated.
    If so, will load using :class:`CommentLoader`.
    Also, it determines the language of a comment, and skip rows that are not in English.

    Args:
        comment_loader (CommentLoader): An instance of comment loader.
        dac_classifier (Classifier): An instance of Dialogue Act Classification classifier.
        github_helper (GitHubPullRequestHelper): An instance GitHub Pull Request Helper.
    """

    def __init__(self, comment_loader: CommentLoader, dac_classifier: Classifier, github_helper: GitHubPullRequestHelper):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.comment_loader = comment_loader
        self.dac_classifier = dac_classifier
        self.github_helper = github_helper

    def process(self, csv_file: Path):
        """Process the given BigQuery result .csv file.

        Args:
            csv_file (Path): File path that points to the .csv file to be processed.

        Returns:
            tuple: (
                Path: The file path of the processed file.
                Path: The file containing the processing statistics.
            )
        """
        final_csv = Path(csv_file.absolute().as_posix().replace(
            '.csv', '_cleaned_classified.csv'))
        final_stats_csv = Path(csv_file.absolute().as_posix().replace(
            '.csv', '_cleaned_classified_stats.csv'))

        if final_csv.exists() and final_stats_csv.exists():
            self.logger.info(
                f'Processed file already exists, stop further processing: {final_csv}')
            return final_csv, final_stats_csv

        self.logger.info(f'Start processing {csv_file}...')

        tmp_csv = Path(csv_file.absolute().as_posix().replace(
            '.csv', '_processing.csv'))
        tmp_stats_csv = Path(
            csv_file.absolute().as_posix().replace('.csv', '_processing_stats.csv'))

        data_frame = pandas.read_csv(csv_file)
        total_rows = data_frame.shape[0]
        self.logger.info(f'No. of rows in {csv_file}: {total_rows}')

        ctr = truncated_ctr = del_from_mongo_ctr = del_from_github_ctr = non_eng_ctr = skip_ctr = 0
        tmp_stats_df = None

        if tmp_csv.exists():
            tmp_total_rows = pandas.read_csv(tmp_csv).shape[0]
            tmp_stats_df = pandas.read_csv(tmp_stats_csv)
            ctr = int(tmp_stats_df['rows_processed'].iat[0])
            truncated_ctr = int(tmp_stats_df['comments_truncated'].iat[0])
            non_eng_ctr = int(tmp_stats_df['non_english'].iat[0])
            del_from_mongo_ctr = int(
                tmp_stats_df['deleted_from_mongodb'].iat[0])
            del_from_github_ctr = int(
                tmp_stats_df['deleted_from_github'].iat[0])
            skip_ctr = int(tmp_stats_df['total_skipped'].iat[0])

            self.logger.warn(
                f'The file {tmp_csv.name} already exists, no. of rows in the file: {tmp_total_rows}, no. of rows processed: {ctr}. Resuming...')
        else:
            precisions, recalls = self.dac_classifier.get_precision_and_recall()
            stats = {
                'rows_processed': [0],
                'comments_truncated': [0],
                'non_english': [0],
                'deleted_from_mongodb': [0],
                'deleted_from_github': [0],
                'total_skipped': [0],
                'dialogue_act_classifier_accurracy': [self.dac_classifier.get_accuracy()],
                'dialogue_act_class_label': list(precisions.keys()),
                'dialogue_act_class_precision': list(precisions.values()),
                'dialogue_act_class_recall': list(recalls.values())
            }
            tmp_stats_df = pandas.DataFrame(
                data=dict([(key, pandas.Series(value)) for key, value in stats.items()]))

        pbar = tqdm(total=total_rows-ctr, desc='Process CSV')

        # Skip any previously processed rows, but do not skip the header.
        data_frame = pandas.read_csv(
            csv_file, chunksize=100, converters={'body': str}, skiprows=range(1, ctr + 1))
        for chunk in data_frame:
            # Get chunk size first before any filtering.
            chunk_size = chunk.shape[0]

            # Add new columns
            chunk = self.__get_header_fields(chunk)

            # Process comments.
            # Register `pandas.progress_apply` with `tqdm`.
            tqdm.pandas(desc='Load comment', leave=False)
            chunk[['body', 'is_eng', 'comment_from_mongodb', 'is_deleted_from_mongodb']] = chunk.progress_apply(
                lambda row: self.__load_comment(
                    row['body'],
                    row['project_url'],
                    int(row['pullreq_id']),
                    int(row['comment_id'])),
                axis='columns')

            tqdm.pandas(desc='Load pull request', leave=False)
            chunk[['pr_comments_cnt',
                   'pr_review_comments_cnt',
                   'pr_commits_cnt',
                   'pr_additions',
                   'pr_deletions',
                   'pr_changed_files',
                   'pr_merged_by_user_id']] = chunk.progress_apply(
                lambda row: self.__get_pull_request_info(row)
                if row['is_deleted_from_mongodb'] == False and row['is_eng'] == True
                else pandas.Series(['Not Available'] * 7),
                axis='columns')

            tqdm.pandas(desc='Load commit info', leave=False)
            chunk[['comment_author_association',
                   'comment_updated_at',
                   'comment_html_url',
                   'pr_commits_cnt_prior_to_comment',
                   'commit_file_status',
                   'commit_file_additions',
                   'commit_file_deletions',
                   'commit_file_changes']] = chunk.progress_apply(
                       lambda row: self.__get_comment_info(row)
                       if row['pr_commits_cnt'] not in ['Not Available', 'Not Found']
                       else pandas.Series(['Not Available'] * 8),
                axis='columns')

            truncated_ctr += len(
                chunk[chunk['comment_from_mongodb'] == True].index)
            non_eng_ctr += len(chunk[chunk['is_eng'] == False].index)
            del_from_mongo_ctr += len(
                chunk[chunk['is_deleted_from_mongodb'] == True].index)
            del_from_github_ctr += len(
                chunk[
                    (chunk['comment_html_url'] == 'Not Found')
                    | (chunk['pr_commits_cnt'] == 'Not Found')
                ].index)
            skip_ctr += len(
                chunk[
                    (chunk['comment_html_url'] == 'Not Available')
                    | (chunk['comment_html_url'] == 'Not Found')
                ].index)

            # Remove unused columns.
            chunk.drop(columns='is_eng', inplace=True)
            chunk.drop(columns='is_deleted_from_mongodb', inplace=True)

            # Filter comments that no longer exists.
            chunk = chunk[
                (chunk['body'].notnull())
                & (chunk['comment_html_url'] != 'Not Available')
                & (chunk['comment_html_url'] != 'Not Found')]

            if chunk.shape[0] > 0:
                tqdm.pandas(desc='Dialogue Act Classification', leave=False)
                chunk['dialogue_act_classification_ml'] = chunk.progress_apply(
                    lambda row:
                        self.dac_classifier.classify(row['body']),
                    axis=1)

                chunk.to_csv(tmp_csv,
                             index=False,
                             header=False if ctr > 0 else True,
                             mode='w' if ctr == 0 else 'a')

            ctr += chunk_size

            # Save the counters for resume purpose.
            tmp_stats_df['rows_processed'].iat[0] = ctr
            tmp_stats_df['comments_truncated'].iat[0] = truncated_ctr
            tmp_stats_df['non_english'].iat[0] = non_eng_ctr
            tmp_stats_df['deleted_from_mongodb'].iat[0] = del_from_mongo_ctr
            tmp_stats_df['deleted_from_github'].iat[0] = del_from_github_ctr
            tmp_stats_df['total_skipped'].iat[0] = skip_ctr
            tmp_stats_df.to_csv(tmp_stats_csv,
                                index=False, header=True, mode='w')

            pbar.update(chunk_size)
            pbar.write(
                f'Comment truncated: {truncated_ctr}, non English: {non_eng_ctr}, deleted from MongoDB/GitHub: {del_from_mongo_ctr}/{del_from_github_ctr}, total skipped: {skip_ctr}')

        pbar.close()

        tmp_csv.rename(final_csv)
        tmp_stats_csv.rename(final_stats_csv)
        self.logger.info(f'Processing completed, output file: {final_csv}')

        return final_csv, final_stats_csv

    def reprocess(self, processed_csv_file: Path):
        """Reprocess the given processed BigQuery result .csv file, to add any missing information.

        Args:
            processed_csv_file (Path): File path that points to the .csv file that was previously processed.

        Returns:
            Path: The file path of the processed file.
        """
        tmp_csv = Path(processed_csv_file.absolute(
        ).as_posix().replace('.csv', '_reprocessing.csv'))

        self.logger.info(f'Start reprocessing {processed_csv_file}...')

        data_frame = pandas.read_csv(processed_csv_file)
        total_rows = data_frame.shape[0]
        self.logger.info(f'No. of rows in {processed_csv_file}: {total_rows}')

        ctr = 0

        if tmp_csv.exists():
            ctr = pandas.read_csv(tmp_csv).shape[0]

            self.logger.warn(
                f'The file {tmp_csv.name} already exists, no. of rows in the file: {ctr}. Resuming...')

        pbar = tqdm(total=total_rows, desc='Process CSV')
        # Skip any previously processed rows, but do not skip the header.
        data_frame = pandas.read_csv(processed_csv_file, chunksize=100, converters={
                                     'body': str}, skiprows=range(1, ctr + 1))
        for chunk in data_frame:
            # Add any missing columns
            chunk = self.__get_header_fields(chunk)

            tqdm.pandas(desc='Load pull request', leave=False)
            chunk[['pr_comments_cnt',
                   'pr_review_comments_cnt',
                   'pr_commits_cnt',
                   'pr_additions',
                   'pr_deletions',
                   'pr_changed_files',
                   'pr_merged_by_user_id']] = chunk.progress_apply(
                lambda row: self.__get_pull_request_info(row),
                axis='columns')

            tqdm.pandas(desc='Load commit info', leave=False)
            chunk[['comment_author_association',
                   'comment_updated_at',
                   'comment_html_url',
                   'pr_commits_cnt_prior_to_comment',
                   'commit_file_status',
                   'commit_file_additions',
                   'commit_file_deletions',
                   'commit_file_changes']] = chunk.progress_apply(
                       lambda row: self.__get_comment_info(row)
                       if row['pr_commits_cnt'] != 'Not Available'
                       else pandas.Series(['Not Available'] * 8),
                axis='columns')

            chunk.to_csv(tmp_csv,
                         index=False,
                         header=False if ctr > 0 else True,
                         mode='w' if ctr == 0 else 'a')

            ctr += chunk.shape[0]
            pbar.update(chunk.shape[0])

        pbar.close()

        backup_csv = Path(
            processed_csv_file.absolute().as_posix()
            .replace(
                '.csv',
                f'_backup_{time.strftime("%Y%m%d-%H%M%S")}.csv'
            ))
        processed_csv_file.rename(backup_csv)
        tmp_csv.rename(processed_csv_file)
        self.logger.info(
            f'Processing completed, output file: {processed_csv_file}')

        return processed_csv_file

    def __get_header_fields(self, df: pandas.DataFrame):
        columns = ['project_id',
                   'project_url',
                   'pull_request_id',
                   'pullreq_id',
                   'user_id',
                   'comment_id',
                   'position',
                   'body',
                   'commit_id',
                   'created_at',
                   'comment_from_mongodb',
                   'dialogue_act_classification_ml',
                   'pr_comments_cnt',
                   'pr_review_comments_cnt',
                   'pr_commits_cnt',
                   'pr_additions',
                   'pr_deletions',
                   'pr_changed_files',
                   'pr_merged_by_user_id',
                   'comment_author_association',
                   'comment_updated_at',
                   'comment_html_url',
                   'pr_commits_cnt_prior_to_comment',
                   'commit_file_status',
                   'commit_file_additions',
                   'commit_file_deletions',
                   'commit_file_changes']

        # Remove unused columns.
        for col in df:
            if col not in columns:
                df.drop(columns=col, inplace=True)

        # Add missing columns
        for col in columns:
            if col not in df.columns:
                df[col] = None

        return df

    def __load_comment(self, comment: str, project_url: str, pullreq_id: int, comment_id: int):
        is_truncated = False
        is_eng = True
        is_deleted = False

        if LanguageDetector.is_english(comment) is not True:
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

    def __get_comment_info(self, row):
        if (pandas.isna(row['comment_author_association'])
            or pandas.isna(row['comment_updated_at'])
            or pandas.isna(row['comment_html_url'])
            or pandas.isna(row['pr_commits_cnt_prior_to_comment'])
            or pandas.isna(row['commit_file_status'])
            or pandas.isna(row['commit_file_additions'])
            or pandas.isna(row['commit_file_deletions'])
                or pandas.isna(row['commit_file_changes'])):
            # Retrieve from GitHub any missing info.
            return self.github_helper.get_pull_request_comment_info(
                row['project_url'],
                int(row['pullreq_id']),
                int(row['comment_id']))
        else:
            # Return the original info.
            return pandas.Series([
                row['comment_author_association'],
                row['comment_updated_at'],
                row['comment_html_url'],
                row['pr_commits_cnt_prior_to_comment'],
                row['commit_file_status'],
                row['commit_file_additions'],
                row['commit_file_deletions'],
                row['commit_file_changes']
            ])

    def __get_pull_request_info(self, row):
        if (pandas.isna(row['pr_comments_cnt'])
            or pandas.isna(row['pr_review_comments_cnt'])
            or pandas.isna(row['pr_commits_cnt'])
            or pandas.isna(row['pr_additions'])
            or pandas.isna(row['pr_deletions'])
            or pandas.isna(row['pr_changed_files'])
                or pandas.isna(row['pr_merged_by_user_id'])):
            # Retrieve from GitHub any missing info.
            return self.github_helper.get_pull_request_info(
                row['project_url'],
                int(row['pullreq_id']))
        else:
            # Return the original info.
            return pandas.Series([
                row['pr_comments_cnt'],
                row['pr_review_comments_cnt'],
                row['pr_commits_cnt'],
                row['pr_additions'],
                row['pr_deletions'],
                row['pr_changed_files'],
                row['pr_merged_by_user_id']
            ])
