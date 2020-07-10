import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy
import pandas
from tqdm import tqdm

from commentprocessing import LanguageDetector
from ghtorrent import CommentResourceAccess
from github import PullRequestResourceAccess


class BigQueryCsvFileProcessor:
    """A file processor that iterate through all rows in the given BigQuery result .csv file and determine whether a comment is truncated.
    If so, will load using :class:`CommentResourceAccess`.
    Also, it determines the language of a comment, and skip rows that are not in English.

    Args:
        comment_loader (CommentResourceAccess): An instance of comment loader.
        prra (PullRequestResourceAccess): An instance GitHub Pull Request Helper.
    """

    def __init__(self, comment_loader: CommentResourceAccess, prra: PullRequestResourceAccess):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.comment_loader = comment_loader
        self.prra = prra

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
        final_csv = Path(csv_file.absolute().as_posix().replace('.csv', '_cleaned.csv'))
        final_stats_csv = Path(csv_file.absolute().as_posix().replace('.csv', '_cleaned_stats.csv'))

        if final_csv.exists() and final_stats_csv.exists():
            self.logger.info(f'Processed file already exists, stop further processing: {final_csv}')
            return final_csv, final_stats_csv

        self.logger.info(f'Start processing {csv_file}...')

        tmp_csv = Path(csv_file.absolute().as_posix().replace('.csv', '_processing.csv'))
        tmp_stats_csv = Path(csv_file.absolute().as_posix().replace('.csv', '_processing_stats.csv'))

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
            del_from_mongo_ctr = int(tmp_stats_df['deleted_from_mongodb'].iat[0])
            del_from_github_ctr = int(tmp_stats_df['deleted_from_github'].iat[0])
            skip_ctr = int(tmp_stats_df['total_skipped'].iat[0])

            self.logger.warn(
                f'The file {tmp_csv.name} already exists, no. of rows in the file: {tmp_total_rows}, no. of rows processed: {ctr}. Resuming...')
        else:
            stats = {
                'rows_processed': [0],
                'comments_truncated': [0],
                'non_english': [0],
                'deleted_from_mongodb': [0],
                'deleted_from_github': [0],
                'total_skipped': [0]
            }
            tmp_stats_df = pandas.DataFrame(
                data=dict([(key, pandas.Series(value)) for key, value in stats.items()]))

        # Set up before the loop
        pbar = tqdm(desc='Process CSV', total=total_rows, initial=ctr)
        commentExecutor = ThreadPoolExecutor(max_workers=4, thread_name_prefix='CommentResourceAccess')
        gitHubExecutor = ThreadPoolExecutor(max_workers=4, thread_name_prefix='PullRequestResourceAccess')

        # Skip any previously processed rows, but do not skip the header.
        data_frame = pandas.read_csv(csv_file, chunksize=500, converters={'body': str}, skiprows=range(1, ctr + 1))
        for chunk in data_frame:
            # Get chunk size first before any filtering.
            chunk_size = chunk.shape[0]

            # Add new columns
            chunk = self.__get_header_fields(chunk)

            # Filter to only English comments.
            chunk['is_eng'] = chunk.apply(
                lambda row: LanguageDetector.is_english(row['body']),
                axis='columns')
            chunk = chunk[chunk['is_eng'] == True]
            non_eng_ctr += chunk_size - chunk.shape[0]
            chunk.drop(columns='is_eng', inplace=True)

            # Identify possible truncated comments and load from GHTorrent MongoDB.
            chunk['is_truncated'] = numpy.char.str_len(chunk['body'].to_numpy(dtype=str)) == 255
            truncated_ctr_in_loop = len(chunk[chunk['is_truncated'] == True].index)
            truncated_ctr += truncated_ctr_in_loop

            # https://api.github.com/repos/{owner}/{repo}
            chunk.loc[chunk['is_truncated'] == True, 'owner'] = [
                url[url.rfind('/', 0, url.rfind('/')) + 1:url.rfind('/')]
                for url in chunk[chunk['is_truncated'] == True]['project_url']
            ]

            chunk.loc[chunk['is_truncated'] == True, 'repo'] = [
                url[url.rfind('/') + 1:]
                for url in chunk[chunk['is_truncated'] == True]['project_url']
            ]

            # Loading comment from MongoDB has a lot of IO waits, use threading.
            chunk.loc[chunk['is_truncated'] == True, 'body'] = list(tqdm(
                commentExecutor.map(
                    self.comment_loader.load,
                    chunk[chunk['is_truncated'] == True]['owner'],
                    chunk[chunk['is_truncated'] == True]['repo'],
                    chunk[chunk['is_truncated'] == True]['pullreq_id'],
                    chunk[chunk['is_truncated'] == True]['comment_id'],
                    chunk[chunk['is_truncated'] == True]['body'],
                    timeout=600
                ),
                desc='Load comment',
                total=truncated_ctr_in_loop,
                leave=False
            ))

            # Filter out comments deleted from MongoDB.
            del_from_mongo_ctr += len(chunk[chunk['body'].isnull()].index)
            chunk = chunk[chunk['body'].notnull()]

            # Drop temp columns
            chunk.drop(columns='is_truncated', inplace=True)
            chunk.drop(columns='owner', inplace=True)
            chunk.drop(columns='repo', inplace=True)

            # Temp working columns.
            chunk = chunk.assign(comment_user_login='', pr_user_login='')

            chunk[['pr_comments_cnt',
                   'pr_review_comments_cnt',
                   'pr_commits_cnt',
                   'pr_additions',
                   'pr_deletions',
                   'pr_changed_files',
                   'pr_user_login',
                   'pr_merged_by_user_id']] = list(tqdm(
                       gitHubExecutor.map(
                           self.prra.get_pull_request_info,
                           chunk['project_url'],
                           chunk['pullreq_id'],
                           timeout=600
                       ),
                       desc='Load pull request',
                       total=chunk.shape[0],
                       leave=False
                   ))

            # For ones which Pull Request is not found/available, mark commit info as 'Not Available'.
            chunk.loc[
                chunk['pr_commits_cnt'].isin(['Not Available', 'Not Found']),
                [
                    'comment_author_association',
                    'comment_user_login',
                    'comment_updated_at',
                    'comment_html_url',
                    'pr_commits_cnt_prior_to_comment',
                    'commit_file_status',
                    'commit_file_additions',
                    'commit_file_deletions',
                    'commit_file_changes'
                ]
            ] = ['Not Available'] * 9

            # Load commit info.
            chunk = self.__get_comment_info(
                chunk,
                gitHubExecutor,
                (~chunk['pr_commits_cnt'].isin(['Not Available', 'Not Found']))
            )

            chunk['comment_is_by_author'] = numpy.char.equal(
                chunk['pr_user_login'].to_numpy(dtype=str),
                chunk['comment_user_login'].to_numpy(dtype=str))
            chunk.drop(columns='pr_user_login', inplace=True)
            chunk.drop(columns='comment_user_login', inplace=True)

            # Filter out records not found from GitHub.
            del_from_github_ctr += len(
                chunk[
                    (chunk['comment_html_url'] == 'Not Found')
                    | (chunk['pr_commits_cnt'] == 'Not Found')
                ].index)
            chunk = chunk[
                (chunk['comment_html_url'] != 'Not Found')
                & (chunk['pr_commits_cnt'] != 'Not Found')
            ]

            skip_ctr += chunk_size - chunk.shape[0]

            if chunk.shape[0] > 0:
                pbar.write(f'Writing to {tmp_csv}')
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

            pbar.write(f'Writing to {tmp_stats_csv}')
            tmp_stats_df.to_csv(tmp_stats_csv,
                                index=False, header=True, mode='w')

            pbar.update(chunk_size)
            pbar.write(
                f'Comment truncated: {truncated_ctr}, non English: {non_eng_ctr}, deleted from MongoDB/GitHub: {del_from_mongo_ctr}/{del_from_github_ctr}, total skipped: {skip_ctr}')

        # Clean up after the loop
        pbar.close()
        commentExecutor.shutdown()
        gitHubExecutor.shutdown()

        tmp_csv.rename(final_csv)
        tmp_stats_csv.rename(final_stats_csv)
        self.logger.info(f'Processing completed, output file: {final_csv}')

        return final_csv, final_stats_csv

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
                   'pr_comments_cnt',
                   'pr_review_comments_cnt',
                   'pr_commits_cnt',
                   'pr_additions',
                   'pr_deletions',
                   'pr_changed_files',
                   'pr_merged_by_user_id',
                   'comment_author_association',
                   'comment_is_by_author',
                   'comment_updated_at',
                   'comment_html_url',
                   'pr_commits_cnt_prior_to_comment',
                   'commit_file_status',
                   'commit_file_additions',
                   'commit_file_deletions',
                   'commit_file_changes']

        # Remove unused columns.
        for col in (col for col in df if col not in columns):
            df.drop(columns=col, inplace=True)

        # Add missing columns
        for col in (col for col in columns if col not in df.columns):
            df[col] = None

        return df

    def __get_comment_info(self, chunk: pandas.DataFrame, gitHubExecutor: ThreadPoolExecutor, filter_gen_exp):
        # For ones which Pull Request is not found/available, mark commit info as 'Not Available'.
        chunk.loc[
            chunk['pr_commits_cnt'].isin(['Not Available', 'Not Found']),
            [
                'comment_author_association',
                'comment_user_login',
                'comment_updated_at',
                'comment_html_url',
                'pr_commits_cnt_prior_to_comment',
                'commit_file_status',
                'commit_file_additions',
                'commit_file_deletions',
                'commit_file_changes'
            ]
        ] = ['Not Available'] * 9

        if chunk[filter_gen_exp].shape[0] > 0:
            # Find the commit info, filtered by the given condition.
            chunk.loc[
                filter_gen_exp,
                [
                    'body',
                    'comment_author_association',
                    'comment_user_login',
                    'comment_updated_at',
                    'comment_html_url',
                    'pr_commits_cnt_prior_to_comment',
                    'commit_file_status',
                    'commit_file_additions',
                    'commit_file_deletions',
                    'commit_file_changes'
                ]
            ] = list(tqdm(
                gitHubExecutor.map(
                    self.prra.get_pull_request_comment_info,
                    chunk.loc[filter_gen_exp, 'project_url'],
                    chunk.loc[filter_gen_exp, 'pullreq_id'],
                    chunk.loc[filter_gen_exp, 'comment_id'],
                    timeout=600
                ),
                desc='Load commit info',
                total=chunk[filter_gen_exp].shape[0],
                leave=False
            ))

        return chunk
