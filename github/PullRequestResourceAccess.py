import logging
import time

import requests
import requests_cache


class PullRequestResourceAccess:
    """A helper class that retrieves GitHub pull request information.

    Args:
        personal_access_tokens (list): A list of personal access tokens.
        requests_cache_file (str): File path to the request cache file.
    """

    def __init__(self, personal_access_tokens: list, requests_cache_file: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.personal_access_tokens = personal_access_tokens

        # Set up request cache, to minimize the long lived chance of hitting GitHub API's rate limit, i.e. 5000 requests per hour per token.
        # And use a session for connection pooling.
        self.session = requests_cache.CachedSession(
            cache_name=requests_cache_file,
            backend='sqlite',
            expire_after=None,
            allowable_codes={200, 404})

        self.token_idx = float('NaN')
        for i, token in enumerate(personal_access_tokens):
            if token != None:
                self.token_idx = i
                self.session.headers.update({'Authorization': 'token ' + token})
                break

    def get_pull_request_info(self, project_url: str, pull_number: int):
        """Returns pull request comment related information.

        The information is retrieved from: https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}.

        Example: https://api.github.com/repos/realm/realm-java/pulls/5473

        Args:
            project_url (str): GitHub project API url in the format https://api.github.com/repos/{owner}/{repo}.
            pull_number (int): GitHub pull request number.
        Returns:
            Series: [
                pr_comments_cnt,
                pr_review_comments_cnt,
                pr_commits_cnt,
                pr_additions,
                pr_deletions,
                pr_changed_files,
                pr_user_login,
                pr_merged_by_user_id
            ]
        """
        url = project_url + '/pulls/' + str(pull_number)
        status_code, json = self.__invoke(url)

        if status_code == 200:
            return [
                json['comments'],
                json['review_comments'],
                json['commits'],
                json['additions'],
                json['deletions'],
                json['changed_files'],
                json['user']['login'],
                json['merged_by'].get('id') if json.get('merged_by') != None else 'Not Available'
            ]
        elif status_code == 404:
            return ['Not Found'] * 8

        raise RuntimeError('Unknown error occurred.', project_url, pull_number)

    def get_pull_request_comment_info(self, project_url: str, pull_number: int, comment_id: int):
        """Returns pull request comment related information, and the commit that the comment pertains.

        The information is retrieved from: https://api.github.com/repos/{owner}/{repo}/pulls/comments/{comment_id}.

        Example: https://api.github.com/repos/realm/realm-java/pulls/comments/147137750

        Args:
            project_url (str): GitHub project API url in the format https://api.github.com/repos/{owner}/{repo}.
            pull_number (int): Pull request number.
            comment_id (int): GitHub comment ID.
        Returns:
            Series: [
                body,
                author_association,
                comment_user_login,
                updated_at,
                html_url,
                pr_commits_cnt_prior_to_comment,
                commit_file_status,
                commit_file_additions,
                commit_file_deletions,
                commit_file_changes
            ]
        """
        url = project_url + '/pulls/comments/' + str(comment_id)
        status_code, comment = self.__invoke(url)

        if status_code == 200:
            commit_file_series = ['Not Available'] * 5

            try:
                commit_file_series = self.get_commit_file_for_comment(
                    project_url,
                    pull_number,
                    comment['path'],
                    comment['original_commit_id']
                )
            except ValueError as e:
                try:
                    if e.args[4] == 250:
                        # GitHub API can only list a maximum of 250 commits: https://developer.github.com/v3/pulls/#list-commits-on-a-pull-request.
                        self.logger.warn(
                            f'Commit or file not found for comment: {url}, file: {e.args[2]}, original_commit_id: {e.args[3]}, commit count: {e.args[4]}')
                except IndexError:
                    self.logger.exception(f'Unknown error while finding commit or file for comment: {url}.')
                    raise e  # Some other error, reraise.

            result = [
                comment['body'],
                comment['author_association'],
                comment['user'].get('login') if comment.get('user') != None else 'Not Available',
                comment['updated_at'],
                comment['html_url']
            ]
            result.extend(commit_file_series)
            return result

        elif status_code == 404:
            return ['Not Found'] * 10

        raise RuntimeError('Unknown error occurred.', project_url, pull_number, comment_id)

    def get_commit_file_for_comment(self, project_url: str, pull_number: int, filename: str, original_commit_id: str):
        """Returns commit file for that a pull request comment is pertaining..

        The information is retrieved from: https://api.github.com/repos/{owner}/{repo}/commits/{commit_id}
        and https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/commits?per_page=250

        Example: https://api.github.com/repos/OpenClinica/OpenClinica/pulls/738/commits?per_page=250
        and: https://api.github.com/repos/OpenClinica/OpenClinica/commits/a2c4d09c70695aaab18695a45414b8d827a2d86f

        Limitation: GitHub API can only list a maximum of 250 commits: https://developer.github.com/v3/pulls/#list-commits-on-a-pull-request.
        So if the comment is not for a commit from the first 250 commits, an ValueError will be raised.

        In addition, sometimes commits can disappear after a force-push.

        Args:
            project_url (str): GitHub project API url in the format https://api.github.com/repos/{owner}/{repo}.
            pull_number (int): Pull request number.
            filename (str): Path of the file being commented on.
            original_commit_id (str): Reference to the commit.
        Returns:
            Series: [
                pr_commits_cnt_prior_to_comment,
                commit_file_status,
                commit_file_additions,
                commit_file_deletions,
                commit_file_changes
            ]
        """
        url = project_url + '/pulls/' + str(pull_number) + '/commits?per_page=250'
        status_code, commits = self.__invoke(url)

        if status_code == 200:
            # Find the commit that the comment pertains.
            commits_cnt = len(commits)
            original_commit_idx = next(iter(
                [i for i in range(commits_cnt)
                 if commits[i]['sha'] == original_commit_id]
            ), None)

            if original_commit_idx != None:
                # Get only commits prior to the comment was created.
                commits = commits[:original_commit_idx + 1]

                # Loop backwards to go from latest commit to earliest commit.
                for commit in reversed(commits):
                    commit_url = project_url + '/commits/' + commit['sha']
                    status_code, commit = self.__invoke(commit_url)

                    if status_code == 200 and commit.get('files') != None:
                        # Find the file committed.
                        commit_file = next(iter(
                            [f for f in commit['files'] if f['filename'] == filename]
                        ), None)

                        if commit_file != None:
                            return [
                                original_commit_idx,
                                commit_file['status'],
                                commit_file['additions'],
                                commit_file['deletions'],
                                commit_file['changes']
                            ]
                        # Else move on to earlier commit.

            raise ValueError('Related commit or file not found.', url, filename, original_commit_id, commits_cnt)

        raise RuntimeError('Unknown error occurred.', project_url, pull_number, filename, original_commit_id)

    def __invoke(self, url: str):
        try:
            resp = self.session.get(url)
            json = resp.json()
            headers = resp.headers
        except:
            self.logger.exception(f'Failed to load from {url}.')
            raise

        if resp.status_code == 200:
            remaining = int(headers['X-RateLimit-Remaining'])
            if remaining < 50 and not resp.from_cache:
                token = self.session.headers['Authorization']
                self.logger.warn(
                    f'API rate limit remaining: {remaining}, {token}, index: {self.token_idx}, moving to the next token...')
                self.token_idx, self.session = self.__next_token(
                    self.personal_access_tokens, self.token_idx, self.session)

            return resp.status_code, json
        elif resp.status_code == 403:
            token = self.session.headers['Authorization']

            if json['message'].startswith('API rate limit exceeded'):
                reset = time.strftime(
                    '%Y-%m-%d %H:%M:%S', time.localtime(int(headers['X-RateLimit-Reset'])))
                self.logger.warn(
                    f'API rate limit exceeded, {token}, index: {self.token_idx}, reset: {reset}, retrying with the next token...')
            elif json['message'].startswith('You have triggered an abuse detection mechanism'):
                retry_after = int(headers['Retry-After'])
                self.logger.warn(
                    f'Triggered abuse detection, {token}, index: {self.token_idx}, retry after: {retry_after}, retrying with the next token instead...')
            else:
                raise Exception(f'Unknown HTTP 403 error, from {url}, headers: {headers} response: {json}')

            self.token_idx, self.session = self.__next_token(
                self.personal_access_tokens, self.token_idx, self.session)

            # Recursive call with the new token.
            return self.__invoke(url)
        elif resp.status_code == 404:
            return resp.status_code, None
        elif resp.status_code == 502:
            self.logger.warn(
                f'Failed to load from {url}, HTTP code: {resp.status_code}, headers: {headers} response: {json}, retry in 5 seconds...')
            time.sleep(5)

            # Recursive to retry.
            return self.__invoke(url)

        raise Exception(
            f'Failed to load from {url}, HTTP code: {resp.status_code}, header: {headers} response: {json}')

    def __next_token_idx(self, tokens: list, ptr: int):
        index = ptr + 1 if ptr + 1 < len(tokens) else 0
        for i, token in enumerate(tokens[index:], start=index):
            if token != None:
                return i, token

        return float('NaN'), None

    def __next_token(self, tokens: list, token_idx: int, session: requests.Session):
        token_idx, token = self.__next_token_idx(tokens, token_idx)

        if token_idx != float('NaN'):
            session.headers.update({'Authorization': 'token ' + token})

        return token_idx, session
