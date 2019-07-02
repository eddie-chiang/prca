import logging
import pandas
import requests
import requests_cache


class GitHubPullRequestHelper:
    """A helper class that retrieves GitHub pull request information.

    Args:
        personal_access_tokens (list): A list of personal access tokens.
        requests_cache_file (str): File path to the request cache file.
    """

    def __init__(self, personal_access_tokens: list, requests_cache_file: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.personal_access_tokens = personal_access_tokens

        self.token_idx = float('NaN')
        for i, token in enumerate(personal_access_tokens):
            if token != None:
                self.token_idx = i
                break

        # Set up request cache, to minimize the long lived chance of hitting GitHub API's rate limit, i.e. 5000 requests per hour per token.
        # And use a session for connection pooling.
        self.session = requests_cache.CachedSession(
            cache_name=requests_cache_file,
            backend='sqlite',
            expire_after=None,
            allowable_codes={200})

        if self.token_idx != float('NaN'):
            token = personal_access_tokens[self.token_idx]
            self.session.headers.update({'Authorization': 'token ' + token})

    def get_pull_request_comment_info(self, project_url: str, comment_id: int):
        """Returns pull request comment related information.

        The information is retrieved from: https://api.github.com/repos/{owner}/{repo}/pulls/comments/{comment_id}.

        Example: https://api.github.com/repos/realm/realm-java/pulls/comments/147137750

        Args:
            project_url (str): GitHub project API url in the format https://api.github.com/repos/{owner}/{repo}.
            comment_id (int): GitHub comment ID.
        Returns: 
            Series: [author_association, updated_at, html_url]
        """
        url = project_url + '/pulls/comments/' + str(comment_id)
        status_code, json = self.__invoke(url)

        if status_code == 200:
            return pandas.Series([json['author_association'], json['updated_at'], json['html_url']])
        elif status_code == 403:
            # Recursive call with the new token.
            return self.get_pull_request_comment_info(project_url, comment_id)

    def get_pull_request_info(self, project_url: str, pullreq_id: int):
        """Returns pull request comment related information.

        The information is retrieved from: https://api.github.com/repos/{owner}/{repo}/pulls/{pullreq_id}.

        Example: https://api.github.com/repos/realm/realm-java/pulls/5473

        Args:
            project_url (str): GitHub project API url in the format https://api.github.com/repos/{owner}/{repo}.
            pullreq_id (int): GitHub pull request ID.
        Returns: 
            Series: [
                pr_comments_cnt, 
                pr_review_comments_cnt, 
                pr_commits_cnt,
                pr_additions,
                pr_deletions,
                pr_changed_files,
                pr_merged_by_user_id
            ]
        """
        url = project_url + '/pulls/' + str(pullreq_id)
        status_code, json = self.__invoke(url)

        if status_code == 200:
            return pandas.Series([
                json['comments'],
                json['review_comments'],
                json['commits'],
                json['additions'],
                json['deletions'],
                json['changed_files'],
                json['merged_by'].get('id') if json.get('merged_by') != None else ''])
        elif status_code == 403:
            # Recursive call with the new token.
            return self.get_pull_request_info(project_url, pullreq_id)

    def __invoke(self, url: str):
        resp = self.session.get(url)

        if resp.status_code == 200:
            return resp.status_code, resp.json()
        elif resp.status_code == 403 and resp.json()['message'].startswith('API rate limit exceeded'):
            token = self.session.headers['Authorization']
            self.logger.warn(
                f'API rate limit exceeded, {token}, index: {self.token_idx}, retrying with the next token...')
            self.token_idx, token = self.__next_token_idx(
                self.personal_access_tokens, self.token_idx)

            if self.token_idx != float('NaN'):
                self.session.headers.update(
                    {'Authorization': 'token ' + token})

            return resp.status_code, None
        else:
            raise Exception(
                f'Failed to retrieve pull request info from {url}, HTTP status code {resp.status_code}.')

    def __next_token_idx(self, tokens: list, ptr: int):
        index = ptr + 1 if ptr + 1 < len(tokens) else 0
        for i, token in enumerate(tokens[index:], start=index):
            if token != None:
                return i, token

        return float('NaN'), None
