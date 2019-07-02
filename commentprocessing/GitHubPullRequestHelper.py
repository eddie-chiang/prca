import logging
import pandas
import requests


class GitHubPullRequestHelper:
    """A helper class that retrieves GitHub pull request information.

    Args:
        personal_access_tokens (list): A list of personal access tokens.
    """

    def __init__(self, personal_access_tokens: list):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.personal_access_tokens = personal_access_tokens

        self.token_idx = float('NaN')
        for i, token in enumerate(personal_access_tokens):
            if token != None:
                self.token_idx = i
                break
        
        # Create a session for connection pooling.
        self.session = requests.session()

        if self.token_idx != float('NaN'):
            token = personal_access_tokens[self.token_idx]
            self.session.headers.update({'Authorization': 'token ' + token})

    def get_pull_request_comment_info(self, project_url: str, comment_id: int):
        """Returns pull request comment related information.

        Args:
            project_url (str): GitHub project API url in the format https://api.github.com/repos/{owner}/{repo}.
            comment_id (int): GitHub comment ID.
        Returns: 
            Series: [author_association, updated_at, html_url]
        """
        url = project_url + '/pulls/comments/' + str(comment_id)

        resp = self.session.get(url)

        if resp.status_code == 200:
            json = resp.json()
            return pandas.Series([json['author_association'], json['updated_at'], json['html_url']])
        elif resp.status_code == 403 and resp.json()['message'].startswith('API rate limit exceeded'):
            token = self.session.headers['Authorization']
            self.logger.warn(f'API rate limit exceeded, {token}, index: {self.token_idx}, retrying with the next token...')
            self.token_idx, token = self.__next_token_idx(self.personal_access_tokens, self.token_idx)

            if self.token_idx != float('NaN'):
                self.session.headers.update({'Authorization': 'token ' + token})

            # Recursive call with the new token.
            return self.get_pull_request_comment_info(project_url, comment_id)
        else:
            raise Exception(
                f'Failed to retrieve pull request info from {url}, HTTP status code {resp.status_code}.')

    def __next_token_idx(self, tokens: list, ptr: int):
        index = ptr + 1 if ptr + 1 < len(tokens) else 0
        for i, token in enumerate(tokens[index:], start=index):
            if token != None:
                return i, token

        return float('NaN'), None
