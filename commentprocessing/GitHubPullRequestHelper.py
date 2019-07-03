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
                pr_merged_by_user_id
            ]
        """
        url = project_url + '/pulls/' + str(pull_number)
        status_code, json = self.__invoke(url)

        if status_code == 200:
            return pandas.Series([
                json['comments'],
                json['review_comments'],
                json['commits'],
                json['additions'],
                json['deletions'],
                json['changed_files'],
                json['merged_by'].get('id') if json.get('merged_by') != None else 'NA']
            )
        elif status_code == 403:
            # Recursive call with the new token.
            return self.get_pull_request_info(project_url, pull_number)
        elif status_code == 404:
            self.logger.warn(f'Pull request not found: {url}.')
            return pandas.Series(['Not Found'] * 7)

        raise RuntimeError(
            'Unknown error occurred.', project_url, pull_number)

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
                author_association,
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
            commit_file_series = pandas.Series(['NA'] * 5)

            try:
                commit_file_series = self.get_commit_file_for_comment(
                    project_url,
                    pull_number,
                    comment['path'],
                    comment['original_commit_id']
                )
            except ValueError as e:
                self.logger.warn(
                    f'Commit or file not found for comment: {url}, file: {e.args[2]}, original_commit_id: {e.args[3]}.')

            result = pandas.Series([
                comment['author_association'],
                comment['updated_at'],
                comment['html_url']
            ])
            return result.append(commit_file_series, ignore_index=True)
        elif status_code == 403:
            # Recursive call with the new token.
            return self.get_pull_request_comment_info(project_url, pull_number, comment_id)
        elif status_code == 404:
            self.logger.warn(f'Pull request comment not found: {url}.')
            return pandas.Series(['Not Found'] * 5)

        raise RuntimeError(
            'Unknown error occurred.', project_url, pull_number, comment_id)

    def get_commit_file_for_comment(self, project_url: str, pull_number: int, filename: str, original_commit_id: str):
        """Returns commit file for that a pull request comment is pertaining..

        The information is retrieved from: https://api.github.com/repos/{owner}/{repo}/commits/{commit_id}
        and https://api.github.com/repos/{owner}/{repo}/pulls/{pull_number}/commits?per_page=250

        Example: https://api.github.com/repos/OpenClinica/OpenClinica/pulls/738/commits?per_page=250
        and: https://api.github.com/repos/OpenClinica/OpenClinica/commits/a2c4d09c70695aaab18695a45414b8d827a2d86f

        Limitation: GitHub API can only list a maximum of 250 commits: https://developer.github.com/v3/pulls/#list-commits-on-a-pull-request.
        So if the comment is not for a commit from the first 250 commits, a series populated with NA will be returned.

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
        url = project_url + '/pulls/' + \
            str(pull_number) + '/commits?per_page=250'
        status_code, commits = self.__invoke(url)

        if status_code == 200:
            # Find the commit that the comment pertains.
            original_commit_idx = next(iter(
                [i for i in range(len(commits))
                 if commits[i]['sha'] == original_commit_id]
            ), None)

            if original_commit_idx != None:
                # Get only commits prior to the comment was created.
                commits = commits[:original_commit_idx + 1]

                # Loop backwards to go from latest commit to earliest commit.
                for commit in reversed(commits):
                    commit_url = project_url + '/commits/' + commit['sha']
                    status_code, commit = self.__invoke(commit_url)

                    if status_code == 200:
                        # Find the file committed.
                        commit_file = next(iter(
                            [f for f in commit['files']
                             if f['filename'] == filename]
                        ), None)

                        if commit_file == None:
                            continue  # Move on to earlier commit.
                        else:
                            return pandas.Series([
                                original_commit_idx,
                                commit_file['status'],
                                commit_file['additions'],
                                commit_file['deletions'],
                                commit_file['changes']
                            ])
                    elif status_code == 403:
                        # Recursive call with the new token.
                        return self.get_commit_file_for_comment(project_url, pull_number, filename, original_commit_id)

            raise ValueError(
                'Cannot find the pertaining commit or file', url, filename, original_commit_id)
        elif status_code == 403:
            # Recursive call with the new token.
            return self.get_commit_file_for_comment(project_url, pull_number, filename, original_commit_id)

        raise RuntimeError(
            'Unknown error occurred.', project_url, pull_number, filename, original_commit_id)

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
        elif resp.status_code == 404:
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
