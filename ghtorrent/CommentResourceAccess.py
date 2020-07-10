import logging
import threading
import time

from playsound import playsound
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from sshtunnel import HandlerSSHTunnelForwarderError, SSHTunnelForwarder


class CommentResourceAccess:
    """A data loader class that expands a truncated pull request comment from GHTorrent MongoDB.

    Args:
        ssh_host (str): SSH tunnel host
        ssh_port (int): SSH tunnel port number
        ssh_username (str): SSH tunnel username
        ssh_pkey (str): Path to the SSH private key
        ssh_private_key_password (str): password to the SSH private key
        db_host (str): MongoDB host
        db_port (int): MongoDB port number
        db_username (str): MongoDB username
        db_password (str): MongoDB password
        db (str): MongoDB database
        error_alert_sound_file (str): A path pointing to the error alert sound.
    """

    # Static variables
    server = None
    __lock = threading.Lock()
    ssh_tunnel_error_count = 0
    show_ssh_tunnel_warning = True

    def __init__(self,
                 ssh_host: str,
                 ssh_port: int,
                 ssh_username: str,
                 ssh_pkey: str,
                 ssh_private_key_password: str,
                 db_host: str,
                 db_port: int,
                 db_username: str,
                 db_password: str,
                 db: str,
                 error_alert_sound_file: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.server = SSHTunnelForwarder((ssh_host, ssh_port),
                                        ssh_username=ssh_username,
                                        ssh_pkey=ssh_pkey,
                                        ssh_private_key_password=db_password,
                                        remote_bind_address=(db_host, db_port),
                                        logger=logging.getLogger('SSHTunnelForwarder'))

        # When server starts/restarts, run a check to ensure the tunnel is working.
        self.server.skip_tunnel_checkup = False

        self.db_username = db_username
        self.db_password = db_password
        self.db = db
        self.error_alert_sound_file = error_alert_sound_file
        self.mongo_client = None
        self.collection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.mongo_client != None:
            self.mongo_client.close()
        self.server.stop()  # Close SSH tunnel

    def __get_connection(self):
        if self.server.is_active and self.collection != None:
            return self.collection

        # Using a thread lock to avoid creating multiple SSH tunnels.
        with self.__lock:
            if not self.server.is_active:
                self.logger.info(
                    f'SSH Tunnel is not active, connecting to {self.server.ssh_username}@{self.server.ssh_host}:{self.server.ssh_port}...')
                try:
                    self.server.restart()
                except HandlerSSHTunnelForwarderError as e:
                    self.ssh_tunnel_error_count += 1
                    self.logger.error(
                        f'Cannot establish SSH Tunnel {self.server.ssh_username}@{self.server.ssh_host}:{self.server.ssh_port}, error: {e}')
                    
                    # This is to get around a bug in SSHTunnel, where threads spawned during start() do not get shutdown if the tunnel id down.
                    # https://github.com/pahaz/sshtunnel/issues/170
                    for key, _ in self.server.tunnel_is_up.items():
                        self.server.tunnel_is_up[key] = True

                    self.server.stop()  # Thus setting is_active = False.
                    raise

        self.mongo_client = MongoClient('127.0.0.1',
                                        self.server.local_bind_port,
                                        username=self.db_username,
                                        password=self.db_password,
                                        authSource=self.db,
                                        authMechanism='SCRAM-SHA-1')
        mongo_db = self.mongo_client[self.db]
        self.collection = mongo_db['pull_request_comments']

        self.logger.info(
            f'Connecting to MongoDB 127.0.0.1:{self.server.local_bind_port}.')
        # The ismaster command is cheap and does not require auth.
        self.mongo_client.admin.command('ismaster')
        self.logger.info('Successfully connected to MongoDB server.')
        return self.collection

    def load(self, owner: str, repo: str, pullreq_id: int, comment_id: int, comment: str):
        """Load the full comment.

        Args:
            owner (str): GitHub repository owner.
            repo (str): GitHub repository name.
            pullreq_id (int): Pull request ID.
            comment_id (int): Pull request comment ID.
            comment (str): Existing comment in the dataset.

        Returns:
            str: The full comment text. None if the comment ID does not exist in MongoDB.
        """
        query = {"owner": owner,
                 "repo": repo,
                 "pullreq_id": pullreq_id,
                 "id": comment_id}

        while True:
            try:
                if self.ssh_tunnel_error_count >= 3:
                    if self.show_ssh_tunnel_warning:
                        self.logger.warning(f'SSH Tunnel is down, reached max number of attempts, returning the existing comment.')
                        self.show_ssh_tunnel_warning = False # Avoid repetitive warning log.
                    return comment

                collection = self.__get_connection()
                doc = collection.find_one(query)
                break
            except Exception as e:
                playsound(self.error_alert_sound_file, False)

                self.logger.exception(
                    f'Failed to load comment, owner: {owner}, repo: {repo}, pullreq_id: {pullreq_id}, comment_id: {comment_id}, retry after 5 seconds.')

                if isinstance(e, ServerSelectionTimeoutError):
                    try:
                        if self.mongo_client != None:
                            self.mongo_client.close()
                    except:
                        self.logger.exception(f'Failed to close Mongo Client.')

                time.sleep(5)

        if doc is not None:
            return doc['body']
        else:
            return None # When the Pull Request Comment has been deleted.
