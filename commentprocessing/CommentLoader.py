import logging
import threading
import time
from playsound import playsound
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from sshtunnel import SSHTunnelForwarder


class CommentLoader:
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
    server = None  # Static variable
    __lock = threading.Lock()

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
        CommentLoader.server = SSHTunnelForwarder((ssh_host, ssh_port),
                                                  ssh_username=ssh_username,
                                                  ssh_pkey=ssh_pkey,
                                                  ssh_private_key_password=db_password,
                                                  remote_bind_address=(db_host, db_port))

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
        CommentLoader.server.stop()  # Close SSH tunnel

    def __get_connection(self):
        if CommentLoader.server.is_active and self.collection != None:
            return self.collection

        # Using a thread lock to avoid creating multiple SSH tunnels.
        with CommentLoader.__lock:
            if not CommentLoader.server.is_active:
                self.logger.info(
                    f'SSH Tunnel is not active, connecting to {CommentLoader.server.ssh_host}:{CommentLoader.server.ssh_port}...')
                CommentLoader.server.restart()

        self.mongo_client = MongoClient('127.0.0.1',
                                        CommentLoader.server.local_bind_port,
                                        username=self.db_username,
                                        password=self.db_password,
                                        authSource=self.db,
                                        authMechanism='SCRAM-SHA-1')
        mongo_db = self.mongo_client[self.db]
        self.collection = mongo_db['pull_request_comments']

        self.logger.info(
            f'Connecting to MongoDB 127.0.0.1:{CommentLoader.server.local_bind_port}.')
        # The ismaster command is cheap and does not require auth.
        self.mongo_client.admin.command('ismaster')
        self.logger.info('Successfully connected to MongoDB server.')
        return self.collection

    def load(self, owner: str, repo: str, pullreq_id: int, comment_id: int):
        """Load the full comment.

        Args:
            owner (str): GitHub repository owner.
            repo (str): GitHub repository name.
            pullreq_id (int): Pull request ID.
            comment_id (int): Pull request comment ID.

        Returns:
            str: The full comment text. None if the comment ID does not exist in MongoDB.
        """
        query = {"owner": owner,
                 "repo": repo,
                 "pullreq_id": pullreq_id,
                 "id": comment_id}

        success = False
        while success != True:
            try:
                collection = self.__get_connection()
                doc = collection.find_one(query)
                success = True
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
            return None
