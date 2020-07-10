import logging
import time
from pathlib import Path

import confuse
from playsound import playsound

from ghtorrent import BigQueryCsvFileProcessor, CommentResourceAccess
from github import PullRequestResourceAccess


def main():
    cfg = confuse.LazyConfig('prca', __name__)
    # Add overrides on top of config.yaml for the workspace.
    cfg.set_file('./config.workspace.yaml')

    # Setting up logging.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s, %(levelname)s, %(name)s, %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z',
        handlers=[logging.StreamHandler(), logging.FileHandler(filename=cfg['log_file'].as_filename(), mode='a')])
    logger = logging.getLogger('prca')
    logger.info('Program started.')

    error_alert_sound_file = cfg['error_alert_sound_file'].as_filename()

    prra = PullRequestResourceAccess(
        cfg['github']['personal_access_tokens'].get(list),
        cfg['github']['requests_cache_file'].as_filename())

    pull_request_comments_csv_file = Path(
        cfg['bigquery']['pull_request_comments_results_csv_file'].as_filename())

    try:
        with CommentResourceAccess(
                cfg['ghtorrent_mongodb']['ssh_tunnel_host'].get(),
                cfg['ghtorrent_mongodb']['ssh_tunnel_port'].get(int),
                cfg['ghtorrent_mongodb']['ssh_username'].get(),
                cfg['ghtorrent_mongodb']['ssh_private_key'].get(),
                cfg['ghtorrent_mongodb']['ssh_private_key_password'].get(),
                cfg['ghtorrent_mongodb']['host'].get(),
                cfg['ghtorrent_mongodb']['port'].get(int),
                cfg['ghtorrent_mongodb']['username'].get(),
                cfg['ghtorrent_mongodb']['password'].get(),
                cfg['ghtorrent_mongodb']['database'].get(),
                error_alert_sound_file) as comment_loader:
            file_processor = BigQueryCsvFileProcessor(comment_loader, prra)
            file_processor.process(pull_request_comments_csv_file)
    except Exception:
        logger.exception(f'Failed to process the BigQuery .csv file.')
        # Continuously make alert sound until manual interruption.
        while True:
            for _ in range(3):
                playsound(error_alert_sound_file, True)
            time.sleep(30)

    logger.info('Program ended.')


# Execute only if run as a script
if __name__ == "__main__":
    main()
