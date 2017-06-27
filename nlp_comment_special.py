from nlp_comment import log_init, read_lines, insert_data, get_data
from datetime import datetime as dt
import argparse
import os
#BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'chat_data_mining', 'DM_sentiment')
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'nlp')


def init_parser():
    parser = argparse.ArgumentParser(
        description='statistic the comment from the treasure ids')
    parser.add_argument(
        '-ids', '--i',  dest='ids', help='string of ids, as like 1563554,568656,568445')
    args = parser.parse_args()
    if args.ids is not None:
        t_ids = args.ids.split(',')

        if len(t_ids) == 1:
            t_ids.append('')

        return tuple(t_ids)
    else:
        print 'do not have id input'
        exit(1)


if __name__ == '__main__':
    created = dt.today()
    begin_date = dt(1900, 2, 16)

    log = log_init('%s.log' % created.strftime('%Y_%m_%d'))
    log.info('initiation the data.....')

    STOP_WORDS = read_lines(os.path.join(BASE_DIR, 's_w.txt'))

    treasure_ids = init_parser()

    log.info('Having %d treasures to do' % len(treasure_ids))
    insert_data(get_data(treasure_ids, begin_date, created, log), treasure_ids, log)


    log.info('------ Finish: %s  -------' % str(treasure_ids))

    log.info('-------------Finish the work---------------')