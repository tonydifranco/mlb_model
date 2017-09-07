import gd2.gd2 as gd2
import covers.covers as covers
import itertools
from multiprocessing import Pool
import os
import pandas as pd


def scrape_covers(year, team):
    return covers.MlbLines(year, team).table_rows()

scraper = gd2.Scraper('http://gd2.mlb.com/components/game/mlb')

years = ['%04d' % (m) for m in range(2012, 2018)]
months = ['%02d' % (m + 1) for m in range(12)]
days = ['%02d' % (m + 1) for m in range(31)]

gd_iter = itertools.product(years, months, days)
covers_iter = itertools.product(years, covers.covers_team_number.keys())

print('starting up multiprocessing pool of {} workers...'.format(os.cpu_count()))
pool = Pool(os.cpu_count())

print('starting gd2 scraping process... searching through {} days...'.format(len(years) * len(months) * len(days)))
gd2_df = pd.concat(pool.starmap(scraper.scrape_day, gd_iter), ignore_index=True)

print('starting covers scraping process... searching through {} covers pages...'.format(len(years) * len(covers.covers_team_number.keys())))
covers_df = pd.concat(pool.starmap(scrape_covers, covers_iter), ignore_index=True)

print('merging covers historical odds to gd2 data')
df = pd.merge(gd2_df, covers_df, left_on=['gid', 'team_code'], right_on=['gid', 'team'])

print('saving DataFrame to file...')
df.to_pickle('data/games_with_odds.pkl')

print('done!')