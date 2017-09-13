import gd2.gd2 as gd2
import covers.covers as covers
import oddsshark.oddsshark as oddsshark
import itertools
from multiprocessing import Pool
import os
import pandas as pd


def scrape_covers(year, team):
    return covers.MlbLines(year, team).table_rows()

gd2_scraper = gd2.Scraper('http://gd2.mlb.com/components/game/mlb')
os_scraper = oddsshark.Scraper()

years = ['%04d' % (m) for m in range(2014, 2018)]
months = ['%02d' % (m + 1) for m in range(3, 12)]
days = ['%02d' % (m + 1) for m in range(1, 32)]

total_days = (len(years) * len(months) * len(days))
total_pages = (len(years) * len(covers.covers_team_number.keys()))

print('starting pool of {} workers...'.format(os.cpu_count()))
pool = Pool(os.cpu_count())

# gd_iter = itertools.product(years, months, days)
# print('scraping gd2 data... searching {} total days'.format(total_days))
# gd2_df = pd.concat(pool.starmap(gd2_scraper.scrape_day, gd_iter),
#                    ignore_index=True)
# print('saving gd2 DataFrame to file...')
# gd2_df.to_pickle('data/gd2_df.pkl')

# covers_iter = itertools.product(years, covers.covers_team_number.keys())
# print('scraping covers data... searching {} total pages'.format(total_pages))
# covers_df = pd.concat(pool.starmap(scrape_covers, covers_iter),
#                       ignore_index=True)
# print('saving covers DataFrame to file...')
# covers_df.to_pickle('data/covers_df.pkl')

os_iter = itertools.product(years, months, days)
print('scraping oddsshark data... searching {} total days'.format(total_days))
os_df = pd.concat(pool.starmap(os_scraper.scrape_day, os_iter),
                   ignore_index=True)
print('saving oddsshark DataFrame to file...')
os_df.to_pickle('data/os_df.pkl')

# print('merging covers historical odds to gd2 data')
# df = pd.merge(gd2_df, covers_df,
#               left_on=['gid', 'team_code'],
#               right_on=['gid', 'team'])

# print('saving DataFrame to file...')
# df.to_pickle('data/games_with_odds.pkl')

print('done!')
