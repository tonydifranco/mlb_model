import gd2.gd2 as gd2
import covers.covers as covers
import oddsshark.oddsshark as oddsshark
import itertools
from multiprocessing import Pool
import os
import pandas as pd


def scrape_covers(year, team):
    return covers.MlbLines(year, team).table_rows()

years = ['%04d' % (m) for m in range(2012, 2018)]
months = ['%02d' % (m + 1) for m in range(3, 12)]
days = ['%02d' % (m + 1) for m in range(1, 32)]

gd2_scraper = gd2.Scraper('http://gd2.mlb.com/components/game/mlb')
os_scraper = oddsshark.Scraper()

gd_iter = itertools.product(years, months, days)
covers_iter = itertools.product(years, covers.covers_team_number.keys())
os_iter = itertools.product(years, months, days)

if __name__ == '__main__':
    print('starting pool of {} workers...'.format(os.cpu_count()))
    with Pool(os.cpu_count()) as pool:
        print('scraping gd2 data...')
        gd2_df = pd.concat(pool.starmap(gd2_scraper.scrape_day, gd_iter),
                           ignore_index=True)
        print('saving gd2 DataFrame to file...')
        gd2_df.to_pickle('data/gd2_df.pkl')

        print('scraping covers data...')
        covers_df = pd.concat(pool.starmap(scrape_covers, covers_iter),
                              ignore_index=True)
        print('saving covers DataFrame to file...')
        covers_df.to_pickle('data/covers_df.pkl')

        print('scraping oddsshark data...')
        os_df = pd.concat(pool.starmap(os_scraper.scrape_day, os_iter),
                          ignore_index=True)
        print('saving oddsshark DataFrame to file...')
        os_df.to_pickle('data/os_df.pkl')

    print('done!')
