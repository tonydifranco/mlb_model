import os
import pandas as pd
import random
import requests

from datetime import datetime
from time import sleep

gd2_team_lookup = {
    'ARI': 'ari',
    'ATL': 'atl',
    'BAL': 'bal',
    'BOS': 'bos',
    'CHC': 'chn',
    'CHW': 'cha',
    'CIN': 'cin',
    'CLE': 'cle',
    'COL': 'col',
    'DET': 'det',
    'HOU': 'hou',
    'KC': 'kca',
    'LA': 'lan',
    'LAA': 'ana',
    'LAD': 'lan',
    'MIA': 'mia',
    'MIL': 'mil',
    'MIN': 'min',
    'NYM': 'nyn',
    'NYY': 'nya',
    'OAK': 'oak',
    'PHI': 'phi',
    'PIT': 'pit',
    'SD': 'sdn',
    'SEA': 'sea',
    'SF': 'sfn',
    'STL': 'sln',
    'TB': 'tba',
    'TEX': 'tex',
    'TOR': 'tor',
    'WSH': 'was',
}


class Scraper:
    referer = 'http://www.oddsshark.com/mlb/scores'
    scores_url = 'http://io.oddsshark.com/scores/mlb/'
    history_url = 'http://www.oddsshark.com/mlb/odds/line-history/'
    failed_event_ids = []

    def __init__(self, use_proxy=True, timeout=30, max_retries=2):
        self.use_proxy = use_proxy
        self.timeout = timeout
        self.max_retries = max_retries
        self.proxies = self.get_proxies()
        ua_file = os.path.join(os.path.dirname(__file__), 'user_agents.txt')
        with open(ua_file, 'r') as f:
            self.user_agents = f.read().split('\n')

    def get_proxies(self, max_proxies=100, url='https://www.us-proxy.org/',
                    test_url='http://localhost:80', timeout=1):

        if not self.use_proxy:
            return [None]

        r = requests.get(url)
        tables = pd.read_html(r.text)
        df = tables[0]
        df = df.loc[df['IP Address'].notnull() & df['Port'].notnull()]
        proxies = df['IP Address'] + ':' + df['Port'].astype(int).astype(str)
        proxies = proxies.tolist()

        print('testing proxies with {} seconds timeout...'.format(timeout))
        fast_proxies = []
        for p in proxies:
            try:
                r = requests.get(test_url, proxies={'http': p},
                                 timeout=timeout)
                print('{} made the cut'.format(p))
                fast_proxies.append(p)
                if len(fast_proxies) >= max_proxies:
                    break
            except:
                print('{} is too slow'.format(p))

        print('retained {} total proxies'.format(len(fast_proxies)))

        return fast_proxies

    def proxied_request(self, url, headers=None):
        user_agent = random.choice(self.user_agents)
        proxy = random.choice(self.proxies)

        proxy_headers = {'User-Agent': user_agent, 'Connection': 'close'}
        if headers is not None:
            proxy_headers.update(headers)

        try:
            sleep(random.uniform(0, 1))
            r = requests.get(url, headers=proxy_headers,
                             proxies={'http': proxy},
                             timeout=self.timeout)
        except:
            print('exception when using proxy {}... removing'.format(proxy))
            self.proxies.remove(proxy)

            if len(self.proxies) == 0:
                raise Exception('no more proxies')

            return

        return r

    def scrape_day(self, year, month, day):
        cal_day = '{}-{}-{}'.format(year, month, day)
        url = self.scores_url + cal_day

        tries = 0
        r = None
        while not r:
            tries += 1
            r = self.proxied_request(url, headers={'referer': self.referer})
            if tries > self.max_retries:
                break

        if not r:
            return

        if r.status_code != requests.codes.ok:
            return

        try:
            content = r.json()
        except:
            print('error parsing json from cal_day {}'.format(cal_day))
            return

        if not content:
            return

        keep_cols = ['event_id', 'event_date', 'gid', 'home_abbreviation',
                     'home_money_line', 'home_votes', 'home_spread',
                     'away_abbreviation', 'away_money_line', 'away_votes']

        df = pd.DataFrame(content)

        if 'home_money_line' not in df.columns:
            print('skipping oddshark day {} with no lines'.format(cal_day))
            return
        else:
            print('scraping oddshark day {}'.format(cal_day))

        df = df.sort_values('event_date')
        df['game_number'] = df.groupby('home_abbreviation').cumcount() + 1
        indexer = (df['home_abbreviation'].notnull() &
                   df['away_abbreviation'].notnull() &
                   df['game_number'].notnull())
        df = df.loc[indexer]

        if df.shape[0] == 0:
            return

        df['gid'] = df.apply(lambda x: 'gid_{}_{}_{}_{}mlb_{}mlb_{}'.format(
            year, month, day, gd2_team_lookup[x['away_abbreviation']],
            gd2_team_lookup[x['home_abbreviation']], x['game_number']), axis=1)

        df = df[keep_cols]

        line_histories = []
        for ev in df.event_id.tolist():
            hist_df = self.get_line_history(ev)

            if isinstance(hist_df, pd.DataFrame):
                line_histories.append(hist_df)

        if line_histories:
            df = pd.merge(df, pd.concat(line_histories, ignore_index=True),
                          on='event_id', how='left')

        return df

    def get_line_history(self, event_id):
        url = self.history_url + str(event_id)

        tries = 0
        r = None
        while not r:
            tries += 1
            r = self.proxied_request(url)
            if tries > self.max_retries:
                break

        if not r:
            self.failed_event_ids.append(event_id)
            return

        if r.status_code != requests.codes.ok:
            self.failed_event_ids.append(event_id)
            return

        try:
            tables = pd.read_html(r.text)
        except ValueError:
            print('no line history for event {}'.format(event_id))
            return

        fmt = '%m/%d/%y %I:%M:%S %p'

        columns = []
        values = []
        for t in tables:
            t = t[t['Line (Home)'].notnull()]
            if t.shape[0] == 0:
                continue

            if t['Line (Home)'].dtype == 'O':
                t.loc[t['Line (Home)'] == 'Ev', 'Line (Home)'] = '-100'
                t['Line (Home)'] = t['Line (Home)'].astype(int)

            name = t.columns[0]
            line_open = t['Line (Home)'].iloc[0]
            line_current = t['Line (Home)'].iloc[-1]
            movement = line_current - line_open
            time_open = datetime.strptime(t[name].iloc[0], fmt)
            time_close = datetime.strptime(t[name].iloc[-1], fmt)
            line_hours = (time_close - time_open).total_seconds() / 60 / 60

            if not line_hours > 0:
                velocity = 0
            else:
                velocity = movement / line_hours

            line_std = t['Line (Home)'].std()

            name = name.lower().replace('.', '_')
            names = ['open', 'close', 'movement', 'velocity', 'std']
            columns = columns + ['{}_{}'.format(name, n) for n in names]
            values = values + [line_open, line_current, movement,
                               velocity, line_std]

        if not values:
            return

        df = pd.DataFrame([dict(zip(columns, values))])
        df['event_id'] = event_id

        return df
