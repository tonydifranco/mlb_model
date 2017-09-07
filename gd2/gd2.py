import numpy as np
import pandas as pd
import re
import requests

from datetime import datetime
from tqdm import tqdm


class Scraper:
    def __init__(self, base_url):
        self.base_url = base_url

    def scrape_day(self, year, month, day):
        gids = self.get_gids(year, month, day)

        if gids:
            day_bs = [self.get_boxscore(year, month, day, g) for g in gids]

            if day_bs:
                day_bs = pd.DataFrame([bs for team in day_bs
                                       if team is not None
                                       for bs in team
                                       if bs is not None])

            return day_bs

    def get_gids(self, year, month, day):
        url = '{}/year_{}/month_{}/day_{}'.format(
          self.base_url,
          year,
          month,
          day)
        r = requests.get(url)
        gids = re.findall('<a href="(gid.*)/"', r.text)

        return gids

    def get_linescore(self, year, month, day, gid):
        url = '{}/year_{}/month_{}/day_{}/{}/linescore.json'.format(
            self.base_url,
            year,
            month,
            day,
            gid)

        try:
            r = requests.get(url)
        except:
            print('error scraping linescore from gid {}'.format(gid))
            return

        if r.status_code != requests.codes.ok:
            return

        content = r.json()
        ls = content['data']['game']

        if ls['ind'] != 'F':
            print('ignoring gid {} with ind of {}'.format(gid, ls['ind']))
            return

        if ls['game_type'] in ['S', 'E']:
            print('ignoring gid {} with game_type {}'.format(gid,
                                                             ls['game_type']))
            return

        ls_keep = ['league', 'game_type', 'home_division', 'away_division',
                   'home_time', 'away_time', 'home_ampm', 'away_ampm',
                   'home_games_back', 'away_games_back',
                   'home_games_back_wildcard', 'away_games_back_wildcard']

        game = {k: ls[k] if k in ls else None for k in ls_keep}
        game['gid'] = gid

        return game

    def games_back_to_number(self, games_back):
        if not isinstance(games_back, str):
            return None
        elif games_back.startswith('+') or games_back.startswith('-'):
            return 0
        else:
            try:
                return float(games_back)
            except:
                print('error parsing games_back value {}'.format(games_back))
                return None

    def get_boxscore(self, year, month, day, gid):
        ls = self.get_linescore(year, month, day, gid)

        if ls is None:
            return

        url = '{}/year_{}/month_{}/day_{}/{}/boxscore.json'.format(
            self.base_url,
            year,
            month,
            day,
            gid)

        try:
            r = requests.get(url)
        except:
            print('error scraping boxsore from gid {}'.format(gid))
            return

        if r.status_code != requests.codes.ok:
            return

        content = r.json()
        bs = content['data']['boxscore']

        if bs['status_ind'] not in ['F', 'FR']:
            print('ignoring gid {} with status_ind {}'.format(gid,
                                                              bs['status_ind']))
            return

        division_game = 0
        same_div = (ls['home_division'] == ls['away_division'])
        not_inter = (ls['league'] in ['AA', 'NN'])
        if same_div and not_inter:
            division_game = 1

        fmt = '%B %d, %Y %I:%M %p'
        home_time = datetime.strptime('{} {} {}'.format(bs['date'],
                                                        ls['home_time'],
                                                        ls['home_ampm']), fmt)
        away_time = datetime.strptime('{} {} {}'.format(bs['date'],
                                                        ls['away_time'],
                                                        ls['away_ampm']), fmt)
        away_time_diff = (home_time - away_time).total_seconds() / 60 / 60

        teams = [
            {
                'team_code': bs['away_team_code'],
                'games_back': self.games_back_to_number(ls['away_games_back']),
                'games_back_wildcard': self.games_back_to_number(ls['away_games_back_wildcard']),
                'home_away': 'away'
            },
            {
                'team_code': bs['home_team_code'],
                'games_back': self.games_back_to_number(ls['home_games_back']),
                'games_back_wildcard': self.games_back_to_number(ls['home_games_back_wildcard']),
                'home_away': 'home'
            }
        ]

        hp_ump = None
        wind_speed = None
        wind_dir = None
        temp = None
        clouds = None

        if 'game_info' in bs:
            hp_ump = re.findall('position="HP" name="(.*)"></umpire>',
                                bs['game_info'])
            if hp_ump:
                hp_ump = hp_ump[0]
            check_wind_speed = re.findall('<wind>(\\d+) mph', bs['game_info'])
            if check_wind_speed:
                wind_speed = check_wind_speed[0]
            check_wind_dir = re.findall('mph, (.*)</wind>', bs['game_info'])
            if check_wind_dir:
                wind_dir = check_wind_dir[0]
            check_temp = re.findall('<weather>(\\d+) degrees', bs['game_info'])
            if check_temp:
                temp = check_temp[0]
            check_clouds = re.findall('degrees, (.*)</weather>',
                                      bs['game_info'])
            if check_clouds:
                clouds = check_clouds[0]

        for i, t in enumerate(teams):
            teams[i]['gid'] = gid
            teams[i]['hp_ump'] = hp_ump
            teams[i]['wind_speed'] = wind_speed
            teams[i]['wind_dir'] = wind_dir
            teams[i]['temp'] = temp
            teams[i]['clouds'] = clouds
            teams[i]['date'] = bs['date']
            teams[i]['status_ind'] = bs['status_ind']
            teams[i]['wins'] = bs['{}_wins'.format(t['home_away'])]
            teams[i]['loss'] = bs['{}_loss'.format(t['home_away'])]
            teams[i]['division_game'] = division_game
            teams[i]['league'] = ls['league']
            teams[i]['away_time_diff'] = away_time_diff
            teams[i]['game_type'] = ls['game_type']

        batting_ignored = ['batter', 'text_data', 'text_data_es', 'note',
                           'note_es', 'team_flag']
        for b in bs['batting']:
            for k in set(b.keys() - batting_ignored):
                if b['team_flag'] == 'away':
                    teams[0]['batting_{}'.format(k)] = b[k]
                else:
                    teams[1]['batting_{}'.format(k)] = b[k]

        pitching_ignored = ['pitcher']
        starter_ignored = ['id', 'name', 'pos', 'sv', 'bs', 'hld', 'note']
        for p in bs['pitching']:

            if isinstance(p['pitcher'], dict):
                starter = p['pitcher']
            else:
                starter = p['pitcher'][0]

            for k in set(starter.keys() - starter_ignored):
                if p['team_flag'] == 'away':
                    teams[0]['starter_{}'.format(k)] = starter[k]
                else:
                    teams[1]['starter_{}'.format(k)] = starter[k]

            for k in set(p.keys() - pitching_ignored):
                if p['team_flag'] == 'away':
                    teams[0]['pitching_{}'.format(k)] = p[k]
                else:
                    teams[1]['pitching_{}'.format(k)] = p[k]

        return teams
