from bs4 import BeautifulSoup
import pandas as pd
import requests

mlbam_team_lookup = {
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
    'WAS': 'was',
}

covers_team_number = {
    'ARI': '2968',
    'ATL': '2957',
    'BAL': '2959',
    'BOS': '2966',
    'CHC': '2982',
    'CHW': '2974',
    'CIN': '2961',
    'CLE': '2980',
    'COL': '2956',
    'DET': '2978',
    'HOU': '2981',
    'KC': '2965',
    'LAA': '2979',
    'LAD': '2967',
    'MIA': '2963',
    'MIL': '2976',
    'MIN': '2983',
    'NYM': '2964',
    'NYY': '2970',
    'OAK': '2969',
    'PHI': '2958',
    'PIT': '2971',
    'SD': '2955',
    'SEA': '2973',
    'SF': '2962',
    'STL': '2975',
    'TB': '2960',
    'TEX': '2977',
    'TOR': '2984',
    'WAS': '2972'
}


class MlbLines:
    gid_template = 'gid_20{year}_{month}_{day}_{away}mlb_{home}mlb_{game_num}'
    covers_url_template = ('http://www.covers.com/pageLoader/pageLoader.aspx?'
                           'page=/data/mlb/teams/pastresults/{year}/'
                           'team{team_num}.html')

    def __init__(self, year, team):
        self.year = year
        self.team = team

    def _get_regular_season_odds_table(self):
        team_url = self.covers_url_template.format(
            year=str(self.year), team_num=covers_team_number[self.team])
        req = requests.get(team_url)
        soup = BeautifulSoup(req.text, 'html.parser')

        tables = soup.find_all('table')

        # if team made the playoffs there will be 2 tables
        if len(tables) == 2:
            odds_table = pd.read_html(str(tables[1]), header=0)[0]
        else:
            odds_table = pd.read_html(str(tables[0]), header=0)[0]

        return odds_table[::-1].iterrows()

    def odds_table_to_rows(self):
        previous_date = ''
        for idx, row in self._get_regular_season_odds_table():

            # if double header second game num is 2
            month, day, year = row[0].split('/')

            if previous_date == row[0]:
                game_num = 2
            else:
                game_num = 1
            previous_date = row[0]

            try:
                us_line = int(row[5].split()[-1])
            except ValueError:
                us_line = None

            if not us_line:
                decimal_line = None
            elif us_line < 1:
                decimal_line = round(-100 / us_line + 1, 2)
            else:
                decimal_line = round(us_line / 100+1, 2)

            try:
                over_under = float(row[6].split()[1])
            except ValueError:
                over_under = None

            if row[1].startswith("@"):
                away = mlbam_team_lookup[self.team]
                home = mlbam_team_lookup[row[1].split()[-1]]
            else:
                home = mlbam_team_lookup[self.team]
                away = mlbam_team_lookup[row[1]]

            yield {
                'gid': self.gid_template.format(year=year, month=month,
                                                day=day, away=away, home=home,
                                                game_num=game_num),
                'team': mlbam_team_lookup[self.team],
                'us_line': us_line,
                'decimal_line': decimal_line,
                'over_under': over_under}

    def table_rows(self):
        return pd.DataFrame([row for row in self.odds_table_to_rows()
                             if row is not None])
