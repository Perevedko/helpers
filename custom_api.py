"""Decompose custom URL.

    URL format (? marks optional parameter):

    {domain}/series/{varname}/{freq}/{?suffix}/{?start}/{?end}/{?finaliser}

    Examples:
        oil/series/BRENT/m/eop/2015/2017/csv
        ru/series/EXPORT_GOODS/m/bln_rub

    Tokens:
        {domain} is reserved, future use: 'all', 'ru', 'oil', 'ru:bank', 'ru:77'

        {varname} is GDP, GOODS_EXPORT, BRENT (capital letters with underscores)

        {freq} is any of:
            a (annual)
            q (quarterly)
            m (monthly)
            w (weekly)
            d (daily)

        {?suffix} may be:

            unit of measurement (unit):
                example: bln_rub, bln_usd, tkm

            rate of change for real variable (rate):
                rog - change to previous period
                yoy - change to year ago
                base - base index

            aggregation command (agg):
                eop - end of period
                avg - average

    To integrate here:
        <https://github.com/mini-kep/frontend-app/blob/master/apps/views/time_series.py>

Decomposition procedure involves:

    CustomGET class
    InnerPath class
    to_csv()

"""

from datetime import date

import pandas as pd
import requests


ALLOWED_FREQUENCIES = ('d', 'w', 'm', 'q', 'a')

ALLOWED_REAL_RATES = (
    'rog',
    'yoy',
    'base'
)
ALLOWED_AGGREGATORS = (
    'eop',
    'avg'
)
ALLOWED_FINALISERS = (
    'info',  # resereved: retrun json with variable and url description
    'csv',   # to implement: return csv (default)
    'json',  # to implement: return list of dictionaries
    'xlsx'   # resereved: return Excel file
)


class InvalidUsage(Exception):
    """Shorter version of
       <http://flask.pocoo.org/docs/0.12/patterns/apierrors/>.

       Must also register a handler (see link above).
    """
    status_code = 400

    def __init__(self, message, status_code=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code

    def to_dict(self):
        return dict(message=self.message)


def make_freq(freq: str):
    if freq not in ALLOWED_FREQUENCIES:
        raise InvalidUsage(f'Frequency <{freq}> is not valid')
    return freq


class TokenHelper:
    def __init__(self, tokens: list):
        self.tokens = tokens

    def pop(self, value):
        self.tokens.pop(self.tokens.index(value))

    def years(self):
        """Extract years from *tokens* list. Pops values found away from *tokens*."""
        start, end = None, None
        integers = [x for x in self.tokens if x.isdigit()]
        if len(integers) in (1, 2):
            start = integers[0]
            self.pop(start)
        if len(integers) == 2:
            end = integers[1]
            self.pop(end)
        return start, end

    def fin(self):
        return self._find_one(ALLOWED_FINALISERS)

    def rate(self):
        return self._find_one(ALLOWED_REAL_RATES)

    def agg(self):
        return self._find_one(ALLOWED_AGGREGATORS)

    def _find_one(self, allowed_values):
        """Find entries of *allowed_values* into *tokens*.
           Pops values found away from *tokens*.
        """
        values_found = [p for p in allowed_values if p in self.tokens]
        if not values_found:
            return None
        elif len(values_found) == 1:
            x = values_found[0]
            self.pop(x)
            return x
        else:
            raise InvalidUsage(values_found)

    @staticmethod
    def _as_date(year: str, month: int, day: int):
        """Generate YYYY-MM-DD dates based on components."""
        if year:
            return date(year=int(year),
                        month=month,
                        day=day).strftime('%Y-%m-%d')
        else:
            return year


class InnerPath:

    @staticmethod
    def as_date(year: str, month: int, day: int):
        """Generate YYYY-MM-DD dates based on components."""
        if year:
            return date(year=int(year),
                        month=month,
                        day=day).strftime('%Y-%m-%d')
        else:
            return None

    def __init__(self, inner_path: str):
        """Extract parameters from *inner_path* string.

           Args:
              inner_path is a string like 'eop/2015/2017/csv'

           Methods:
              get_dict() returns inner path tokens as dictionary
        """
        # list of non-empty strings
        tokens = [token.strip() for token in inner_path.split('/') if token]
        helper = TokenHelper(tokens)
        self.dict = {}
        # 1. extract dates, if any
        start_year, end_year = helper.years()
        if start_year:
            self.dict['start_date'] = self.as_date(start_year, month=1, day=1)
        if end_year:
            self.dict['end_date'] = self.as_date(end_year, month=12, day=31)
        # 2. find finaliser, if any
        self.dict['fin'] = helper.fin()
        # 3. find transforms, if any
        self.dict['rate'] = helper.rate()
        self.dict['agg'] = helper.agg()
        #    but cannot have both
        if self.dict['rate'] and self.dict['agg']:
            raise InvalidUsage("Cannot combine rate and aggregation.")
        # 4. find unit name, if present
        if tokens:
            self.dict['unit'] = tokens[0]
        else:
            self.dict['unit'] = self.dict['rate'] or None

    def get_dict(self):
        return self.dict


class CustomGET:

    endpoint = 'http://minikep-db.herokuapp.com/api/datapoints'

    @staticmethod
    def make_name(varname, unit=None):
        name = varname
        if unit:
            name = f'{name}_{unit}'
        return name

    def __init__(self, domain, varname, freq, inner_path):
        ip = InnerPath(inner_path).get_dict()
        self.params = dict(name=self.make_name(varname, ip['unit']),
                           freq=make_freq(freq))
        for key in ['start_date', 'end_date']:
            val = ip.get(key)
            if val:
                self.params[key] = val

    def get_csv(self):
        _params = self.params
        _params['format'] = 'csv'
        r = requests.get(endpoint, params=_params)
        if r.status_code == 200:
            return r.text
        else:
            raise InvalidUsage(f'Cannot read from {endpoint}.')

# serialiser moved to db api


if __name__ == "__main__":
    import pytest
    from pprint import pprint

    def mimic_custom_api(path: str):
        """Decode path like:

           api/oil/series/BRENT/m/eop/2015/2017/csv
    index    0   1      2     3 4   5 ....

        """
        assert path.startswith('api/')
        tokens = [token.strip() for token in path.split('/') if token]
        # mandatoy part - in actual code taken care by flask
        ctx = dict(domain=tokens[1],
                   varname=tokens[3],
                   freq=tokens[4])
        # optional part
        if len(tokens) >= 6:
            inner_path_str = "/".join(tokens[5:])
            d = InnerPath(inner_path_str).get_dict()
            ctx.update(d)
        return ctx

    def make_db_api_get_call_parameters(path):
        ctx = mimic_custom_api(path)
        name, unit = (ctx[key] for key in ['varname', 'unit'])
        if unit:
            name = f"{name}_{unit}"
        params = dict(name=name, freq=ctx['freq'])
        upd = [(key, ctx[key])
               for key in ['start_date', 'end_date'] if ctx.get(key)]
        params.update(upd)
        return params

    # valid inner urls
    'api/oil/series/BRENT/m/eop/2015/2017/csv'  # will fail of db GET call
    'api/ru/series/EXPORT_GOODS/m/bln_rub'  # will pass
    'api/ru/series/USDRUR_CB/d/xlsx'  # will fail

    # FIXME: test for failures
    # invalid urls
    'api/oil/series/BRENT/q/rog/eop'
    'api/oil/series/BRENT/z/'

    test_pairs = {
        'api/oil/series/BRENT/m/eop/2015/2017/csv': {
            'domain': 'oil',
            'varname': 'BRENT',
            'unit': None,
            'freq': 'm',
            'rate': None,
            'start_date': '2015-01-01',
            'end_date': '2017-12-31',
            'agg': 'eop',
            'fin': 'csv'
        },
        'api/ru/series/EXPORT_GOODS/m/bln_rub': {
            'domain': 'ru',
            'varname': 'EXPORT_GOODS',
            'unit': 'bln_rub',
            'freq': 'm',
            'rate': None,
            'agg': None,
            'fin': None,
            #'start_date': None,
            #'end_date': None
        },

        'api/ru/series/USDRUR_CB/d/xlsx': {
            'domain': 'ru',
            'varname': 'USDRUR_CB',
            'freq': 'd',
            'unit': None,
            'rate': None,
            'agg': None,
            'fin': 'xlsx',
            #'start_date': None,
            #'end_date': None
        }

    }

    for url, d in test_pairs.items():
        print()
        print(url)
        pprint(d)
        assert mimic_custom_api(url) == d
        print(make_db_api_get_call_parameters(url))

    test_pairs2 = {
        'api/oil/series/BRENT/m/eop/2015/2017/csv': {
            'name': 'BRENT',
            'freq': 'm',
            'start_date': '2015-01-01',
            'end_date': '2017-12-31'},

        'api/ru/series/EXPORT_GOODS/m/bln_rub': {
            'name': 'EXPORT_GOODS_bln_rub',
            'freq': 'm'},

        'api/ru/series/USDRUR_CB/d/xlsx': {
            'name': 'USDRUR_CB',
            'freq': 'd'}
    }

    for url, d in test_pairs2.items():
        assert make_db_api_get_call_parameters(url) == d

    # get actual data from url
    # http://minikep-db.herokuapp.com/api/datapoints?name=USDRUR_CB&freq=d&start_date=2017-08-01&end_date=2017-10-01

    # using http, https fails loaclly
    endpoint = 'http://minikep-db.herokuapp.com/api/datapoints'

    # cut out calls to API if in interpreter
    d = {'format': 'json', 'freq': 'd', 'name': 'USDRUR_CB'}
    try:
        r
    except NameError:
        r = requests.get(endpoint, params=d)
    assert r.status_code == 200
    data = r.json()
    control_datapoint_1 = {
        'date': '1992-07-01',
        'freq': 'd',
        'name': 'USDRUR_CB',
        'value': 0.1253}
    control_datapoint_2 = {
        'date': '2017-09-28',
        'freq': 'd',
        'name': 'USDRUR_CB',
        'value': 58.0102}
    assert control_datapoint_1 in data
    assert control_datapoint_2 in data

    # reference dataframe
    df = pd.DataFrame(data)
    df.date = df.date.apply(pd.to_datetime)
    df = df.pivot(index='date', values='value', columns='name')
    df.index.name = None
    df = df.sort_index()

    assert df.USDRUR_CB['1992-07-01'] == control_datapoint_1['value']
    assert df.USDRUR_CB['2017-09-28'] == control_datapoint_2['value']

    s = CustomGET('oil', 'BRENT', 'd', '2017').get_csv()
    assert '2017-05-23,53.19\n' in s

    cg = CustomGET('all', 'ZZZ', 'd', '2017')
    assert len(cg.get_csv()) == 0

    assert make_freq('a') == 'a'
    with pytest.raises(InvalidUsage):
        make_freq('z')
