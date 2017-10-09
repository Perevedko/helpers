"""Decompose time series API URL.

URL format:
    
    {domain}/series/{varname}/{freq}/{?suffix}/{?start}/{?end}/{?finaliser}
    
Rule1:
    {?suffix} is translated to unit in a simple version (v1)              
    
Rule2:
    in a v2 version: 
        {?suffix} is {?rate} or {?agg} they are mutually exclusive:
            if {?suffix} is in (eop, avg) then {agg} is defined
            if {?suffix} is in (yoy, rog, base) then {rate} is defined
            {unit} name must be defined 
            
To integrate here:    
    <https://github.com/mini-kep/frontend-app/blob/master/apps/views/time_series.py>    

"""

from datetime import date

import pandas as pd
import requests
import json


ALLOWED_DOMAINS = (
    'ru',
    'oil',
    'all',
    # more domains?
)

ALLOWED_FREQUENCIES = 'dwmqa'

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
    'info',
    'csv',
    'list', # default 
    'pandas',  
    'xlsx'
)

class InnerPath:   
    
    def __init__(self, inner_path: str):
        """Extract parameters from *inner_path* string.
           
           Args:
              inner_path is a string like 'eop/2015/2017/csv' 
        """        
        # *tokens* is a list of non-empty strings
        tokens = [token.strip() for token in inner_path.split('/') if token]        
        # date parameters
        self.dict = self.assign_dates(tokens)
        # finaliser
        self.dict['fin']  = self.assign_values(tokens, ALLOWED_FINALISERS)
        # transforms
        self.dict['rate'] = self.assign_values(tokens, ALLOWED_REAL_RATES)
        self.dict['agg']  = self.assign_values(tokens, ALLOWED_AGGREGATORS)
        if self.dict['rate'] and self.dict['agg']:
            raise ValueError("Cannot combine rate and aggregation.")
        # unit name
        if tokens:
            self.dict['unit'] = tokens[0]
        else:
            self.dict['unit'] = self.dict['rate'] or None
        
    def get_dict(self):
        return self.dict

    def assign_dates(self, tokens):
        result = {}
        start_year, end_year = self.get_years(tokens)
        result['start_date'] = self.as_date(start_year, month=1, day=1)
        result['end_date'] = self.as_date(end_year, month=12, day=31)  
        return result 

    @staticmethod
    def as_date(year: str, month: int, day: int):
        if year:
            return date(year=int(year), 
                        month=month, 
                        day=day).strftime('%Y-%m-%d')
        else:
            return year             

    @staticmethod
    def get_years(tokens):
        """Extract years from a list of *tokens* strings."""
        start, end = None, None
        integers = [x for x in tokens if x.isdigit()]
        if len(integers) in (1, 2):
            start = integers[0]
            tokens.pop(tokens.index(start))
        if len(integers) == 2:
            end = integers[1]
            tokens.pop(tokens.index(end))
        return start, end

    @staticmethod
    def assign_values(tokens, allowed_values):
        """Find entries of *allowed_values* into *tokens*."""
        values_found = [p for p in allowed_values if p in tokens]
        if not values_found:
            return None
        elif len(values_found) == 1:
            x = values_found[0]
            tokens.pop(tokens.index(x))
            return x
            
        else:
            raise ValueError(values_found)

def get_freq(freq: str):
    if freq not in ALLOWED_FREQUENCIES:
        raise ValueError(f"Frequency <{freq}> is not valid")
    return freq

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
               freq=get_freq(tokens[4]))
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
    params = dict(name=name,  freq=ctx['freq'])
    upd = [(key, ctx[key]) for key in ['start_date', 'end_date'] if ctx[key]]
    params.update(upd)
    return params       

if __name__ == "__main__":
    
    from pprint import pprint
    import io
    import numpy as np


    # valid urls 
    'api/oil/series/BRENT/m/eop/2015/2017/csv' # will fail of db GET call
    'api/ru/series/EXPORT_GOODS/m/bln_rub' # will pass
    'api/ru/series/USDRUR_CB/d/xlsx' # will fail
    
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
            'start_date': None,
            'end_date': None
        },
                
        'api/ru/series/USDRUR_CB/d/xlsx': {
            'domain': 'ru',
            'varname': 'USDRUR_CB',
            'freq': 'd',
            'unit': None,
            'rate': None,
            'agg': None,
            'fin': 'xlsx',
            'start_date': None,
            'end_date': None
        }
                
    }
        
    for url, d in test_pairs.items():
        print()
        print (url)
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
    try:
        r
    except NameError:    
        r = requests.get(endpoint, params=d)            
    assert r.status_code == 200
    data = r.json()
    control_datapoint_1 = {'date': '1992-07-01', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 0.1253}
    control_datapoint_2 = {'date': '2017-09-28', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 58.0102}
    assert control_datapoint_1 in data
    assert control_datapoint_2 in data
    
    # TODO: need 'pandas' formatting parameter or another database endpoint to be able
    # to use pd.read_json(<long url>)
    
    def to_json(dicts):    
        df = pd.DataFrame(dicts)
        df.date = df.date.apply(pd.to_datetime)
        df = df.pivot(index='date', values='value', columns='name')
        return df.to_json(orient='columns')
    
       
    df = pd.DataFrame(data)
    df.date = df.date.apply(pd.to_datetime)
    df = df.pivot(index='date', values='value', columns='name')
    df = df.sort_index()
    
    assert df.USDRUR_CB['1992-07-01'] == control_datapoint_1['value']
    assert df.USDRUR_CB['2017-09-28'] == control_datapoint_2['value']
    
    
    # ERROR: something goes wrong with date handling
    #        if we use df.to_json(), we shoudl be able to read it with pd.read_json()

    data2 = to_json(dicts=data)
    f = io.StringIO(data2)
    df2 = pd.read_json(f)        
    
    # COMMENT: there are two sources of an error 
    #
    #           - one is rounding error and this is a smaller evil
    #             precise_float=True and np.close come to help
    #
    #           - the other is order of rows in df2 - this is a bigger problem
    #
    #             with default orent='columns' we cannot gaurantee the 
    #             order of rows, unless a) we sort the rows on client side,
    #             b) we change orient to something different, like 'split',
    #             both on server and client side 
    #
    df2 = df2.sort_index()
    assert np.isclose(df, df2).all()
    
    # QUESTION:
    # the original intent was to provide user with no-parameter import
    # soultion like pd.read_json('<long url>'), this does not seem to be able to work
    
    # options: 
    # 1) pd.import_json('<long url>', orient='split')
    # 2) pd.read_csv('<long url>', converters={0: pd.to_datetime}, index_col=0)
    
    # I slightly favour 2) because htis way we will hvae one format less, even
    # though it is slightly londer on client side. What is your opinion?
    f = io.StringIO(to_json(dicts=data))
    df2 = pd.read_json(f, orient='split', precise_float=True)
    assert df.equals(df2)        
 