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
    'info',  # resereved - retrun json with variable and url description 
    'csv',   # to implement: return csv (default) 
    'json',  # to implement: return list of dictionaries
    'xlsx'   # resereved - return Excel file
)

class InnerPath:   
    
    def __init__(self, inner_path: str):
        """Extract parameters from *inner_path* string.
           
           Args:
              inner_path is a string like 'eop/2015/2017/csv' 
        """        
        # *tokens* is a list of non-empty strings
        tokens = [token.strip() for token in inner_path.split('/') if token]        
        # 1. extract dates, if any
        self.dict = self.assign_dates(tokens)
        # 2. find finaliser, if any
        self.dict['fin']  = self.assign_values(tokens, ALLOWED_FINALISERS)
        # 3. find transforms, if any
        self.dict['rate'] = self.assign_values(tokens, ALLOWED_REAL_RATES)
        self.dict['agg']  = self.assign_values(tokens, ALLOWED_AGGREGATORS)
        if self.dict['rate'] and self.dict['agg']:
            raise InvalidUsage("Cannot combine rate and aggregation.")
        # 4. find unit name, if present
        if tokens:
            self.dict['unit'] = tokens[0]
        else:
            self.dict['unit'] = self.dict['rate'] or None
        
    def get_dict(self):
        return self.dict

    def assign_dates(self, tokens):        
        start_year, end_year = self.get_years(tokens)
        result = {}
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
        """Extract years from a list of *tokens* strings.
           Pops values found away from *tokens*.
        """
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
        """Find entries of *allowed_values* into *tokens*.
           Pops values found away from *tokens*.
        """
        values_found = [p for p in allowed_values if p in tokens]
        if not values_found:
            return None
        elif len(values_found) == 1:
            x = values_found[0]
            tokens.pop(tokens.index(x))
            return x            
        else:
            raise InvalidUsage(values_found)
            
            
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

class CustomGET:
    
    @staticmethod
    def make_freq(freq: str):
        if freq not in ALLOWED_FREQUENCIES:
            raise InvalidUsage(f'Frequency <{freq}> is not valid')
        return freq           
    
    @staticmethod
    def make_name(varname, unit):
        name = varname
        if unit:
            name = f'{name}_{unit}'
        return name

    @staticmethod
    def make_dates(ip: dict):
        return {key:ip[key] for key in ['start_date', 'end_date'] if ip.get(key)}
    
    def __init__(self, domain, varname, freq, inner_path):
        ip = InnerPath(inner_path).get_dict()
        self.params = dict(name=self.make_name(varname, ip['unit']),
                           freq=self.make_freq(freq))
        self.params.update(self.make_dates(ip))
        
    def get_csv(self):
        endpoint = 'http://minikep-db.herokuapp.com/api/datapoints'
        r = requests.get(endpoint, params=self.params)            
        if r.status_code == 200:
            data = r.json()
            return to_csv(data)            
        else:
            raise InvalidUsage(f'Cannot read from {endpoint}.')

# serialiser fucntion 
def yield_csv_row(dicts):
    """
    Arg: 
       dicts - list of dictionaries like 
               {'date': '1992-07-01', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 0.1253}
       
    Returns:
       string like ',USDRUR_CB\n1992-07-01,0.1253\n'           
       
    """
    datapoints = list(dicts)
    name = datapoints[0]['name']
    yield ',{}'.format(name)
    for d in datapoints:
        yield '{},{}'.format(d['date'], d['value'])
    # this yield is responsible for last \n in csv     
    yield ''
        
def to_csv(dicts):
    if dicts: 
        rows = list(yield_csv_row(dicts)) 
        return '\n'.join(rows)
    else:
        return ''


if __name__ == "__main__":
    import pytest
    from pprint import pprint
    import io
    import numpy as np
    
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
        params = dict(name=name,  freq=ctx['freq'])
        upd = [(key, ctx[key]) for key in ['start_date', 'end_date'] if ctx[key]]
        params.update(upd)
        return params  


    # valid inner urls 
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
    
    # reference dataframe
    df = pd.DataFrame(data)
    df.date = df.date.apply(pd.to_datetime)
    df = df.pivot(index='date', values='value', columns='name')
    df.index.name = None
    df = df.sort_index()
    
    assert df.USDRUR_CB['1992-07-01'] == control_datapoint_1['value']
    assert df.USDRUR_CB['2017-09-28'] == control_datapoint_2['value']    
    
    # serialisation issues 
    
    def to_json(dicts, orient='columns'):    
        df = pd.DataFrame(dicts)
        df.date = df.date.apply(pd.to_datetime)
        df = df.pivot(index='date', values='value', columns='name')
        df.index.name = None
        return df.to_json(orient=orient)

    def to_csv_df(dicts):
        df = pd.DataFrame(dicts)
        df.date = df.date.apply(pd.to_datetime)
        df = df.pivot(index='date', values='value', columns='name')
        df.index.name = None
        return df.to_csv()

    # ERROR: something goes wrong with date handling
    serialised = to_json(dicts=data)
    f = io.StringIO(serialised)
    df2 = pd.read_json(f)    
    with pytest.raises(AssertionError):
        assert df.equals(df2) 
    
    # solution 1: split + precise_float=True (@Perevedko)
    serialised = to_json(dicts=data, orient='split')
    f = io.StringIO(serialised)
    df2 = pd.read_json(f, orient='split', precise_float=True)    
    assert df.equals(df2)   
    
    # solution 2: sort index + different comparison func (@zarak)
    serialised = to_json(dicts=data, orient='columns')
    f = io.StringIO(serialised)
    df3 = pd.read_json(f) 
    df3 = df3.sort_index()
    assert np.isclose(df, df3).all()

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
    
    # QUESTION:
    # the original intent was to provide user with no-parameter import
    # soultion like pd.read_json('<long url>'), this does not seem to be able to work
    
    # options: 
    # 1) pd.import_json('<long url>', orient='split')
    # 2) pd.read_csv('<long url>', converters={0: pd.to_datetime}, index_col=0)
    
    # EP: I favour 2) because we will have less formats, even
    # though it is slightly longer on client side. 

    # EP: recommended serialisation
    serialised = to_csv(data)
    f = io.StringIO(serialised)
    df_csv = pd.read_csv(f, converters={0: pd.to_datetime}, index_col=0)
    assert np.isclose(df, df3).all()
    
    s = CustomGET('oil','BRENT', 'd', '2017').get_csv()
    assert '2017-05-23,53.19\n' in s
    
    cg2 = CustomGET('all','ZZZ', 'd', '2017')
    assert len(cg2.get_csv()) == 0
    
    assert CustomGET.make_freq('a')
    with pytest.raises(InvalidUsage):
        CustomGET.make_freq('z')
 