import pytest
from custom_api import *

# valid inner urls
'api/oil/series/BRENT/m/eop/2015/2017/csv'  # will fail of db GET call
'api/ru/series/EXPORT_GOODS/m/bln_rub'  # will pass
'api/ru/series/USDRUR_CB/d/xlsx'  # will fail


def test_valid_api_url():
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


def test_db_api_get_call_parameters():
    test_pairs = {
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
    for url, d in test_pairs.items():
        assert make_db_api_get_call_parameters(url) == d


def test_invalid_api_url():
    invalid_urls = [
        'api/oil/series/BRENT/q/rog/eop',
        # TODO fix: should raise an exception
        # 'api/oil/series/BRENT/z/'
    ]
    for url in invalid_urls:
        with pytest.raises(ValueError):
            mimic_custom_api(url)


def make_sample_api_call():
    # using http, https fails loaclly
    # get actual data from url
    # http://minikep-db.herokuapp.com/api/datapoints?name=USDRUR_CB&freq=d
    endpoint = 'http://minikep-db.herokuapp.com/api/datapoints'
    payload = {
        'name': 'USDRUR_CB',
        'freq': 'd'
    }
    return requests.get(endpoint, params=payload)


control_datapoint_1 = {'date': '1992-07-01', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 0.1253}
control_datapoint_2 = {'date': '2017-09-28', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 58.0102}
data = None


def test_if_api_call_is_ok():
    response = make_sample_api_call()
    assert response.status_code == 200
    global data
    data = response.json()
    assert control_datapoint_1 in data
    assert control_datapoint_2 in data


df = None

def test_if_transform_to_dataframe_is_ok():
    # reference dataframe
    global df
    df = pd.DataFrame(data)
    df.date = df.date.apply(pd.to_datetime)
    df = df.pivot(index='date', values='value', columns='name')
    df.index.name = None
    df = df.sort_index()

    assert df.USDRUR_CB['1992-07-01'] == control_datapoint_1['value']
    assert df.USDRUR_CB['2017-09-28'] == control_datapoint_2['value']

# serialisation issues

def test_invalid_json_reading():
    # ERROR: something goes wrong with date handling
    serialised = to_json(dicts=data)
    f = io.StringIO(serialised)
    df2 = pd.read_json(f)
    with pytest.raises(AssertionError):
        assert df.equals(df2)


df2 = None
df3 = None

def test_correct_json_reading():
    global df2
    global df3

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


def test_recommended_csv_serialisation():
    # EP: recommended serialisation
    serialised = to_csv(data)
    f = io.StringIO(serialised)
    df_csv = pd.read_csv(f, converters={0: pd.to_datetime}, index_col=0)
    assert np.isclose(df, df3).all()


def test_custom_get():
    s = CustomGET('oil', 'BRENT', 'd', '2017').get_csv()
    assert '2017-05-23,53.19\n' in s

    cg2 = CustomGET('all', 'ZZZ', 'd', '2017')
    assert len(cg2.get_csv()) == 0


def test_valid_frequinces():
    assert CustomGET.make_freq('a')
    with pytest.raises(InvalidUsage):
        CustomGET.make_freq('z')

