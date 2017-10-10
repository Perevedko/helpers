import pytest
import custom_api


class TestCustomGET(object):
    def test_make_freq_with_valid_param_is_ok(self):
        freq = 'q'
        produced_freq = custom_api.CustomGET.make_freq(freq)
        assert freq == produced_freq

    def test_make_freq_with_invalid_param_should_fail(self):
        with pytest.raises(custom_api.InvalidUsage):
            custom_api.CustomGET.make_freq('t')

    def test_make_freq_with_empty_string_param_should_fail(self):
        with pytest.raises(custom_api.InvalidUsage):
            custom_api.CustomGET.make_freq('')

    def test_make_dates_with_empty_dict_should_produce_empty_dict(self):
        assert {} == custom_api.CustomGET.make_dates({})

    def test_get_csv_with_valid_params_should_fetch_data(self):
        get = custom_api.CustomGET('oil', 'BRENT', 'd', '2017').get_csv()
        assert '2017-05-23,53.19\n' in get

    def test_get_csv_with_bad_params_should_return_empty_csv_string(self):
        get = custom_api.CustomGET('all', 'ZZZ', 'd', '2017')
        assert len(get.get_csv()) == 0


class TestInnerPath(object):
    def test_as_date_with_valid_date(self):
        as_date = custom_api.InnerPath.as_date('2010', 5, 3)
        assert as_date == '2010-05-03'

    def test_as_date_with_invalid_date(self):
        with pytest.raises(ValueError):
            custom_api.InnerPath.as_date('2010', 0, 0)

    def test_get_years_with_two_years_given(self):
        start_year, end_year = custom_api.InnerPath.get_years(['avg', '2005', '2007', 'json'])
        assert start_year == '2005' and end_year == '2007'

    def test_get_years_with_one_year_given(self):
        start_year, end_year = custom_api.InnerPath.get_years(['avg', '2001', 'csv'])
        assert start_year == '2001' and end_year is None

    def test_get_with_no_years_given(self):
        start_year, end_year = custom_api.InnerPath.get_years(['eop', 'csv'])
        assert start_year is None and end_year is None

    def test_get_dict_with_valid_inner_path(self):
        path = custom_api.InnerPath('eop/2015/2018/csv')
        assert path.get_dict() == {
            'start_date': '2015-01-01',
            'end_date': '2018-12-31',
            'fin': 'csv',
            'rate': None,
            'agg': 'eop',
            'unit': None
        }

    def test_constructor_with_both_rate_and_agg_given_should_fail(self):
        with pytest.raises(ValueError):
            custom_api.InnerPath('eop/rog/2015/2018/csv')

    def test_assign_values_should_pop_value(self):
        tokens = ['a', 'b', 'c', 'd']
        allowed_values = ['d', 'e', 'f']
        value = custom_api.InnerPath.assign_values(tokens, allowed_values)
        assert value == 'd' and tokens == ['a', 'b', 'c']

    def test_assign_values_should_fail_if_found_multiple_values_(self):
        tokens = ['a', 'b', 'c', 'd']
        allowed_values = ['a', 'b', 'c']
        with pytest.raises(ValueError):
            custom_api.InnerPath.assign_values(tokens, allowed_values)

    def test_assign_values_on_empty_lists_is_ok(self):
        tokens = []
        allowed_values = []
        value = custom_api.InnerPath.assign_values(tokens, allowed_values)
        assert value is None and tokens == [] and allowed_values == []


class TestToCSV(object):
    def test_empty_dict_should_produce_empty_string(self):
        assert custom_api.to_csv({}) == ''

    def test_on_sample_valid_data(self):
        data = [
            {'date': '1992-07-01', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 0.1253},
            {'date': '2017-09-28', 'freq': 'd', 'name': 'USDRUR_CB', 'value': 58.0102}
        ]
        expected_result = ',USDRUR_CB\n1992-07-01,0.1253\n2017-09-28,58.0102\n'
        assert custom_api.to_csv(data) == expected_result

    def test_should_fail_on_invalid_data(self):
        data = [
            {'a': 1, 'b': 2, 'c': 3},
            {'x': [], 'y': {}},
            {}
        ]
        with pytest.raises(KeyError):
            custom_api.to_csv(data)

