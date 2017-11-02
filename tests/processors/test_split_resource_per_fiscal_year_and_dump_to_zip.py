import datapackage_pipelines_fiscal.processors.split_resource_per_fiscal_year_and_dump_to_zip as processor
import unittest.mock as mock
import pytest
import tempfile
from datapackage import Package


class TestSplitResourcePerFiscalYearAndDumpToZip(object):
    def test_create_one_resource_per_fiscal_year(self, parameters, dp_descriptor):
        dp_descriptor['resources'][0].update({
            'data': [
                {'fiscalYear': 2017, 'value': 0},
                {'fiscalYear': 2017, 'value': 2},
                {'fiscalYear': 2016, 'value': 10},
                {'fiscalYear': 2017, 'value': 3},
                {'fiscalYear': 2018, 'value': 5},
                {'fiscalYear': 2017, 'value': 8},
                {'fiscalYear': 2018, 'value': 21},
                {'fiscalYear': 2016, 'value': 50},
            ],
        })

        parameters['in-file'] = dp_descriptor
        processor.run(parameters)

        dp = Package(parameters['out-file'])

        main_resource = dp_descriptor['resources'][0]
        assert len(dp.resources) == 4
        assert dp.resources[0].descriptor.get('schema', {}).get('fields') == main_resource['schema']['fields']

        for year_resource in dp.resources[1:]:
            year = int(year_resource.descriptor['name'])
            expected_data = [
                row for row in main_resource['data']
                if row['fiscalYear'] == year
            ]
            assert year_resource.descriptor['schema'] == main_resource['schema']
            assert year_resource.read(keyed=True) == expected_data

    def test_resource_per_fiscal_year_are_sorted_desc(self, parameters, dp_descriptor):
        dp_descriptor['resources'][0].update({
            'data': [
                {'fiscalYear': 2017, 'value': 0},
                {'fiscalYear': 2016, 'value': 10},
                {'fiscalYear': 2018, 'value': 5},
                {'fiscalYear': 2015, 'value': 21},
                {'fiscalYear': 2019, 'value': 50},
            ],
        })

        parameters['in-file'] = dp_descriptor
        processor.run(parameters)

        dp = Package(parameters['out-file'])
        year_resource_names = [res.descriptor['name'] for res in dp.resources[1:]]

        assert year_resource_names == sorted(year_resource_names, reverse=True)

    def test_dont_create_extra_resources_if_theres_only_one_fiscal_year(self, parameters, dp_descriptor):
        dp_descriptor['resources'][0].update({
            'data': [
                {'fiscalYear': 2017, 'value': 0},
                {'fiscalYear': 2017, 'value': 2},
                {'fiscalYear': 2017, 'value': 20},
            ],
        })

        parameters['in-file'] = dp_descriptor
        processor.run(parameters)

        dp = Package(parameters['out-file'])

        assert len(dp.resources) == 1

    def test_dont_create_extra_resources_if_theres_no_fiscal_year_column(self, parameters, dp_descriptor):
        del dp_descriptor['resources'][0]['schema']['fields']
        original_num_resources = len(dp_descriptor['resources'])
        parameters['in-file'] = dp_descriptor

        processor.run(parameters)

        dp = Package(parameters['out-file'])
        assert len(dp.resources) == original_num_resources

    def test_doesnt_add_resources_if_if_theres_more_than_one_resource(self, parameters, dp_descriptor):
        dp_descriptor['resources'].append(dp_descriptor['resources'][0])
        original_num_resources = len(dp_descriptor['resources'])
        parameters['in-file'] = dp_descriptor

        processor.run(parameters)

        dp = Package(parameters['out-file'])
        assert len(dp.resources) == original_num_resources

    def test_doesnt_add_resources_if_theres_no_fiscal_year_field(self, parameters, dp_descriptor):
        dp_descriptor['resources'][0].update({
            'schema': {
                'fields': [
                    {
                        'name': 'value',
                        'type': 'integer',
                    },
                ],
            },
            'data': [
                {'value': 20},
            ],
        })
        original_num_resources = len(dp_descriptor['resources'])
        parameters['in-file'] = dp_descriptor


        processor.run(parameters)

        dp = Package(parameters['out-file'])
        assert len(dp.resources) == original_num_resources

    def test_doesnt_add_resources_if_therere_multiple_fields_with_fiscal_year(self, parameters, dp_descriptor):
        dp_descriptor['resources'][0].update({
            'schema': {
                'fields': [
                    {
                        'name': 'fiscalYear',
                        'osType': 'date:fiscal-year',
                        'type': 'integer',
                    },
                    {
                        'name': 'fiscalYear2',
                        'osType': 'date:fiscal-year',
                        'type': 'integer',
                    },
                ],
            },
            'data': [
                {'fiscalYear': 2017, 'fiscalYear2': 2018},
            ],
        })
        parameters['in-file'] = dp_descriptor

        processor.run(parameters)

        dp = Package(parameters['out-file'])
        assert len(dp.resources) == 1



@pytest.fixture
def dp_descriptor():
    return {
        'name': 'my_datapackage',
        'profile': 'tabular-data-package',
        'resources': [
            {
                'name': 'main',
                'profile': 'tabular-data-resource',
                'encoding': 'utf-8',
                'schema': {
                    'fields': [
                        {
                            'name': 'fiscalYear',
                            'osType': 'date:fiscal-year',
                            'type': 'integer',
                            'format': 'default',
                        },
                        {
                            'name': 'value',
                            'type': 'integer',
                            'format': 'default',
                        },
                    ],
                    'missingValues': [''],
                },
                'data': [
                    {'fiscalYear': 2017, 'value': 0},
                    {'fiscalYear': 2018, 'value': 2},
                    {'fiscalYear': 2019, 'value': 20},
                ],
            },
        ],
    }


@pytest.fixture
def parameters(dp_descriptor):
    with tempfile.NamedTemporaryFile() as out_file:
        yield {
            'in-file': dp_descriptor,
            'out-file': out_file,
        }
