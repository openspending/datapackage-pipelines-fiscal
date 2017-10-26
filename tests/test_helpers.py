import datapackage_pipelines_fiscal.helpers as helpers
import unittest.mock as mock


class TestHelpers(object):
    def test_run_after_yielding_elements(self):
        resources = [
            'resource 1',
            'resource 2',
        ]
        callback = mock.MagicMock()
        iterator = helpers.run_after_yielding_elements(iter(resources), callback)

        while True:
            try:
                next(iterator)
                callback.assert_not_called()
            except StopIteration:
                callback.assert_called_once()
                break
