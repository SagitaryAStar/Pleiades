from unittest import TestCase

import pytest

from pleiades import pleiades

pytestmark = pytest.mark.unit

class PleiadesUnitTest(TestCase):
    def test_example(self):
        self.assertEqual(True, True)
