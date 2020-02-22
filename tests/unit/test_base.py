from typing import Dict, List
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch


class TestBase(TestCase):
    """Base class for unit tests."""

    # Paths in this list will be automatically patched for all test cases.
    # Overwrite in subclasses to populate the list.
    _to_patch: List[str] = []

    def __init__(self, *args: str, **kwargs: str):
        """Initialize a TestBase instance.

        Args
            args: positional arguments for unittest.TestCase
            kwargs: keyword arguments for unittest.TestCase

        """
        super().__init__(*args, **kwargs)

        self._mocks: Dict[str, MagicMock]

    def setUp(self):
        self._mocks = {}
        for path in self._to_patch:
            if path.endswith('#PROPERTY'):
                path, _ = path.split('#PROPERTY')
                name = path.split('.')[-1]
                patcher = patch(path, new_callable=PropertyMock)
                prop_mock = patcher.start()
                self._mocks[name] = prop_mock
            else:
                patcher = patch(path)
                name = path.split('.')[-1]
                self._mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)
