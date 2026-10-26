"""MCOL-6609: loadbrm checks every BRM file it pulls from the primary.

get_bundle() must refuse a bundle whose files don't match their size and
sha256, aren't valid base64, or carry an empty extent map, rather than load a
damaged BRM image on a replica.

mcs-loadbrm.py.in is loaded as is: its @VAR@ placeholders are only string
literals, and nothing here uses them. It must stay Python 3.6-compatible, and
so must this test.
"""
import base64
import hashlib
import importlib.machinery
import importlib.util
import json
import os
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

LOADBRM_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'mcs-loadbrm.py.in'
)


def load_loadbrm():
    loader = importlib.machinery.SourceFileLoader('mcs_loadbrm', LOADBRM_PATH)
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


loadbrm = load_loadbrm()

FILES = {
    'em': b'extent map',
    'journal': b'journal',
    'vbbm': b'vbbm',
    'vss': b'vss',
}
EMPTY_EM = dict(FILES, em=b'')


def bundle_entry(data):
    return {
        'data': base64.b64encode(data).decode(),
        'size': len(data),
        'sha256': hashlib.sha256(data).hexdigest(),
    }


def make_bundle(files=None, generation='BRM_savesA'):
    files = FILES if files is None else files
    return {
        'generation': generation,
        'files': {name: bundle_entry(data) for name, data in files.items()},
    }


class GetBundleTest(unittest.TestCase):
    def get_bundle(self, bundle):
        response = json.dumps(bundle).encode()
        with patch.object(loadbrm, 'get_meta', return_value=response) as get_meta:
            result = loadbrm.get_bundle('conf root')
        get_meta.assert_called_once_with('conf root', 'bundle')
        return result

    def test_valid_bundle_returns_generation_and_files(self):
        generation, files = self.get_bundle(make_bundle())
        self.assertEqual(generation, 'BRM_savesA')
        self.assertEqual(files, FILES)

    def test_size_mismatch_raises(self):
        bundle = make_bundle()
        bundle['files']['vss']['size'] += 1
        with self.assertRaisesRegex(ValueError, 'vss .*failed verification'):
            self.get_bundle(bundle)

    def test_sha256_mismatch_raises(self):
        bundle = make_bundle()
        # same length, different bytes: only the hash can catch it
        bundle['files']['em']['data'] = base64.b64encode(b'extent mop').decode()
        with self.assertRaisesRegex(ValueError, 'em .*failed verification'):
            self.get_bundle(bundle)

    def test_bad_base64_raises(self):
        bundle = make_bundle()
        # a lenient decode drops the '!' and the size and sha256 still match
        data = bundle['files']['journal']['data']
        bundle['files']['journal']['data'] = data[:4] + '!' + data[4:]
        with self.assertRaises(ValueError):
            self.get_bundle(bundle)

    def test_empty_em_raises(self):
        with self.assertRaisesRegex(ValueError, 'empty extent map'):
            self.get_bundle(make_bundle(EMPTY_EM))


def http_error(code):
    return HTTPError('https://primary:8640/', code, 'error', {}, None)


class PullBrmFilesTest(unittest.TestCase):
    """pull_brm_files() retries with backoff, within PULL_DEADLINE."""

    def setUp(self):
        self.clock = 0.0
        self.sleeps = []

        def sleep(seconds):
            self.sleeps.append(seconds)
            self.clock += seconds

        for patcher in (
            patch.object(loadbrm.time, 'time', side_effect=lambda: self.clock),
            patch.object(loadbrm.time, 'sleep', side_effect=sleep),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def pull(self, bundle_results, get_meta=None):
        with patch.object(
            loadbrm, 'get_bundle', side_effect=bundle_results
        ) as get_bundle, patch.object(
            loadbrm, 'get_meta', side_effect=get_meta
        ) as meta:
            try:
                return loadbrm.pull_brm_files('conf root')
            finally:
                self.bundle_calls = get_bundle.call_count
                self.meta_calls = [c[0] for c in meta.call_args_list]

    def test_503_twice_then_ok(self):
        files = self.pull([http_error(503), http_error(503), ('BRM_savesA', FILES)])
        self.assertEqual(files, FILES)
        self.assertEqual(self.bundle_calls, 3)
        self.assertEqual(self.sleeps, [1, 2])

    def test_a_damaged_bundle_is_retried(self):
        files = self.pull([ValueError('em failed verification'), ('BRM_savesA', FILES)])
        self.assertEqual(files, FILES)
        self.assertEqual(self.sleeps, [1])

    def test_every_attempt_failing_raises(self):
        with self.assertRaisesRegex(RuntimeError, 'in 5 attempts.*503'):
            self.pull([http_error(503)] * loadbrm.PULL_ATTEMPTS)
        self.assertEqual(self.bundle_calls, loadbrm.PULL_ATTEMPTS)
        self.assertEqual(self.sleeps, [1, 2, 4, 8])

    def test_no_attempt_starts_after_the_deadline(self):
        # 1 + 2 s of backoff fit in 4 s; the next 4 s would not
        with patch.object(loadbrm, 'PULL_DEADLINE', 4):
            with self.assertRaisesRegex(RuntimeError, 'in 3 attempts'):
                self.pull([http_error(503)] * loadbrm.PULL_ATTEMPTS)
        self.assertEqual(self.bundle_calls, 3)
        self.assertEqual(self.sleeps, [1, 2])

    def test_404_pulls_the_files_one_by_one(self):
        # an older CMAPI on the primary has no bundle endpoint
        files = self.pull(
            [http_error(404)], get_meta=lambda conf, meta_type: FILES[meta_type]
        )
        self.assertEqual(files, FILES)
        self.assertEqual(
            self.meta_calls, [('conf root', name) for name in loadbrm.META_ELEMS]
        )
        self.assertEqual(self.sleeps, [])

    def test_404_with_an_empty_em_raises(self):
        with self.assertRaisesRegex(ValueError, 'empty extent map'):
            self.pull(
                [http_error(404)],
                get_meta=lambda conf, meta_type: EMPTY_EM[meta_type],
            )


if __name__ == '__main__':
    unittest.main()
