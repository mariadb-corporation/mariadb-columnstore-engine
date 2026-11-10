"""MCOL-6609: the BRM bundle a replica's loadbrm pulls from the primary.

Every file must come from the one save generation BRM_saves_current names,
even when a save moves it during the read.
"""
import base64
import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cmapi_server.controllers import endpoints
from cmapi_server.controllers.error import APIError


class BRMBundleFixture(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        for generation in ('BRM_savesA', 'BRM_savesB'):
            for element in ('em', 'vbbm', 'vss'):
                path = self.dir / f'{generation}_{element}'
                path.write_bytes(f'{generation} {element}'.encode())
        self.current = self.dir / 'BRM_saves_current'
        self.current.write_text('BRM_savesA\n')
        # a save writes current, then truncates the journal
        self.journal = self.dir / 'BRM_saves_journal'
        self.journal.write_bytes(b'journal')
        self.set_mtime(self.current, 100)
        self.set_mtime(self.journal, 200)

        node_config = patch.object(endpoints, 'NodeConfig')
        node_config.start().return_value.s3_enabled.return_value = False
        for patcher in (
            node_config,
            patch.object(endpoints, 'MCS_EM_PATH', str(self.dir)),
            patch.object(endpoints, 'MCS_BRM_CURRENT_PATH', str(self.current)),
            patch.object(endpoints.time, 'sleep'),
        ):
            if patcher is not node_config:
                patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def set_mtime(path, seconds):
        os.utime(path, ns=(seconds * 10**9, seconds * 10**9))

    def read_with(self, hook):
        """read_brm_bundle(), calling hook(element) after each file it reads."""
        read_element = endpoints._read_brm_element  # pylint: disable=protected-access
        reads = []

        def read_and_hook(s3_enabled, current, element):
            data = read_element(s3_enabled, current, element)
            reads.append((current, element))
            hook(element)
            return data

        with patch.object(endpoints, '_read_brm_element', side_effect=read_and_hook):
            return endpoints.read_brm_bundle(), reads

    def decode(self, bundle):
        files = {}
        for element, entry in bundle['files'].items():
            data = base64.b64decode(entry['data'])
            self.assertEqual(entry['size'], len(data))
            self.assertEqual(entry['sha256'], hashlib.sha256(data).hexdigest())
            files[element] = data
        return files


class TestBRMBundle(BRMBundleFixture):
    def test_one_generation(self):
        bundle = endpoints.read_brm_bundle()
        self.assertEqual(bundle['generation'], 'BRM_savesA')
        self.assertEqual(
            self.decode(bundle),
            {
                'em': b'BRM_savesA em',
                'journal': b'journal',
                'vbbm': b'BRM_savesA vbbm',
                'vss': b'BRM_savesA vss',
            },
        )

    def test_save_during_read_is_retried(self):
        read_element = endpoints._read_brm_element  # pylint: disable=protected-access
        moved = []

        def save_after_vbbm(s3_enabled, current, element):
            data = read_element(s3_enabled, current, element)
            if element == 'vbbm' and not moved:
                moved.append(current)
                self.current.write_text('BRM_savesB\n')
            return data

        with patch.object(endpoints, '_read_brm_element', side_effect=save_after_vbbm):
            bundle = endpoints.read_brm_bundle()

        self.assertEqual(moved, ['BRM_savesA'])
        self.assertEqual(bundle['generation'], 'BRM_savesB')
        files = self.decode(bundle)
        self.assertEqual(files['em'], b'BRM_savesB em')
        self.assertEqual(files['vss'], b'BRM_savesB vss')

    def test_unreadable_set_is_refused(self):
        for name, data in (('BRM_savesA_em', b''), ('BRM_savesA_vss', None)):
            path = self.dir / name
            original = path.read_bytes()
            with self.subTest(name):
                if data is None:
                    path.unlink()
                else:
                    path.write_bytes(data)
                with self.assertRaises(APIError) as ctx:
                    endpoints.read_brm_bundle()
                self.assertEqual(ctx.exception.code, 503)
            path.write_bytes(original)

class TestBRMBundleSaveRace(BRMBundleFixture):
    """A save on the primary while the bundle is being read."""

    def test_save_rewriting_the_same_generation_is_retried(self):
        # After a worker restart the toggle starts at A again, so a save can
        # rewrite the generation current already names.
        saved = []

        def save_after_vbbm(element):
            if element == 'vbbm' and not saved:
                saved.append(True)
                (self.dir / 'BRM_savesA_em').write_bytes(b'BRM_savesA em v2')
                self.current.write_text('BRM_savesA\n')
                self.set_mtime(self.current, 300)
                self.set_mtime(self.journal, 400)

        bundle, reads = self.read_with(save_after_vbbm)
        self.assertEqual(len(reads), 8)
        self.assertEqual(bundle['generation'], 'BRM_savesA')
        self.assertEqual(self.decode(bundle)['em'], b'BRM_savesA em v2')

    def test_journal_older_than_current_waits_one_attempt(self):
        # Between writing current and truncating the journal, the journal
        # still belongs to the previous generation.
        self.set_mtime(self.journal, 50)
        bundle, reads = self.read_with(lambda element: None)
        self.assertEqual(len(reads), 8)
        self.assertEqual(bundle['generation'], 'BRM_savesA')


class TestBRMBundleMissingJournal(BRMBundleFixture):
    def test_missing_journal_is_a_503(self):
        # The first worker creates the journal when it starts, before any
        # save writes current, so this means it was removed or a restore left
        # it out. Each attempt fails to stat it; the answer must be the
        # retryable 503, not an unhandled error.
        self.journal.unlink()
        with patch.object(
            endpoints, '_brm_save_marker', wraps=endpoints._brm_save_marker
        ) as marker:
            with self.assertRaises(APIError) as ctx:
                endpoints.read_brm_bundle()
        self.assertEqual(ctx.exception.code, 503)
        self.assertEqual(marker.call_count, endpoints.BRM_BUNDLE_ATTEMPTS)


class TestBRMBundleS3(BRMBundleFixture):
    """On S3 every file is read with smcat, and there is no stat to compare."""

    def setUp(self):
        super().setUp()
        self.files = {
            element: f'S3 {element}'.encode() for element in endpoints.BRM_ELEMENTS
        }
        prefix = endpoints.EM_PATH_SUFFIX
        self.smcat_files = {endpoints.S3_BRM_CURRENT_PATH: b'BRM_savesB\n'}
        for element, data in self.files.items():
            generation = 'BRM_saves' if element == 'journal' else 'BRM_savesB'
            self.smcat_files[f'{prefix}/{generation}_{element}'] = data
        missing = str(self.dir / 'not-on-local-disk')
        node_config = patch.object(endpoints, 'NodeConfig')
        node_config.start().return_value.s3_enabled.return_value = True
        for patcher in (
            node_config,
            patch.object(endpoints, 'MCS_EM_PATH', missing),
            patch.object(endpoints, 'MCS_BRM_CURRENT_PATH', missing),
            patch.object(endpoints.MCSProcessManager, 'dispatcher_name', 'container'),
        ):
            if patcher is not node_config:
                patcher.start()
            self.addCleanup(patcher.stop)
        self.smcat = patch.object(
            endpoints.subprocess, 'run', side_effect=self.fake_smcat
        ).start()
        self.addCleanup(patch.stopall)

    def fake_smcat(self, args, stdout=None):
        self.assertEqual(args[0], 'smcat')
        data = self.smcat_files.get(args[1])
        if data is None:
            return subprocess.CompletedProcess(args, 1, stdout=b'')
        return subprocess.CompletedProcess(args, 0, stdout=data)

    def test_one_generation_through_smcat(self):
        bundle = endpoints.read_brm_bundle()
        self.assertEqual(bundle['generation'], 'BRM_savesB')
        self.assertEqual(self.decode(bundle), self.files)

    def test_smcat_failing_on_current_is_a_503(self):
        del self.smcat_files[endpoints.S3_BRM_CURRENT_PATH]
        with self.assertRaises(APIError) as ctx:
            endpoints.read_brm_bundle()
        self.assertEqual(ctx.exception.code, 503)
        # smcat is retried 10 times within each attempt
        self.assertEqual(self.smcat.call_count, 10 * endpoints.BRM_BUNDLE_ATTEMPTS)

    def test_smcat_failing_on_a_file_is_a_503(self):
        del self.smcat_files[f'{endpoints.EM_PATH_SUFFIX}/BRM_savesB_vss']
        with self.assertRaises(APIError) as ctx:
            endpoints.read_brm_bundle()
        self.assertEqual(ctx.exception.code, 503)


if __name__ == '__main__':
    unittest.main()
