import io
from pathlib import Path
import tarfile
import tempfile
import unittest

from check import Checker, parse_document


class MarkdownChecks(unittest.TestCase):
    def test_fences_and_reference_links(self):
        doc = parse_document('# Page\n\n[Guide][g]\n\n[g]: guide.md#next\n\n```sh\n[ignored](missing.md)\n```\n')
        self.assertEqual(doc.errors, [])
        self.assertEqual(doc.links, [(3, 'guide.md#next')])

    def test_github_headings_and_explicit_anchors(self):
        doc = parse_document('# Page\n\n## Use `Open()`\n\n## Use `Open()`\n\n<a id="custom"></a>\n')
        self.assertTrue({'page', 'use-open', 'use-open-1', 'custom'} <= doc.anchors)

    def test_authored_page_structure(self):
        self.assertTrue(parse_document('## No title\n').errors)
        self.assertTrue(parse_document('# Page\n\n```go\nunclosed\n').errors)
        self.assertEqual(parse_document('Frozen report\n', authored=False).errors, [])

    def test_target_and_anchor_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            page = root / 'README.md'
            page.write_text('# Page\n')
            target = root / 'a b.md'
            target.write_text('# Target\n\n## Next\n')
            checker = Checker(root)
            self.assertIsNone(checker.check_link(page, 'a%20b.md#next'))
            self.assertEqual(checker.check_link(page, 'a%20b.md#absent'), 'missing heading or HTML anchor')
            self.assertEqual(checker.check_link(page, 'missing.md'), 'missing local target')
            self.assertEqual(checker.check_link(page, '../outside.md'), 'local target escapes repository')
            self.assertIsNone(checker.check_link(page, 'https://example.com/missing'))
            code = root / 'main.go'
            code.write_text('package main\n\nfunc main() {}\n')
            self.assertIsNone(checker.check_link(page, 'main.go#L1-L3'))
            for anchor in ('L0', 'L4', 'L3-L2', 'L0-L1'):
                self.assertEqual(checker.check_link(page, 'main.go#' + anchor), 'source line anchor out of range')

    def test_archive_members_are_checked_without_extraction(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent = root / 'docs/benchmarks/run'
            parent.mkdir(parents=True)
            page = parent / 'README.md'
            page.write_text('# Report\n')
            with tarfile.open(parent / 'raw.tar.gz', 'w:gz') as tf:
                info = tarfile.TarInfo('trial/report.json')
                info.size = 2
                tf.addfile(info, io.BytesIO(b'{}'))
            checker = Checker(root)
            self.assertIsNone(checker.check_link(page, 'trial/report.json'))
            self.assertEqual(checker.check_link(page, 'trial/missing.json'), 'missing local target')
            self.assertEqual(checker.archive_links, 1)
            self.assertFalse((parent / 'trial').exists())


if __name__ == '__main__':
    unittest.main()
