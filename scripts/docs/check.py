#!/usr/bin/env python3
"""Check repository Markdown links and authored-page structure without network I/O."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
import re
import subprocess
import tarfile
import unicodedata
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt


class Anchors(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = set()

    def handle_starttag(self, tag, attrs):
        for key, value in attrs:
            if value and (key == "id" or (tag == "a" and key == "name")):
                self.ids.add(value)


def slug(text):
    text = text.lower()
    return "".join(c for c in text if c in " -_" or unicodedata.category(c)[0] in "LMN").replace(" ", "-")


def inline_text(tokens):
    return "".join(inline_text(t.children) if t.children else t.content
                   for t in tokens if t.type not in ("html_inline", "softbreak", "hardbreak"))


@dataclass
class Document:
    anchors: set[str]
    links: list[tuple[int, str]]
    errors: list[str]


def parse_document(text, authored=True):
    tokens = MarkdownIt("commonmark").enable("table").parse(text)
    html = Anchors()
    anchors, used, links, errors = set(), set(), [], []
    h1 = 0
    lines = text.splitlines()
    for i, token in enumerate(tokens):
        if token.type == "heading_open":
            h1 += token.tag == "h1"
            base = slug(inline_text(tokens[i + 1].children or []))
            name, n = base, 0
            while name in used:
                n += 1
                name = f"{base}-{n}"
            used.add(name)
            anchors.add(name)
        if token.type in ("html_inline", "html_block"):
            html.feed(token.content)
        if token.type == "fence" and authored:
            last = lines[token.map[1] - 1].strip() if token.map[1] else ""
            if not re.fullmatch(re.escape(token.markup[0]) + "{" + str(len(token.markup)) + ",}", last):
                errors.append(f"{token.map[0] + 1}: unclosed code fence")
        if token.type == "inline":
            line = token.map[0] + 1 if token.map else 1
            pending = list(token.children or [])
            while pending:
                child = pending.pop(0)
                if child.type == "link_open":
                    links.append((line, child.attrGet("href")))
                if child.type == "image":
                    links.append((line, child.attrGet("src")))
                if child.type == "html_inline":
                    html.feed(child.content)
                pending.extend(child.children or [])
    if authored and h1 != 1:
        errors.append(f"1: expected one page title (H1), found {h1}")
    return Document(anchors | html.ids, links, errors)


def evidence_page(relative):
    return relative.parts[:2] in (("docs", "benchmarks"), ("docs", "qualification"))


class Checker:
    def __init__(self, root):
        self.root = root.resolve()
        self.documents = {}
        self.archives = {}
        self.archive_links = 0
        self.local_links = 0

    def document(self, path):
        if path not in self.documents:
            self.documents[path] = parse_document(path.read_text(), not evidence_page(path.relative_to(self.root)))
        return self.documents[path]

    def archive_contains(self, source, target):
        # Frozen reports sometimes link to members of a sibling archive and
        # explicitly instruct readers to extract it. Validate without extraction.
        if not evidence_page(source.relative_to(self.root)):
            return False
        parent = source.parent
        while parent != self.root and evidence_page(parent.relative_to(self.root)):
            if target.is_relative_to(parent):
                name = target.relative_to(parent).as_posix()
                for archive in sorted(parent.glob("*.tar.gz")):
                    if archive not in self.archives:
                        with tarfile.open(archive, "r:gz") as tf:
                            self.archives[archive] = {m.name.removeprefix("./") for m in tf if m.isfile()}
                    if name in self.archives[archive]:
                        return True
            parent = parent.parent
        return False

    def check_link(self, source, destination):
        source = source.resolve()
        url = urlsplit(destination)
        if url.scheme or url.netloc:
            return None
        self.local_links += 1
        target = (source.parent / unquote(url.path)).resolve() if url.path else source
        if not target.is_relative_to(self.root):
            return "local target escapes repository"
        if not target.exists():
            if self.archive_contains(source, target):
                self.archive_links += 1
                return None
            return "missing local target"
        fragment = unquote(url.fragment)
        if fragment and target.suffix.lower() in (".md", ".markdown"):
            if fragment not in self.document(target).anchors:
                return "missing heading or HTML anchor"
        elif fragment and target.is_file() and re.fullmatch(r"L\d+(?:-L\d+)?", fragment):
            bounds = [int(value.removeprefix("L")) for value in fragment.split("-")]
            if not 1 <= bounds[0] <= bounds[-1] <= len(target.read_text().splitlines()):
                return "source line anchor out of range"
        return None

    def check(self, files):
        errors = []
        for path in files:
            relative = path.relative_to(self.root)
            doc = self.document(path)
            errors.extend(f"{relative}:{error}" for error in doc.errors)
            for line, destination in doc.links:
                problem = self.check_link(path, destination)
                if problem:
                    errors.append(f"{relative}:{line}: {problem}: {destination}")
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    root = args.root.resolve()
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    ).decode().split("\0")
    files = sorted({root / name for name in names if Path(name).suffix.lower() in (".md", ".markdown")
                    and (root / name).is_file()})
    checker = Checker(root)
    errors = checker.check(files)
    if errors:
        print("\n".join(errors))
        return 1
    print(f"Checked {len(files)} Markdown files and {checker.local_links} local links "
          f"({checker.archive_links} verified archive members); no errors.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
