import argparse
import csv
import json
from dataclasses import asdict, dataclass
from itertools import zip_longest
from pathlib import Path
from typing import Final

HEADERS: Final[list[str]] = [
    "article_id",
    "issue_number",
    "item_url",
    "journal_id",
    "journal_label",
    "language",
    "parent_id",
    "parent_label",
    "publication_year",
    "title",
    "type",
    "vendor",
    "volume_number",
    "abstract_str",
    "pdf_url",
    "pages",
    "authors",
    "doi",
    "keywords",
    "license",
    "page_contents",
    "page_ids",
    "page_labels",
    "page_text_urls",
    "page_urls",
    "parts",
]


def parse_list_of_items(value: str) -> list[str]:
    value = value.replace('"', '\\"').replace("'", '"')
    try:
        return json.loads(value) if value else []
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Failed to parse list of items from value: {value}. Error: {e}"
        ) from e
        return []


def parse_list_of_dicts(value: str) -> list[dict[str, str]]:
    value = (
        value.replace("'{", "{")
        .replace("}'", "}")
        .replace("\\'", "'")
        .replace('\\"', '"')
    )
    try:
        return json.loads(value) if value else []
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Failed to parse list of dictionaries from value: {value}. Error: {e}"
        ) from e
        return []


@dataclass
class Page:
    page_id: str
    page_content: str
    page_label: str
    page_url: str
    page_text_url: str


class PagesList(list[Page]):
    @classmethod
    def from_row(cls, row: dict[str, str]) -> list[Page]:
        page_id, page_content, page_label, page_url, page_text_url = map(
            parse_list_of_items,
            (
                row.pop(key)
                for key in (
                    "page_ids",
                    "page_contents",
                    "page_labels",
                    "page_urls",
                    "page_text_urls",
                )
            ),
        )
        return [
            Page(*args)
            for args in zip_longest(
                page_id, page_content, page_label, page_url, page_text_url, fillvalue=""
            )
        ]


@dataclass
class Metadata:
    article_id: str
    issue_number: str
    item_url: str
    journal_id: str
    journal_label: str
    language: str
    parent_id: str
    parent_label: str
    publication_year: str
    title: str
    type: str
    vendor: str
    volume_number: str
    abstract_str: str
    pdf_url: str
    pages: str
    authors: list[dict[str, str]]
    doi: str
    keywords: list[str]
    license: str
    parts: list[str]

    pages_list: list[Page]

    def __post_init__(self):
        if isinstance(self.authors, str):
            self.authors = parse_list_of_dicts(self.authors) if self.authors else []
        if isinstance(self.keywords, str):
            self.keywords = parse_list_of_items(self.keywords) if self.keywords else []
        if isinstance(self.parts, str):
            self.parts = parse_list_of_items(self.parts) if self.parts else []


def row_list_to_dict(row: list[str]) -> dict[str, str]:
    return {header: value for header, value in zip(HEADERS, row)}


def process_row(row: list[str]) -> Metadata:
    row_obj = row_list_to_dict(row)
    pages = PagesList.from_row(row_obj)
    return Metadata(**row_obj, pages_list=pages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV metadata to JSON format.")
    parser.add_argument("input", type=Path, help="Path to the input CSV file.")
    parser.add_argument("output", type=Path, help="Path to the output JSON file.")
    args = parser.parse_args()

    metadata = []

    csv.field_size_limit(1310720)  # default: 131072
    with args.input.open() as csvfile:
        reader = csv.reader(csvfile, quotechar='"', delimiter=",")
        _header = next(reader)
        for row in reader:
            metadata.append(asdict(process_row(row[1:])))

    with args.output.open("w") as jsonfile:
        json.dump(metadata, jsonfile, indent=2, ensure_ascii=False)
