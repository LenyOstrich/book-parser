"""Модуль автотестов для проекта books scraper.

Содержит тесты для проверки ключевых функций:
- get_book_data
- scrape_books

Содержит тесты для проверки внутренних функций:
- _collect_all_books_links
- _get_book_response
"""

import requests

from scraper import (
    _collect_all_books_links,
    _get_book_response,
    get_book_data,
    scrape_books,
)

DICTIONARY_FIELDS_AMOUNT = 12
TEST_SESSION = requests.Session()
TOTAL_BOOKS_AMOUNT = 1000


def test_get_book_data() -> None:
    """Проверка, что get_book_data возвращает словарь с нужными ключами."""
    url = "https://books.toscrape.com/catalogue/shakespeares-sonnets_989/index.html"
    data = get_book_data(url, TEST_SESSION)
    assert isinstance(data, dict)
    assert len(data) == DICTIONARY_FIELDS_AMOUNT
    assert data["Book name"] == "Shakespeare's Sonnets"


def test_scrape_books() -> None:
    """Проверка, что scrape_books возвращает список словарей и собирает все книги."""
    data = scrape_books()
    assert isinstance(data, list)
    assert len(data) == TOTAL_BOOKS_AMOUNT


def test__collect_all_books_links() -> None:
    """Проверка, что были собраны все ссылки на книги."""
    assert len(_collect_all_books_links(TEST_SESSION)) == TOTAL_BOOKS_AMOUNT


def test__get_book_response_invalid_url(capsys) -> None:
    """Проверка корректности обработки ошибок при неверном url."""
    invalid_url = "https://books.toscrape555.com/catalogue/shakespeares-sonnets_989/index.html"
    result = _get_book_response(invalid_url, TEST_SESSION)
    assert result is None
    captured = capsys.readouterr()
    assert (
        "Произошла ошибка получения данных. Проверьте подключение и правильность url:"
        in captured.out
    )


def test__get_book_response_http_err(capsys) -> None:
    """Проверка корректности обработки ошибок при ошибке HTTP запроса."""
    http_err_url = "http://httpbin.org/status/400"
    http_res = _get_book_response(http_err_url, TEST_SESSION)
    assert http_res is None
    captured = capsys.readouterr()
    assert "Произошла ошибка HTTP" in captured.out
