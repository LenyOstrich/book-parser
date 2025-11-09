"""scraper.py.

Модуль для сбора информации о книгах с сайта "Books to Scrape".

Содержит функции для:
- Получения HTML страницы книги;
- Извлечения данных о книге в виде словаря;
- Сбора всех ссылок на книги из каталога;
- Обработки книг пакетами с возможностью параллельной загрузки;
- Сохранения данных в текстовый файл;
- Настройки регулярного запуска сбора данных.

Пример использования:
    from scraper import scrape_books

    books_data = scrape_books(save=True)
"""

import pathlib
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
import schedule
from bs4 import BeautifulSoup, Tag
from requests import Session

NOT_FOUND_STATUS_CODE = 404


def get_book_data(
    book_url: str, session: Session, timeout: int = 10
) -> dict[str, str]:
    """Обрабатывает и записывает данные конкретной книги в словарь.

    Parameters:
    ----------
    book_url : str
        Адрес книги на сайте.
    session : Session
        Сессия, используемая для запроса.
    timeout : int
        Таймаут в секундах, по истечении которого запрос прерывается.

    Returns:
    -------
    dict[str, str]
        Словарь с основной информацией о книге.
    """
    soup = _get_book_response(book_url, session, timeout)
    if not soup:
        return {}
    main_info = soup.find(class_="product_main")
    if not main_info:
        return {}
    book_dict = {
        "Book name": _get_attr_text(main_info, "h1"),
        "Book price": _get_attr_text(main_info, "p", class_="price_color"),
        "In stock amount": _get_stock_amount(
            _get_attr_text(main_info, "p", class_=["instock", "availability"])
        ),
        "Rate": _get_rate(main_info, "p", class_="star-rating"),
        "Book description": _collect_description(soup),
    }
    book_dict.update(_collect_additional_info(soup))
    return book_dict


def scrape_books(*, save: bool = False) -> list[dict[str, str]]:
    """Собирает данные обо всех книгах с сайта Books to Scrape.

    Parameters:
    ----------
    save : bool
        Флаг для сохранения данных в файл.

    Returns:
    -------
    list[dict[str, str]]
        Список словарей для всех книг с сайта.
    """
    with requests.Session() as session:
        all_books_links = _collect_all_books_links(session)
        all_books_data = _process_books_in_batches(
            list(all_books_links), session
        )
        if save:
            _save_books_to_file(all_books_data)
        return all_books_data


def every_day_parser() -> None:
    """Настройка ежедневного запуска парсинга в 19:00."""
    schedule.every().day.at("19:00").do(_job)
    print("Автоматический парсинг настроен. Ожидание запуска.")
    while True:
        schedule.run_pending()
        time.sleep(45)


def _get_book_response(
    book_url: str, session: Session, timeout: int = 10
) -> BeautifulSoup | None:
    """Обрабатывает запрос на получение html страницы конкретной книги.

    Parameters:
    ----------
    book_url : str
        Адрес книги на сайте.
    session : Session
        Сессия, используемая для запроса.
    timeout : int
        Таймаут в секундах, по истечении которого запрос прерывается.

    Returns:
    -------
    BeautifulSoup
        Представление страницы в виде объекта BeautifulSoup.
    None
        Если не удалось получить данные
    """
    try:
        response = session.get(book_url, timeout=timeout)
        response.raise_for_status()
        response.encoding = "utf-8"
        return BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.HTTPError as e:
        print(f"Произошла ошибка HTTP: {e}")
    except requests.exceptions.RequestException as e:
        print(
            f"Произошла ошибка получения данных. Проверьте подключение и правильность url: {e}"
        )


def _get_attr_text(html_elem: Tag, tag: str, **kwargs) -> str:  # noqa: ANN003
    """Ищет данные для словаря под указанным тегом и преобразует в текстовый формат.

    Parameters:
    ----------
    html_elem : Tag
        Тег, под которым необходимо искать информацию для словаря.
    tag : str
        Тег, который содержит информацию для словаря.
    **kwargs : dict[str, Unknown]
        Дополнительные параметры для поиска.

    Returns:
    -------
    str
        Строка с информацией для словаря с данными книги.
    """
    return (
        elem.get_text(strip=True)
        if (elem := html_elem.find(tag, **kwargs))
        else ""
    )


def _get_stock_amount(stock_info: str) -> str:
    """Ищет в строке с информацией о количестве книг в наличии число книг в наличии.

    Parameters:
    ----------
    stock_info : str
        Строка, содержащая число книг в наличии.

    Returns:
    -------
    str
        Число книг в наличии преобразованное в строку.
    """
    in_stock = re.search(r"\d+", stock_info)
    return in_stock.group() if in_stock else ""


def _get_rate(html_elem: Tag, tag: str, **kwargs) -> str:  # noqa: ANN003
    """Ищет под указанным тегом класс, который соответствует рейтингу книги.

    Parameters:
    ----------
    html_elem : Tag
        Тег, под которым необходимо искать информацию для словаря.
    tag : str
        Тег, который содержит информацию для словаря.
    **kwargs : dict[str, Unknown]
        Дополнительные параметры для поиска.

    Returns:
    -------
    str
        Рейтинг книги, преобразованный в строку.
    """
    rating_map = {
        "Zero": "0",
        "One": "1",
        "Two": "2",
        "Three": "3",
        "Four": "4",
        "Five": "5",
    }
    rate_tag = html_elem.find(tag, **kwargs)
    if not rate_tag:
        return ""
    for cls in rate_tag["class"]:
        if cls in rating_map:
            return rating_map[cls]
    return ""


def _collect_description(soup: BeautifulSoup) -> str:
    """Ищет описание книги на странице.

    Parameters:
    ----------
    soup : BeautifulSoup
        Представление страницы в виде объекта BeautifulSoup.

    Returns:
    -------
    str
        Описание книги.
    """
    header = soup.find("div", id="product_description", class_="sub-header")
    if not header:
        return ""
    return (
        description.get_text(strip=True)
        if (description := header.find_next_sibling())
        else ""
    )


def _collect_additional_info(soup: BeautifulSoup) -> dict[str, str]:
    """Собирает данные из таблицы с дополнительной информацией.

    Parameters:
    ----------
    soup : BeautifulSoup
        Представление страницы в виде объекта BeautifulSoup.

    Returns:
    -------
    dict[str, str]
        Словарь виде "Имя колонки" : "Содержание колонки".
    """
    additional_info_table = soup.find(
        "table", class_=["table", "table-striped"]
    )
    if not additional_info_table:
        return {}
    additional_info_dict = {}
    for table_row in additional_info_table.find_all("tr"):
        key = header.get_text() if (header := table_row.find("th")) else ""
        if key:
            additional_info_dict[key] = (
                row.get_text() if (row := table_row.find("td")) else ""
            )
    return additional_info_dict


def _collect_all_books_links(
    session: Session,
    start_page_num: int = 1,
) -> set[str]:
    """Собирает все ссылки на книги в каталоге.

    Parameters:
    ----------
    session : Session
        Сессия для прохода по всем страницам каталога.
    start_page_num : int
        Номер стартовой страницы для обхода каталога.

    Returns:
    -------
    set[str]
        Список уникальных ссылок на книги.
    """
    links = set()
    while True:
        current_page = _process_page(session, start_page_num, len(links))
        if not current_page:
            break
        print(f"Обрабатываем страницу номер: {start_page_num}")
        print(f"Текущее количество ссылок: {len(links)}")
        links.update(_collect_links_from_page(current_page))
        start_page_num += 1
    return links


def _process_page(
    session: Session, page_num: int, links_amount: int
) -> BeautifulSoup | None:
    """Получает по url страницу каталога и преобразует её в BeautifulSoup.

    Parameters:
    ----------
    session : Session
        Сессия для получения страницы каталога.
    start_page_num : int
        Номер стартовой страницы для обхода каталога.

    Returns:
    -------
    BeautifulSoup
        Представление страницы в виде объекта BeautifulSoup.
    None
        Если не удалось получить данные.
    """
    try:
        page_url = f"https://books.toscrape.com/catalogue/page-{page_num}.html"
        response = session.get(page_url)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.HTTPError as e:
        if response.status_code == NOT_FOUND_STATUS_CODE:
            print(
                "Обработка завершена, дальнейших страниц в каталоге не существует. "
                f"Итоговое количество ссылок: {links_amount}"
            )
        else:
            print(f"Произошла ошибка HTTP: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Произошла ошибка подключения к сети: {e}")


def _collect_links_from_page(soup: BeautifulSoup) -> set[str]:
    """Ищет и формирует ссылки на книги.

    Parameters:
    ----------
    soup : BeautifulSoup
        Представление страницы в виде объекта BeautifulSoup.

    Returns:
    -------
    set[str]
        Список уникальных ссылок на книги.
    """
    base_url = "https://books.toscrape.com/catalogue/"
    links = set()
    if not soup:
        return set()
    for h3 in soup.find_all("h3"):
        links.update(
            base_url + str(a["href"]) for a in h3.find_all("a", href=True)
        )
    return links


def _process_books_in_batches(
    links: list[str],
    session: Session,
    batch_size: int = 50,
    delay: float = 2.0,
    max_workers: int = 10,
) -> list[dict[str, str]]:
    """Разбивает список ссылок на пакеты. Параллельно обрабатывает запросы.

    Собирает данные о книгах.

    Parameters:
    ----------
    links : list[str]
        Список всех ссылок на книги.
    session : Session
        Сессия для отправки запросов.
    batch_size : int
        Размер пакета - количество ссылок внутри пакета
    delay : float
        Задержка между обработкой пакетов.
    max_workers : int
        Количество потоков.

    Returns:
    -------
    list[dict[str, str]]
        Список словарей для всех книг с сайта.
    """
    all_books_data = []
    for i in range(0, len(links), batch_size):
        batch = links[i: i + batch_size]
        print(f"Обрабатываем пакет {i // batch_size + 1}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_link = {
                executor.submit(get_book_data, link, session): link
                for link in batch
            }
            for future in as_completed(future_to_link):
                link = future_to_link[future]
                book_data = future.result()
                all_books_data.append(book_data)
                print(f"Обработана книга: {link}")
        time.sleep(delay)
    return all_books_data


def _save_books_to_file(
    books: list[dict[str, str]], filename: str = "books_data.txt"
) -> None:
    """Сохраняет данные обо всех книгах в файл.

    Parameters:
    ----------
    books : list[dict[str, str]]
        Список словарей для всех книг с сайта.
    filename : str
        Имя файла.
    """
    with pathlib.Path(filename).open("w", encoding="utf-8") as f:
        for i, book in enumerate(books, start=1):
            f.write(f"Книга №{i}\n")
            f.writelines(f"{key}: {value}\n" for key, value in book.items())
            f.write("\n")
    print(f"Данные сохранены в файл {filename}")


def _job() -> None:
    """Внутренний вызов парсинга книг и логирование процесса."""
    print(f"[{datetime.now()}] Запуск парсинга книг")
    books_data = scrape_books(save=True)
    print(
        f"[{datetime.now()}] Парсинг завершен. "
        f"Сохранено {len(books_data)} книг."
    )
