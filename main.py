import os
import sys
from pprint import pprint
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime, timedelta
from constants import mfd_links
import time
import pandas as pd
from tqdm import tqdm

def check_directory():
    if not os.path.exists('parsed_data'):
        os.mkdir('parsed_data')


def open_previously_parsed(filename):
    return pd.read_csv(f'parsed_data/{filename}', sep='|', dayfirst=True).drop_duplicates('time') if os.path.exists(f'parsed_data/{filename}') else pd.DataFrame()


class MFDParser:
    sys.setrecursionlimit(999999999)
    constants = None
    start_date = None
    end_date = None

    def __init__(self):
        self.choose_companies_to_parse(mfd_links)
        self.input_choose_date_range()
        self.main()

    # Функция отбирает перед парсингом компании, которые будут спаршены
    def choose_companies_to_parse(self, mfd_links):
        for n, i in enumerate(mfd_links.items()):
            print(f'{n} -- {i[0]}')
        print('\nЧерез запятую без пробелов перечислите компании, которые необходимо спарсить.')
        chosen_list = input('Пример ввода: 0,1,2,3\n')
        chosen_list = self.input_tests(chosen_list)
        chosen_comps = {company[1][0]: company[1][1] for company in enumerate(mfd_links.items()) if company[0] in chosen_list}
        print(f'Были выбраны компании {" ".join([i for i in chosen_comps])}')
        self.constants = chosen_comps

    def input_tests(self, chosen_list):
        try:
            chosen_list = chosen_list.split(',')
            chosen_list = [int(i) for i in chosen_list if int(i) < 65]
        except:
            _ = input('Компании были выбраны неверно. Попробуйте снова.\n')
            sys.exit()
        return chosen_list

    # Функция для задания промежутка парсинга
    def input_choose_date_range(self):
        start_date = str(input('\nВведите начальную дату для парсинга в формате DD.MM.YYYY:\n'))
        end_date = str(input('\nВведите конечную дату для парсинга в формате DD.MM.YYYY:\n'))
        try:
            self.start_date = datetime.strptime(start_date, '%d.%m.%Y')
            self.end_date = datetime.strptime(end_date, '%d.%m.%Y')
        except:
            print('Неправильный формат ввода данных.')
            _ = input('\n')

    # Функция для перехода по ссылке на конкретную дату
    def _get_soup_at_date(self, url, date=None, page=None):
        is_passed = False
        while not is_passed:
            try:
                parameters = {'period': 'SelectedDate', 'selectedDate': date, 'page': page}
                parameters_link = "".join([f'&{dict_tuple[0]}={dict_tuple[1]}' for dict_tuple in parameters.items() if dict_tuple[1]])
                html_doc = urlopen(url + parameters_link).read()
                soup = BeautifulSoup(html_doc, features='html.parser')
                is_passed = True
            except:
                print('Соединение было оборвано. Колдаун 100 секунд.')
                time.sleep(100)
        return soup
    # Функция для определения последней доступной страницы для парсинга конкретной компании
    def _get_last(self, soup):
        return int(soup.find_all('div', 'mfd-paginator')[0].contents[-2].text) - 2

    ### Процесс парсинга
    # Циклы
    def main(self):
        current_date, end_date = self.start_date, self.end_date

        for company_name, company_link in self.constants.items():
            current_dataframe = open_previously_parsed(f'parsed_data/{company_name}')
            pbar = tqdm(total=(end_date - current_date).days)
            while (end_date - current_date).days != 0:
                soup = self._get_soup_at_date(url=company_link, date=datetime.strftime(current_date, '%d.%m.%Y'))
                extracted_list = self.iterate_over_all_pages_at_specific_date(url=company_link, date=datetime.strftime(current_date, '%d.%m.%Y'), soup=soup)
                current_dataframe = current_dataframe.append(pd.DataFrame(extracted_list).drop_duplicates('time'))
                current_dataframe.to_csv(f'parsed_data/{company_name}_parsed.csv', sep='|')
                current_date += timedelta(days=1)
                pbar.update(1)


    # Функция перелистывает все странички, для определенной даты
    def iterate_over_all_pages_at_specific_date(self, url, date, soup):
        pages_available = self._get_last(soup)
        extracted_list = self._extract_soup(soup)
        if pages_available != -1:
            for page in range(pages_available + 1):
                soup = self._get_soup_at_date(url=url, date=date, page=page)
                extracted_list.extend(self._extract_soup(soup))
                time.sleep(2)
        time.sleep(2)
        return extracted_list

    # Функция полностью ищет все, что нужно в супе
    def _extract_soup(self, soup):
        return [self.__get_post_info(post) for post in soup.find_all('div', 'mfd-post')] if soup.find_all('div', 'mfd-post') else None
    # Функция для извлечения данных из поста
    def __get_post_info(self, post):
        likes = self.___likes_finder(post)
        user_rate = self.___rating_finder(post)
        times = self.___time_finder(post)
        text = self.___text_finder(post)

        return {
            'user_rate': user_rate,
            'time': times,
            'likes': likes,
            'text': text
        }

    # Функция ищет текст поста
    def ___text_finder(self, post):
        # Если в посте есть цитаты
        if post.find_all('div', 'mfd-quote-text'):
            # То само сообщение находится под таким же тегом, но уже последним в списке
            text = post.find_all('div', 'mfd-quote-text')[-1].text
        else:
            # Бывает также, что сообщения удалено
            text = 'DELETED'
        return text
    # Функция ищет количество лайков на посте
    def ___likes_finder(self, post):
        likes = post.find('span', 'u')
        return 0 if likes is None else likes.text
    # Ищем рейтинг пользователя
    def ___rating_finder(self, post):
        user_rate = post.find('div', 'mfd-poster-info-rating mfd-icon-profile-star')
        return 0 if user_rate is None else user_rate.text
    # Ищем время сообщения
    def ___time_finder(self, post):
        return post.find('div', 'mfd-post-top-1').text


if __name__ == '__main__':
    check_directory()
    parser = MFDParser()
