import os
import re
from dotenv import load_dotenv
import logging
import logging.handlers as handlers
import time
import requests
from datetime import datetime, date
from dataclasses import dataclass
import sys
from http import HTTPStatus
import csv
import json


DEFAULT_360_API_URL = "https://api360.yandex.net"
ITEMS_PER_PAGE = 100
MAX_RETRIES = 3
LOG_FILE = "add_users.log"
RETRIES_DELAY_SEC = 2
SLEEP_TIME_BETWEEN_API_CALLS = 0.5
ALL_USERS_REFRESH_IN_MINUTES = 15
SENSITIVE_FIELDS = ['password', 'oauth_token', 'access_token', 'token']
# DEFAULT_PASSWORD_PATTERN is used to validate the password
DEFAULT_PASSWORD_PATTERN = r'^(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]).{10,}$'

# DEFAULT_EMAIL_PATTERN is used to validate the personal email
DEFAULT_EMAIL_PATTERN = r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$'

USERS_CSV_REQUIRED_HEADERS = ["login", "password", "password_change_required", "first_name", "last_name", "middle_name", "position", "gender", "birthday", "language", "work_phone", "mobile_phone", "personal_email", "department"]

# MAX value is 1000
USERS_PER_PAGE_FROM_API = 1000
DEPARTMENTS_PER_PAGE_FROM_API = 100

DEPS_SEPARATOR = '|'

EXIT_CODE = 1

logger = logging.getLogger("add_users.log")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
#file_handler = handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30, encoding='utf-8')
file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024 * 10,  backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def read_file_data(settings: "SettingParams"):
    data = []
    with open(settings.users_file, 'r', encoding='utf-8') as csvfile:
        for line in csvfile:
            data.append(line.strip().split(';'))
    return data

def add_users_from_file_phase_1(settings: "SettingParams", analyze_only=False):
    logger.info("-" * 100)
    logger.info(f'Чтение пользователей из файла {settings.users_file} и проверка корректности данных.')
    logger.info("-" * 100)
    users_file_name = settings.users_file
    if not os.path.exists(users_file_name):
        full_path = os.path.join(os.path.dirname(__file__), users_file_name)
        if not os.path.exists(full_path):
            logger.error(f'Ошибка! Файл {users_file_name} не существует!')
            return
        else:
            users_file_name = full_path
    
    ## Another way to read file with needed transfromations
    headers = []
    data = []
    try:
        logger.info("-" *100)
        logger.info(f'Чтение файла {users_file_name}')
        logger.info("-" *100)
        bad_header = False
        with open(users_file_name, 'r', encoding='utf-8') as csvfile:
            headers = csvfile.readline().replace('"', '').split(";")
            for header in headers:
                if header.strip() not in USERS_CSV_REQUIRED_HEADERS:
                    logger.error(f'Ошибка! Заголовок {header} не соответствует требуемым: {";".join(USERS_CSV_REQUIRED_HEADERS)}')
                    bad_header = True
            if bad_header:
                return
            logger.debug(f'Headers: {headers}')
            for line in csvfile:
                logger.debug(f'Чтение строки из файла - {mask_csv_line_safe(line)}')
                fields = line.replace('"','').split(";")
                if len(fields) != len(USERS_CSV_REQUIRED_HEADERS):
                    logger.error(f'Ошибка! Строка {mask_csv_line_safe(line)} - количество полей не соответствует требуемым заголовкам: {USERS_CSV_REQUIRED_HEADERS}. Возможно, в значении какого-либо поля есть точка с запятой. Попробуйте заменить её на другой символ.')
                    return
                entry = {}
                for i,value in enumerate(fields):
                    entry[headers[i].strip()] = value.strip()
                data.append(entry)
        logger.info(f'Конец чтения файла {users_file_name}')
        logger.info("\n")
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    correct_lines = []
    error_lines = []
    suspiciose_lines = []
    error_lines = []
    stop_adding = False
    line_number = 0

    logger.info("-" *100)
    logger.info('Проверка корректности данных.')
    logger.info("-" *100)
    api_deps_hierarchy = generate_deps_hierarchy_from_api(settings)
    for element in data:
        entry = {}
        correct = True
        stop_adding = False
        line_number += 1
        logger.debug(f'Обработка строки #{line_number} {mask_sensitive_data(element)}')
        try:
            temp_login = element["login"].lower()
            if temp_login:
                if '@' in temp_login:
                    temp_login = element["login"].split('@')[0]
                no_conflicts, conflicts = validate_login(settings, temp_login)
                if not no_conflicts:
                    if not conflicts:
                        correct = False
                        logger.error(f'Строка #{line_number}. Возможный некорректный логин _"{temp_login}"_')
                    else:
                        for conflict in conflicts:
                            logger.error(f'Строка #{line_number}. Конфликт логина _"{temp_login}"_ с существующем пользователем {conflict["nickname"]} ({conflict["name"]["last"]} {conflict["name"]["first"]}). Добавление пользователя отменено.')
                        stop_adding = True
                else:
                    entry["login"] = temp_login
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Логин пуст. Отмена добавления пользователя.')

            temp_firest_name = element["first_name"]
            if temp_firest_name:
                if not validate_name(temp_firest_name):
                    correct = False
                    logger.warning(f'Строка #{line_number}. Возможный некорректное Имя пользвоателя _"{temp_firest_name}"_')
                entry["first"] = temp_firest_name
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Имя пользвоателя пусто. Отмена добавления пользователя.')

            temp_last_name = element["last_name"]
            if temp_last_name:
                if not validate_name(temp_last_name):
                    correct = False
                    logger.warning(f'Строка #{line_number}. Возможная некорректная фамилия пользвоателя _"{temp_last_name}"_')
                entry["last"] = temp_last_name
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Фамилия пользвоателя пуста. Отмена добавления пользователя.')

            temp_middle_name = element["middle_name"]
            if temp_middle_name:
                if not validate_name(temp_middle_name):
                    correct = False
                    logger.warning(f'Строка #{line_number}. Возможная некорректное отчество пользвоателя _"{temp_middle_name}"_')
            entry["middle"] = temp_middle_name

            temp_password = element["password"]
            if temp_password:
                # Проверяем пароль с помощью регулярного выражения
                password_valid, password_message = validate_password(settings, temp_password)
                if not password_valid:
                    #stop_adding = True
                    logger.error(f'Строка #{line_number}. Возможно слабый пароль, который не может быть установлен: {password_message}')
                else:
                    entry["password"] = temp_password
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Пароль пуст. Отмена добавления пользователя.')

            password_change_required = element["password_change_required"].lower()
            if password_change_required not in ['true', 'false']:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Неккорректный параметр password_change_required _"{password_change_required}"_. Должно быть true или false. Отмена добавления пользователя.')
            else:
                entry["password_change_required"] = password_change_required

            temp_language = element["language"].lower()
            if temp_language not in ['ru', 'en']:
                #stop_adding = True
                logger.error(f'Строка #{line_number}. Некорректный язык _"{temp_language}"_. Должно быть ru или en. Будет записано пустое значение.')
            else:
                entry["language"] = temp_language

            temp_gender = element["gender"].lower()
            if temp_gender not in ['male', 'female']:
                #stop_adding = True
                logger.error(f'Строка #{line_number}. Некорректный пол _"{temp_gender}"_. Должно быть male или female. Будет записано пустое значение.')
            else:
                entry["gender"] = temp_gender   

            temp_birthday = element["birthday"]
            if temp_birthday:
                check_date, date_value = is_valid_date(temp_birthday)
                if not check_date:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректная дата рождения _"{temp_birthday}"_ ({date_value}). Отмена добавления пользователя.')
                else:
                    entry["birthday"] = date_value.strftime('%Y-%m-%d')

            entry["position"] = element["position"]

            found_dep = False
            if len(element["department"]) == 0:
                entry["department"] = "1"
            else:
                entry["department"] = element["department"]
                if entry["department"].isdigit():
                    if int(entry["department"]) > 1:
                        for dep in api_deps_hierarchy:
                            if dep['id'] == int(entry["department"]):
                                found_dep = True
                                break
                        if not found_dep:
                            stop_adding = True
                            logger.error(f'Строка #{line_number}. Подразделение с номером {entry["department"]} не найдено в организации. Отмена добавления пользователя.')

            temp_work_phone = element["work_phone"]
            if temp_work_phone:
                check_phone, phone_value = validate_phone_number(temp_work_phone)
                if not check_phone:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректный рабочий телефон _"{temp_work_phone}"_. Отмена добавления пользователя.')
                else:
                    entry["work_phone"] = phone_value

            temp_mobile_phone = element["mobile_phone"]
            if temp_mobile_phone:
                check_phone, phone_value = validate_phone_number(temp_mobile_phone)
                if not check_phone:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректный мобильный телефон _"{temp_mobile_phone}"_. Отмена добавления пользователя.')
                else:
                    entry["mobile_phone"] = phone_value

            temp_personal_email = element["personal_email"]
            if temp_personal_email:
                check_email, email_value = validate_email(temp_personal_email)
                if not check_email:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректный личный email _"{temp_personal_email}"_. Отмена добавления пользователя.')
                else:
                    entry["personal_email"] = temp_personal_email

            if stop_adding:
                if element not in error_lines:
                    error_lines.append(element)
            else:
                correct_lines.append(entry)

            if not correct:
                suspiciose_lines.append(element)

        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            error_lines.append(element)

        logger.debug("." * 100)

    logger.info('Конец проверки корректности данных.')
    logger.info("\n")

    if len(error_lines) > 0:
        logger.error('!' * 100)
        logger.error('Некорректные строки в файле. Исправьте их и попробуйте снова.')
        logger.error('!' * 100)
        for element in error_lines:
            logger.error(f'Bad line: {mask_sensitive_data(element)}')
            logger.error("." * 100)
        logger.error('Выход.')
        logger.error('\n')
        return False, []
    
    if len(suspiciose_lines) > 0:
        logger.warning('*' * 100)
        logger.warning(f'В файле есть {len(suspiciose_lines)} некорректных строк. Проверьте кириллические буквы или неподдерживаемые символы в этих полях: login, first_name, last_name, middle_name')
        logger.warning('*' * 100)
        for element in suspiciose_lines:
            logger.warning(f'login: {element["login"]}; first_name: {element["first_name"]}; last_name: {element["last_name"]}; middle_name: {element["middle_name"]}')
            logger.warning("." * 100)
        logger.warning('\n')
        if not analyze_only:
            answer = input("Продолжить импорт? (Y/n): ")
            if answer.upper() not in ["Y", "YES"]:
                return False, []
    
    if analyze_only:
        if len(suspiciose_lines) == 0 and len(error_lines) == 0:
            logger.info('*' * 100)
            logger.info('Все строки корректны.')
            logger.info('*' * 100)
            return True, correct_lines
    
    return True, correct_lines
    
def add_users_from_file_phase_2(settings: "SettingParams", users: list):
    logger.info("-" * 100)
    if len(users) == 0:
        logger.info('Нет пользователей для добавления.')
        return
    logger.info(f'Добавление {len(users)} пользователей в Y360.')
    logger.info("-" * 100)
    user = {}
    added_users = []
    for u in users:
        user["name"] = {
            "first": u.get('first'),
            "last": u.get('last'),
            "middle": u.get('middle')
        }
        user["nickname"] = u.get('login')
        user["password"] = u.get('password')
        user["passwordChangeRequired"] = u.get('password_change_required')
        user["position"] = u.get('position')
        user["language"] = u.get('language')
        user["gender"] = u.get('gender')
        user["birthday"] = u.get('birthday')
        user["isAdmin"] = False
        user["contacts"] = []
        if u.get('work_phone',''):
            user["contacts"].append({
                "type": "phone",
                "value": u.get('work_phone'),
                'label': 'Work'
            })
        if u.get('mobile_phone',''):
            user["contacts"].append({
                "type": "phone",
                "value": u.get('mobile_phone'),
                'label': 'Mobile'
            })
        
        if u.get('personal_email',''):
            user["about"] = json.dumps({"email": u.get('personal_email')})

        if u["department"].isdigit():
            user["departmentId"] = u['department']
        else:
            user["departmentId"] = 1

        if settings.dry_run:
            logger.info(f"Пробный запуск. Пользователь {user['nickname']} ({user['name']['last']} {user['name']['first']}) не будет добавлен.")
            return False, []
        else:
            result, created_user = create_user_by_api(settings, user)
            if result:
                user["id"] = created_user["id"]
                temp_dict = {
                    "id": user["id"],
                    "department": u['department']
                }
                added_users.append(temp_dict)

    return True,added_users

def add_users_from_file_phase_3(settings: "SettingParams", users: list):
    logger.info("-" * 100)
    if len(users) == 0:
        logger.info('Пользователи не добавлены.')
        return
    logger.info('Работа с подразделениями пользователей.')
    api_deps_hierarchy = generate_deps_hierarchy_from_api(settings)
    deps_to_add = []
    users_without_deps = []
    count = 1
    logger.info("-" * 100)
    for user in users:
        strip_list = [x.strip() for x in user['department'].split(DEPS_SEPARATOR)]
        user_dep = DEPS_SEPARATOR.join(strip_list)
        user['department'] = user_dep
        found_flag = False
        if not user_dep.isdigit():
            for dep in api_deps_hierarchy:
                if dep['path'] == user_dep:
                    found_flag = True
                    patch_data={"departmentId": dep['id']}
                    patch_user_by_api(settings, user_id=user["id"], patch_data=patch_data)
                    break
            if not found_flag:
                temp_dict = {
                    "id": count,
                    "path": user_dep
                }
                deps_to_add.append(temp_dict)
                users_without_deps.append(user)
                count += 1

    if len(deps_to_add) > 0:
        logger.info(f'Добавление {len(deps_to_add)} подразделений для пользователей.')
        final_list = prepare_deps_list_from_raw_data(settings, deps_to_add)
        max_levels = max([len(s['path'].split(DEPS_SEPARATOR)) for s in final_list])
        # Добавление в 360
        
        create_dep_from_prepared_list(settings, final_list,max_levels)

    api_deps_hierarchy = generate_deps_hierarchy_from_api(settings)
    #time.sleep(1)
    for user in users_without_deps:
        for dep in api_deps_hierarchy:
            if dep['path'] == user['department']:
                patch_data={"departmentId": dep['id']}
                patch_user_by_api(settings, user_id=user["id"], patch_data=patch_data)
                break

    logger.info("-" * 100)
    logger.info('Добавление пользователей в подразделения завершено.')
    logger.info("-" * 100)
    return

def add_users_from_file(settings: "SettingParams", analyze_only=False):
    result, data = add_users_from_file_phase_1(settings, analyze_only)
    if not result:
        return False, []
    if analyze_only:
        return True, data
    result, data = add_users_from_file_phase_2(settings, data)
    if not result:
        return False, []
    data = add_users_from_file_phase_3(settings, data)
    return True, data

# Регулярное выражение для проверки фамилии
def validate_name(line):
    pattern = r'^[А-ЯЁ][а-яё]+(-[А-ЯЁ][а-яё]+)?$'
    if re.match(pattern, line):
        return True
    return False

def validate_login(settings: "SettingParams", alias: str):
    alias = alias.lower()

    users = get_all_api360_users(settings)
    first_iteration = True
    while True:
        no_conflicts = True
        conflicts = []
        for user in users:
            if alias == user['nickname'].lower():
                conflicts.append(user)
                no_conflicts = False
                
            aliases = [a.lower() for a in user['aliases']]
            if alias in aliases:
                if user not in conflicts:
                    conflicts.append(user)
                no_conflicts = False
                
            for contact in user['contacts']:
                if contact['type'] == 'email' and contact['value'].split('@')[0].lower() == alias:
                    if user not in conflicts:
                        conflicts.append(user)
                    no_conflicts = False

        if not no_conflicts and first_iteration:
            users = get_all_api360_users(settings, force=True)
            first_iteration = False
        else:
            break

    if no_conflicts:
        pattern = r'^[a-z0-9.-]+$'
        if not re.match(pattern, alias):
            return False, []
        if alias.startswith('_'):
            return False, []
    
    return no_conflicts, conflicts

def is_valid_date(date_string, min_years_diff=10, max_years_diff=100):
    """
    Проверяет, можно ли преобразовать строку в дату.
    
    Поддерживает несколько распространенных форматов даты:
    - DD.MM.YYYY
    - DD/MM/YYYY
    - DD-MM-YYYY
    - YYYY-MM-DD
    - YYYY/MM/DD
    
    Args:
        date_string (str): Строка для проверки
        
    Returns:
        bool: True если строка может быть преобразована в дату, иначе False
        datetime.date: Объект даты в случае успеха, иначе None
    """
    # Проверяем, что строка не пустая
    if not date_string or not isinstance(date_string, str):
        return False, None
    
    # Набор возможных форматов для проверки
    date_formats = [
        '%d.%m.%Y',  # DD.MM.YYYY
        '%d/%m/%Y',  # DD/MM/YYYY
        '%d-%m-%Y',  # DD-MM-YYYY
        '%Y-%m-%d',  # YYYY-MM-DD (ISO формат)
        '%Y/%m/%d',  # YYYY/MM/DD
        '%m/%d/%Y',  # MM/DD/YYYY (US формат)
        '%d.%m.%y',  # DD.MM.YY
        '%Y.%m.%d',  # YYYY.MM.DD
    ]
    
    # Попытка парсинга каждым из форматов
    current_date = date.today()
    for date_format in date_formats:
        try:
            date_obj = datetime.strptime(date_string, date_format).date()

            years_diff = abs((current_date.year - date_obj.year) + 
                (current_date.month - date_obj.month) / 12 +
                (current_date.day - date_obj.day) / 365.25)
            
            if years_diff < min_years_diff:
                return False, f"Дата отстоит от текущей менее, чем на {min_years_diff} лет"
            if years_diff > max_years_diff:
                return False, f"Дата отстоит от текущей более, чем на {max_years_diff} лет"
            # Дополнительная проверка на валидность (для високосных лет и т.д.)
            # Эта проверка не требуется, т.к. strptime уже выбросит исключение для невалидной даты
            return True, date_obj
        except ValueError:
            continue
    
    # Если ни один из форматов не подошел, проверяем с помощью регулярных выражений
    # для потенциально более сложных форматов
    date_patterns = [
        # Месяц прописью на английском: 25 December 2021, December 25, 2021
        r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
    ]
    
    month_map = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
        'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    
    for pattern in date_patterns:
        match = re.search(pattern, date_string, re.IGNORECASE)
        if match:
            groups = match.groups()
            try:
                if len(groups) == 3:
                    # 25 December 2021
                    if groups[0].isdigit() and groups[2].isdigit():
                        day = int(groups[0])
                        month = month_map[groups[1].capitalize()]
                        year = int(groups[2])
                    # December 25, 2021
                    else:
                        month = month_map[groups[0].capitalize()]
                        day = int(groups[1])
                        year = int(groups[2])
                    
                    date_obj = datetime.date(year, month, day)
                    return True, date_obj
            except (ValueError, KeyError):
                continue
    
    return False, None

def validate_password(settings: "SettingParams", password: str) -> tuple[bool, str]:
    """
    Проверяет корректность пароля с помощью регулярного выражения.
    
    Args:
        password (str): Пароль для проверки
        settings (SettingParams): Параметры конфигурации
    Returns:
        tuple: (bool, str) - (результат проверки, сообщение об ошибке или "OK")
    """
    if not password:
        return False, "Пароль не может быть пустым"
    
    pattern = settings.password_pattern
    try:
        if re.match(pattern, password):
            return True, "OK"
        else:
            return False, f"Пароль не соответствует требованиям безопасности. Используемый шаблон: {pattern}"
    except re.error as e:
        return False, f"Ошибка в регулярном выражении: {e}"

def mask_csv_line(line: str, headers: list) -> str:
    """
    Безопасно маскирует чувствительные данные в CSV строке.
    В случае несоответствия количества полей заголовкам, маскирует ВСЕ поля для безопасности.
    
    Args:
        line (str): CSV строка для маскировки
        headers (list): Список заголовков CSV файла
        
    Returns:
        str: CSV строка с замаскированными чувствительными полями
    """
    if not line or not headers:
        return line
    
    # Разделяем строку на поля
    fields = line.replace('"', '').split(";")
    
    # Список чувствительных полей
    sensitive_fields = ['password', 'oauth_token', 'access_token', 'token']
    
    # Если количество полей не совпадает с заголовками - МАСКИРУЕМ ВСЕ ПОЛЯ для безопасности
    if len(fields) != len(headers):
        logger.warning("Несоответствие количества полей в CSV строке. Маскируем все поля для безопасности.")
        return "***MASKED***" * len(fields) if fields else "***MASKED***"
    
    # Создаем массив для маскированных полей
    masked_fields = []
    
    for i, field in enumerate(fields):
        header = headers[i].strip().lower()
        
        # Если поле чувствительное, маскируем его
        if header in sensitive_fields:
            masked_fields.append("***MASKED***")
        else:
            masked_fields.append(field)
    
    # Собираем строку обратно
    return ";".join(masked_fields)

def mask_csv_line_safe(line: str) -> str:
    """
    Максимально безопасная маскировка CSV строки.
    Анализирует каждое поле отдельно, учитывая разделители CSV.
    
    Args:
        line (str): CSV строка для маскировки
        
    Returns:
        str: CSV строка с замаскированными чувствительными данными
    """
    if not line:
        return line
    
    # Разделяем строку на поля
    fields = line.split(";")
    masked_fields = []
    
    # Регулярные выражения для определения типов полей
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    # Пароль: минимум 8 символов, минимум одна заглавная буква, одна цифра, один спецсимвол
    password_pattern = r'^(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?])[A-Za-z0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]{8,}$'
    
    for field in fields:
        field = field.strip()
        
        # Проверяем, является ли поле email адресом
        if re.match(email_pattern, field):
            # Email адреса не маскируем
            masked_fields.append(field)
        # Проверяем, является ли поле потенциальным паролем
        elif re.match(password_pattern, field) and len(field) >= 8:
            # Маскируем пароли
            masked_fields.append("***MASKED***")
        else:
            # Обычные поля оставляем без изменений
            masked_fields.append(field)
    
    # Собираем строку обратно
    return ";".join(masked_fields)

def mask_sensitive_data(data: dict) -> dict:
    """
    Создает копию словаря с замаскированными чувствительными данными для безопасного логирования.
    
    Args:
        data (dict): Исходный словарь с данными
        
    Returns:
        dict: Копия словаря с замаскированными паролями и токенами
    """
    import copy
    
    # Создаем глубокую копию для безопасного изменения
    masked_data = copy.deepcopy(data)
    
    # Список полей, которые нужно замаскировать
    sensitive_fields = SENSITIVE_FIELDS
    
    def mask_recursive(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key.lower() in sensitive_fields:
                    obj[key] = "***MASKED***"
                elif isinstance(value, (dict, list)):
                    mask_recursive(value)
        elif isinstance(obj, list):
            for item in obj:
                mask_recursive(item)
    
    mask_recursive(masked_data)
    return masked_data

def validate_email(email: str) -> tuple[bool, str]:
    """
    Проверяет корректность адреса электронной почты.
    
    Args:
        email (str): Email адрес для проверки
        
    Returns:
        tuple: (bool, str) - (результат проверки, сообщение об ошибке или "OK")
    """
    if not email:
        return False, "Email адрес не может быть пустым"
    
    # Базовая проверка на пустую строку и пробелы
    email = email.strip()
    if not email:
        return False, "Email адрес не может быть пустым"
    
    # Проверка на наличие символа @
    if '@' not in email:
        return False, "Email адрес должен содержать символ @"
    
    # Проверка на множественные символы @
    if email.count('@') > 1:
        return False, "Email адрес не может содержать более одного символа @"
    
    # Разделение на локальную часть и домен
    local_part, domain = email.split('@', 1)
    
    # Проверка локальной части
    if not local_part:
        return False, "Локальная часть email адреса не может быть пустой"
    
    if len(local_part) > 64:
        return False, "Локальная часть email адреса не может быть длиннее 64 символов"
    
    # Проверка домена
    if not domain:
        return False, "Домен email адреса не может быть пустым"
    
    if len(domain) > 253:
        return False, "Домен email адреса не может быть длиннее 253 символов"
    
    # Проверка на точки в начале и конце
    if local_part.startswith('.') or local_part.endswith('.'):
        return False, "Локальная часть email адреса не может начинаться или заканчиваться точкой"
    
    if domain.startswith('.') or domain.endswith('.'):
        return False, "Домен email адреса не может начинаться или заканчиваться точкой"
    
    # Проверка на последовательные точки
    if '..' in local_part or '..' in domain:
        return False, "Email адрес не может содержать последовательные точки"
    
    # Регулярное выражение для проверки формата email
    email_pattern = DEFAULT_EMAIL_PATTERN
    
    try:
        if re.match(email_pattern, email):
            return True, "OK"
        else:
            return False, "Email адрес имеет некорректный формат"
    except re.error as e:
        return False, f"Ошибка в регулярном выражении: {e}"

def validate_phone_number(phone):
    """
    Проверяет корректность номера телефона.
    
    Допустимые символы: цифры, точки, пробелы, тире, плюс, круглые скобки.
    Также поддерживается указание добавочного номера через "ext" или "extension".
    Функция проверяет базовую структуру номера и очищает его от
    всех символов кроме цифр для проверки длины.
    
    Args:
        phone (str): Номер телефона для проверки
        
    Returns:
        tuple: (bool, str) - (результат проверки, причина ошибки или очищенный номер)
    """
    if not phone:
        return False, "Номер телефона не может быть пустым"
    
    # Обработка добавочного номера
    extension = None
    ext_pattern = re.compile(r'(?:ext|extension|доб|добавочный)(?:ension)?\.?\s*(\d+)', re.IGNORECASE)
    ext_match = ext_pattern.search(phone)
    
    # Если нашли добавочный номер, сохраняем его и удаляем из основного номера для проверки
    if ext_match:
        extension = ext_match.group(1)
        phone = phone[:ext_match.start()].strip() + phone[ext_match.end():].strip()
    
    # Проверка на допустимые символы
    if not re.match(r'^[0-9\s\.\-\+\(\)]+$', phone):
        return False, "Номер содержит недопустимые символы"
    
    # Проверка, что + только в начале номера
    if '+' in phone and not phone.startswith('+'):
        return False, "Символ '+' может быть только в начале номера"
    
    # Проверка на множественные + в начале
    if phone.count('+') > 1:
        return False, "Символ '+' может встречаться только один раз"
    
    # Проверка балансировки скобок
    if phone.count('(') != phone.count(')'):
        return False, "Несбалансированные скобки в номере"
    
    # Проверка правильного порядка скобок
    open_brackets = [i for i, char in enumerate(phone) if char == '(']
    close_brackets = [i for i, char in enumerate(phone) if char == ')']
    
    for i in range(len(open_brackets)):
        if i >= len(close_brackets) or open_brackets[i] > close_brackets[i]:
            return False, "Неправильный порядок скобок в номере"
    
    # Очистка номера от всех символов кроме цифр для проверки длины
    clean_number = re.sub(r'[^\d]', '', phone)
    
    # Проверка длины номера (минимум 10, максимум 15 цифр)
    if len(clean_number) < 3:
        return False, "Номер телефона слишком короткий (минимум 3 цифры)"
    
    if len(clean_number) > 16:
        return False, "Номер телефона слишком длинный (максимум 16 цифр)"
    
    # Проверка международного формата
    if phone.startswith('+'):
        # Если номер начинается с +, проверяем, что следующий символ - цифра или открывающая скобка
        if not re.match(r'^\+(\d|\()', phone):
            return False, "После '+' должна следовать цифра или открывающая скобка"
    
    # Форматирование номера
    formatted_number = clean_number
    
    # Российский формат номера с добавочным номером (если есть)
    if len(clean_number) == 11 and (clean_number.startswith('7') or clean_number.startswith('8')):
        formatted_number = f"+7 ({clean_number[1:4]}) {clean_number[4:7]}-{clean_number[7:9]}-{clean_number[9:11]}"
        if extension:
            formatted_number += f" доб. {extension}"
    # Для других номеров просто добавляем добавочный номер
    elif extension:
        formatted_number += f" доб. {extension}"
    
    return True, formatted_number

def get_all_api360_users(settings: "SettingParams", force = False):
    if not force:
        logger.info("Получение всех пользователей организации из кэша...")

    if not settings.all_users or force or (datetime.now() - settings.all_users_get_timestamp).total_seconds() > ALL_USERS_REFRESH_IN_MINUTES * 60:
        #logger.info("Получение всех пользователей организации из API...")
        settings.all_users = get_all_api360_users_from_api(settings)
        settings.all_users_get_timestamp = datetime.now()
    return settings.all_users

def get_all_api360_users_from_api(settings: "SettingParams"):
    logger.info("Получение всех пользователей организации из API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    has_errors = False
    users = []
    current_page = 1
    last_page = 1
    while current_page <= last_page:
        params = {'page': current_page, 'perPage': USERS_PER_PAGE_FROM_API}
        try:
            retries = 1
            while True:
                logger.debug(f"GET URL - {url}")
                response = requests.get(url, headers=headers, params=params)
                logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        has_errors = True
                        break
                else:
                    for user in response.json()['users']:
                        if not user.get('isRobot') and int(user["id"]) >= 1130000000000000:
                            users.append(user)
                    logger.debug(f"Загружено {len(response.json()['users'])} пользователей. Текущая страница - {current_page} (всего {last_page} страниц).")
                    current_page += 1
                    last_page = response.json()['pages']
                    break

        except requests.exceptions.RequestException as e:
            logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            has_errors = True
            break

        if has_errors:
            break

    if has_errors:
        print("Есть ошибки при GET запросах. Возвращается пустой список пользователей.")
        return []
    
    return users

@dataclass
class SettingParams:
    oauth_token: str
    org_id: int
    users_file : str
    all_users : list
    all_users_get_timestamp : datetime
    dry_run : bool
    password_pattern : str
    deps_file : str
    all_users_file : str

def get_settings():
    exit_flag = False
    oauth_token_bad = False
    settings = SettingParams (
        users_file = os.environ.get("USERS_FILE","users.csv"),
        oauth_token = os.environ.get("OAUTH_TOKEN"),
        org_id = os.environ.get("ORG_ID"),
        all_users = [],
        all_users_get_timestamp = datetime.now(),
        dry_run = os.environ.get("DRY_RUN_ARG","false").lower() == "true",
        password_pattern = os.environ.get("PASSWORD_PATTERN"),
        deps_file = os.environ.get("DEPS_FILE","deps.csv"),
        all_users_file = os.environ.get("ALL_USERS_FILE","all_users.csv"),
    )

    if not settings.users_file:
        logger.error("USERS_FILE_ARG не установлен.")
        exit_flag = True
    
    if not settings.oauth_token:
        logger.error("OAUTH_TOKEN_ARG не установлен.")
        oauth_token_bad = True

    if not settings.org_id:
        logger.error("ORG_ID_ARG не установлен.")
        exit_flag = True

    if not (oauth_token_bad or exit_flag):
        if not check_oauth_token(settings.oauth_token, settings.org_id):
            logger.error("OAUTH_TOKEN_ARG не является действительным")
            oauth_token_bad = True

    if not settings.password_pattern:
        logger.error("PASSWORD_PATTERN не установлен. Используется значение по умолчанию.")
        settings.password_pattern = DEFAULT_PASSWORD_PATTERN

    if oauth_token_bad:
        exit_flag = True
    
    if exit_flag:
        return None
    
    return settings


def check_oauth_token(oauth_token, org_id):
    """Проверяет, что токен OAuth действителен."""
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{org_id}/users?perPage=100"
    headers = {
        "Authorization": f"OAuth {oauth_token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == HTTPStatus.OK:
        return True
    return False

def create_user_by_api(settings: "SettingParams", user: dict):

    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    logger.debug(f"POST URL: {url}")
    logger.debug(f"POST DATA: {mask_sensitive_data(user)}")
    retries = 1
    added_user = {}
    success = False
    while True:
        try:
            response = requests.post(f"{url}", headers=headers, json=user)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during POST request: {response.status_code}. Error message: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Ошибка. Создание пользователя {user['nickname']} ({user['name']['last']} {user['name']['first']}) не удалось.")
                    break
            else:
                logger.info(f"Успех - пользователь {user['nickname']} ({user['name']['last']} {user['name']['first']}) создан успешно.")
                added_user = response.json()
                success = True
                break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    return success, added_user

def patch_user_by_api(settings: "SettingParams", user_id: int, patch_data: dict):

    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users/{user_id}"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    logger.debug(f"PATCH URL: {url}")
    logger.debug(f"PATCH DATA: {mask_sensitive_data(patch_data)}")
    retries = 1
    success = False
    while True:
        try:
            response = requests.patch(f"{url}", headers=headers, json=patch_data)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Error during PATCH request: {response.status_code}. Error message: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Ошибка. Изменение пользователя {user_id} не удалось.")
                    break
            else:
                logger.info(f"Успех - данные пользователя {user_id} изменены успешно.")
                success = True
                break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    return success

def get_all_api360_departments(settings: "SettingParams"):
    logger.info("Получение всех подразделений организации из API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/departments"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}

    has_errors = False
    departments = []
    current_page = 1
    last_page = 1
    while current_page <= last_page:
        params = {'page': current_page, 'perPage': DEPARTMENTS_PER_PAGE_FROM_API}
        try:
            retries = 1
            while True:
                logger.debug(f"GET URL - {url}")
                response = requests.get(url, headers=headers, params=params)
                logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        has_errors = True
                        break
                else:
                    for deps in response.json()['departments']:
                        departments.append(deps)
                    logger.debug(f"Загружено {len(response.json()['departments'])} подразделений. Текущая страница - {current_page} (всего {last_page} страниц).")
                    current_page += 1
                    last_page = response.json()['pages']
                    break

        except requests.exceptions.RequestException as e:
            logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            has_errors = True
            break

        if has_errors:
            break

    if has_errors:
        print("Есть ошибки при GET запросах. Возвращается пустой список подразделений.")
        return []
    
    return departments

def delete_department_by_api(settings: "SettingParams", department: dict):
    logger.info(f"Удаление подразделения {department['id']} ({department['name']}) из API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/departments/{department['id']}"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    logger.debug(f"DELETE URL: {url}")
    try:
        retries = 1
        while True:
            response = requests.delete(f"{url}", headers=headers)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"!!! ОШИБКА !!! при DELETE запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    has_errors = True
                    break
            else:
                logger.info(f"Успех - подразделение {department['id']} ({department['name']}) удалено успешно.")
                return True
    except requests.exceptions.RequestException as e:
        logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        has_errors = True

    if has_errors:
        print("Есть ошибки при DELETE запросах. Возвращается False.")
        return False

    return True


def delete_all_departments(settings: "SettingParams"):
    logger.info("Удаление всех подразделений организации...")
    departments = get_all_api360_departments(settings)
    if len(departments) == 0:
        logger.info("Нет подразделений для удаления.")
        return
    logger.info(f"Удаление {len(departments)} подразделений...")
    for department in departments:
        delete_department_by_api(settings, department)
    logger.info("Удаление всех подразделений завершено.")
    return

def create_department_by_api(settings: "SettingParams", department: dict):
    logger.info(f"Создание подразделения {department['name']} в API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/departments"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    logger.debug(f"POST URL: {url}")
    logger.debug(f"POST DATA: {department}")
    try:
        retries = 1
        while True:
            response = requests.post(f"{url}", headers=headers, json=department)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"!!! ОШИБКА !!! при POST запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    has_errors = True
                    break
            else:
                logger.info(f"Успех - подразделение {department['name']} создано успешно.")
                return True

    except requests.exceptions.RequestException as e:
        logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        has_errors = True

    if has_errors:
        print("Есть ошибки при POST запросах. Возвращается False.")
        return False

    return True

# ------------------------------------------------------------

def clear_dep_info_for_users(settings: "SettingParams"):
    # Функция для удаления признака членства пользователя в каком-либо департаменте
    users = get_all_api360_users(settings)
    print('Перемещение пользователей в департамент "Все"...')
    for user in users:
        if user.get("departmentId") != 1:
            patch_user_by_api(settings,
                            user_id=user.get("id"),
                            patch_data={
                                "departmentId": 1,
                            })
    print('Перемещение пользователей в департамент "Все" завершено.')
    return


def create_dep_from_prepared_list(settings: "SettingParams", deps_list, max_levels):
    # Фнункция создания департамента из предварительно подготовленного списка
    logger.info('Создание новых подразделений...')
    api_prepared_list = generate_deps_hierarchy_from_api(settings)
    for i in range(0, max_levels):
            #Выбираем департаменты, которые будем добавлять на каждом шаге (зависит от уровня level)
            deps_to_add = [d for d in deps_list if d['level'] == i+1]
            need_update_deps = False
            for item in deps_to_add:         
                #Ищем в основном словаре элемент-родитель для данного департамента
                d = next((e for e in deps_list if e['path'] == item['prev']), None)
                item['prevId'] = d['360id']
                #Проверяем, что данный департамент уже добавлен в систему
                t = next((e for e in api_prepared_list if e['path'] == item['path']), None)   
                if t is None:
                    department_info = {
                                    "name": item['current'],
                                    "parentId": d['360id']
                                }
                    create_department_by_api(settings, department_info)
                    need_update_deps = True
            #all_deps_from_api = organization.get_departments_list()
            if need_update_deps:
                api_prepared_list = generate_deps_hierarchy_from_api(settings)
            for item in deps_to_add:
                # Ищем в списке департаментов в 360 конкретное значение
                #d = next(i for i in all_deps_from_api if i['name'] == item['current'] and i['parentId'] == item['prevId'])
                d = next(i for i in api_prepared_list if i['path'] == item['path'])
                #Обновляем информацию в final_list для записанных в 360 департаментов
                item['360id'] = d['id']
    logger.info('Создание новых подразделений завершено.')


def prepare_deps_list_from_raw_data(settings: "SettingParams", raw_data):
    """     Входящий список должен быть в таком формате:
            34|Barb Corp
            35|Yandry Corp
            36|Yandry Corp|ИТ
            37|Barb Corp|ИТ
            38|Yandry Corp|Дирекция
            39|Barb Corp|ИТ|Отдел сопровождения
            40|Yandry Corp|ИТ|Отдел внедрения 
    """

    temp_list = [{'current': 'All', 'prev': 'None', 'level': 0, '360id': 1, 'prevId': 0, 'path': 'All'}]
    max_levels = 1
    # Формируем уникальный список всей иерархии подразделений (каждое подразделение имеет отдельную строку в списке)
    for item in raw_data:
        length = len(item['path'].split(DEPS_SEPARATOR))
        if length > max_levels:
            max_levels = length
        for i in range(0,length):
            if i == 0:
                temp_list.append({'current':item['path'].split(DEPS_SEPARATOR)[i], 'prev':'All', 'level':i+1, '360id':0, 'prevId':0, 'path':''})
            else:
                temp_list.append({'current':item['path'].split(DEPS_SEPARATOR)[i], 'prev':DEPS_SEPARATOR.join(item['path'].split(DEPS_SEPARATOR)[:i]), 'level':i+1, '360id':0, 'prevId':0, 'path':''})
    # Фильрация уникальных значений из списка словарей, полученного на предыдущем этапе
    final_list = [dict(t) for t in {tuple(d.items()) for d in temp_list}]
    # Заполнение поля path (полный путь к подразделению)
    for item in final_list:
        if not item['current'] == 'All':
            if item['prev'] == 'All':
                item['path'] = item['current']
            else:
                item['path'] = f'{item["prev"]}{DEPS_SEPARATOR}{item["current"]}'
    # Добавление в 360
    return final_list


def create_deps_from_scratch_entry(settings: "SettingParams"):
    answer = input("Выбран вариант удаления и создания подразделений из файла. Продолжить? (Y/n): ")
    if answer.upper() in ["Y", "YES"]:
        # Читаем файл из файла-образца
        deps_data = read_deps_file(settings)
        if len(deps_data) == 0:
            return
        
        delete_all_departments(settings)        
        
        final_list = prepare_deps_list_from_raw_data(settings, deps_data)
        max_levels = max([len(s['path'].split(DEPS_SEPARATOR)) for s in deps_data])
        # Добавление в 360
        create_dep_from_prepared_list(settings, final_list,max_levels)


def read_deps_file(settings: "SettingParams"):
    deps_file_name = settings.deps_file
    if not os.path.exists(deps_file_name):
        full_path = os.path.join(os.path.dirname(__file__), deps_file_name)
        if not os.path.exists(full_path):
            logger.error(f'ERROR! Файл {deps_file_name} не существует!')
            return []
        else:
            deps_file_name = full_path
    
    # ## Another way to read file with needed transfromations
    # with open(deps_file_name, 'r') as csvfile:
    #     header = csvfile.readline().split(";")
    #     for line in csvfile:
    #         fields = line.split(";")
    #         entry = {}
    #         for i,value in enumerate(fields):
    #             entry[header[i].strip()] = value.strip()
    #         data.append(entry)
    # print(data)

    data = []
    data_for_print = []
    with open(deps_file_name, 'r') as csvfile:
        
        for line in csvfile:
            entry_for_print = {}
            entry= {}
            fields = line.split(DEPS_SEPARATOR)
            fields = [x.strip() for x in fields]            
            entry_for_print[fields[0]] = DEPS_SEPARATOR.join(fields[1:])
            data_for_print.append(entry_for_print)
            entry['id'] = fields[0]
            entry['path'] = DEPS_SEPARATOR.join(fields[1:])
            data.append(entry)
    logger.info('*' * 100)
    logger.info('Data to import')
    logger.info('-' * 100)
    for line in data_for_print:
        logger.info(line)
    logger.info('-' * 100)
    answer = input("Continue to import? (Y/n): ")
    if answer.upper() in ["Y", "YES"]:
        return data
    else:
        return []
        

def del_all_deps(settings: "SettingParams"):
    answer = input("Удалить все подразделения? (Y/n): ")
    if answer.upper() in ["Y", "YES"]:
        delete_all_departments(settings)


# def delete_selected_deps(settings: "SettingParams", deps_list):
#     if len(deps_list) == 0:
#         return
#     for item in deps_list[::-1]:
#         if item['id'] > 1:
#             organization.delete_department_by_id(item['id'])


# def generate_deleted_deps():
#     #Для анализа используется файл DEPS_UNUSED_FILE 
#     file_data = read_deps_file('DEPS_UNUSED_FILE')
#     if len(file_data) == 0:
#         print('There are no departments to delete.')
#         return []
#     api_data = generate_deps_list_from_api()
#     deps_to_delete = []
#     for file in file_data:
#         found = False
#         for api in api_data:
#             if file['path'] == api['path']:
#                 found = True
#                 deps_to_delete.append(api)
#             elif api['path'].startswith(f'{file["path"]};'):
#                 found = True
#                 deps_to_delete.append(api)
#         if not found:
#             deps_to_delete.append({'id':-1,'path':file['path']})
#     return deps_to_delete


# def delete_selected_deps_entry():
#     deps_to_delete = generate_deleted_deps()
#     if len(deps_to_delete) == 0:
#         return
    
#     print('Selected departments will be deleted.')
#     for item in deps_to_delete:
#         if item['id'] != -1:
#             print(item)

#     d = next((i for i in deps_to_delete if i['id'] == -1), None)
#     if d is not None:
#         print('Selected departments NOT EXIST IN ORGANIZATION.')
#         for item in deps_to_delete:
#             if item['id'] == -1:
#                 print(item)

#     answer = input("Continue? (Y/n): ")
#     if answer.upper() in ["Y", "YES"]:
#         delete_selected_deps(deps_to_delete)
#     print('Done.')


def generate_unique_file_name(name): 
    name_without_ext = '.'.join(name.split('.')[0:-1])
    file_ext = name.split('.')[-1]
    now = datetime.now()
    file_var_part  = now.strftime("%Y%m%d_%H%M%S")
    final_file_name = f'{name_without_ext}_{file_var_part}.{file_ext}'
    return final_file_name


def generate_deps_hierarchy_from_api(settings: "SettingParams"):
    all_deps_from_api = get_all_api360_departments(settings)
    if len(all_deps_from_api) == 1:
        #print('There are no departments in organozation! Exit.')
        return []
    all_deps = []
    for item in all_deps_from_api:        
        path = item['name'].strip()
        prevId = item['parentId']
        if prevId > 0:
            while not prevId == 1:
                d = next(i for i in all_deps_from_api if i['id'] == prevId)
                path = f'{d["name"].strip()}{DEPS_SEPARATOR}{path}'
                prevId = d['parentId']
            element = {'id':item['id'], 'parentId':item['parentId'], 'path':path}
            all_deps.append(element)
    return all_deps

def generate_deps_hierarchy_and_count_users_from_api(settings: "SettingParams"):
    users = get_all_api360_users(settings)
    if not users:
        return []
    all_deps_from_api = generate_deps_hierarchy_from_api(settings)
    if len(all_deps_from_api) == 1:
        #print('There are no departments in organozation! Exit.')
        return []
    all_deps = []
    for item in all_deps_from_api:        
        path = item['name'].strip()
        users_count = sum( user['departmentId'] == item['id'] for user in users)
        prevId = item['parentId']
        if prevId > 0:
            while not prevId == 1:
                users_count += sum( user['departmentId'] == prevId for user in users)
                d = next(i for i in all_deps_from_api if i['id'] == prevId)
                path = f'{d["name"].strip()}{DEPS_SEPARATOR}{path}'
                prevId = d['parentId']
            element = {'id':item['id'], 'parentId':item['parentId'], 'path':path, 'users_count':users_count}
            all_deps.append(element)
    return all_deps

def load_dep_info_to_file(settings: "SettingParams"):
    all_deps = generate_deps_hierarchy_from_api(settings)
    write_deps_to_file(settings, all_deps)
    
def write_deps_to_file(settings: "SettingParams", deps_list):
    file_name = settings.deps_file  
    file_name_random = generate_unique_file_name(file_name)
    while os.path.exists(file_name_random):
        file_name_random = generate_unique_file_name(file_name)

    if len(deps_list) == 0:
        logger.info('Отсутствуют подразделения для экспорта! Выход.')
    else:        
        with open(file_name_random, 'w') as file:
            for item in deps_list:
                file.write(f'{item["id"]}{DEPS_SEPARATOR}{item["path"]}\n')
        logger.info(f'Данные экспортированы в файл {file_name_random}.')


def generate_unused_deps():
    #Для анализа используется файл DEPS_FILE_NAME (как источник используемых и актуальных департаментов)
    file_data = read_deps_file(settings)
    api_data = generate_deps_hierarchy_from_api(settings)
    deps_to_delete = []
    for api in api_data:
        found = False
        for file in file_data:
            if file['path'] == api['path']:
                found = True
                break
            elif file['path'].startswith(f'{api["path"]}{DEPS_SEPARATOR}'):
                found = True
                break
        if not found:
            if api['parentId'] > 0:
                deps_to_delete.append(api)
    return deps_to_delete

def export_empty_deps_to_file(settings: "SettingParams"):

    api_deps = generate_deps_hierarchy_and_count_users_from_api(settings)
    if not api_deps:
       print('В организации нет подразделений.') 
       return

    deps_to_delete = list( dept for dept in api_deps if dept['users_count'] == 0 )
    write_deps_to_file(settings, deps_to_delete) 
    return 

def export_unused_deps_to_file(settings: "SettingParams"):
    all_deps = generate_unused_deps(settings)
    write_deps_to_file(settings, all_deps)    


def update_deps_from_file(settings: "SettingParams"):
    file_data = read_deps_file(settings)
    if not file_data:
        return
    api_data = generate_deps_hierarchy_from_api(settings)
    deps_to_delete = []
    for api in api_data:
        found = False
        for file in file_data:
            if file['path'] == api['path']:
                found = True
                break
        if not found:
            deps_to_delete.append(api)

    final_list = prepare_deps_list_from_raw_data(settings, file_data)
    max_levels = max([len(s['path'].split(DEPS_SEPARATOR)) for s in file_data])
    # Добавление в 360
    create_dep_from_prepared_list(settings, final_list,max_levels)

def show_user_attributes_prompt(settings: "SettingParams"):
    print("\n")
    print("Введите данные для поиска пользователя в формате: <UID> или <API_360_NICKNAME> или <API_360_ALIAS> или часть фамилии (пустая строка для выхода)")
    while True:
        print("\n")
        answer = input("Искать: ")
        if not answer.strip():
            break
        else:
            show_user_attributes(settings, answer.lower())

def show_user_attributes(settings: "SettingParams", answer):

    pattern = r'[;,\s]+'
    search_users = re.split(pattern, answer)
    users_to_add = []
    #rus_pattern = re.compile('[-А-Яа-яЁё]+')
    #anti_rus_pattern = r'[^\u0400-\u04FF\s]'

    logger.info(f"Поиск пользователя {answer}.")
    users = get_all_api360_users(settings)
    departments = generate_deps_hierarchy_from_api(settings)
    if not users:
        logger.error("Не найдено пользователей из API 360. Проверьте ваши настройки.")
        return

    found_last_name_user = []
    double_users_flag = False
    for searched in search_users:
        found_flag = False
        target_user = None

        if "@" in searched.strip():
            searched = searched.split("@")[0]
        found_flag = False
        if all(char.isdigit() for char in searched.strip()):
            if len(searched.strip()) == 16 and searched.strip().startswith("113"):
                for user in users:
                    if user["id"] == searched.strip():
                        logger.debug(f"Пользователь найден: {user['nickname']} ({user['id']})")
                        target_user = user
                        found_flag = True
                        break
        else:
            found_last_name_user = []
            for user in users:
                aliases_lower_case = [r.lower() for r in user["aliases"]]
                if user["nickname"].lower() == searched.lower().strip() or searched.lower().strip() in aliases_lower_case:
                    logger.debug(f"Пользователь найден: {user['nickname']} ({user['id']})")
                    target_user = user
                    found_flag = True
                    break
                if user["name"]["last"].lower() == searched.lower().strip():
                    found_last_name_user.append(user)
            if not found_flag and found_last_name_user:
                if len(found_last_name_user) == 1:
                    logger.debug(f"Пользователь найден ({searched}): {found_last_name_user[0]['nickname']} ({found_last_name_user[0]['id']}, {found_last_name_user[0]['position']})")
                    target_user = found_last_name_user[0]
                    found_flag = True
                else:
                    logger.error(f"Пользователь {searched} найден более одного раза:")
                    for user in found_last_name_user:
                        logger.error(f" - last name {user['name']['last']}, nickname {user['nickname']} ({user['id']}, {user['position']})")
                    logger.error("Уточните параметры поиска.")
                    double_users_flag = True
                    #break

        if not found_flag and not double_users_flag:
            logger.error(f"Пользователь {searched} не найден в организации Y360.")
            continue
        else:
            users_to_add.append(target_user)

    if not users_to_add and not double_users_flag:
        logger.error(f"Поиск {answer} не найден в организации Y360.")
        return

    for target_user in users_to_add:
        logger.info("\n")
        logger.info("--------------------------------------------------------")
        logger.info(f'Атрибуты пользователя с id: {target_user["id"]}')
        logger.info("--------------------------------------------------------")
        for k, v in target_user.items():
            if k.lower() == "departmentid":
                if v == 1:
                    logger.info("departmentId: 1")
                    logger.info("Подразделение не указано")
                else:
                    department = next((d for d in departments if d['id'] == v), None)
                    logger.info(f"departmentId: {department['id']}")
                    logger.info(f"Department: {department['path']}")
            elif k.lower() == "contacts":
                logger.info("Contacts:")
                for l in v: 
                    for k1, v1 in l.items():  
                        logger.info(f" - {k1}: {v1}")
                    logger.info(" -")
            elif k.lower() == "aliases":
                if not v:
                    logger.info("Aliases: []")
                else:
                    logger.info("Aliases:")
                    for l in v:
                        logger.info(f" - {l}")
            elif k.lower() == "name":
                logger.info("Name:")
                for k1, v1 in v.items():  
                    logger.info(f" - {k1}: {v1}")
            else:
                logger.info(f"{k}: {v}")
        logger.info("--------------------------------------------------------")

        logger.info("\n")
        with open(f"{target_user['nickname']}.txt", "w", encoding="utf-8") as f:
            f.write(f'Атрибуты пользователя с id: {target_user["id"]}\n')
            f.write("--------------------------------------------------------\n")
            for k, v in target_user.items():
                if k.lower() == "departmentid":
                    if v == 1:
                        logger.info("departmentId: 1")
                        logger.info("Подразделение не указано")
                    else:
                        department = next((d for d in departments if d['id'] == v), None)
                        logger.info(f"departmentId: {department['id']}")
                        logger.info(f"Department: {department['path']}")
                elif k.lower() == "contacts":
                    f.write("Contacts:\n")
                    for l in v: 
                        for k1, v1 in l.items():  
                            f.write(f" - {k1}: {v1}\n")
                        f.write(" -\n")
                elif k.lower() == "aliases":
                    if not v:
                        f.write("Aliases: []\n")
                    else:
                        f.write("Aliases:\n")
                        for l in v:
                            f.write(f" - {l}\n")
                elif k.lower() == "name":
                    f.write("Name:\n")
                    for k1, v1 in v.items():  
                        f.write(f" - {k1}: {v1}\n")
                else:
                    f.write(f"{k}: {v}\n")
            f.write("--------------------------------------------------------\n")
        logger.info(f"Атрибуты пользователя сохранены в файл: {target_user['nickname']}.txt")
    return

def download_users_attrib_to_file(settings: "SettingParams"):
    users = get_all_api360_users(settings, force=True)
    if not users:
        logger.error("Не найдено пользователей из API 360. Проверьте ваши настройки.")
        return
    else:
        with open(settings.all_users_file, 'w', encoding='utf-8', newline='') as csv_file:
            fieldnames = list(users[0].keys())
            if "isEnabledUpdatedAt" not in fieldnames:
                fieldnames.append("isEnabledUpdatedAt")
            writer = csv.DictWriter(csv_file, delimiter=';', fieldnames=fieldnames)
            writer.writeheader()
            for user in users:
                writer.writerow(user)
            logger.info(f"Сохранено {len(users)} пользователей в файл {settings.all_users_file}")

def search_department_prompt(settings: "SettingParams"):
    print("\n")
    print("Введите данные для поиска подразделения в формате: <ID> или часть названия или алиас подразделения (пустая строка для выхода)")
    while True:
        print("\n")
        answer = input("Искать: ")
        if not answer.strip():
            break
        else:
            search_department_by_name(settings, answer.lower())

def search_department_by_name(settings: "SettingParams", name: str):
    all_deps_from_api = get_all_api360_departments(settings)
    if len(all_deps_from_api) == 1:
        logger.error('В организации нет никаких подразделений! Выход.')
        return 
    if '@' in name:
        name = name.split('@')[0]
    all_deps = {}
    target_dep = []
    deps_found = False
    for item in all_deps_from_api:        
        path = item['name'].strip()
        prevId = item['parentId']
        if prevId > 0:
            while not prevId == 1:
                d = next(i for i in all_deps_from_api if i['id'] == prevId)
                path = f'{d["name"].strip()}{DEPS_SEPARATOR}{path}'
                prevId = d['parentId']
            all_deps[str(item['id'])] = path
        if name.isdigit():
            if item['id'] == int(name):
                logger.info(f"Подразделение найдено: {item['name']} ({item['id']})")
                target_dep.append(item)
                deps_found = True
        elif name in item['name'].lower():
            logger.info(f"Подразделение найдено: {item['name']} ({item['id']})")
            target_dep.append(item)
            deps_found = True
        elif name == item['label'] or name in [alias.lower() for alias in item['aliases']]:
            if item not in target_dep:
                logger.info(f"Подразделение найдено: {item['name']} ({item['id']})")
                target_dep.append(item)
                deps_found = True

    if not deps_found:
        logger.error("Подразделения не найдены.")
    else:
        logger.info(f"Найдено подразделений - {len(target_dep)}")
        for item in target_dep:
            print("\n")
            logger.info(f"Информация о подразделении: id - {item['id']}, name - {item['name']}")
            logger.info(f"ID          : {item['id']}")
            logger.info(f"ParentId    : {item['parentId']}")
            count = 0
            for label in all_deps[str(item['id'])].split(DEPS_SEPARATOR):
                logger.info(f"Name + {count}    : {label}")
                count += 1
            logger.info(f"label       : {item['label']}")
            logger.info(f"email       : {item['email']}")
            logger.info(f"emailId     : {item['emailId']}")
            logger.info(f"description : {item['description']}")
            logger.info(f"createdAt   : {item['createdAt']}")
            if item['aliases']:
                logger.info(f"aliases     : {item['aliases']}")
            logger.info(f"membersCount: {item['membersCount']}")
            logger.info("--------------------------------------------------------")
    return



def main_menu(settings: "SettingParams"):

    while True:
        print("\n")
        print("Выберите опцию:")
        print("1. Добавить пользователей из файла.")
        print("2. Анализировать входной файл на ошибки.")
        print("3. Поиск подразделения по названию или алиасу.")
        print("4. Показать атрибуты пользователя.")
        print("5. Выгрузить всех пользователей в файл.")
        # print("3. Delete all contacts.")
        # print("4. Output bad records to file")
        print("0. (Ctrl+C) Выход")
        print("\n")
        choice = input("Введите ваш выбор (0-5): ")

        if choice == "0":
            print("До свидания!")
            break
        elif choice == "1":
            print('\n')
            add_users_from_file(settings)
        elif choice == "2":
            print('\n')
            add_users_from_file(settings, analyze_only=True )
        elif choice == "3":
            search_department_prompt(settings)
        elif choice == "4":
            show_user_attributes_prompt(settings)
        elif choice == "5":
            download_users_attrib_to_file(settings)
        # elif choice == "4":
        #     analyze_data = add_contacts_from_file(True)
        #     OutputBadRecords(analyze_data)
        else:
            logger.error("Неверный выбор. Попробуйте снова.")


if __name__ == "__main__":
    denv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(denv_path):
        load_dotenv(dotenv_path=denv_path,verbose=True, override=True)
    else:
        logger.error("Не найден файл .env. Выход.")
        sys.exit(EXIT_CODE)

    logger.info("\n")
    logger.info("---------------------------------------------------------------------------.")
    logger.info("Запуск скрипта.")
    
    settings = get_settings()
    
    if settings is None:
        logger.error("Проверьте настройки в файле .env и попробуйте снова.")
        sys.exit(EXIT_CODE)
    
    try:
        main_menu(settings)
    except KeyboardInterrupt:
        logger.info("\nCtrl+C pressed. До свидания!")
        sys.exit(EXIT_CODE)
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno}: {e}")
        sys.exit(EXIT_CODE)
    