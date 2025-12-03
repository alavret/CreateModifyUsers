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
from typing import Tuple
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import secrets
import string
import glob


DEFAULT_360_API_URL = "https://api360.yandex.net"
ITEMS_PER_PAGE = 100
MAX_RETRIES = 3
LOG_FILE = "add_users.log"
RETRIES_DELAY_SEC = 2
SLEEP_TIME_BETWEEN_API_CALLS = 0.5

SENSITIVE_FIELDS = ['password', 'oauth_token', 'access_token', 'token']
# DEFAULT_PASSWORD_PATTERN is used to validate the password
DEFAULT_PASSWORD_PATTERN = r'^(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]).{10,}$'

# DEFAULT_EMAIL_PATTERN is used to validate the personal email
DEFAULT_EMAIL_PATTERN = r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$'

USERS_CSV_REQUIRED_HEADERS = ["id", "login", "password", "password_change_required", "first_name", "last_name", "middle_name", "position", "gender", "birthday", "language", "work_phone", "mobile_phone", "personal_email", "department", "is_enabled", "is_admin", "aliases", "update_password"]

# MAX value is 1000
USERS_PER_PAGE_FROM_API = 1000
DEPARTMENTS_PER_PAGE_FROM_API = 100
GROUPS_PER_PAGE_FROM_API = 1000

DEPS_SEPARATOR = '|'
CLEAR_FIELD_VALUE = '-'

EXIT_CODE = 1

# Email constants
EMAIL_TEMPLATE_FILE = "email_template.html"
PASSWORD_CHANGE_TEMPLATE_FILE = "password_change_template.html"
SMTP_TIMEOUT = 10

ALL_GROUPS_REFRESH_IN_MINUTES = 15
ALL_DEPS_REFRESH_IN_MINUTES = 15
EXTENDED_USERS_REFRESH_IN_MINUTES = 15
ALL_USERS_REFRESH_IN_MINUTES = 15

# Необходимые права доступа для работы скрипта
NEEDED_PERMISSIONS = [
    "directory:read_users",
    "directory:write_users",
    "directory:read_departments",
    "directory:write_departments",
    "directory:read_groups",
    "directory:write_groups",
    "directory:read_organization",
    "ya360_admin:mail_read_shared_mailbox_inventory",
    "ya360_admin:mail_read_shared_mailbox_inventory",
]

# Глобальная переменная для кэширования загруженных алиасов поисковых атрибутов
_search_aliases_cache = None

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
                return False, []
            logger.debug(f'Headers: {headers}')
            for line in csvfile:
                if line.startswith('#'):
                    logger.debug(f'Строка начинается с "#"Пропуск строки из файла - {mask_csv_line_safe(line)}')
                    continue
                
                logger.debug(f'Чтение строки из файла - {mask_csv_line_safe(line)}')
                #fields = line.replace('"','').split(";")
                fields = line.split(";")
                if len(fields) != len(headers):
                    logger.error(f'Ошибка! Строка {mask_csv_line_safe(line)} - количество полей не соответствует количеству заголовков в первой строке файла. Возможно, в значении какого-либо поля есть точка с запятой. Попробуйте заменить её на другой символ.')
                    return False, []
                entry = {}
                for i,value in enumerate(fields):
                    # Удаляем кавычки только если они обрамляют всю строку
                    value = remove_quotes_if_wrapped(value)
                    entry[headers[i].strip()] = value.strip()
                data.append(entry)
        logger.info(f'Конец чтения файла {users_file_name}')
        logger.info("\n")
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return False, []

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
    # заполнение кэша пользователей API 360 
    users = get_all_api360_users(settings, force=True)

    if not analyze_only:
        check_aliases_uniqueness_result, check_aliases_uniqueness_errors = check_aliases_uniqueness(data, mode="add")
    else:
        check_aliases_uniqueness_result, check_aliases_uniqueness_errors = check_aliases_uniqueness(data, mode="modify")
    if not check_aliases_uniqueness_result:
        logger.error('Некоторые алиасы не уникальны. Обновление отменено.')
        return False, []

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
                        if not analyze_only:
                            for conflict in conflicts:
                                logger.error(f'Строка #{line_number}. Конфликт логина _"{temp_login}"_ с существующем пользователем {conflict["nickname"]} ({conflict["name"]["last"]} {conflict["name"]["first"]}). Добавление пользователя отменено.')
                            stop_adding = True
                else:
                    entry["login"] = temp_login
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Логин пуст. Отмена добавления пользователя.')

            temp_first_name = element.get("first_name","")
            if temp_first_name:
                if not validate_name(temp_first_name):
                    correct = False
                    logger.warning(f'Строка #{line_number}. Возможный некорректное Имя пользвоателя _"{temp_first_name}"_')
                entry["first"] = temp_first_name
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Имя пользвоателя пусто. Отмена добавления пользователя.')

            temp_last_name = element.get("last_name","")
            if temp_last_name:
                if not validate_name(temp_last_name):
                    correct = False
                    logger.warning(f'Строка #{line_number}. Возможная некорректная фамилия пользвоателя _"{temp_last_name}"_')
                entry["last"] = temp_last_name
            else:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Фамилия пользвоателя пуста. Отмена добавления пользователя.')

            temp_middle_name = element.get("middle_name","")
            if temp_middle_name:
                if not validate_name(temp_middle_name):
                    correct = False
                    logger.warning(f'Строка #{line_number}. Возможная некорректное отчество пользвоателя _"{temp_middle_name}"_')
            entry["middle"] = temp_middle_name

            temp_password = element.get("password","")
            if temp_password:
                # Проверяем пароль с помощью регулярного выражения
                password_valid, password_message = validate_password(settings, temp_password)
                if not password_valid:
                    #stop_adding = True
                    logger.error(f'Строка #{line_number}. Возможно слабый пароль, который не может быть установлен: {password_message}')
                else:
                    entry["password"] = temp_password
            else:
                # Если пароль пустой, проверяем возможность автогенерации
                if settings.auto_generate_password:
                    generated_password = generate_temp_password(settings.generated_password_length)
                    logger.info(f'Строка #{line_number}. Пароль не указан. Сгенерирован временный пароль длиной {len(generated_password)} символов.')
                    entry["password"] = generated_password
                else:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Пароль пуст. Функция автогенерации пароля отключена в настройках. Отмена добавления пользователя.')

            password_change_required = element.get("password_change_required","").lower()
            if password_change_required not in ['true', 'false']:
                stop_adding = True
                logger.error(f'Строка #{line_number}. Неккорректный параметр password_change_required _"{password_change_required}"_. Должно быть true или false. Отмена добавления пользователя.')
            else:
                entry["password_change_required"] = password_change_required

            temp_language = element.get("language","").lower()
            if temp_language and temp_language not in ['ru', 'en']:
                #stop_adding = True
                logger.error(f'Строка #{line_number}. Некорректный язык _"{temp_language}"_. Должно быть ru или en. Будет записано пустое значение.')
            else:
                entry["language"] = temp_language

            temp_gender = element.get("gender","").lower()
            if temp_gender and temp_gender not in ['male', 'female']:
                #stop_adding = True
                logger.error(f'Строка #{line_number}. Некорректный пол _"{temp_gender}"_. Должно быть male или female. Будет записано пустое значение.')
            else:
                entry["gender"] = temp_gender   

            temp_birthday = element.get("birthday","")
            if temp_birthday:
                check_date, date_value = is_valid_date(temp_birthday)
                if not check_date:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректная дата рождения _"{temp_birthday}"_ ({date_value}). Отмена добавления пользователя.')
                else:
                    entry["birthday"] = date_value.strftime('%Y-%m-%d')

            entry["position"] = element.get("position","")

            temp_is_enabled = element.get("is_enabled", "true").lower()
            if temp_is_enabled and temp_is_enabled == 'false':
                logger.info(f'Строка #{line_number}, login {element["position"]}. Установлено False в значении поля is_enabled. При создании пользователя данное поле игнорируется, пользователь будет создан в статусе Enabled.')
            # if temp_is_enabled and temp_is_enabled not in ['true', 'false']:
            #     logger.error(f'Строка #{line_number}. Некорректный параметр is_enabled _"{temp_is_enabled}"_. Должно быть true или false. Будет использовано значение true.')
            #     entry["is_enabled"] = True
            # else:
            #     entry["is_enabled"] = temp_is_enabled

            temp_is_admin = element.get("is_admin", "false").lower()
            if temp_is_admin and temp_is_admin not in ['true', 'false']:
                logger.error(f'Строка #{line_number}. Некорректный параметр is_admin _"{temp_is_admin}"_. Должно быть true или false. Будет использовано значение false')
                entry["is_admin"] = False
            else:
                entry["is_admin"] = temp_is_admin

            temp_aliases = element.get("aliases", "").split(",")
            if temp_aliases:
                for alias in temp_aliases:
                    if not validate_alias(settings, alias.split("@")[0].lower().strip()):
                        stop_adding = True
                        logger.error(f'Строка #{line_number}. Некорректный алиас _"{alias}"_. Отмена добавления пользователя.')
                entry["aliases"] = temp_aliases

            found_dep = False
            if len(element.get("department","")) == 0:
                entry["department"] = "1"
            else:
                entry["department"] = element.get("department","")
                if entry["department"].isdigit():
                    if int(entry["department"]) > 1:
                        for dep in api_deps_hierarchy:
                            if dep['id'] == int(entry["department"]):
                                found_dep = True
                                break
                        if not found_dep:
                            stop_adding = True
                            logger.error(f'Строка #{line_number}. Подразделение с номером {entry["department"]} не найдено в организации. Отмена добавления пользователя.')

            temp_work_phone = element.get("work_phone","")
            if temp_work_phone:
                check_phone, phone_value = validate_phone_number(temp_work_phone)
                if not check_phone:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректный рабочий телефон _"{temp_work_phone}"_. Отмена добавления пользователя.')
                else:
                    entry["work_phone"] = phone_value

            temp_mobile_phone = element.get("mobile_phone","")
            if temp_mobile_phone:
                check_phone, phone_value = validate_phone_number(temp_mobile_phone)
                if not check_phone:
                    stop_adding = True
                    logger.error(f'Строка #{line_number}. Некорректный мобильный телефон _"{temp_mobile_phone}"_. Отмена добавления пользователя.')
                else:
                    entry["mobile_phone"] = phone_value

            temp_personal_email = element.get("personal_email","")
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
        if u.get('is_admin'):
            user["isAdmin"] = u.get('is_admin')
        if u.get('is_enabled'):
            user["isEnabled"] = u.get('is_enabled')
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
            user["about"] = json.dumps({"personal_email": u.get('personal_email')})

        if u["department"].isdigit():
            user["departmentId"] = u['department']
        else:
            user["departmentId"] = 1

        if settings.dry_run:
            logger.info(f"Пробный запуск. Пользователь {user['nickname']} ({user['name']['last']} {user['name']['first']}) не будет добавлен.")
            #return False, []
        else:
            result, created_user = create_user_by_api(settings, user)
            if result:
                user["id"] = created_user["id"]
                temp_dict = {
                    "id": user["id"],
                    "department": u['department'],
                    "isAdmin": u['is_admin'],
                    "login": u['login']
                }
                added_users.append(temp_dict)
                if len(u.get('aliases', [])) > 0:
                    for alias in u.get('aliases', []):
                        if alias:
                            create_user_alias_by_api(settings, user_id=user["id"], alias=alias.split("@")[0].lower().strip())
                
                # Отправка приветственного письма
                if settings.send_welcome_email:
                    # Добавляем данные для email шаблона
                    email_data = {
                        'first': u.get('first'),
                        'middle': u.get('middle'),
                        'last': u.get('last'),
                        'login': u.get('login'),
                        'password': u.get('password'),
                        'password_change_required': u.get('password_change_required'),
                        'position': u.get('position'),
                        'department_name': u.get('department').split(DEPS_SEPARATOR)[-1] if not u.get('department', '').isdigit() else '',
                        'personal_email': u.get('personal_email')
                    }
                    send_welcome_email(settings, email_data)

    return True,added_users

def add_users_from_file_phase_3(settings: "SettingParams", users: list):
    logger.info("-" * 100)
    if len(users) == 0:
        logger.info('Пользователи не добавлены.')
        return

    logger.info('Проверка, есть ли пользователи с is_admin = true.')
    for user in users:
        if user['isAdmin']:
            logger.info(f'Пользователь {user["login"]} имеет isAdmin = true. Установка этого значения.')
            patch_user_by_api(settings, user_id=user["id"], patch_data={"isAdmin": "true"})

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
                # Если пользователь передан через функцию обновления, то используем поле user_id из словаря "пользователь", иначе используем поле id из словаря "пользоатель"
                user_id_from_update_func = user.get('user_id',None)
                user_id = user.get('id',user_id_from_update_func)
                patch_user_by_api(settings, user_id=user_id, patch_data=patch_data)
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
    if settings.dry_run:
        return True, data
    if not result:
        return False, []
    data = add_users_from_file_phase_3(settings, data)
    return True, data

def update_users_from_file_phase_1(settings: "SettingParams"):
    """
    Фаза 1: Чтение и валидация данных из файла для обновления пользователей
    """
    logger.info("-" * 100)
    logger.info(f'Чтение пользователей из файла {settings.users_file} и проверка корректности данных для обновления.')
    logger.info("-" * 100)
    users_file_name = settings.users_file
    if not os.path.exists(users_file_name):
        full_path = os.path.join(os.path.dirname(__file__), users_file_name)
        if not os.path.exists(full_path):
            logger.error(f'Ошибка! Файл {users_file_name} не существует!')
            return False, []
        else:
            users_file_name = full_path
    
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
                return False, []
            logger.debug(f'Headers: {headers}')
            for line in csvfile:
                if line.startswith('#'):
                    logger.debug(f'Строка начинается с "#". Пропуск строки из файла - {mask_csv_line_safe(line)}')
                    continue
                
                logger.debug(f'Чтение строки из файла - {mask_csv_line_safe(line)}')
                fields = line.split(";")
                if len(fields) != len(headers):
                    logger.error(f'Ошибка! Строка {mask_csv_line_safe(line)} - количество полей не соответствует количеству заголовков в первой строке файла. Возможно, в значении какого-либо поля есть точка с запятой. Попробуйте заменить её на другой символ.')
                    return False, []
                entry = {}
                for i,value in enumerate(fields):
                    value = remove_quotes_if_wrapped(value)
                    if value.strip() == CLEAR_FIELD_VALUE:
                        entry[headers[i].strip()] = ' '
                    else:
                        entry[headers[i].strip()] = value.strip()
                data.append(entry)
        logger.info(f'Конец чтения файла {users_file_name}')
        logger.info("\n")
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return False, []

    correct_lines = []
    error_lines = []
    line_number = 0

    logger.info("-" *100)
    logger.info('Проверка корректности данных для обновления.')
    logger.info("-" *100)
    api_deps_hierarchy = generate_deps_hierarchy_from_api(settings)
    # заполнение кэша пользователей API 360 
    users = get_all_api360_users(settings, force=True)

    check_aliases_uniqueness_result, check_aliases_uniqueness_errors = check_aliases_uniqueness(data, mode="update")
    if not check_aliases_uniqueness_result:
        logger.error('Некоторые алиасы не уникальны. Обновление отменено.')
        return False, []

    for element in data:
        entry = {}
        stop_updating = False
        user_id = 0
        line_number += 1
        logger.debug(f'Обработка строки #{line_number} {mask_sensitive_data(element)}')
        
        try:
            temp_user_id = element.get("id", "0")
            if not all(c.isdigit() for c in temp_user_id):
                logger.error(f'Строка #{line_number}. Некорректный ID пользователя _"{temp_user_id}"_. Должно быть число или пустая строка. Пропуск строки.')
                if element not in error_lines:
                    error_lines.append(element)
                stop_updating = True

            else:
                temp_user_id = int(temp_user_id)
                if temp_user_id > 0:
                    if not temp_user_id >= 1130000000000000:
                        logger.error(f'Строка #{line_number}. Некорректный ID пользователя _"{temp_user_id}"_. Должно быть число >= 1130000000000000. Пропуск строки.')
                        if element not in error_lines:
                            error_lines.append(element)
                        stop_updating = True
                    else:
                        for user in users:
                            if user['id'] == str(temp_user_id):
                                entry["user_id"] = user["id"]
                                user_id = int(user["id"])
                                entry["existing_user"] = user
                                break
                        if user_id == 0:
                            logger.error(f'Строка #{line_number}. Пользователь с ID _"{temp_user_id}"_ не найден в системе. Обновление отменено.')
                            if element not in error_lines:
                                error_lines.append(element)
                            stop_updating = True

            # Логин (обязательное поле для поиска пользователя если нет ID пользователя)
            temp_login = element["login"].lower()
            if temp_login and user_id > 0:
                entry["login"] = temp_login
            elif temp_login and user_id == 0:
                if '@' in temp_login:
                    temp_login = element["login"].split('@')[0]
                
                # Ищем пользователя
                found, existing_user = find_user_by_login(settings, temp_login)
                if not found:
                    logger.error(f'Строка #{line_number}. Пользователь с логином "{temp_login}" не найден в системе. Обновление отменено.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
                else:
                    entry["login"] = temp_login
                    entry["user_id"] = existing_user["id"]
                    
                    entry["existing_user"] = existing_user
                    logger.info(f'Строка #{line_number}. Найден пользователь: {existing_user["nickname"]} (ID: {existing_user["id"]})')
            else:
                logger.error(f'Строка #{line_number}. Логин и ID пользователя пусты. Отмена обновления пользователя.')
                if element not in error_lines:
                    error_lines.append(element)
                stop_updating = True

            if stop_updating:
                continue

            # Имя
            entry["first"] = element.get("first_name",'')
            if entry["first"] and entry["first"].strip():
                if not validate_name(entry["first"]):
                    logger.warning(f'Строка #{line_number}. Возможное некорректное имя пользователя _"{entry["first"]}"_')
            elif entry["first"] and not entry["first"].strip():
                logger.error(f'Строка #{line_number}. Очистить параметр first нельзя. Нужно указать имя.')
                if element not in error_lines:
                    error_lines.append(element)
                    stop_updating = True
            # Если пустое - не обновляем

            # Фамилия
            entry["last"] = element.get("last_name",'')
            if entry["last"] and entry["last"].strip():
                if not validate_name(entry["last"]):
                    logger.warning(f'Строка #{line_number}. Возможная некорректная фамилия пользователя _"{entry["last"]}"_')
            elif entry["last"] and not entry["last"].strip():
                logger.error(f'Строка #{line_number}. Очистить параметр last нельзя. Нужно указать фамилию.')
                if element not in error_lines:
                    error_lines.append(element)
                    stop_updating = True
            # Отчество
            entry["middle"] = element.get("middle_name",'')
            if entry["middle"] and entry["middle"].strip():
                if not validate_name(entry["middle"]):
                    logger.warning(f'Строка #{line_number}. Возможное некорректное отчество пользователя _"{entry["middle"]}"_')

            # Обработка пароля
            temp_password = element.get("password",'').strip()
            password_change_required = element.get("password_change_required","").lower()
            
            if password_change_required not in ['true', 'false', '']:
                logger.error(f'Строка #{line_number}. Некорректный параметр password_change_required _"{password_change_required}"_. Должно быть true, false или пусто.')
                if element not in error_lines:
                    error_lines.append(element)
                stop_updating = True
            
            entry["update_password"] = element.get("update_password",'false').lower()
            if entry["update_password"] and entry["update_password"] not in ['true', 'false']:
                logger.error(f'Строка #{line_number}. Неккорректный параметр update_password _"{entry["update_password"]}"_. Должно быть true или false. Параметр будет записан как false.')
                entry["update_password"] = 'false'

            if entry["update_password"] == 'true':
            # Если password_change_required = true и password пустой - генерируем новый пароль
                if password_change_required == 'true' and not temp_password.strip():
                    if settings.auto_generate_password:
                        generated_password = generate_temp_password(settings.generated_password_length)
                        logger.info(f'Строка #{line_number}. Пароль будет сгенерирован автоматически (длина {len(generated_password)} символов).')
                        entry["password"] = generated_password
                        entry["password_was_generated"] = True
                    else:
                        logger.error(f'Строка #{line_number}. Требуется изменение пароля, но пароль не указан и автогенерация отключена.')
                        if element not in error_lines:
                            error_lines.append(element)
                        stop_updating = True
                elif temp_password:
                    # Если пароль указан, проверяем его
                    password_valid, password_message = validate_password(settings, temp_password)
                    if not password_valid:
                        logger.error(f'Строка #{line_number}. Некорректный пароль: {password_message}')
                        if element not in error_lines:
                            error_lines.append(element)
                        stop_updating = True
                    else:
                        entry["password"] = temp_password
                        entry["password_was_generated"] = False
                
                if password_change_required:
                    entry["password_change_required"] = password_change_required
                else:
                    logger.error(f'Строка #{line_number}. Требуется изменение пароля, но не указан параметр password_change_required.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
            else:
                if temp_password:
                    logger.error(f'Строка #{line_number}. Login - "{temp_login}". Пароль указан, но update_password = false. Пароль не будет изменен.')
            
            # Язык
            entry["language"] = element.get("language",'').lower()
            if entry["language"] and entry["language"].strip():
                if entry["language"] not in ['ru', 'en']:
                    logger.error(f'Строка #{line_number}. Некорректный язык _"{entry["language"]}"_. Должно быть ru или en.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
            elif entry["language"] and not entry["language"].strip():
                logger.error(f'Строка #{line_number}. Очистить параметр language после создания пользователя нельзя. Нужно указать ru или en.') 
                if element not in error_lines:
                    error_lines.append(element)
                stop_updating = True

            # Пол
            entry["gender"] = element.get("gender",'').lower()
            if entry["gender"] and entry["gender"].strip():
                if entry["gender"] not in ['male', 'female']:
                    logger.error(f'Строка #{line_number}. Некорректный пол _"{entry["gender"]}"_. Должно быть male или female.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True

            # Дата рождения
            entry["birthday"] = element.get("birthday",'')
            if entry["birthday"] and entry["birthday"].strip():
                check_date, date_value = is_valid_date(entry["birthday"])
                if not check_date:
                    logger.error(f'Строка #{line_number}. Некорректная дата рождения _"{entry["birthday"]}"_ ({date_value}).')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
                else:
                    entry["birthday"] = date_value.strftime('%Y-%m-%d')

            # Должность
            entry["position"] = element.get("position",'')

            entry["is_enabled"] = element.get("is_enabled", "").lower()
            if entry["is_enabled"] and entry["is_enabled"].strip():
                if entry["is_enabled"] not in ['true', 'false']:
                    logger.error(f'Строка #{line_number}. Некорректный параметр is_enabled _"{entry["is_enabled"]}"_. Должно быть true или false.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
            elif entry["is_enabled"] and not entry["is_enabled"].strip():
                logger.error(f'Строка #{line_number}. Очистить параметр is_enabled нельзя. Нужно указать true или false.')

            entry["is_admin"] = element.get("is_admin", "").lower()
            if entry["is_admin"] and entry["is_admin"].strip():
                if entry["is_admin"] not in ['true', 'false']:
                    logger.error(f'Строка #{line_number}. Некорректный параметр is_admin _"{entry["is_admin"]}"_. Должно быть true или false.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
            elif entry["is_admin"] and not entry["is_admin"].strip():
                logger.error(f'Строка #{line_number}. Очистить параметр is_admin нельзя. Нужно указать true или false.')


            temp_aliases = element.get("aliases", "")
            entry['raw_aliases'] = temp_aliases.lower()
            entry["aliases"] = []
            if temp_aliases and temp_aliases.strip():
                bad_aliases = False
                for alias in temp_aliases.split(","):
                    if not validate_alias(settings, alias.split("@")[0].lower().strip()):
                        bad_aliases = True
                        logger.error(f'Строка #{line_number}. Некорректный алиас _"{alias}"_. Отмена добавления пользователя.')
                    else:
                        entry["aliases"].append(alias.split("@")[0].lower().strip())
                if bad_aliases:
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True

            # Подразделение
            entry["department"] = element.get("department",'')
            if entry["department"] and entry["department"].strip():
                if entry["department"].isdigit():
                    if int(entry["department"]) > 1:
                        found_dep = False
                        for dep in api_deps_hierarchy:
                            if dep['id'] == int(entry["department"]):
                                found_dep = True
                                break
                        if not found_dep:
                            logger.error(f'Строка #{line_number}. Подразделение с номером {entry["department"]} не найдено в организации.')
                            if element not in error_lines:
                                error_lines.append(element)
                            stop_updating = True


            # Рабочий телефон
            entry["work_phone"] = element.get("work_phone",'')
            if entry["work_phone"] and entry["work_phone"].strip():
                check_phone, phone_value = validate_phone_number(entry["work_phone"])
                if not check_phone:
                    logger.error(f'Строка #{line_number}. Некорректный рабочий телефон _"{entry["work_phone"]}"_.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
                else:
                    entry["work_phone"] = phone_value

            # Мобильный телефон
            entry["mobile_phone"] = element.get("mobile_phone",'')
            if entry["mobile_phone"] and entry["mobile_phone"].strip():
                check_phone, phone_value = validate_phone_number(entry["mobile_phone"])
                if not check_phone:
                    logger.error(f'Строка #{line_number}. Некорректный мобильный телефон _"{entry["mobile_phone"]}"_.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True
                else:
                    entry["mobile_phone"] = phone_value

            # Личный email
            entry["personal_email"] = element.get("personal_email",'')
            if entry["personal_email"] and entry["personal_email"].strip():
                check_email, email_value = validate_email(entry["personal_email"])
                if not check_email:
                    logger.error(f'Строка #{line_number}. Некорректный личный email _"{entry["personal_email"]}"_.')
                    if element not in error_lines:
                        error_lines.append(element)
                    stop_updating = True

            if not stop_updating:
                correct_lines.append(entry)

        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            if element not in error_lines:
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
    
    return True, correct_lines

def update_users_from_file_phase_2(settings: "SettingParams", users: list):
    """
    Фаза 2: Обновление пользователей в Yandex 360
    """
    logger.info("-" * 100)
    if len(users) == 0:
        logger.info('Нет пользователей для обновления.')
        return True, []
    logger.info(f'Обновление {len(users)} пользователей в Y360.')
    logger.info("-" * 100)
    
    updated_users = []
    users_with_new_deps = []
    # Часть атрибутов, нельзя изменить, если пользователь заблокирован в 360. Для них будем записывать в этот список и потом обновлять отдельным процессом 
    
    api_deps_hierarchy = generate_deps_hierarchy_from_api(settings)
    
    for u in users:
        try:
            existing_user = u.get('existing_user')
            user_id = u.get('user_id')
            changes = {}
            changes_for_disabled_users = {}
            password_changed = False
            
            logger.info(f"Обработка пользователя: {u.get('login')} (ID: {user_id})")
            
            # if u.get('login') and u.get('login').strip():
            #     if existing_user.get('nickname') != u.get('login'):
            #         changes['nickname'] = u.get('login').strip()
            #         logger.debug(f"  Изменение логина: {existing_user.get('nickname')} -> {u.get('login')}")
            # Проверяем изменения в имени
            changes['name'] = {}
            if u.get('first') and existing_user['name'].get('first').lower().strip() != u.get('first').lower().strip():
                changes['name']['first'] = u.get('first').strip()
                logger.debug(f"  Изменение имени: {existing_user['name'].get('first')} -> {u.get('first')}")
            
            if u.get('last') and existing_user['name'].get('last').lower().strip() != u.get('last').lower().strip():
                changes['name']['last'] = u.get('last').strip()
                logger.debug(f"  Изменение фамилии: {existing_user['name'].get('last')} -> {u.get('last')}")
            
            if u.get('middle') and existing_user['name'].get('middle').lower().strip() != u.get('middle').lower().strip():
                changes['name']['middle'] = u.get('middle').strip()
                logger.debug(f"  Изменение отчества: {existing_user['name'].get('middle')} -> {u.get('middle')}")

            if not changes['name']:
                del changes['name']
            else:
                if 'first' not in changes['name']:
                    changes['name']['first'] = existing_user['name'].get('first')
                if 'last' not in changes['name']:
                    changes['name']['last'] = existing_user['name'].get('last')
                if 'middle' not in changes['name']:
                    changes['name']['middle'] = existing_user['name'].get('middle')
            
            # Проверяем изменение пароля
            if u.get('update_password') and u.get('update_password') == 'true':
                if u.get('password'):
                    changes['password'] = u.get('password')
                    password_changed = True
                    logger.debug("  Изменение пароля")
                
                if u.get('password_change_required'):
                    changes['passwordChangeRequired'] = u.get('password_change_required')
                    logger.debug(f"  Установка passwordChangeRequired: {u.get('password_change_required')}")
            
            # Должность
            if u.get('position') and existing_user.get('position').lower().strip() != u.get('position').lower().strip():
                changes['position'] = u.get('position').strip()
                logger.debug(f"  Изменение должности: {existing_user.get('position')} -> {u.get('position')}")
            
            # Язык
            if u.get('language') and existing_user.get('language').lower().strip() != u.get('language').lower().strip():
                if not existing_user.get('isEnabled'):
                    changes_for_disabled_users['language'] = u.get('language').strip()
                    logger.debug(f"  Язык нельзя изменить, пользователь {u.get('login')} заблокирован в 360. Откладываем процесс изменений.")
                else:
                    changes['language'] = u.get('language').strip()
                    logger.debug(f"  Изменение языка: {existing_user.get('language')} -> {u.get('language')}")
            
            # Пол
            if u.get('gender') and existing_user.get('gender').lower().strip() != u.get('gender').lower().strip():
                changes['gender'] = u.get('gender').strip()
                logger.debug(f"  Изменение пола: {existing_user.get('gender')} -> {u.get('gender')}")
            
            # Дата рождения
            if u.get('birthday') and existing_user.get('birthday') != u.get('birthday'):
                changes['birthday'] = u.get('birthday').strip()
                logger.debug(f"  Изменение даты рождения: {existing_user.get('birthday')} -> {u.get('birthday')}")

            if u.get('is_enabled') and u.get('is_enabled').strip():
                if str(existing_user.get('isEnabled')).lower() != u.get('is_enabled').lower():
                    changes['isEnabled'] = u.get('is_enabled').strip().lower()
                    logger.debug(f"  Изменение статуса блокировки пользователя: {existing_user.get('is_enabled')} -> {u.get('is_enabled')}")

            if u.get('is_admin') and u.get('is_admin').strip():
                if str(existing_user.get('isAdmin')).lower() != u.get('is_admin').lower():
                    changes['isAdmin'] = u.get('is_admin').strip().lower()
                    logger.debug(f"  Изменение статуса администратора: {existing_user.get('is_admin')} -> {u.get('is_admin')}")
            
            # Обработка контактов (телефоны)
            new_contacts = []
            update_contacts = False
            found_mobile = False
            found_work = False
            # Копируем существующие контакты, кроме телефонов (которые будем обновлять)
            for contact in existing_user.get('contacts', []):
                if not contact['synthetic']:
                    if not contact['alias']:
                        if contact['type'] == 'phone':
                            label = contact.get('label', '').lower()
                            if label == 'mobile':
                                if u.get('mobile_phone') and u.get('mobile_phone').strip():
                                    if contact['value'] != u.get('mobile_phone'):
                                        if u.get('mobile_phone').strip():
                                            contact['value'] = u.get('mobile_phone').strip()
                                            new_contacts.append(contact)
                                            update_contacts = True
                                            found_mobile = True
                                    else:
                                        new_contacts.append(contact)
                                        found_mobile = True
                                elif u.get('mobile_phone') and not u.get('mobile_phone').strip():
                                    update_contacts = True
                                    found_mobile = True
                            elif label == 'work':
                                if u.get('work_phone') and u.get('work_phone').strip():
                                    if contact['value'] != u.get('work_phone'):
                                        if u.get('work_phone').strip():
                                            contact['value'] = u.get('work_phone').strip()
                                            new_contacts.append(contact)
                                            update_contacts = True
                                            found_work = True
                                    else:
                                        new_contacts.append(contact)
                                        found_work = True
                                elif u.get('work_phone') and not u.get('work_phone').strip():
                                    update_contacts = True
                                    found_work = True
                        else:   
                            new_contacts.append(contact)
            
            if not found_mobile:
                if u.get('mobile_phone').strip():
                    new_contacts.append({
                        'value': u.get('mobile_phone').strip(),
                        'label': 'mobile',
                        'type': 'phone',
                    })
                    update_contacts = True
                    found_mobile = True
            if not found_work:
                if u.get('work_phone').strip():
                    new_contacts.append({
                        'value': u.get('work_phone').strip(),
                        'label': 'work',
                        'type': 'phone',
                    })
                    update_contacts = True
                    found_work = True
            if new_contacts and update_contacts:
                changes['contacts'] = new_contacts
                logger.debug("Обновление контактов")
 
            
            # Обновление personal_email в поле about
            if u.get('personal_email'):
                # Безопасно извлекаем about в словарь
                about_dict = {}
                existing_about = existing_user.get('about', '')
                if existing_about:
                    try:
                        about_dict = json.loads(existing_about)
                        if not isinstance(about_dict, dict):
                            about_dict = {}
                    except Exception:
                        about_dict = {}
                # Обновляем поле personal_email
                if u.get('personal_email').strip():
                    about_dict['personal_email'] = u.get('personal_email').strip()
                else:
                    del about_dict['personal_email']
                if about_dict:
                    new_about = json.dumps(about_dict, ensure_ascii=False)
                else:
                    new_about = ""
                if existing_user.get('about') != new_about:
                    changes['about'] = new_about
                    logger.debug("  Обновление about (personal_email)")
            
            # Подразделение
            
            if u.get('department') and u.get('department').strip():
                found_deps = False
                dep_id = None
                if u['department'].isdigit():
                    dep_id = int(u['department'])
                    found_deps = True
                elif u.get('department').strip() == "Все пользователи":
                    dep_id = 1
                    found_deps = True
                else:
                    # Ищем подразделение по пути
                    strip_list = [x.strip() for x in u['department'].split(DEPS_SEPARATOR)]
                    user_dep = DEPS_SEPARATOR.join(strip_list)
                    for dep in api_deps_hierarchy:
                        if dep['path'] == user_dep:
                            dep_id = dep['id']
                            found_deps = True
                            break
                if not found_deps:
                    logger.error(f"Подразделение {u['department']} для пользователя {u.get('login')} (UID - {user_id}) не найдено в иерархии подразделений. Запрос на создание нового подразделения.")
                
                if dep_id and existing_user.get('departmentId') != dep_id:
                    changes['departmentId'] = dep_id
                    logger.debug(f"  Изменение подразделения: {existing_user.get('departmentId')} -> {dep_id}")
                    changes['departmentId'] = dep_id
                elif not dep_id:
                    users_with_new_deps.append(u)

            elif u.get('department') and not u.get('department').strip():
                changes['departmentId'] = 1

            
            # Если есть изменения - применяем их
            if changes or u['raw_aliases'] or changes_for_disabled_users or users_with_new_deps:
                if settings.dry_run:
                    logger.info(f"Пробный запуск. Пользователь {u.get('login')} не будет обновлен. Изменения: {mask_sensitive_data(changes)}")
                else:
                    if changes:
                        result = patch_user_by_api(settings, user_id=user_id, patch_data=changes)
                        if result:
                            logger.info(f"Успех - пользователь {u.get('login')} обновлен.")
                            updated_users.append(u)
                        else:
                            logger.error(f"Ошибка при обновлении пользователя {u.get('login')}")
                    if u['raw_aliases'] and u['raw_aliases'].strip():
                        # Обработка алиасов
                        new_aliases = [a.lower() for a in u.get('aliases', [])]
                        old_aliases = [a.lower() for a in existing_user.get('aliases', [])]
                        add_aliases = []
                        remove_aliases = []
                        for alias in new_aliases:
                            if alias and alias.split("@")[0].lower().strip() not in old_aliases:
                                add_aliases.append(alias.split("@")[0].lower().strip())
                        for alias in old_aliases:
                            if alias and alias.split("@")[0].lower().strip() not in new_aliases:
                                remove_aliases.append(alias.split("@")[0].lower().strip())
                        if add_aliases:
                            for alias in add_aliases:
                                create_user_alias_by_api(settings, existing_user["id"], alias)
                        if remove_aliases:
                            for alias in remove_aliases:
                                delete_user_alias_by_api(settings, existing_user["id"], alias)
                    elif u['raw_aliases'] and not u['raw_aliases'].strip():
                        old_aliases = existing_user.get('aliases', [])
                        for alias in old_aliases:
                            delete_user_alias_by_api(settings, existing_user["id"], alias)

                    if changes_for_disabled_users:
                        logger.info(f"Изменение данных пользователя, требующих разблокировки: {changes_for_disabled_users}")
                        logger.info(f"Разблокировка пользователя {u.get('login')} (UID - {user_id})")
                        result = patch_user_by_api(settings, user_id=user_id, patch_data={"isEnabled":"true"})
                        if result:
                            logger.info(f"Установка новых параметров для пользователя {u.get('login')} (UID - {user_id})")
                            result = patch_user_by_api(settings, user_id=user_id, patch_data=changes_for_disabled_users)
                            if not result:
                                logger.error(f"Ошибка при обновлении пользователя {u.get('login')} (UID - {user_id})")
                            logger.info(f"Блокировка пользователя {u.get('login')} (UID - {user_id})")
                            result = patch_user_by_api(settings, user_id=user_id, patch_data={"isEnabled":"false"})
                            if not result:
                                logger.error(f"!!! Пользователь {u.get('login')} (UID - {user_id}) не был снова заблокирован.")
                            updated_users.append(u)

                    # Если пароль был изменен - отправляем письмо
                    if password_changed:
                        # Извлекаем personal_email из about
                        personal_email = u.get('personal_email', '')
                        if not personal_email:
                            # Пытаемся извлечь из existing_user
                            try:
                                about_data = json.loads(existing_user.get('about', '{}'))
                                personal_email = about_data.get('personal_email', '')
                            except Exception:
                                pass
                        
                        if personal_email.strip():
                            email_data = {
                                'first': u.get('first') or existing_user['name'].get('first'),
                                'middle': u.get('middle') or existing_user['name'].get('middle'),
                                'last': u.get('last') or existing_user['name'].get('last'),
                                'login': u.get('login'),
                                'password': u.get('password'),
                                'password_change_required': u.get('password_change_required', 'false'),
                                'personal_email': personal_email
                            }
                            send_password_change_email(settings, email_data)
                        else:
                            logger.warning(f"Не найден personal_email для отправки письма пользователю {u.get('login')}")
            else:
                if not users_with_new_deps:
                    pass
                    #logger.info(f"Нет изменений для пользователя {u.get('login')}")
                else:
                    logger.info(f"Пользователь {u.get('login')} имеет новые подразделения. Запрос на создание новых подразделений отложен до завершения обновления всех пользователей.")
        
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            continue

    if users_with_new_deps:
        logger.info(f"Есть запрос на добавление новых подразделений для {len(users_with_new_deps)} пользователей. Выполняем.")
        add_users_from_file_phase_3(settings, users_with_new_deps)
    
    logger.info("-" * 100)
    logger.info(f'Обновление пользователей завершено. Обновлено: {len(updated_users)}')
    logger.info("-" * 100)
    return True, updated_users

def update_users_from_file(settings: "SettingParams"):
    """
    Основная функция для обновления пользователей из файла
    """
    # Сканирование каталога на наличие файлов с шаблоном short_file_name_prefix_<timestamp>.csv
    search_dir = settings.short_file_dir
    search_pattern = f"{settings.short_file_name_prefix}_*.csv"
    search_path = os.path.join(search_dir, search_pattern)
    
    logger.info(f"Сканирование каталога '{search_dir}' для поиска файлов с шаблоном '{search_pattern}'...")
    
    matching_files = glob.glob(search_path)
    
    default_file = "users.csv"  # Значение по умолчанию
    
    if matching_files:
        # Сортируем файлы по времени изменения (самый новый последний)
        matching_files.sort(key=os.path.getmtime, reverse=True)
        latest_file = matching_files[0]
        default_file = latest_file
        logger.info(f"Найдено {len(matching_files)} файл(ов) с указанным шаблоном.")
        logger.info(f"Самый последний файл: {latest_file}")
    else:
        logger.info(f"Файлы с шаблоном '{search_pattern}' не найдены в каталоге '{search_dir}'.")
        logger.info(f"Будет использоваться значение по умолчанию: {default_file}")
    
    # Запрос имени файла у пользователя
    print("\n" + "=" * 100)
    print("ОБНОВЛЕНИЕ ПОЛЬЗОВАТЕЛЕЙ ИЗ ФАЙЛА")
    print("=" * 100)
    user_input = input(f"\nВведите путь к файлу для загрузки данных (по умолчанию '{default_file}'): ").strip()
    
    if not user_input:
        user_input = default_file
    
    # Сохраняем оригинальное значение и временно заменяем его
    original_users_file = settings.users_file
    settings.users_file = user_input
    
    try:
        result, data = update_users_from_file_phase_1(settings)
        if not result:
            return False, []
        
        result, data = update_users_from_file_phase_2(settings, data)
        return result, data
    finally:
        # Восстанавливаем оригинальное значение
        settings.users_file = original_users_file

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
                return False, f'Дата отстоит от текущей менее, чем на {min_years_diff} лет'
            if years_diff > max_years_diff:
                return False, f'Дата отстоит от текущей более, чем на {max_years_diff} лет'
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

def generate_temp_password(length: int = 12) -> str:
    """
    Генерирует временный пароль средней сложности.
    
    Пароль содержит:
    - Минимум одну заглавную букву
    - Минимум одну строчную букву
    - Минимум одну цифру
    - Минимум один специальный символ
    - Общая длина не менее 12 символов (по умолчанию)
    
    Args:
        length (int): Длина пароля (минимум 12 символов)
        
    Returns:
        str: Сгенерированный пароль
        
    Example:
        generate_temp_password(12) -> 'Ab3$xY9mNp2!'
    """
    if length < 12:
        length = 12
    
    # Определяем наборы символов
    uppercase_letters = string.ascii_uppercase
    lowercase_letters = string.ascii_lowercase
    digits = string.digits
    special_chars = '!@#$%^&*()_+-=[]{}|'
    
    # Гарантируем наличие минимум одного символа каждого типа
    password_chars = [
        secrets.choice(uppercase_letters),
        secrets.choice(lowercase_letters),
        secrets.choice(digits),
        secrets.choice(special_chars)
    ]
    
    # Заполняем остаток пароля случайными символами из всех категорий
    all_chars = uppercase_letters + lowercase_letters + digits + special_chars
    password_chars.extend(secrets.choice(all_chars) for _ in range(length - 4))
    
    # Перемешиваем символы для случайного порядка
    # Используем secrets.SystemRandom для криптографически стойкого перемешивания
    rng = secrets.SystemRandom()
    rng.shuffle(password_chars)
    
    # Преобразуем список в строку
    password = ''.join(password_chars)
    
    return password

def validate_password(settings: "SettingParams", password: str) -> Tuple[bool, str]:
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
            return False, f'Пароль не соответствует требованиям безопасности. Используемый шаблон: {pattern}'
    except re.error as e:
        return False, f'Ошибка в регулярном выражении: {e}'

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

def load_email_template(template_file: str) -> str:
    """
    Загружает HTML-шаблон email из файла.
    
    Args:
        template_file (str): Путь к файлу шаблона
        
    Returns:
        str: Содержимое шаблона или None в случае ошибки
    """
    template_path = template_file
    if not os.path.exists(template_path):
        template_path = os.path.join(os.path.dirname(__file__), template_file)
        if not os.path.exists(template_path):
            logger.error(f'Файл шаблона email {template_file} не найден!')
            return None
    
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Ошибка при чтении файла шаблона email: {type(e).__name__}: {e}")
        return None

def render_email_template(template: str, user_data: dict, settings: "SettingParams") -> str:
    """
    Заполняет шаблон email данными пользователя.
    Упрощенная версия шаблонизатора для подстановки значений.
    
    Args:
        template (str): HTML-шаблон
        user_data (dict): Данные пользователя
        settings (SettingParams): Настройки приложения
        
    Returns:
        str: HTML с подставленными данными
    """
    # Базовые подстановки
    rendered = template.replace('{{first_name}}', user_data.get('first', ''))
    rendered = rendered.replace('{{middle_name}}', user_data.get('middle', ''))
    rendered = rendered.replace('{{last_name}}', user_data.get('last', ''))
    rendered = rendered.replace('{{login}}', user_data.get('login', ''))
    rendered = rendered.replace('{{password}}', user_data.get('password', ''))
    rendered = rendered.replace('{{position}}', user_data.get('position', ''))
    rendered = rendered.replace('{{department}}', user_data.get('department_name', ''))
    rendered = rendered.replace('{{domain}}', settings.email_domain if hasattr(settings, 'email_domain') else '')
    rendered = rendered.replace('{{year}}', str(datetime.now().year))
    
    # Условные блоки для password_change_required
    password_change_required = user_data.get('password_change_required', 'false').lower() == 'true'
    
    # Простая обработка условных блоков {{#if password_change_required}}...{{/if}}
    if password_change_required:
        # Удаляем альтернативный блок {{else}}...{{/if}} вместе с тегами
        rendered = re.sub(r'\{\{else\}\}.*?\{\{/if\}\}', '', rendered, flags=re.DOTALL)
        # Теперь удаляем открывающий тег {{#if password_change_required}}, оставляя содержимое
        rendered = re.sub(r'\{\{#if password_change_required\}\}', '', rendered)
    else:
        # Удаляем блок между {{#if password_change_required}} и {{else}} или {{/if}}
        rendered = re.sub(r'\{\{#if password_change_required\}\}.*?\{\{else\}\}', '', rendered, flags=re.DOTALL)
        rendered = re.sub(r'\{\{#if password_change_required\}\}.*?\{\{/if\}\}', '', rendered, flags=re.DOTALL)
    
    # Обработка условных блоков для department и position
    if user_data.get('department_name'):
        rendered = re.sub(r'\{\{#if department\}\}', '', rendered)
    else:
        rendered = re.sub(r'\{\{#if department\}\}.*?\{\{/if\}\}', '', rendered, flags=re.DOTALL)
    
    if user_data.get('position'):
        rendered = re.sub(r'\{\{#if position\}\}', '', rendered)
    else:
        rendered = re.sub(r'\{\{#if position\}\}.*?\{\{/if\}\}', '', rendered, flags=re.DOTALL)
    
    # Удаляем оставшиеся теги
    rendered = re.sub(r'\{\{/if\}\}', '', rendered)
    
    return rendered

def send_email(settings: "SettingParams", to_email: str, subject: str, html_body: str) -> bool:
    """
    Отправляет email сообщение по SMTP с SSL.
    
    Args:
        settings (SettingParams): Настройки с параметрами SMTP
        to_email (str): Email получателя
        subject (str): Тема письма
        html_body (str): HTML-содержимое письма
        
    Returns:
        bool: True если отправка успешна, False в противном случае
    """
    if not all([settings.smtp_server, settings.smtp_port, settings.smtp_login, settings.smtp_password]):
        logger.error("Не заданы параметры SMTP сервера в файле .env")
        return False
    
    try:
        # Создаем сообщение
        msg = MIMEMultipart('alternative')
        msg['From'] = settings.smtp_from_email if hasattr(settings, 'smtp_from_email') else settings.smtp_login
        msg['To'] = to_email
        msg['Subject'] = Header(subject, 'utf-8')
        
        # Добавляем HTML часть
        html_part = MIMEText(html_body, 'html', 'utf-8')
        msg.attach(html_part)
        
        # Подключаемся к SMTP серверу через SSL
        logger.debug(f"Подключение к SMTP серверу {settings.smtp_server}:{settings.smtp_port}")
        with smtplib.SMTP_SSL(settings.smtp_server, settings.smtp_port, timeout=SMTP_TIMEOUT) as server:
            # Аутентификация
            logger.debug(f"Аутентификация как {settings.smtp_login}")
            server.login(settings.smtp_login, settings.smtp_password)
            
            # Отправка письма
            logger.debug(f"Отправка письма на {to_email}")
            server.send_message(msg)
            
        logger.info(f"Email успешно отправлен на адрес {to_email}")
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"Ошибка аутентификации SMTP: {e}")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"Ошибка SMTP при отправке email на {to_email}: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке email на {to_email}: {type(e).__name__}: {e}")
        return False

def send_welcome_email(settings: "SettingParams", user_data: dict) -> bool:
    """
    Отправляет приветственное письмо новому пользователю.
    
    Args:
        settings (SettingParams): Настройки приложения
        user_data (dict): Данные пользователя
        
    Returns:
        bool: True если отправка успешна, False в противном случае
    """
    personal_email = user_data.get('personal_email', '').strip()
    
    if not personal_email:
        logger.warning(f"Не указан personal_email для пользователя {user_data.get('login', 'unknown')}. Письмо не будет отправлено.")
        return False
    
    # Загружаем шаблон
    template = load_email_template(EMAIL_TEMPLATE_FILE)
    if not template:
        logger.error("Не удалось загрузить шаблон email. Письмо не будет отправлено.")
        return False
    
    # Заполняем шаблон данными пользователя
    html_body = render_email_template(template, user_data, settings)
    
    # Формируем тему письма
    subject = f"Добро пожаловать в Yandex 360! Ваш логин: {user_data.get('login', '')}"
    
    # Отправляем письмо
    return send_email(settings, personal_email, subject, html_body)

def send_password_change_email(settings: "SettingParams", user_data: dict) -> bool:
    """
    Отправляет письмо об изменении пароля пользователю.
    
    Args:
        settings (SettingParams): Настройки приложения
        user_data (dict): Данные пользователя (должен содержать personal_email в формате JSON в поле about)
        
    Returns:
        bool: True если отправка успешна, False в противном случае
    """
    # Извлекаем personal_email из поля about (JSON)
    personal_email = user_data.get('personal_email', '').strip()
    
    if not personal_email:
        logger.warning(f"Не указан personal_email для пользователя {user_data.get('login', 'unknown')}. Письмо не будет отправлено.")
        return False
    
    # Загружаем шаблон
    template = load_email_template(PASSWORD_CHANGE_TEMPLATE_FILE)
    if not template:
        logger.error("Не удалось загрузить шаблон email для изменения пароля. Письмо не будет отправлено.")
        return False
    
    # Заполняем шаблон данными пользователя
    html_body = render_email_template(template, user_data, settings)
    
    # Формируем тему письма
    subject = "Изменение пароля в Yandex 360"
    
    # Отправляем письмо
    return send_email(settings, personal_email, subject, html_body)

def find_user_by_login(settings: "SettingParams", login: str, search_in_aliases: bool = True) -> Tuple[bool, dict]:
    """
    Находит пользователя по логину (nickname или alias).
    
    Args:
        settings (SettingParams): Настройки приложения
        login (str): Логин для поиска
        
    Returns:
        Tuple[bool, dict]: (найден ли пользователь, объект пользователя или None)
    """
    login = login.lower().strip()
    if '@' in login:
        login = login.split('@')[0]
    
    users = get_all_api360_users(settings, force=False)
    
    for user in users:
        # Проверяем nickname
        if user['nickname'].lower() == login:
            logger.debug(f"Пользователь найден по nickname: {user['nickname']} (ID: {user['id']})")
            return True, user
        
        # Проверяем aliases
    if search_in_aliases:
        aliases_lower = [a.lower() for a in user.get('aliases', [])]
        if login in aliases_lower:
            logger.debug(f"Пользователь найден по alias: {user['nickname']} (ID: {user['id']})")
            return True, user
    
    logger.warning(f"Пользователь с логином '{login}' не найден")
    return False, None

def remove_quotes_if_wrapped(text: str) -> str:
    """
    Удаляет кавычки из строки только если они находятся в начале и конце строки,
    и их ровно две (одна в начале, одна в конце).
    
    Args:
        text (str): Строка для обработки
        
    Returns:
        str: Строка без кавычек или исходная строка
        
    Examples:
        remove_quotes_if_wrapped('"это нормальная строка"') -> 'это нормальная строка'
        remove_quotes_if_wrapped('это неправильная "строка"') -> 'это неправильная "строка"'
        remove_quotes_if_wrapped('""много"" кавычек') -> '""много"" кавычек'
    """
    if not text:
        return text
    
    # Подсчитываем количество кавычек в строке
    quote_count = text.count('"')
    
    # Если кавычек не ровно 2, возвращаем исходную строку
    if quote_count != 2:
        return text
    
    # Проверяем, что кавычки находятся в начале и конце строки
    if text.startswith('"') and text.endswith('"') and len(text) >= 2:
        return text[1:-1]
    
    return text

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

def validate_email(email: str) -> Tuple[bool, str]:
    """
    Проверяет корректность адреса электронной почты.
    
    Args:
        email (str): Email адрес для проверки
        
    Returns:
        Tuple: (bool, str) - (результат проверки, сообщение об ошибке или "OK")
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
        return False, f'Ошибка в регулярном выражении: {e}'

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
        Tuple: (bool, str) - (результат проверки, причина ошибки или очищенный номер)
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
        formatted_number = f'+7 ({clean_number[1:4]}) {clean_number[4:7]}-{clean_number[7:9]}-{clean_number[9:11]}'
        if extension:
            formatted_number += f' доб. {extension}'
    # Для других номеров просто добавляем добавочный номер
    elif extension:
        formatted_number += f' доб. {extension}'
    
    return True, formatted_number

def validate_alias(settings: "SettingParams", alias: str) -> Tuple[bool, str]:
    """
    Проверяет корректность алиаса пользователя.
    
    Алиас должен:
    - Не быть пустым
    - Содержать только допустимые символы (буквы, цифры, точки, дефисы, подчеркивания)
    - Начинаться и заканчиваться буквой или цифрой
    - Иметь длину от 2 до 50 символов
    
    Args:
        alias (str): Алиас для проверки
        
    Returns:
        Tuple: (bool, str) - (результат проверки, причина ошибки или очищенный алиас)
    """
    if not alias:
        return False, "Алиас не может быть пустым"
    
    # Убираем лишние пробелы
    alias = alias.strip()
    
    if not alias:
        return False, "Алиас не может состоять только из пробелов"
    
    # Проверка длины
    if len(alias) < 2:
        return False, "Алиас должен содержать минимум 2 символа"
    
    if len(alias) > 50:
        return False, "Алиас не может содержать более 50 символов"
    
    # Проверка на допустимые символы (буквы, цифры, точки, дефисы, подчеркивания)
    if not re.match(r'^[a-zA-Z0-9._-]+$', alias):
        return False, "Алиас может содержать только буквы, цифры, точки, дефисы и подчеркивания"
    
    # Проверка на начало и конец (должны быть буквой или цифрой)
    if not re.match(r'^[a-zA-Z0-9]', alias):
        return False, "Алиас должен начинаться с буквы или цифры"
    
    if not re.match(r'[a-zA-Z0-9]$', alias):
        return False, "Алиас должен заканчиваться буквой или цифрой"
    
    # Проверка на последовательные точки
    if '..' in alias:
        return False, "Алиас не может содержать последовательные точки"
    
    # Проверка на последовательные дефисы
    if '--' in alias:
        return False, "Алиас не может содержать последовательные дефисы"
    
    return True, alias

def validate_shared_mailbox_email(settings: "SettingParams", email: str) -> Tuple[bool, str]:
    """
    Проверяет корректность email адреса для общего ящика.
    Допустимые форматы: alias или alias@domain.com
    
    Args:
        settings: Параметры настроек
        email: Email адрес для проверки
        
    Returns:
        Tuple: (bool, str) - (результат проверки, сообщение об ошибке или "OK")
    """
    if not email:
        return False, "Email адрес не может быть пустым"
    
    email = email.strip()
    if not email:
        return False, "Email адрес не может быть пустым"
    
    # Проверка на недопустимые символы
    if email.count('@') > 1:
        return False, "Email адрес не может содержать более одного символа @"
    
    # Если есть @, проверяем полный формат alias@domain.com
    if '@' in email:
        local_part, domain = email.split('@', 1)
        
        if not local_part:
            return False, "Локальная часть email адреса не может быть пустой"
        
        if not domain:
            return False, "Домен email адреса не может быть пустым"
        
        # Проверка локальной части (alias)
        if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$', local_part):
            return False, "Локальная часть должна начинаться и заканчиваться буквой или цифрой, может содержать точки, дефисы и подчеркивания"
        
        # Проверка домена
        if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?(\.[a-zA-Z]{2,})+$', domain):
            return False, "Домен имеет некорректный формат"
    else:
        # Если нет @, проверяем только alias
        if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$', email):
            return False, "Алиас должен начинаться и заканчиваться буквой или цифрой, может содержать точки, дефисы и подчеркивания"
    
    return True, "OK"


def read_shared_mailboxes_file(settings: "SettingParams", file_path: str):
    """
    Читает файл shared.csv с общими ящиками и проводит валидацию.
    
    Формат файла:
    - Разделитель: точка с запятой (;)
    - Колонки: email, name, description
    - Строки начинающиеся с # пропускаются
    
    Args:
        settings: Параметры настроек
        file_path: Путь к файлу shared.csv
        
    Returns:
        Tuple: (bool, list, list) - (успех, список ящиков, список ошибок)
    """
    if not os.path.exists(file_path):
        full_path = os.path.join(os.path.dirname(__file__), file_path)
        if not os.path.exists(full_path):
            logger.error(f'Ошибка! Файл {file_path} не существует!')
            return False, [], [(0, f'Файл {file_path} не найден')]
        else:
            file_path = full_path
    
    mailboxes = []
    errors = []
    emails_seen = {}
    line_number = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            
            # Читаем заголовок
            try:
                headers = next(reader)
                line_number += 1
                
                # Очищаем заголовки от кавычек и пробелов
                headers = [h.strip().replace('"', '') for h in headers]
                
                # Проверяем наличие необходимых колонок
                required_headers = ['email', 'name', 'description']
                if headers != required_headers:
                    errors.append((line_number, f'Неверные заголовки. Ожидаются: {";".join(required_headers)}, получены: {";".join(headers)}'))
                    return False, [], errors
                    
            except StopIteration:
                errors.append((0, 'Файл пустой'))
                return False, [], errors
            
            # Читаем данные
            for row in reader:
                line_number += 1
                
                # Пропускаем пустые строки
                if not row or len(row) == 0:
                    continue
                
                # Пропускаем строки начинающиеся с #
                if row[0].strip().startswith('#'):
                    logger.debug(f'Строка {line_number}: пропущена (комментарий)')
                    continue
                
                # Проверяем количество колонок
                if len(row) != 3:
                    errors.append((line_number, f'Неверное количество колонок (ожидается 3, получено {len(row)})'))
                    continue
                
                email = row[0].strip()
                name = row[1].strip()
                description = row[2].strip()
                
                # Проверяем email
                is_valid, error_msg = validate_shared_mailbox_email(settings, email)
                if not is_valid:
                    errors.append((line_number, f'Некорректный email "{email}": {error_msg}'))
                    continue
                
                # Проверяем на дубликаты
                if email.lower() in emails_seen:
                    errors.append((line_number, f'Дублирующийся email "{email}" (уже встречался в строке {emails_seen[email.lower()]})'))
                    continue
                
                # Проверяем обязательные поля
                if not name:
                    errors.append((line_number, f'Поле "name" не может быть пустым для email "{email}"'))
                    continue
                
                emails_seen[email.lower()] = line_number
                mailboxes.append({
                    'email': email,
                    'name': name,
                    'description': description,
                    'line_number': line_number
                })
                
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno}: {e}")
        errors.append((0, f'Ошибка при чтении файла: {e}'))
        return False, [], errors
    
    if errors:
        return False, mailboxes, errors
    
    return True, mailboxes, []


def create_shared_mailbox_by_api(settings: "SettingParams", mailbox: dict):
    """
    Создает общий ящик через API Yandex 360.
    
    API endpoint: PUT /admin/v1/org/{orgId}/mailboxes/shared
    
    Args:
        settings: Параметры настроек с OAuth токеном и org_id
        mailbox: Словарь с данными ящика (email, name, description)
        
    Returns:
        Tuple: (bool, dict) - (успех, ответ API)
    """
    url = f'{DEFAULT_360_API_URL}/admin/v1/org/{settings.org_id}/mailboxes/shared'
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    
    # Подготовка данных для API
    api_data = {
        "name": mailbox['name']
    }
    
    # Добавляем email
    if '@' in mailbox['email']:
        api_data['email'] = mailbox['email']
    else:
        # Если указан только alias, добавляем домен
        api_data['email'] = f"{mailbox['email']}@{settings.email_domain}" if settings.email_domain else mailbox['email']
    
    # Добавляем описание, если оно есть
    if mailbox.get('description'):
        api_data['description'] = mailbox['description']
    
    logger.debug(f"PUT URL: {url}")
    logger.debug(f"PUT DATA: {api_data}")
    
    retries = 1
    success = False
    response_data = {}
    
    while True:
        try:
            if settings.dry_run:
                logger.info(f"[DRY RUN] Пропущено создание общего ящика '{api_data['email']}' ('{mailbox['name']}')")
                return True, {'email': api_data['email'], 'dry_run': True}
            
            response = requests.put(url, headers=headers, json=api_data)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            
            if response.status_code == HTTPStatus.OK or response.status_code == HTTPStatus.CREATED:
                logger.info(f"Успех - общий ящик '{api_data['email']}' ('{mailbox['name']}') создан успешно.")
                response_data = response.json()
                success = True
                break
            else:
                # Проверяем на специфические ошибки
                try:
                    error_data = response.json()
                    error_message = error_data.get('message', '')
                    
                    # Обработка ошибки "email уже занят"
                    if error_message == 'passport_email_taken':
                        logger.error(f"Ошибка: Email адрес '{api_data['email']}' уже используется в организации (не общим ящиком).")
                        logger.error(f"!!! Общий ящик '{api_data['email']}' не может быть создан - Email адрес занят (не общим ящиком).")
                        response_data = {
                            'error': 'Email адрес уже используется в организации (не общим ящиком)',
                            'error_code': error_data.get('code'),
                            'email': api_data['email'],
                            'status_code': response.status_code,
                        }
                        break  # Прерываем без повторных попыток
                    elif error_message == 'Unauthorized':
                        logger.error("Ошибка: Неверный токен.")
                        logger.error(f"!!! Общий ящик '{api_data['email']}' не может быть создан - Неверный токен.")
                        response_data = {
                            'error': 'Неверный токен',
                            'error_code': error_data.get('code'),
                            'email': api_data['email'],
                            'status_code': response.status_code,
                            'forceStop': True
                        }
                        break  # Прерываем без повторных попыток
                    elif error_message == 'No required scope':
                        logger.error("Ошибка: Не хватает прав для создания общего ящика.")
                        logger.error("Добавьте права:")
                        logger.error(" - ya360_admin:mail_write_shared_mailbox_inventory")
                        logger.error(" - ya360_admin:mail_write_shared_mailbox_inventory")
                        logger.error("в консоли управления доступом к API 360 (oauth.yandex.ru).")
                        logger.error(f"!!! Общий ящик '{api_data['email']}' не может быть создан - Не хватает прав для создания общего ящика.")
                        response_data = {
                            'error': 'Не хватает прав для создания общего ящика',
                            'error_code': error_data.get('code'),
                            'email': api_data['email'],
                            'status_code': response.status_code,
                            'forceStop': True
                        }
                        break  # Прерываем без повторных попыток
                    elif error_message == 'resource_already_exists':
                        logger.error(f"Ошибка: Общий ящик '{api_data['email']}' уже существует в организации.")
                        logger.error(f"!!! Общий ящик '{api_data['email']}' не может быть создан - Общий ящик уже существует в организации.")
                        response_data = {
                            'error': 'Общий ящик уже существует в организации',
                            'error_code': error_data.get('code'),
                            'email': api_data['email'],
                            'status_code': response.status_code,
                        }
                        break  # Прерываем без повторных попыток
                    elif error_message == 'invalid_data':
                        logger.error("Ошибка: Неверные данные в запросе Проверьте правильность заполнения полей email и name.")
                        logger.error("В поле email должен быть указан email адрес в ОСНОВНОМ (ПО УМОЛЧАНИЮ) ДОМЕНЕ организации.")
                        logger.error("Eсли в поле email указан только alias, то в параметре EMAIL_DOMAIN должен быть указан")
                        logger.error(f"основной (по умолчанию) домен организации. Сейчас указан: '{settings.email_domain}'")
                        logger.error(f"!!! Общий ящик '{api_data['email']}' не может быть создан - неверные данные в запросе.")
                        response_data = {
                            'error': 'Неверные данные в запросе',
                            'error_code': error_data.get('code'),
                            'email': api_data['email'],
                            'status_code': response.status_code,
                        }
                        break  # Прерываем без повторных попыток
                except (json.JSONDecodeError, ValueError):
                    # Если не удалось распарсить JSON, продолжаем стандартную обработку
                    pass
                
                logger.error(f"Ошибка при создании общего ящика: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"!!! Ошибка. Создание общего ящика '{api_data['email']}' не удалось.")
                    response_data = {'error': response.text, 'status_code': response.status_code}
                    break
                    
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            response_data = {'error': str(e)}
            break
    
    time.sleep(SLEEP_TIME_BETWEEN_API_CALLS)
    return success, response_data


def import_shared_mailboxes_from_file(settings: "SettingParams", file_path: str = None):
    """
    Импортирует общие ящики из CSV файла.
    
    Args:
        settings: Параметры настроек
        file_path: Путь к файлу (по умолчанию берется из settings.shared_mailboxes_file)
        
    Returns:
        bool: Успешность импорта
    """
    if file_path is None:
        file_path = settings.shared_mailboxes_file
    logger.info("-" * 100)
    logger.info(f'Импорт общих ящиков из файла {file_path}')
    logger.info("-" * 100)
    
    # Фаза 1: Чтение и валидация файла
    logger.info("Фаза 1: Чтение и валидация файла")
    success, mailboxes, errors = read_shared_mailboxes_file(settings, file_path)
    
    if errors:
        logger.error("-" * 100)
        logger.error("Обнаружены ошибки в файле:")
        logger.error("-" * 100)
        for line_num, error_msg in errors:
            if line_num == 0:
                logger.error(f"  {error_msg}")
            else:
                logger.error(f"  Строка {line_num}: {error_msg}")
        logger.error("-" * 100)
        logger.error("Импорт прерван. Исправьте ошибки и попробуйте снова.")
        logger.error("-" * 100)
        return False
    
    if not mailboxes:
        logger.warning("В файле нет данных для импорта (все строки пропущены или файл пустой)")
        return False
    
    logger.info(f"Файл успешно прочитан. Найдено {len(mailboxes)} общих ящиков для создания.")
    logger.info("-" * 100)
    
    # Фаза 2: Создание общих ящиков
    logger.info("Фаза 2: Создание общих ящиков")
    logger.info("-" * 100)
    
    created_count = 0
    failed_count = 0
    
    for mailbox in mailboxes:
        logger.info(f"Создание общего ящика {mailbox['email']} (строка {mailbox['line_number']})...")
        success, response = create_shared_mailbox_by_api(settings, mailbox)
        logger.info("\n")
        
        if success:
            created_count += 1
        else:
            failed_count += 1
            if response.get('forceStop', False):
                logger.error("Добавление общих ящиков прервано. Исправьте ошибки и попробуйте снова.")
                logger.error("-" * 100)
                failed_count = len(mailboxes)
                break
    
    # Итоги
    logger.info("-" * 100)
    logger.info("Импорт завершен")
    logger.info("-" * 100)
    logger.info(f"Всего обработано: {len(mailboxes)}")
    logger.info(f"Успешно создано: {created_count}")
    logger.info(f"Ошибок: {failed_count}")
    logger.info("-" * 100)
    
    return failed_count == 0


def import_shared_mailboxes_prompt(settings: "SettingParams"):
    """
    Интерактивная функция для создания общих ящиков.
    """
    print("\n" + "=" * 100)
    print("СОЗДАНИЕ ОБЩИХ ЯЩИКОВ ИЗ ФАЙЛА")
    print("=" * 100)
    print(f"\nФормат файла {settings.shared_mailboxes_file}:")
    print("  - Разделитель: точка с запятой (;)")
    print("  - Заголовок: email;name;description")
    print("  - Строки начинающиеся с # пропускаются")
    print("  - Email может быть в формате 'alias' или 'alias@domain.com'")
    print("\nПример:")
    print("  email;name;description")
    print("  support;Support Team;Общий ящик службы поддержки")
    print("  info@example.com;Information;Информационный ящик")
    print("  # sales;Sales Team;Этот ящик будет пропущен")
    print("=" * 100)
    
    file_path = input(f"\nВведите путь к файлу (по умолчанию '{settings.shared_mailboxes_file}'): ").strip()
    if not file_path:
        file_path = settings.shared_mailboxes_file
    
    if not os.path.exists(file_path):
        full_path = os.path.join(os.path.dirname(__file__), file_path)
        if not os.path.exists(full_path):
            logger.error(f"Файл {file_path} не найден!")
            return
    
    confirm = input(f"\nНачать импорт из файла {file_path}? (да/нет): ").strip().lower()
    if confirm not in ['да', 'yes', 'y', 'д']:
        logger.info("Импорт отменен пользователем.")
        return
    
    import_shared_mailboxes_from_file(settings, file_path)

def get_extended_api360_users(settings: "SettingParams", force = False):
    if not force:
        logger.info("Получение расширенного списка всех пользователей организации из кэша...")

    if not settings.extended_users or force or (datetime.now() - settings.extended_users_get_timestamp).total_seconds() > EXTENDED_USERS_REFRESH_IN_MINUTES * 60:
        #logger.info("Получение всех пользователей организации из API...")
        users = get_all_api360_users(settings, force)
        deps = generate_deps_hierarchy_from_api(settings, force)
        deps.append({'id': 1, 'path': 'Все сотрудники'})
        groups = get_all_api360_groups(settings, force)
        for user in users:
            user['department'] = next((d['path'] for d in deps if d['id'] == user['departmentId']), None)
            user_groups = []
            if user.get('groups'):
                for group_id in user['groups']:
                    found_group = next((g for g in groups if g['id'] == group_id), None)
                    if found_group:
                        user_groups.append(found_group)  
                    else:
                        logger.warning(f"Группа с id {group_id} не найдена в списке групп. Пользователь: {user['name']}")
            user['full_groups'] = user_groups
        settings.extended_users = users
        settings.extended_users_get_timestamp = datetime.now()

    return settings.extended_users

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
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users'
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
    extended_users : list
    extended_users_get_timestamp : datetime
    all_deps : list
    all_deps_get_timestamp : datetime
    all_groups : list
    all_groups_get_timestamp : datetime
    dry_run : bool
    password_pattern : str
    deps_file : str
    all_users_file : str
    shared_mailboxes_file : str
    smtp_server : str
    smtp_port : int
    smtp_login : str
    smtp_password : str
    smtp_from_email : str
    email_domain : str
    send_welcome_email : bool
    auto_generate_password : bool
    generated_password_length : int
    smtp_type : str
    short_file_name_prefix : str
    short_file_dir : str
    search_aliases_file : str
    display_users_fields_file : str

def get_settings():
    exit_flag = False
    oauth_token_bad = False
    settings = SettingParams (
        users_file = os.environ.get("USERS_FILE","users.csv"),
        oauth_token = os.environ.get("OAUTH_TOKEN"),
        org_id = os.environ.get("ORG_ID"),
        all_users = [],
        all_users_get_timestamp = datetime.now(),
        extended_users = [],
        extended_users_get_timestamp = datetime.now(),
        all_deps = [],
        all_deps_get_timestamp = datetime.now(),
        all_groups = [],
        all_groups_get_timestamp = datetime.now(),
        dry_run = os.environ.get("DRY_RUN","false").lower() == "true",
        password_pattern = os.environ.get("PASSWORD_PATTERN"),
        deps_file = os.environ.get("DEPS_FILE","deps.csv"),
        all_users_file = os.environ.get("ALL_USERS_FILE","all_users.csv"),
        shared_mailboxes_file = os.environ.get("SHARED_MAILBOXES_FILE","shared.csv"),
        smtp_server = os.environ.get("SMTP_SERVER", ""),
        smtp_port = int(os.environ.get("SMTP_PORT", "465")),
        smtp_login = os.environ.get("SMTP_LOGIN", ""),
        smtp_password = os.environ.get("SMTP_PASSWORD", ""),
        smtp_from_email = os.environ.get("SMTP_FROM_EMAIL", ""),
        email_domain = os.environ.get("EMAIL_DOMAIN", ""),
        send_welcome_email = os.environ.get("SEND_WELCOME_EMAIL", "false").lower() == "true",
        auto_generate_password = os.environ.get("AUTO_GENERATE_PASSWORD", "false").lower() == "true",
        generated_password_length = int(os.environ.get("GENERATED_PASSWORD_LENGTH", "12")),
        smtp_type = os.environ.get("SMTP_TYPE", "ssl"),
        short_file_name_prefix = os.environ.get("SHORT_FILE_NAME_PREFIX", "users"),
        short_file_dir = os.environ.get("SHORT_FILE_DIR", "."),
        search_aliases_file = os.environ.get("SEARCH_ALIASES_FILE", "search_aliases.txt"),
        display_users_fields_file = os.environ.get("DISPLAY_USERS_IN_CONSOLE_FIELDS", "fields_spec.txt"),
    )

    if not settings.users_file:
        logger.error("USERS_FILE не установлен.")
        exit_flag = True
    
    if not settings.oauth_token:
        logger.error("OAUTH_TOKEN не установлен.")
        oauth_token_bad = True

    if not settings.org_id:
        logger.error("ORG_ID не установлен.")
        exit_flag = True

    if not (oauth_token_bad or exit_flag):
        hard_error, result_ok = check_token_permissions(settings.oauth_token, settings.org_id, NEEDED_PERMISSIONS)
        if hard_error:
            logger.error("OAUTH_TOKEN не является действительным или не имеет необходимых прав доступа")
            oauth_token_bad = True
        if not result_ok:
            print("ВНИМАНИЕ: Функциональность скрипта может быть ограничена. Возможны ошибки при работе с API.")
            print("=" * 100)
            input("Нажмите Enter для продолжения..")


    if not settings.password_pattern:
        logger.error("PASSWORD_PATTERN не установлен. Используется значение по умолчанию.")
        settings.password_pattern = DEFAULT_PASSWORD_PATTERN

    if settings.smtp_port == 465:
        if settings.smtp_type.lower() != "ssl":
            if len(settings.smtp_type) == 0:
                settings.smtp_type = "ssl"
            else:
                logger.warning("SMTP_TYPE для порта 465 обычно должен быть SSL. Могут быть проблемы с отправкой писем.")
    elif settings.smtp_port == 587:
        if settings.smtp_type.lower() != "starttls":
            if len(settings.smtp_type) == 0:
                settings.smtp_type = "starttls"
            else:
                logger.warning("SMTP_TYPE для порта 587 обычно должен быть STARTTLS. Могут быть проблемы с отправкой писем.")

    if oauth_token_bad:
        exit_flag = True
    
    if exit_flag:
        return None
    
    return settings


def check_oauth_token(oauth_token, org_id):
    """Проверяет, что токен OAuth действителен."""
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{org_id}/users?perPage=100'
    headers = {
        'Authorization': f'OAuth {oauth_token}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == HTTPStatus.OK:
        return True
    return False


def check_token_permissions(token: str, org_id: int, needed_permissions: list) -> bool:
    """
    Проверяет права доступа для заданного токена.
    
    Args:
        token: OAuth токен для проверки
        org_id: ID организации
        needed_permissions: Список необходимых прав доступа
        
    Returns:
        bool: True если токен невалидный, False в противном случае, продолжение работы невозможно
        bool: True если все права присутствуют и org_id совпадает, False в противном случае, продолжение работы возможно
    """
    url = 'https://api360.yandex.net/whoami'
    headers = {
        'Authorization': f'OAuth {token}'
    }
    hard_error = False
    try:
        response = requests.get(url, headers=headers)
        
        # Проверка валидности токена
        if response.status_code != HTTPStatus.OK:
            logger.error(f"Невалидный токен. Статус код: {response.status_code}")
            if response.status_code == 401:
                logger.error("Токен недействителен или истек срок его действия.")
            else:
                logger.error(f"Ошибка при проверке токена: {response.text}")
            return True, False
        
        data = response.json()
        
        # Извлечение scopes и orgIds из ответа
        token_scopes = data.get('scopes', [])
        token_org_ids = data.get('orgIds', [])
        login = data.get('login', 'unknown')
        
        logger.info(f"Проверка прав доступа для токена пользователя: {login}")
        logger.debug(f"Доступные права: {token_scopes}")
        logger.debug(f"Доступные организации: {token_org_ids}")
        
        # Проверка наличия org_id в списке доступных организаций
        if str(org_id) not in [str(org) for org in token_org_ids]:
            logger.error("=" * 100)
            logger.error(f"ОШИБКА: Токен не имеет доступа к организации с ID {org_id}")
            logger.error(f"Доступные организации для этого токена: {token_org_ids}")
            logger.error("=" * 100)
            return True, False

        # Проверка наличия всех необходимых прав
        missing_permissions = []
        for permission in needed_permissions:
            if permission not in token_scopes:
                missing_permissions.append(permission)
        
        if missing_permissions:
            logger.error("=" * 100)
            logger.error("ОШИБКА: У токена отсутствуют необходимые права доступа!")
            logger.error("Недостающие права:")
            for perm in missing_permissions:
                logger.error(f"  - {perm}")
            logger.error("=" * 100)
            return False, False

        logger.info("✓ Все необходимые права доступа присутствуют")
        logger.info(f"✓ Доступ к организации {org_id} подтвержден")
        return False, True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return True, False
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка при парсинге ответа от API: {e}")
        return True, False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при проверке прав доступа: {type(e).__name__}: {e}")
        return True, False


def create_user_by_api(settings: "SettingParams", user: dict):

    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users'
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
                added_user = response.json()
                logger.info(f"Успех - пользователь {user['nickname']} ({user['name']['last']} {user['name']['first']}) создан успешно. UID = {added_user.get('uid')}")
                success = True
                break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    return success, added_user

def get_user_by_api(settings: "SettingParams", user_id: str):
    """
    Получает информацию об одном сотруднике по его идентификатору.
    
    Args:
        settings: Параметры настроек с OAuth токеном и org_id
        user_id: Идентификатор сотрудника (string<uint64>)
    
    Returns:
        Tuple[bool, dict]: (success, user_data)
        - success: True если запрос успешен, False в противном случае
        - user_data: Словарь с данными пользователя или пустой словарь при ошибке
    """
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users/{user_id}'
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    logger.debug(f"GET URL: {url}")
    retries = 1
    user_data = {}
    success = False
    
    while True:
        try:
            response = requests.get(url, headers=headers)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            
            if response.status_code == HTTPStatus.OK.value:
                user_data = response.json()
                logger.debug(f"Успешно получены данные пользователя с ID {user_id}")
                success = True
                break
            elif response.status_code == HTTPStatus.NOT_FOUND.value:
                logger.error(f"Пользователь с ID {user_id} не найден (404)")
                break
            elif response.status_code == HTTPStatus.UNAUTHORIZED.value:
                logger.error(f"Ошибка авторизации (401): {response.text}")
                break
            elif response.status_code == HTTPStatus.FORBIDDEN.value:
                logger.error(f"Нет прав доступа (403): {response.text}")
                break
            else:
                logger.error(f"Ошибка при GET запросе: {response.status_code}. Сообщение об ошибке: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Ошибка. Получение данных пользователя с ID {user_id} не удалось.")
                    break
        except requests.exceptions.RequestException as e:
            logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            if retries < MAX_RETRIES:
                logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                time.sleep(RETRIES_DELAY_SEC * retries)
                retries += 1
            else:
                break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            break
    
    return success, user_data

def patch_user_by_api(settings: "SettingParams", user_id: int, patch_data: dict):

    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users/{user_id}'
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

def create_user_alias_by_api(settings: "SettingParams", user_id: str, alias: str):
    """
    Добавляет алиас пользователю через API Yandex 360.
    
    Args:
        settings: Параметры настроек с OAuth токеном и org_id
        user_id: ID пользователя (строка)
        alias: Алиас для добавления
    
    Returns:
        tuple: (success: bool, response_data: dict)
    """
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users/{user_id}/aliases'
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    data = {"alias": alias}
    
    logger.debug(f"POST URL: {url}")
    logger.debug(f"POST DATA: {data}")
    
    retries = 1
    success = False
    response_data = {}
    
    while True:
        try:
            response = requests.post(url, headers=headers, json=data)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            
            if response.status_code == HTTPStatus.OK:
                logger.info(f"Успех - алиас '{alias}' добавлен пользователю {user_id}.")
                response_data = response.json()
                success = True
                break
            else:
                logger.error(f"Ошибка при добавлении алиаса: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Ошибка. Добавление алиаса '{alias}' пользователю {user_id} не удалось.")
                    break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            if retries < MAX_RETRIES:
                logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                time.sleep(RETRIES_DELAY_SEC * retries)
                retries += 1
            else:
                break
    
    return success, response_data

def delete_user_alias_by_api(settings: "SettingParams", user_id: str, alias: str):
    """
    Удаляет алиас пользователю через API Yandex 360.
    
    Args:
        settings: Параметры настроек с OAuth токеном и org_id
        user_id: ID пользователя (строка)
        alias: Алиас для удаления
    
    Returns:
        tuple: (success: bool, response_data: dict)
    """
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users/{user_id}/aliases/{alias}'
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    
    logger.debug(f"DELETE URL: {url}")
    
    retries = 1
    success = False
    response_data = {}
    
    while True:
        try:
            response = requests.delete(url, headers=headers)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            
            if response.status_code == HTTPStatus.OK:
                logger.info(f"Успех - алиас '{alias}' удален пользователю {user_id}.")
                response_data = response.json()
                success = True
                break
            else:
                logger.error(f"Ошибка при удалении алиаса: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Ошибка. Удаление алиаса '{alias}' пользователю {user_id} не удалось.")
                    break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            if retries < MAX_RETRIES:
                logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                time.sleep(RETRIES_DELAY_SEC * retries)
                retries += 1
            else:
                break
    
    return success, response_data

def delete_user_by_api(settings: "SettingParams", user_id: str):
    """
    Удаляет пользователя через API Yandex 360.
    
    Args:
        settings: Параметры настроек с OAuth токеном и org_id
        user_id: ID пользователя (строка)
    
    Returns:
        tuple: (success: bool, response_data: dict)
    """
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/users/{user_id}'
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    
    logger.debug(f"DELETE URL: {url}")
    
    if settings.dry_run:
        logger.info(f"DRY RUN: Пользователь {user_id} был бы удален.")
        return True, {"dry_run": True}
    
    retries = 1
    success = False
    response_data = {}
    
    while True:
        try:
            response = requests.delete(url, headers=headers)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            
            if response.status_code == HTTPStatus.OK:
                logger.info(f"Успех - пользователь {user_id} удален.")
                response_data = response.json() if response.text else {}
                success = True
                break
            elif response.status_code == HTTPStatus.NO_CONTENT:
                logger.info(f"Успех - пользователь {user_id} удален (204 No Content).")
                response_data = {}
                success = True
                break
            else:
                logger.error(f"Ошибка при удалении пользователя: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error(f"Ошибка. Удаление пользователя {user_id} не удалось.")
                    break
        except Exception as e:
            logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
            if retries < MAX_RETRIES:
                logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                time.sleep(RETRIES_DELAY_SEC * retries)
                retries += 1
            else:
                break
    
    return success, response_data

def add_aliases_to_users(settings: "SettingParams", users_data: list):
    """
    Добавляет алиасы пользователям из списка данных пользователей.
    
    Args:
        settings: Параметры настроек с OAuth токеном и org_id
        users_data: Список словарей с данными пользователей, включая поле 'aliases'
    
    Returns:
        tuple: (success_count: int, failed_count: int, errors: list)
    """
    success_count = 0
    failed_count = 0
    errors = []
    
    logger.info(f"Начинаем добавление алиасов для {len(users_data)} пользователей...")
    
    for user_data in users_data:
        user_id = user_data.get('id')
        login = user_data.get('login', '')
        aliases = user_data.get('aliases', [])
        
        if not user_id:
            logger.error(f"Пользователь {login}: отсутствует ID пользователя")
            failed_count += 1
            errors.append(f"Пользователь {login}: отсутствует ID")
            continue
            
        if not aliases:
            logger.debug(f"Пользователь {login}: алиасы не указаны, пропускаем")
            continue
            
        # Обрабатываем алиасы как строку или список
        if isinstance(aliases, str):
            aliases_list = [a.strip() for a in aliases.split(",") if a.strip()]
        else:
            aliases_list = aliases if isinstance(aliases, list) else []
        
        if not aliases_list:
            logger.debug(f"Пользователь {login}: алиасы пусты, пропускаем")
            continue
            
        logger.info(f"Добавляем алиасы для пользователя {login} (ID: {user_id}): {aliases_list}")
        
        user_success = True
        for alias in aliases_list:
            if not alias:
                continue
            
            # Валидация алиаса
            is_valid, validated_alias = validate_alias(alias)
            if not is_valid:
                logger.error(f"Пользователь {login}: некорректный алиас '{alias}': {validated_alias}")
                user_success = False
                errors.append(f"Пользователь {login}: некорректный алиас '{alias}': {validated_alias}")
                continue
                
            success, response_data = create_user_alias_by_api(settings, user_id, validated_alias)
            if not success:
                logger.error(f"Не удалось добавить алиас '{validated_alias}' пользователю {login}")
                user_success = False
                errors.append(f"Пользователь {login}: не удалось добавить алиас '{validated_alias}'")
            else:
                logger.debug(f"Алиас '{validated_alias}' успешно добавлен пользователю {login}")
        
        if user_success:
            success_count += 1
        else:
            failed_count += 1
    
    logger.info(f"Добавление алиасов завершено. Успешно: {success_count}, Ошибок: {failed_count}")
    return success_count, failed_count, errors

def get_all_api360_departments(settings: "SettingParams", force = False, show_messages = False):
    if not force:
        if show_messages:
            logger.info("Получение всех подразделений организации из кэша...")
        else:
            logger.debug("Получение всех подразделений организации из кэша...")
    if not settings.all_deps or force or (datetime.now() - settings.all_deps_get_timestamp).total_seconds() > ALL_DEPS_REFRESH_IN_MINUTES * 60:
        settings.all_deps = get_all_api360_departments_from_api(settings)
        settings.all_deps_get_timestamp = datetime.now()
    return settings.all_deps

def get_all_api360_departments_from_api(settings: "SettingParams"):
    logger.info("Получение всех подразделений организации из API...")
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/departments'
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

def get_all_api360_groups(settings: "SettingParams", force = False):
    if not force:
        logger.info("Получение всех групп организации из кэша...")
    if not settings.all_groups or force or (datetime.now() - settings.all_groups_get_timestamp).total_seconds() > ALL_GROUPS_REFRESH_IN_MINUTES * 60:
        settings.all_groups = get_all_api360_groups_from_api(settings)
        settings.all_groups_get_timestamp = datetime.now()
    return settings.all_groups

def get_all_api360_groups_from_api(settings: "SettingParams"):
    logger.info("Получение всех групп организации из API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/groups"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    has_errors = False
    groups = []
    current_page = 1
    last_page = 1
    while current_page <= last_page:
        params = {'page': current_page, 'perPage': GROUPS_PER_PAGE_FROM_API}
        try:
            retries = 1
            while True:
                logger.debug(f"GET URL - {url}")
                response = requests.get(url, headers=headers, params=params)
                logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"!!! ERROR !!! during GET request url - {url}: {response.status_code}. Error message: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"Retrying ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        has_errors = True
                        break
                else:
                    groups.extend(response.json()['groups'])
                    logger.debug(f"Get {len(response.json()['groups'])} groups from page {current_page} (total {last_page} page(s)).")
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
        logger.error("There are some error during GET requests. Return empty groups list.")
        return []
    
    return groups

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
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{settings.org_id}/departments'
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
    # Удаление дубликатов из списка словарей
    seen = set()
    final_list = []
    for d in temp_list:
        # Создаем строковое представление словаря для проверки уникальности
        dict_key = str(sorted(d.items()))
        if dict_key not in seen:
            seen.add(dict_key)
            final_list.append(d)
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


def generate_deps_hierarchy_from_api(settings: "SettingParams", force = False, show_messages = False):
    all_deps_from_api = get_all_api360_departments(settings, force, show_messages)
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

def generate_deps_hierarchy_and_count_users_from_api(settings: "SettingParams", force = False):
    users = get_all_api360_users(settings, force)
    if not users:
        return []
    all_deps_from_api = generate_deps_hierarchy_from_api(settings, force)
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

def load_dep_info_to_file(settings: "SettingParams", force = False):
    all_deps = generate_deps_hierarchy_from_api(settings, force)
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

    for target_user_short in users_to_add:
        show_one_user_attributes(settings, target_user_short["id"], departments)
    return

def show_one_user_attributes(settings: "SettingParams", uid: str, departments: list = None, save_to_file = True):
    result, target_user = get_user_by_api(settings, uid)
    if departments is None:
        departments = generate_deps_hierarchy_from_api(settings, show_messages=False)
    if not result:
        logger.error(f"Пользователь с id: {uid} не найден в организации Y360.")
        return
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
    if save_to_file:
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
                        logger.info(f'departmentId: {department["id"]}')
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

def download_users_attrib_to_file(settings: "SettingParams", users: list = None):
    """
    Выгружает данные пользователей из API 360 в два файла:
    1. Файл с полным списком атрибутов пользователя, как возвращает API (settings.all_users_file)
    2. Файл с полями, аналогичными полям для создания пользователей (settings.all_users_file + '_short.csv')
    Также добавляет функцию проверки уникальности алиасов.
    """
    if not users:
        all_users = get_all_api360_users(settings, force=True)
    if not users:
        logger.error("Не найдено пользователей из API 360. Проверьте ваши настройки.")
        return

    # --- 1. Выгрузка полного списка атрибутов пользователя ---
    with open(settings.all_users_file, 'w', encoding='utf-8', newline='') as csv_file:
        if not users:
            fieldnames = list(all_users[0].keys())
        else:
            fieldnames = list(users[0].keys())
        # Исключаем ключ full_groups из fieldnames
        if "full_groups" in fieldnames:
            fieldnames.remove("full_groups")
        if "isEnabledUpdatedAt" not in fieldnames:
            fieldnames.append("isEnabledUpdatedAt")
        writer = csv.DictWriter(csv_file, delimiter=';', fieldnames=fieldnames)
        writer.writeheader()
        for user in users:
            # Создаем копию словаря без ключа full_groups
            user_copy = {k: v for k, v in user.items() if k != 'full_groups'}
            writer.writerow(user_copy)
        logger.info(f"Сохранено {len(users)} пользователей в файл {settings.all_users_file}")

def download_users_attrib_to_file_short(settings: "SettingParams", users: list = None, query: str = ""):
    """
    Выгружает данные пользователей из API 360 в два файла:
    1. Файл с полным списком атрибутов пользователя, как возвращает API (settings.all_users_file)
    2. Файл с полями, аналогичными полям для создания пользователей (settings.all_users_file + '_short.csv')
    Также добавляет функцию проверки уникальности алиасов.
    
    Args:
        settings: параметры настроек
        users: список пользователей (если None, будут загружены из API)
        query: запрос, который использовался для создания файла (сохраняется во вторую строку)
    """
    # if not users:
    #     users = get_all_api360_users(settings, force=True)
    # if not users:
    #     logger.error("Не найдено пользователей из API 360. Проверьте ваши настройки.")
    #     return

    # --- 2. Выгрузка в формате для импорта (создания пользователей) ---
    departments = generate_deps_hierarchy_from_api(settings)
    creation_fields = [
        "id",
        "login",
        "password",
        "password_change_required",
        "update_password",
        "last_name",
        "first_name",
        "middle_name",
        "position",
        "gender",
        "birthday",
        "language",
        "is_enabled",
        "is_admin",
        "work_phone",
        "mobile_phone",
        "personal_email",
        "department",
        "aliases"
    ]

    export_rows = []
    for user in users:
        row = {}
        # login
        row["id"] = user.get("id", "")
        row["login"] = user.get("nickname", "")
        # first, middle, last
        name = user.get("name", {})
        row["last_name"] = name.get("last", "")
        row["first_name"] = name.get("first", "")
        row["middle_name"] = name.get("middle", "")
        # gender
        row["gender"] = user.get("gender", "")
        # password (оставляем пустым, т.к. не выгружается из API)
        row["password"] = ""
        # password_change_required
        row["password_change_required"] = "false"
        # update_password
        row["update_password"] = "false"
        # department (id или название подразделения)
        department_id = user.get("departmentId", "1")
        if department_id == 1:
            row["department"] = "Все пользователи"
        else:
            department = next((d for d in departments if d['id'] == department_id), None)
            if department:
                row["department"] = department['path']
            else:
                row["department"] = "Неизвестное подразделение"
        # position
        row["position"] = user.get("position", "")
        # personal_email (ищем в contacts/email или about)
        personal_email = ""
        about = user.get("about", "")
        if about:
            try:
                about_json = json.loads(about)
                personal_email = about_json.get("personal_email", "")
            except Exception:
                pass
        row["personal_email"] = personal_email
        # is_enabled
        row["is_enabled"] = str(user.get("isEnabled", ""))
        # is_admin
        row["is_admin"] = str(user.get("isAdmin", ""))
        # aliases (список через запятую)
        aliases = user.get("aliases", [])
        if isinstance(aliases, list):
            row["aliases"] = ",".join(aliases)
        else:
            row["aliases"] = ""

        row["work_phone"] = ""
        row["mobile_phone"] = ""
        # about (оставляем как есть)
        for contact in user.get('contacts', []):
            if not contact['synthetic']:
                if not contact['alias']:
                    if contact['type'] == 'phone':
                        label = contact.get('label', '').lower()
                        if label == 'mobile':
                            row["mobile_phone"] = contact['value']
                        elif label == 'work':
                            row["work_phone"] = contact['value']
        export_rows.append(row)

    # Генерация имени файла с временной меткой
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    import_file = os.path.join(settings.short_file_dir, f"{settings.short_file_name_prefix}_{timestamp}.csv")
    
    with open(import_file, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, delimiter=';', fieldnames=creation_fields)
        writer.writeheader()
        
        # Записываем запрос во вторую строку как комментарий
        if query:
            # Добавляем символ # в начало и правильное количество разделителей для всех полей
            # Количество разделителей = количество полей - 1
            delimiter_count = len(creation_fields) - 1
            query_line = f"# Запрос: {query}" + ";" * delimiter_count
            csv_file.write(query_line + "\n")
        
        for row in export_rows:
            writer.writerow(row)
        logger.info(f"Сохранено {len(export_rows)} пользователей в файл {import_file}")

def check_aliases_uniqueness(new_users, mode: str = "add"):
    """
    Проверяет уникальность всех алиасов среди nickname и aliases существующих пользователей (existing_users)
    и среди nickname и aliases новых пользователей (new_users).
    Возвращает True если все уникальны, иначе False и список конфликтов.
    """

    conflicts = []
    temp_set = set()
    existing_users = get_all_api360_users(settings, force=False)
    if mode == "add":
        for idx,new_user in enumerate(new_users):
            found_flag = False
            for y360_user in existing_users:
                if new_user.get("login") == y360_user.get("nickname"):
                    conflicts.append((idx+1, "login", new_user.get("login")))
                    found_flag = True
                temp_aliases = [item.lower().strip() for item in y360_user.get("aliases", []) if item.strip()]
                for alias in new_user.get("aliases", "").strip().split(","):
                    if alias:
                        if alias.strip().split("@")[0].lower().strip() in temp_aliases:
                            conflicts.append((idx+1, "alias", alias.strip().split("@")[0].lower().strip()))
                            found_flag = True
                if found_flag:
                    break

        for idx,new_user1 in enumerate(new_users):
            temp_set.clear()
            for new_user2 in new_users:
                if new_user1.get("login") != new_user2.get("login"):
                    temp_set.add(new_user2.get("login"))
                    for alias in new_user2.get("aliases", "").strip().split(","):
                        temp_set.add(alias.strip().split("@")[0].lower().strip())
            if new_user1.get("login") in temp_set:
                conflicts.append((idx+1, "login", new_user1.get("login")))
            for alias in new_user1.get("aliases", "").strip().split(","):
                if alias:
                    if alias.strip().split("@")[0].lower().strip() in temp_set:
                        conflicts.append((idx+1, "alias", alias.strip().split("@")[0].lower().strip()))
    elif mode == "update":
        for idx,new_user in enumerate(new_users):
            if not new_user.get("aliases"):
                continue
            found_flag = False
            temp_set.clear()
            for y360_user in existing_users:
                if new_user.get("login") != y360_user.get("nickname"):
                    temp_set.add(y360_user.get("nickname"))
                    temp_aliases = [item.lower().strip() for item in y360_user.get("aliases", []) if item.strip()]
                    for alias in temp_aliases:
                        temp_set.add(alias)
            for alias in new_user.get("aliases", "").strip().split(","):
                if alias:
                    if alias.strip().split("@")[0].lower().strip() in temp_aliases:
                        conflicts.append((idx+1, "alias", alias.strip().split("@")[0].lower().strip()))
                        found_flag = True
            if found_flag:
                break

        for idx,new_user1 in enumerate(new_users):
            if not new_user1.get("aliases"):
                continue
            temp_set.clear()
            for new_user2 in new_users:
                if new_user1.get("login") != new_user2.get("login"):
                    for alias in new_user2.get("aliases", "").strip().split(","):
                        temp_set.add(alias.split("@")[0].lower().strip())
            
            for alias in new_user1.get("aliases", "").strip().split(","):
                if alias:
                    if alias.split("@")[0].lower().strip() in temp_set:
                        conflicts.append((idx+1, "alias", alias.strip().split("@")[0].lower().strip()))

    if conflicts:
        logger.error("Обнаружены неуникальные алиасы или логины среди новых и/или существующих пользователей:")
        for rownum, typ, val in conflicts:
            logger.error(f"Строка (не учитывая строки с комментариями) {rownum}: {typ} '{val}' уже используется")
        return False, conflicts
    return True, []


def download_users_attrib_to_file2(settings: "SettingParams"):
    """
    Выгружает данные пользователей из API 360 в два файла:
    1. Файл с полным списком атрибутов пользователя, как возвращает API (settings.all_users_file)
    """
    users = get_all_api360_users(settings, force=True)
    if not users:
        logger.error("Не найдено пользователей из API 360. Проверьте ваши настройки.")
        return

    # --- 1. Выгрузка полного списка атрибутов пользователя ---
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

def download_users_attrib_to_file_prompt(settings: "SettingParams"):

    print("\n")
    print("Выгрузка атрибутов пользователей в файл.")
    print("\n")
    
    # Загружаем алиасы из файла при первом запуске функции
    try:
        load_search_aliases(settings)
    except Exception as e:
        logger.error(f"Ошибка при загрузке алиасов: {e}")
        print(f"\n❌ Ошибка при загрузке алиасов: {e}")
        print("Будут использованы встроенные алиасы.\n")

    # Вызываем функцию поиска с внутренним циклом
    find_users_prompt(settings)


def wildcard_match(text: str, pattern: str) -> bool:
    """
    Проверяет, соответствует ли текст паттерну с поддержкой wildcards (*).
    
    Args:
        text: Текст для проверки
        pattern: Паттерн для сопоставления (может содержать * в начале или конце)
    
    Returns:
        True если текст соответствует паттерну, иначе False
    """
    text = text.lower()
    pattern = pattern.lower().strip()
    
    # Если нет wildcards, используем точное совпадение
    if '*' not in pattern:
        return text == pattern
    
    # Паттерн начинается и заканчивается wildcard: *pattern*
    if pattern.startswith('*') and pattern.endswith('*'):
        core = pattern[1:-1]
        return core in text
    
    # Паттерн начинается с wildcard: *pattern
    if pattern.startswith('*'):
        core = pattern[1:]
        return text.endswith(core)
    
    # Паттерн заканчивается wildcard: pattern*
    if pattern.endswith('*'):
        core = pattern[:-1]
        return text.startswith(core)
    
    # Если wildcard в середине (не поддерживается по требованиям)
    return False


def wildcard_contains(text: str, pattern: str) -> bool:
    """
    Проверяет, содержится ли паттерн в тексте с поддержкой wildcards.
    
    Args:
        text: Текст для проверки
        pattern: Паттерн для сопоставления (может содержать * в начале или конце)
    
    Returns:
        True если паттерн найден в тексте, иначе False
    """
    text = text.lower()
    pattern = pattern.lower().strip()
    
    # Если нет wildcards, используем простой поиск подстроки
    if '*' not in pattern:
        return pattern in text
    
    # Паттерн начинается и заканчивается wildcard: *pattern*
    if pattern.startswith('*') and pattern.endswith('*'):
        core = pattern[1:-1]
        return core in text
    
    # Паттерн начинается с wildcard: *pattern
    if pattern.startswith('*'):
        core = pattern[1:]
        return core in text
    
    # Паттерн заканчивается wildcard: pattern*
    if pattern.endswith('*'):
        core = pattern[:-1]
        return core in text
    
    return False


def load_search_aliases(settings: "SettingParams") -> dict:
    """
    Загружает алиасы поисковых атрибутов из файла.
    Выполняет проверку на уникальность атрибутов и алиасов.
    
    Args:
        settings: Объект настроек приложения
    
    Returns:
        Словарь алиасов в формате {alias: (real_attr, invert, contact_type, data_type)}
    
    Raises:
        ValueError: Если найдены дубликаты алиасов или некорректный формат файла
        FileNotFoundError: Если файл алиасов не найден
    """
    global _search_aliases_cache
    
    # Если алиасы уже загружены, возвращаем из кэша
    if _search_aliases_cache is not None:
        return _search_aliases_cache
    
    aliases_file = getattr(settings, 'search_aliases_file', None)
    
    if not aliases_file:
        logger.warning("Не указан путь к файлу алиасов (SEARCH_ALIASES_FILE). Используются встроенные алиасы.")
        # Возвращаем пустой словарь, будут использованы встроенные алиасы
        _search_aliases_cache = {}
        return _search_aliases_cache
    
    if not os.path.exists(aliases_file):
        logger.error(f"Файл алиасов не найден: {aliases_file}")
        raise FileNotFoundError(f"Файл алиасов не найден: {aliases_file}")
    
    logger.info("=" * 100)
    logger.info(f"Загрузка алиасов поисковых атрибутов из файла: {aliases_file}")
    logger.info("=" * 100)
    
    aliases = {}
    seen_aliases = {}  # Для проверки уникальности алиасов
    seen_attributes = {}  # Для отслеживания атрибутов
    line_num = 0
    
    try:
        with open(aliases_file, 'r', encoding='utf-8') as f:
            for line in f:
                line_num += 1
                line = line.strip()
                
                # Пропускаем пустые строки и комментарии
                if not line or line.startswith('#'):
                    continue
                
                # Парсим строку формата: alias=real_attr|invert|contact_type|data_type
                if '=' not in line:
                    logger.warning(f"Строка {line_num}: пропущена некорректная строка (нет символа '='): {line}")
                    continue
                
                alias_part, value_part = line.split('=', 1)
                alias = alias_part.strip().lower()
                
                if not alias:
                    logger.warning(f"Строка {line_num}: пропущена строка с пустым алиасом")
                    continue
                
                # Проверка на дубликаты алиасов
                if alias in seen_aliases:
                    error_msg = f"Строка {line_num}: найден дубликат алиаса '{alias}' (первое использование в строке {seen_aliases[alias]})"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                
                seen_aliases[alias] = line_num
                
                # Парсим значения
                parts = value_part.split('|')
                if len(parts) != 4:
                    logger.warning(f"Строка {line_num}: некорректный формат значения (ожидается 4 части через '|'): {line}")
                    continue
                
                real_attr = parts[0].strip()
                invert_str = parts[1].strip().lower()
                contact_type = parts[2].strip() if parts[2].strip() else None
                data_type = parts[3].strip() if parts[3].strip() else None
                
                if not real_attr:
                    logger.warning(f"Строка {line_num}: пропущена строка с пустым реальным атрибутом")
                    continue
                
                # Преобразуем строку инверсии в boolean
                invert = invert_str == 'true'
                
                # Сохраняем алиас
                aliases[alias] = (real_attr, invert, contact_type, data_type)
                
                # Отслеживаем использование атрибутов (для информации)
                if real_attr not in seen_attributes:
                    seen_attributes[real_attr] = []
                seen_attributes[real_attr].append((alias, line_num))
        
        logger.info(f"Успешно загружено {len(aliases)} алиасов для {len(seen_attributes)} атрибутов")
        
        # Выводим информацию об атрибутах и их алиасах
        logger.debug("Загруженные атрибуты и их алиасы:")
        for attr in sorted(seen_attributes.keys()):
            alias_list = ', '.join([f"'{a[0]}'" for a in seen_attributes[attr]])
            logger.debug(f"  {attr}: {alias_list}")
        
        _search_aliases_cache = aliases
        return aliases
        
    except Exception as e:
        if isinstance(e, ValueError):
            raise
        logger.error(f"Ошибка при загрузке файла алиасов: {e}")
        raise


def normalize_attribute_name(attr_alias: str, settings: "SettingParams" = None) -> tuple:
    """
    Преобразует алиас атрибута в его реальное имя с флагом инверсии, типом контакта и типом данных.
    
    Args:
        attr_alias: Алиас атрибута (например, 'first_name', 'имя', 'admin', 'phone', 'created')
        settings: Объект настроек приложения (опционально, для загрузки алиасов из файла)
    
    Returns:
        Кортеж (реальное_имя_атрибута, нужна_инверсия, тип_контакта, тип_данных)
        Например: ('name.first', False, None, None), ('contacts', False, 'phone', None), ('createdAt', False, None, 'date')
    
    Raises:
        ValueError: Если атрибут не найден в списке допустимых атрибутов и алиасов
    """
    # Загружаем алиасы из файла, если settings предоставлен
    loaded_aliases = {}
    if settings is not None:
        try:
            loaded_aliases = load_search_aliases(settings)
        except Exception as e:
            logger.warning(f"Не удалось загрузить алиасы из файла: {e}. Используются встроенные алиасы.")
    
    # Встроенные алиасы атрибутов (используются как резервные)
    builtin_aliases = {
        # name.first алиасы
        'first_name': ('name.first', False, None, None),
        'имя': ('name.first', False, None, None),
        'first': ('name.first', False, None, None),
        
        # name.last алиасы
        'last_name': ('name.last', False, None, None),
        'фамилия': ('name.last', False, None, None),
        'last': ('name.last', False, None, None),
        
        # name.middle алиасы
        'middle_name': ('name.middle', False, None, None),
        'отчество': ('name.middle', False, None, None),
        'middle': ('name.middle', False, None, None),
        
        # isAdmin алиасы
        'admin': ('isAdmin', False, None, None),
        'админ': ('isAdmin', False, None, None),
        'is_admin': ('isAdmin', False, None, None),
        
        # isEnabled алиасы
        'enabled': ('isEnabled', False, None, None),
        'is_enabled': ('isEnabled', False, None, None),
        
        # isEnabled алиасы с инверсией (заблокирован = NOT isEnabled)
        'blocked': ('isEnabled', True, None, None),
        'заблокирован': ('isEnabled', True, None, None),
        
        # position алиасы
        'должность': ('position', False, None, None),
        
        # department алиасы
        'department': ('department', False, None, None),
        'подразделение': ('department', False, None, None),
        'ou': ('department', False, None, None),
        'dep': ('department', False, None, None),
        
        # nickname алиасы
        'алиас': ('nickname', False, None, None),
        
        # aliases алиасы (массив алиасов)
        'aliases': ('aliases', False, None, 'array'),
        'алиасы': ('aliases', False, None, 'array'),
        
        # contacts - phone алиасы
        'телефон': ('contacts', False, 'phone', None),
        'phone': ('contacts', False, 'phone', None),
        'work_phone': ('contacts', False, 'phone', None),
        'mobile_phone': ('contacts', False, 'phone', None),
        
        # contacts - email алиасы
        'почта': ('contacts', False, 'email', None),
        'mail': ('contacts', False, 'email', None),
        'email': ('contacts', False, 'email', None),
        
        # date атрибуты - createdAt
        'created': ('createdAt', False, None, 'date'),
        'создан': ('createdAt', False, None, 'date'),
        
        # date атрибуты - isEnabledUpdatedAt
        'дата_блокировки': ('isEnabledUpdatedAt', False, None, 'date'),
        
        # date атрибуты - updatedAt
        'updated': ('updatedAt', False, None, 'date'),
        'изменен': ('updatedAt', False, None, 'date'),
        
        # group атрибуты - имя группы
        'groupname': ('full_groups.name', False, None, None),
        'group_name': ('full_groups.name', False, None, None),
        'имя_группы': ('full_groups.name', False, None, None),
        
        # group атрибуты - алиасы группы (поиск по group.aliases и group.label)
        'groupaliases': ('full_groups.aliases', False, 'group_aliases', 'array'),
        'group_aliases': ('full_groups.aliases', False, 'group_aliases', 'array'),
        'алиасы_группы': ('full_groups.aliases', False, 'group_aliases', 'array'),
    }
    
    # Объединяем загруженные и встроенные алиасы (загруженные имеют приоритет)
    aliases = {**builtin_aliases, **loaded_aliases}
    
    # Список допустимых реальных имен атрибутов из API Yandex 360
    valid_real_attributes = {
        # Основные атрибуты
        'id': (None, None),
        'nickname': (None, None),
        'aliases': (None, 'array'),  # массив алиасов
        'name.first': (None, None),
        'name.last': (None, None),
        'name.middle': (None, None),
        'isadmin': (None, None),
        'isenabled': (None, None),
        'position': (None, None),
        'department': (None, None),
        'departmentid': (None, None),
        'gender': (None, None),
        'language': (None, None),
        'timezone': (None, None),
        'about': (None, None),
        'birthday': (None, None),
        'contacts': (None, None),
        
        # Атрибуты группы
        'full_groups.aliases': ('full_groups.aliases', 'array'),  # массив алиасов группы
        'full_groups.label': (None, None),  # label группы (строка)
        'full_groups.name': (None, None),  # имя группы
        
        # Атрибуты дат
        'createdat': (None, 'date'),
        'updatedat': (None, 'date'),
        'isenabledupdatedat': (None, 'date'),  # правильное имя
    }
    
    # Маппинг для нормализации регистра ключевых атрибутов
    case_normalization = {
        'isadmin': 'isAdmin',
        'isenabled': 'isEnabled',
        'createdat': 'createdAt',
        'updatedat': 'updatedAt',
        'isenabledupdatedat': 'isEnabledUpdatedAt',
        'departmentid': 'departmentId',
    }
    
    # Приводим к нижнему регистру для поиска
    attr_lower = attr_alias.lower().strip()
    
    # Если это алиас, возвращаем реальное имя с флагами
    if attr_lower in aliases:
        return aliases[attr_lower]
    
    # Проверяем, является ли это реальным именем атрибута
    if attr_lower in valid_real_attributes:
        contact_type, data_type = valid_real_attributes[attr_lower]
        # Нормализуем регистр для ключевых атрибутов
        normalized_attr = case_normalization.get(attr_lower, attr_alias)
        return (normalized_attr, False, contact_type, data_type)
    
    # Атрибут не найден - возбуждаем исключение
    raise ValueError(f"Неизвестный атрибут: '{attr_alias}'. Используйте 'help' для списка допустимых атрибутов.")


def parse_relative_date(value: str):
    """
    Парсит относительную дату в формате: [-]число[d|m|w|д|м|н]
    
    Args:
        value: Строка с относительной датой (например, '-7d', '30д', '2w', '-1м')
    
    Returns:
        datetime объект или None если не удалось распарсить
    """
    import re
    from datetime import datetime, timedelta
    
    # Паттерн для относительных дат: опциональный минус, число, суффикс
    pattern = r'^(-?)(\d+)([dmwдмн])$'
    match = re.match(pattern, value.lower().strip())
    
    if not match:
        return None
    
    sign, number, suffix = match.groups()
    number = int(number)
    
    # Если есть минус, делаем число отрицательным
    if sign == '-':
        number = -number
    
    now = datetime.now()
    
    # Вычисляем дату в зависимости от суффикса
    if suffix in ['d', 'д']:  # дни
        return now + timedelta(days=number)
    elif suffix in ['w', 'н']:  # недели
        return now + timedelta(weeks=number)
    elif suffix in ['m', 'м']:  # месяцы
        # Простое вычисление месяцев без внешних библиотек
        month = now.month + number
        year = now.year
        
        while month > 12:
            month -= 12
            year += 1
        while month < 1:
            month += 12
            year -= 1
        
        try:
            return now.replace(year=year, month=month)
        except ValueError:
            # Если день месяца невалидный (например, 31 февраля), используем последний день месяца
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            return now.replace(year=year, month=month, day=min(now.day, last_day))
    
    return None


def parse_date_value(value: str):
    """
    Парсит значение даты - может быть относительная дата или различные форматы.
    
    Поддерживает форматы:
    - Относительные: -7d, 30д, 2w, -1м
    - DD.MM.YYYY, DD/MM/YYYY, DD-MM-YYYY
    - YYYY-MM-DD, YYYY/MM/DD
    - MM/DD/YYYY, DD.MM.YY, YYYY.MM.DD
    - ISO формат с временем: YYYY-MM-DDTHH:MM:SSZ
    
    Args:
        value: Строка с датой
    
    Returns:
        datetime объект или None
    """
    from datetime import datetime
    
    # Сначала пробуем относительную дату
    relative = parse_relative_date(value)
    if relative:
        return relative
    
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
    for date_format in date_formats:
        try:
            return datetime.strptime(value, date_format)
        except:
            pass
    
    # Пробуем ISO формат с временем
    try:
        if 'T' in value:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except:
        pass
    
    return None


def compare_dates(date1_str: str, operator: str, date2_str: str) -> bool:
    """
    Сравнивает две даты с учетом оператора.
    
    Args:
        date1_str: Первая дата (строка в различных форматах)
        operator: Оператор сравнения (<, >, <=, >=, =)
        date2_str: Вторая дата (может быть относительной)
    
    Returns:
        True если условие выполняется, иначе False
    """
    from datetime import datetime
    
    # Парсим первую дату (из API или других источников)
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
    
    date1 = None
    
    # Пробуем ISO формат с временем
    try:
        if 'T' in date1_str:
            date1 = datetime.fromisoformat(date1_str.replace('Z', '+00:00'))
    except:
        pass
    
    # Если не удалось, пробуем другие форматы
    if not date1:
        for date_format in date_formats:
            try:
                date1 = datetime.strptime(date1_str, date_format)
                break
            except:
                pass
    
    if not date1:
        return False
    
    # Парсим вторую дату (из запроса)
    date2 = parse_date_value(date2_str)
    if not date2:
        return False
    
    # Убираем timezone для сравнения
    if date1.tzinfo:
        date1 = date1.replace(tzinfo=None)
    if date2.tzinfo:
        date2 = date2.replace(tzinfo=None)
    
    # Выполняем сравнение
    if operator == '<':
        return date1 < date2
    elif operator == '>':
        return date1 > date2
    elif operator == '<=':
        return date1 <= date2
    elif operator == '>=':
        return date1 >= date2
    elif operator in ['=', 'is']:
        # Для равенства сравниваем только даты, игнорируя время
        return date1.date() == date2.date()
    
    return False


def get_user_attribute(user: dict, attr_path: str):
    """
    Получает значение атрибута пользователя по пути (например, name.first).
    Поиск атрибутов выполняется без учета регистра.
    
    Args:
        user: Объект пользователя
        attr_path: Путь к атрибуту (например, 'name.first', 'nickname', 'isAdmin')
    
    Returns:
        Значение атрибута или None если атрибут не найден
    """
    parts = attr_path.split('.')
    value = user
    
    for part in parts:
        if isinstance(value, dict):
            # Поиск атрибута без учета регистра
            part_lower = part.lower()
            found_key = None
            
            for key in value.keys():
                if key.lower() == part_lower:
                    found_key = key
                    break
            
            if found_key is not None:
                value = value[found_key]
            else:
                return None
        else:
            return None
    
    return value


def is_complex_query(query: str) -> bool:
    """
    Определяет, является ли запрос сложным (содержит операторы сложного поиска).
    
    Args:
        query: Строка запроса
    
    Returns:
        True если запрос сложный, иначе False
    """
    query_lower = query.lower().strip()
    
    # Проверяем, начинается ли запрос с оператора not
    if query_lower.startswith('not ') or any(a for a in query_lower.split(' ') if a in ["isadmin", "isenabled"]):
        return True
    
    # Ключевые слова сложного поиска (включая операторы сравнения дат)
    keywords = [' and ', ' or ', ' not ', ' in ', ' contains ', ' is ', '=', ' < ', ' > ', ' <= ', ' >= ', ' between ']
    
    # Проверяем наличие хотя бы одного ключевого слова
    for keyword in keywords:
        if keyword in query_lower:
            return True
    
    # Проверяем, является ли это одиночным boolean атрибутом
    # Список boolean атрибутов и их алиасов
    boolean_attributes = [
        'isadmin', 'admin', 'админ', 'is_admin',
        'isenabled', 'enabled', 'blocked', 'заблокирован', 'is_enabled'
    ]
    
    # Если запрос состоит из одного слова и это boolean атрибут
    if ' ' not in query_lower and query_lower in boolean_attributes:
        return True
    
    return False


def parse_complex_query(query: str):
    """
    Парсит сложный запрос и возвращает структуру для выполнения.
    
    Args:
        query: Строка сложного запроса
    
    Returns:
        Список условий и логических операторов
    """
    # Разбираем запрос на токены
    # Формат: attribute operator value [logical_op attribute operator value ...]
    
    tokens = []
    current_token = ""
    in_quotes = False
    
    for char in query:
        if char in ['"', "'"]:
            in_quotes = not in_quotes
            current_token += char
        elif char == ' ' and not in_quotes:
            if current_token:
                tokens.append(current_token)
                current_token = ""
        else:
            current_token += char
    
    if current_token:
        tokens.append(current_token)
    
    return tokens


def evaluate_condition(user: dict, attribute: str, operator: str, value: str, contact_type: str = None, data_type: str = None) -> bool:
    """
    Оценивает одно условие для пользователя.
    
    Args:
        user: Объект пользователя
        attribute: Атрибут для проверки
        operator: Оператор сравнения (=, is, contains, in, not, has_value, <, >, <=, >=, between)
        value: Значение для сравнения
        contact_type: Тип контакта для поиска в contacts ('phone', 'email') или 'group_aliases' для поиска в группах
        data_type: Тип данных атрибута ('date' для дат, 'array' для массивов)
    
    Returns:
        True если условие выполняется, иначе False
    
    Notes:
        - Для атрибутов full_groups.* (name, label, aliases) выполняется поиск по всем группам пользователя
        - Пользователь может быть в нескольких группах, условие считается выполненным, если хотя бы одна группа соответствует критерию
    """
    # Специальная обработка для массивов (например, aliases, group.aliases)
    if data_type == 'array':
        # Специальная обработка для full_groups.aliases - также проверяем full_groups.label
        if contact_type == 'group_aliases':
            full_groups = user.get('full_groups')
            if not full_groups:
                return False
            
            # Проверяем, что это список
            if not isinstance(full_groups, list):
                return False
            
            # has_value для full_groups.aliases - проверяем наличие хотя бы одной группы
            if operator == 'has_value':
                return len(full_groups) > 0
            
            # Очищаем значение от кавычек
            search_value = value.strip('"').strip("'")
            search_str = search_value.lower()
            operator_lower = operator.lower()
            
            # Перебираем все группы пользователя
            for group in full_groups:
                if not isinstance(group, dict):
                    continue
                
                # Собираем все значения для поиска из текущей группы: aliases (массив) + label (строка)
                search_values = []
                
                # Добавляем aliases из group['aliases'] (если это массив)
                group_aliases = group.get('aliases', [])
                if isinstance(group_aliases, list):
                    search_values.extend([str(alias).lower() for alias in group_aliases])
                elif group_aliases:
                    search_values.append(str(group_aliases).lower())
                
                # Добавляем label из group['label']
                group_label = group.get('label')
                if group_label:
                    search_values.append(str(group_label).lower())
                
                # Проверяем каждое значение в текущей группе
                for search_val in search_values:
                    if operator_lower in ['=', 'is']:
                        if '*' in search_str:
                            if wildcard_match(search_val, search_str):
                                return True
                        else:
                            if search_val == search_str:
                                return True
                    elif operator_lower == 'contains':
                        if '*' in search_str:
                            if wildcard_contains(search_val, search_str):
                                return True
                        else:
                            if search_str in search_val:
                                return True
                    elif operator_lower == 'in':
                        values = [v.strip().strip('"').strip("'").lower() for v in value.split(',')]
                        if search_val in values:
                            return True
            
            return False
        
        # Обычная обработка массивов (aliases и т.д.)
        attr_value = get_user_attribute(user, attribute)
        
        if attr_value is None:
            return False
        
        # Если это не список, преобразуем в список
        if not isinstance(attr_value, list):
            return False
        
        # has_value для массивов - проверяем, что массив не пустой
        if operator == 'has_value':
            return len(attr_value) > 0
        
        # Операторы = и contains для массивов - ищут элемент в массиве
        if operator in ['=', 'is', 'contains']:
            # Очищаем значение от кавычек
            search_value = value.strip('"').strip("'")
            
            # Проверяем каждый элемент массива
            for item in attr_value:
                item_str = str(item).lower()
                search_str = search_value.lower()
                
                # Если есть wildcard, используем wildcard_match
                if '*' in search_str:
                    if wildcard_match(item_str, search_str):
                        return True
                else:
                    # Точное совпадение
                    if item_str == search_str:
                        return True
            
            return False
        
        return False
    
    # Специальная обработка для дат
    if data_type == 'date':
        attr_value = get_user_attribute(user, attribute)
        
        logger.debug(f"Проверка даты: атрибут={attribute}, значение={attr_value}, оператор={operator}, сравнение_с={value}")
        
        if attr_value is None or not attr_value:
            return False
        
        # has_value для дат - проверяем, что дата существует
        if operator == 'has_value':
            return True
        
        # Операторы сравнения дат
        if operator in ['<', '>', '<=', '>=', '=', 'is']:
            result = compare_dates(str(attr_value), operator, value)
            logger.debug(f"Результат сравнения дат: {result}")
            return result
        
        # Оператор between для дат
        if operator == 'between':
            # Ожидаем формат: "date1 date2" или "date1,date2"
            dates = value.replace(',', ' ').split()
            if len(dates) >= 2:
                date1 = parse_date_value(dates[0])
                date2 = parse_date_value(dates[1])
                
                if date1 and date2:
                    from datetime import datetime
                    try:
                        if 'T' in str(attr_value):
                            attr_date = datetime.fromisoformat(str(attr_value).replace('Z', '+00:00'))
                        else:
                            attr_date = datetime.strptime(str(attr_value), '%Y-%m-%d')
                        
                        if attr_date.tzinfo:
                            attr_date = attr_date.replace(tzinfo=None)
                        if date1.tzinfo:
                            date1 = date1.replace(tzinfo=None)
                        if date2.tzinfo:
                            date2 = date2.replace(tzinfo=None)
                        
                        return date1 <= attr_date <= date2
                    except:
                        pass
            
            return False
        
        return False
    
    # Специальная обработка для контактов
    if contact_type:
        contacts = user.get('contacts', [])
        if not contacts:
            return False
        
        # Фильтруем контакты по типу
        filtered_contacts = [c for c in contacts if c.get('type') == contact_type]
        
        if not filtered_contacts:
            return False
        
        # Специальный оператор для проверки наличия контакта
        if operator == 'has_value':
            return len(filtered_contacts) > 0
        
        # Убираем кавычки из значения
        value = value.strip().strip('"').strip("'")
        value_lower = value.lower()
        operator_lower = operator.lower()
        
        # Проверяем каждый контакт нужного типа
        for contact in filtered_contacts:
            contact_value = str(contact.get('value', '')).lower()
            
            if operator_lower in ['=', 'is']:
                if wildcard_match(contact_value, value_lower):
                    return True
            elif operator_lower == 'contains':
                if wildcard_contains(contact_value, value_lower):
                    return True
            elif operator_lower == 'in':
                values = [v.strip().strip('"').strip("'").lower() for v in value.split(',')]
                if contact_value in values:
                    return True
        
        return False
    
    # Специальная обработка для full_groups.name и full_groups.label
    if attribute in ['full_groups.name', 'full_groups.label']:
        full_groups = user.get('full_groups')
        if not full_groups:
            return False
        
        # Проверяем, что это список
        if not isinstance(full_groups, list):
            return False
        
        # has_value - проверяем наличие хотя бы одной группы
        if operator == 'has_value':
            return len(full_groups) > 0
        
        # Убираем кавычки из значения
        value = value.strip().strip('"').strip("'")
        value_lower = value.lower()
        operator_lower = operator.lower()
        
        # Определяем, какое поле проверяем
        field_name = attribute.split('.')[-1]  # 'name' или 'label'
        
        # Перебираем все группы пользователя
        for group in full_groups:
            if not isinstance(group, dict):
                continue
            
            # Получаем значение поля из группы
            field_value = group.get(field_name)
            if not field_value:
                continue
            
            attr_str = str(field_value).lower()
            
            # Операторы сравнения
            if operator_lower in ['=', 'is']:
                if wildcard_match(attr_str, value_lower):
                    return True
            elif operator_lower == 'contains':
                if wildcard_contains(attr_str, value_lower):
                    return True
            elif operator_lower == 'in':
                values = [v.strip().strip('"').strip("'").lower() for v in value.split(',')]
                if attr_str in values:
                    return True
        
        return False
    
    # Обычная обработка для не-контактов
    attr_value = get_user_attribute(user, attribute)
    
    if attr_value is None:
        return False
    
    # Специальный оператор для проверки наличия значения
    if operator == 'has_value':
        # Проверяем, что значение не пустое
        if isinstance(attr_value, str):
            return bool(attr_value.strip())
        elif isinstance(attr_value, (list, dict)):
            return len(attr_value) > 0
        elif isinstance(attr_value, bool):
            return True  # Boolean всегда имеет значение
        elif isinstance(attr_value, (int, float)):
            return True  # Числа всегда имеют значение
        else:
            return attr_value is not None
    
    # Убираем кавычки из значения
    value = value.strip().strip('"').strip("'")
    
    # Специальная обработка для атрибута department - удаляем все пробелы
    if attribute.lower() == 'department':
        if isinstance(attr_value, str):
            attr_str = attr_value.replace(' ', '').lower()
        else:
            attr_str = str(attr_value).replace(' ', '').lower()
        value_lower = value.replace(' ', '').lower()
        operator_lower = operator.lower()
    else:
        # Преобразуем атрибут в строку для сравнения
        if isinstance(attr_value, bool):
            attr_str = str(attr_value).lower()
        elif isinstance(attr_value, (int, float)):
            attr_str = str(attr_value)
        elif isinstance(attr_value, list):
            # Для списков проверяем каждый элемент
            attr_str = None
        else:
            attr_str = str(attr_value).lower()
        
        value_lower = value.lower()
        operator_lower = operator.lower()
    
    # Операторы сравнения
    if operator_lower in ['=', 'is']:
        if isinstance(attr_value, list):
            # Для списков проверяем вхождение
            return any(wildcard_match(str(item).lower(), value_lower) for item in attr_value)
        else:
            return wildcard_match(attr_str, value_lower)
    
    elif operator_lower == 'contains':
        if isinstance(attr_value, list):
            # Для списков проверяем вхождение
            return any(wildcard_contains(str(item).lower(), value_lower) for item in attr_value)
        else:
            return wildcard_contains(attr_str, value_lower)
    
    elif operator_lower == 'in':
        # Проверка вхождения в список значений
        values = [v.strip().strip('"').strip("'").lower() for v in value.split(',')]
        if isinstance(attr_value, list):
            return any(str(item).lower() in values for item in attr_value)
        else:
            return attr_str in values
    
    return False


def execute_complex_query(users: list, query: str, settings: "SettingParams" = None) -> list:
    """
    Выполняет сложный запрос и возвращает список найденных пользователей.
    
    Args:
        users: Список всех пользователей
        query: Строка сложного запроса
        settings: Объект настроек приложения (опционально, для загрузки алиасов из файла)
    
    Returns:
        Список пользователей, соответствующих запросу
    """
    tokens = parse_complex_query(query)
    
    logger.debug(f"Сложный запрос: {query}")
    logger.debug(f"Токены: {tokens}")
    
    if not tokens:
        return []
    
    result = []
    
    # Список boolean атрибутов (в нормализованной форме)
    boolean_attrs = ['isAdmin', 'isEnabled']
    
    # Список операторов сравнения (включая операторы для дат)
    comparison_operators = ['=', 'is', 'contains', 'not', 'in', '<', '>', '<=', '>=', 'between']
    
    # Простой парсер для обработки запросов
    i = 0
    conditions = []
    logical_ops = []
    
    while i < len(tokens):
        # Пропускаем лишние операторы в начале
        if tokens[i].lower() in ['and', 'or']:
            i += 1
            continue
        
        try:
            # Проверяем на "not <boolean_attribute>"
            if tokens[i].lower() == 'not' and i + 1 < len(tokens):
                next_attr, attr_negate, contact_type, data_type = normalize_attribute_name(tokens[i + 1], settings)
                if next_attr in boolean_attrs:
                    # Это паттерн "not <boolean_attribute>"
                    # Инвертируем negate: если атрибут уже инвертирован, NOT отменяет инверсию
                    final_negate = not attr_negate
                    conditions.append((next_attr, '=', 'true', final_negate, contact_type, data_type))
                    i += 2
                    
                    # Проверяем наличие логического оператора
                    if i < len(tokens) and tokens[i].lower() in ['and', 'or']:
                        logical_ops.append(tokens[i].lower())
                        i += 1
                    continue
            
            # Нормализуем атрибут для проверки
            current_attr, attr_negate, contact_type, data_type = normalize_attribute_name(tokens[i], settings)
            
        except ValueError as e:
            # Неизвестный атрибут
            logger.error(f"Ошибка в запросе: {str(e)}")
            print(f"\n❌ {str(e)}")
            return []
        
        # Проверяем, является ли это одиночным boolean атрибутом
        if current_attr in boolean_attrs:
            # Проверяем, есть ли следующий токен и является ли он оператором
            if i + 1 < len(tokens):
                next_token = tokens[i + 1].lower()
                # Если следующий токен - логический оператор, значит это одиночный атрибут
                if next_token in ['and', 'or']:
                    conditions.append((current_attr, '=', 'true', attr_negate, contact_type, data_type))
                    i += 1
                    # Добавляем логический оператор
                    if i < len(tokens):
                        logical_ops.append(tokens[i].lower())
                        i += 1
                    continue
                # Если следующий токен - оператор сравнения, обрабатываем как обычно
                elif next_token not in comparison_operators:
                    # Это одиночный boolean атрибут в конце запроса
                    conditions.append((current_attr, '=', 'true', attr_negate, contact_type, data_type))
                    i += 1
                    continue
            else:
                # Это последний токен и это boolean атрибут
                conditions.append((current_attr, '=', 'true', attr_negate, contact_type, data_type))
                i += 1
                continue
        else:
            # Это не boolean атрибут, проверяем, идет ли дальше оператор
            if i + 1 < len(tokens):
                next_token = tokens[i + 1].lower()
                # Если следующий токен - логический оператор, значит это одиночный атрибут
                # Для не-boolean атрибутов проверяем наличие значения
                if next_token in ['and', 'or']:
                    conditions.append((current_attr, 'has_value', '', attr_negate, contact_type, data_type))
                    i += 1
                    # Добавляем логический оператор
                    if i < len(tokens):
                        logical_ops.append(tokens[i].lower())
                        i += 1
                    continue
                # Если следующий токен - не оператор сравнения
                elif next_token not in comparison_operators:
                    # Это одиночный атрибут в конце или перед чем-то другим
                    conditions.append((current_attr, 'has_value', '', attr_negate, contact_type, data_type))
                    i += 1
                    continue
            else:
                # Это последний токен и это не-boolean атрибут
                # Проверяем наличие значения
                conditions.append((current_attr, 'has_value', '', attr_negate, contact_type, data_type))
                i += 1
                continue
        
        # Ищем паттерн: attribute operator value
        # Специальная обработка для between (требует два значения)
        if i + 1 < len(tokens) and tokens[i + 1].lower() == 'between' and i + 3 < len(tokens):
            attribute = tokens[i]
            operator = tokens[i + 1]
            value1 = tokens[i + 2]
            value2 = tokens[i + 3]
            value = f"{value1} {value2}"
            i += 4
            
            # Нормализуем имя атрибута
            normalized_attribute, attr_negate, contact_type, data_type = normalize_attribute_name(attribute, settings)
            conditions.append((normalized_attribute, operator, value, attr_negate, contact_type, data_type))
            
            # Проверяем наличие логического оператора
            if i < len(tokens) and tokens[i].lower() in ['and', 'or']:
                logical_ops.append(tokens[i].lower())
                i += 1
        elif i + 2 < len(tokens):
            attribute = tokens[i]
            operator = tokens[i + 1]
            
            # Проверяем на NOT operator
            negate = False
            if operator.lower() == 'not':
                negate = True
                if i + 3 < len(tokens):
                    operator = tokens[i + 2]
                    value = tokens[i + 3]
                    i += 4
                else:
                    i += 1
                    continue
            else:
                value = tokens[i + 2]
                i += 3
            
            # Нормализуем имя атрибута (преобразуем алиас в реальное имя)
            normalized_attribute, attr_negate, contact_type, data_type = normalize_attribute_name(attribute, settings)
            # Комбинируем инверсию от алиаса и оператора NOT
            final_negate = negate != attr_negate  # XOR
            conditions.append((normalized_attribute, operator, value, final_negate, contact_type, data_type))
            
            # Проверяем наличие логического оператора
            if i < len(tokens) and tokens[i].lower() in ['and', 'or']:
                logical_ops.append(tokens[i].lower())
                i += 1
        # Проверяем паттерн: attribute operator (без значения для boolean)
        elif i + 1 < len(tokens):
            attribute = tokens[i]
            operator = tokens[i + 1]
            
            # Нормализуем атрибут
            normalized_attribute, attr_negate, contact_type, data_type = normalize_attribute_name(attribute, settings)
            
            # Если это boolean атрибут и оператор = или is, добавляем значение true
            if normalized_attribute in boolean_attrs and operator.lower() in ['=', 'is']:
                conditions.append((normalized_attribute, operator, 'true', attr_negate, contact_type, data_type))
                i += 2
                
                # Проверяем наличие логического оператора
                if i < len(tokens) and tokens[i].lower() in ['and', 'or']:
                    logical_ops.append(tokens[i].lower())
                    i += 1
            else:
                i += 1
        else:
            i += 1
    
    logger.debug(f"Созданные условия: {conditions}")
    logger.debug(f"Логические операторы: {logical_ops}")
    
    # Оцениваем условия для каждого пользователя
    for user in users:
        if not conditions:
            continue
        
        # Оцениваем первое условие
        attr, op, val, negate, contact_type, data_type = conditions[0]
        result_value = evaluate_condition(user, attr, op, val, contact_type, data_type)
        if negate:
            result_value = not result_value
        
        # Применяем остальные условия с логическими операторами
        for idx, (attr, op, val, negate, contact_type, data_type) in enumerate(conditions[1:], start=0):
            if idx < len(logical_ops):
                logical_op = logical_ops[idx]
                cond_result = evaluate_condition(user, attr, op, val, contact_type, data_type)
                if negate:
                    cond_result = not cond_result
                
                if logical_op == 'and':
                    result_value = result_value and cond_result
                elif logical_op == 'or':
                    result_value = result_value or cond_result
        
        if result_value:
            result.append(user)
    
    return result


def show_search_help():
    """
    Отображает справку по поиску пользователей.
    """
    print("\n=== Справка по поиску пользователей ===")
    print("Простой поиск: ID, часть логина, алиаса или фамилии (через пробел, запятую или точку с запятой)")
    print("  Поддерживается wildcard (*). Примеры: iva*, *nov, *петр*")
    print("\nСложный поиск: <атрибут> <оператор> <значение> [and|or <атрибут> <оператор> <значение>]")
    print("  Операторы: =, is, contains, not, in, <, >, <=, >=, between")
    print("\n  Допустимые атрибуты (и их алиасы):")
    print("    Основные:")
    print("      - id - ID пользователя")
    print("      - nickname (алиас) - логин/алиас")
    print("      - aliases (алиасы) - массив алиасов пользователя (поддержка =, contains, wildcard)")
    print("      - name.first (first_name, имя, first) - имя")
    print("      - name.last (last_name, фамилия, last) - фамилия")
    print("      - name.middle (middle_name, отчество, middle) - отчество")
    print("      - position (должность) - должность")
    print("      - department (подразделение, ou, dep) - название подразделения (пробелы игнорируются при сравнении)")
    print("      - departmentId - ID подразделения")
    print("      - gender - пол")
    print("      - language - язык")
    print("      - timezone - часовой пояс")
    print("      - about - информация о пользователе")
    print("      - birthday - дата рождения")
    print("    Атрибуты группы (поиск по всем группам пользователя):")
    print("      - GroupName (group_name, имя_группы) - имя группы (атрибут full_groups.name)")
    print("      - GroupAliases (group_aliases, алиасы_группы) - алиасы группы (поиск в full_groups.aliases и full_groups.label)")
    print("      Примечание: пользователь может быть в нескольких группах, поиск проверяет все группы")
    print("    Boolean атрибуты:")
    print("      - isAdmin (admin, админ, is_admin) - администратор")
    print("      - isEnabled (enabled, is_enabled) - активен")
    print("      - blocked / заблокирован = NOT isEnabled (инверсия)")
    print("    Контакты (поиск в структуре contacts):")
    print("      - телефон, phone, work_phone, mobile_phone - поиск телефонов")
    print("      - почта, mail, email - поиск email в контактах")
    print("    Даты (с операторами <, >, <=, >=, =, between):")
    print("      - createdAt (created, создан) - дата создания")
    print("      - updatedAt (updated, изменен) - дата изменения")
    print("      - isEnabledUpdatedAt (дата_блокировки) - дата блокировки")
    print("      - Форматы дат: DD.MM.YYYY, YYYY-MM-DD, относительные (-7d, 30д, 2w, -1м)")
    print("\n  Особенности:")
    print("    - Для boolean атрибутов без значения = проверка на true")
    print("    - Для не-boolean атрибутов без значения = проверка наличия значения")
    print("    - Для массивов (aliases) операторы = и contains работают одинаково")
    print("    - Неизвестные атрибуты будут отклонены с ошибкой")
    print("\n  Примеры:")
    print("    - admin              (все администраторы)")
    print("    - blocked            (все заблокированные)")
    print("    - middle             (все, у кого есть отчество)")
    print("    - position           (все, у кого указана должность)")
    print("    - aliases = test     (у кого есть алиас 'test')")
    print("    - aliases contains *admin* (у кого есть алиас содержащий 'admin')")
    print("    - phone contains 900 (все, у кого телефон содержит 900)")
    print("    - email = *@gmail.com (все с gmail почтой в контактах)")
    print("    - group_name = Admins (все пользователи, входящие в группу Admins)")
    print("    - group_aliases contains admin (пользователи, входящие в группу с алиасом 'admin')")
    print("    - group_name contains *dev* (пользователи групп, имя которых содержит 'dev')")
    print("    - created > -7d      (созданные за последние 7 дней)")
    print("    - updated < -30d     (не изменялись более 30 дней)")
    print("    - создан between -30d -7d (созданные от 30 до 7 дней назад)")
    print("    - enabled and admin  (активные администраторы)")
    print("    - должность contains manager")
    print("    - фамилия = *ов and админ")
    print("\nПоиск всех пользователей: * (звездочка)")
    print("Справка: ? (вопросительный знак)")
    print("Выход: пустая строка (Enter)")


def display_users_list(users_list, rows_per_page=20, page_number=1, sort_field='nickname', sort_order='asc', fields_config=None):
    """
    Отображает список пользователей в консоли с поддержкой пагинации, сортировки и настройки колонок.
    
    Args:
        users_list: Список пользователей для отображения
        rows_per_page: Количество строк на одной странице (по умолчанию 20)
        page_number: Номер страницы для отображения (по умолчанию 1)
        sort_field: Поле для сортировки (по умолчанию 'nickname')
                   Поддерживаются вложенные поля через точку, например 'name.first', 'name.last'
        sort_order: Порядок сортировки - 'asc' или 'desc' (по умолчанию 'asc')
        fields_config: Список полей для отображения в формате ['поле:заголовок:ширина:порядок', ...]
                      По умолчанию ['nickname:Логин:15:1', 'name.first:Имя:15:2', 'name.last:Фамилия:15:3', 'isAdmin:Админ:8:4', 'isEnabled:Активен:9:5']
                      Форматы:
                      - имя_поля:ширина:порядок (без заголовка, используется имя поля)
                      - имя_поля:заголовок:ширина:порядок (с заголовком)
                      - Если ширина меньше длины заголовка, используется длина заголовка
                      - Порядок определяет позицию колонки (можно опустить)
                      - Если значение не помещается в колонку, оно обрезается, последний символ заменяется на '…'
                      - Нулевая колонка "№" добавляется автоматически с порядковым номером
    
    Returns:
        None
    
    Примеры использования:
        # Отображение первой страницы с настройками по умолчанию
        display_users_list(users_to_add)
        
        # Отображение второй страницы с 10 строками, сортировка по фамилии
        display_users_list(users_to_add, rows_per_page=10, page_number=2, sort_field='name.last')
        
        # Сортировка по имени в обратном порядке
        display_users_list(users_to_add, sort_field='name.first', sort_order='desc')
        
        # Настройка отображаемых полей с указанием порядка (старый формат без заголовков)
        custom_fields = ['nickname:20:1', 'name.last:20:2', 'department:30:3', 'position:25:4']
        display_users_list(users_to_add, fields_config=custom_fields)
        
        # Настройка полей с пользовательскими заголовками (новый формат)
        custom_fields = ['nickname:Логин:20:1', 'name.last:Фамилия:20:2', 'department:Отдел:30:3', 'position:Должность:25:4']
        display_users_list(users_to_add, fields_config=custom_fields)
        
        # Показать всех пользователей на одной странице
        display_users_list(users_to_add, rows_per_page=1000)
    """
    if not users_list:
        print("Список пользователей пуст.")
        return
    
    # Настройка полей по умолчанию
    if fields_config is None:
        fields_config = ['nickname:Логин:15:1', 'name.first:Имя:15:2', 'name.last:Фамилия:15:3', 'isAdmin:Админ:8:4', 'isEnabled:Активен:9:5']
    
    # Парсинг конфигурации полей
    fields = []
    for idx, field_config in enumerate(fields_config):
        parts = field_config.split(':')
        if len(parts) >= 2:
            field_name = parts[0]
            field_header = None
            field_width = None
            field_order = idx + 1
            
            try:
                # Определяем формат: старый (поле:ширина:порядок) или новый (поле:заголовок:ширина:порядок)
                # Проверяем, является ли второй элемент числом
                try:
                    # Пытаемся преобразовать второй элемент в число
                    field_width = int(parts[1])
                    # Если успешно, это старый формат: поле:ширина[:порядок]
                    field_header = field_name  # Используем имя поля как заголовок
                    if len(parts) >= 3:
                        field_order = int(parts[2])
                except ValueError:
                    # Второй элемент не число, это новый формат: поле:заголовок:ширина[:порядок]
                    if len(parts) >= 3:
                        field_header = parts[1]
                        field_width = int(parts[2])
                        if len(parts) >= 4:
                            field_order = int(parts[3])
                    else:
                        logger.warning(f"Некорректный формат поля: {field_config}. В формате с заголовком нужно минимум 3 части: 'имя:заголовок:ширина'.")
                        continue
                
                # Если ширина меньше длины заголовка, используем длину заголовка
                field_width = max(field_width, len(field_header))
                
                fields.append({
                    'name': field_name,
                    'header': field_header,
                    'width': field_width,
                    'order': field_order
                })
            except ValueError as e:
                logger.warning(f"Некорректная конфигурация поля: {field_config}. Ошибка: {e}")
        else:
            logger.warning(f"Некорректный формат поля: {field_config}. Ожидается 'имя:ширина[:порядок]' или 'имя:заголовок:ширина[:порядок]'.")
    
    if not fields:
        print("Не указаны поля для отображения.")
        return
    
    # Сортировка полей по порядку
    fields.sort(key=lambda f: f['order'])
    
    # Функция для получения значения вложенного поля
    def get_nested_value(obj, field_path):
        """Получает значение вложенного поля, например 'name.first'"""
        keys = field_path.split('.')
        value = obj
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, '')
            else:
                return ''
        return value if value is not None else ''
    
    # Функция для получения значения для сортировки
    def get_sort_value(user):
        value = get_nested_value(user, sort_field)
        # Преобразуем значение для корректной сортировки
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, str):
            return value.lower()
        return value if value is not None else ''
    
    # Сортировка списка пользователей
    try:
        sorted_users = sorted(users_list, key=get_sort_value, reverse=(sort_order.lower() == 'desc'))
    except Exception as e:
        logger.error(f"Ошибка при сортировке по полю '{sort_field}': {e}")
        sorted_users = users_list
    
    # Расчет пагинации
    total_users = len(sorted_users)
    total_pages = (total_users + rows_per_page - 1) // rows_per_page  # Округление вверх
    
    # Проверка корректности номера страницы
    if page_number < 1:
        page_number = 1
    elif page_number > total_pages:
        page_number = total_pages if total_pages > 0 else 1
    
    # Определение диапазона пользователей для текущей страницы
    start_idx = (page_number - 1) * rows_per_page
    end_idx = min(start_idx + rows_per_page, total_users)
    page_users = sorted_users[start_idx:end_idx]
    
    # Функция для обрезки текста с добавлением многоточия
    def truncate_text(text, width):
        """Обрезает текст до указанной ширины, заменяя последний символ на '…' если необходимо"""
        text_str = str(text)
        if len(text_str) <= width:
            return text_str.ljust(width)
        else:
            return text_str[:width-1] + '…'
    
    # Расчет ширины колонки с номерами (минимум 3 символа для "№")
    max_number = start_idx + len(page_users)
    number_width = max(3, len(str(max_number)))
    
    # Построение заголовка таблицы
    header_parts = ['№'.ljust(number_width)]  # Нулевая колонка с порядковым номером
    separator_parts = ['-' * number_width]
    
    for field in fields:
        header_parts.append(field['header'].ljust(field['width']))
        separator_parts.append('-' * field['width'])
    
    header_line = ' | '.join(header_parts)
    separator_line = '-+-'.join(separator_parts)
    
    # Вычисление общей ширины таблицы
    # Ширина = ширина колонки с номерами + ширины всех полей + разделители " | "
    table_width = number_width + sum(field['width'] for field in fields) + 3 * len(fields)
    
    # Вывод информации о пагинации
    print(f"\n{'='*table_width}")
    print(f"Страница {page_number} из {total_pages} | Всего пользователей: {total_users} | Сортировка: {sort_field} ({sort_order})")
    print(f"{'='*table_width}")
    
    # Вывод заголовка
    print(header_line)
    print(separator_line)
    
    # Вывод строк пользователей
    for idx, user in enumerate(page_users, start=start_idx + 1):
        # Порядковый номер в нулевой колонке
        row_parts = [str(idx).ljust(number_width)]
        
        for field in fields:
            value = get_nested_value(user, field['name'])
            
            # Форматирование булевых значений
            if isinstance(value, bool):
                value = 'Да' if value else 'Нет'
            
            # Обрезка и форматирование значения
            formatted_value = truncate_text(value, field['width'])
            row_parts.append(formatted_value)
        
        row_line = ' | '.join(row_parts)
        print(row_line)
    
    # Вывод информации о диапазоне
    print(f"{'='*table_width}")
    print(f"Показаны пользователи {start_idx + 1}-{end_idx} из {total_users}")
    print(f"{'='*table_width}\n")


def load_fields_config_from_file(filename):
    """
    Загружает конфигурацию полей для отображения из файла.
    
    Args:
        filename: Имя файла с конфигурацией
    
    Returns:
        Список конфигураций полей в формате ['поле:заголовок:ширина:порядок', ...]
        или None если файл не существует или пуст
    
    Формат файла:
        Каждая строка содержит спецификацию одного поля в одном из форматов:
        - поле
        - поле:ширина
        - поле:ширина:порядок
        - поле:заголовок:ширина
        - поле:заголовок:ширина:порядок
        
        Пустые строки и строки начинающиеся с # игнорируются
    
    Пример содержимого файла fields_spec.txt:
        # Основные поля пользователя
        nickname:Логин:15:1
        name.first:Имя:15:2
        name.last:Фамилия:15:3
        isAdmin:Админ:8:4
        isEnabled:Активен:9:5
        # Дополнительные поля
        email:25
        department:Отдел:30
    """
    try:
        if not os.path.exists(filename):
            logger.debug(f"Файл конфигурации полей '{filename}' не найден. Используются настройки по умолчанию.")
            return None
        
        fields_config = []
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                
                # Пропускаем пустые строки и комментарии
                if not line or line.startswith('#'):
                    continue
                
                # Добавляем строку как есть, парсинг будет выполнен позже
                fields_config.append(line)
        
        if not fields_config:
            logger.debug(f"Файл конфигурации полей '{filename}' пуст. Используются настройки по умолчанию.")
            return None
        
        logger.info(f"Загружено {len(fields_config)} полей из файла '{filename}'")
        return fields_config
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке конфигурации полей из файла '{filename}': {e}")
        return None


def save_fields_config_to_file(filename, fields_config):
    """
    Сохраняет конфигурацию полей в файл.
    Проверяет, отличается ли новая конфигурация от существующей незакомментированной.
    Если конфигурация идентична - сохранение отменяется.
    Если есть отличия - существующие незакомментированные строки закомментируются,
    новые поля добавляются в конец файла.
    
    Args:
        filename: Имя файла с конфигурацией
        fields_config: Список конфигураций полей в формате ['поле:заголовок:ширина:порядок', ...]
    
    Returns:
        True если сохранение успешно, False если сохранение не требуется или произошла ошибка
    """
    try:
        import datetime
        
        # Читаем существующий файл (если есть)
        existing_lines = []
        existing_active_fields = []  # Незакомментированные поля из файла
        
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_lines = f.readlines()
                    
                # Извлекаем незакомментированные поля
                for line in existing_lines:
                    stripped = line.strip()
                    # Если строка не пустая и не является комментарием
                    if stripped and not stripped.startswith('#'):
                        existing_active_fields.append(stripped)
            except Exception as e:
                logger.warning(f"Не удалось прочитать существующий файл '{filename}': {e}")
        
        # Проверяем, совпадает ли новая конфигурация с существующей
        if existing_active_fields:
            # Нормализуем списки для сравнения (убираем лишние пробелы)
            existing_normalized = [field.strip() for field in existing_active_fields]
            new_normalized = [field.strip() for field in fields_config]
            
            # Сравниваем списки (порядок и содержимое)
            if existing_normalized == new_normalized:
                logger.info("Конфигурация полей не изменилась. Сохранение отменено.")
                return False
        
        # Конфигурация отличается или файл пустой - продолжаем сохранение
        logger.info("Обнаружены изменения в конфигурации полей. Сохранение...")
        
        # Формируем новое содержимое
        new_content = []
        
        # Обрабатываем существующие строки
        for line in existing_lines:
            stripped = line.strip()
            # Если строка не пустая и не является комментарием, закомментируем её
            if stripped and not stripped.startswith('#'):
                new_content.append(f"# {line.rstrip()}\n")
            else:
                # Пустые строки и комментарии оставляем как есть
                new_content.append(line)
        
        # Добавляем разделитель и новые поля
        if new_content:
            new_content.append("\n")
        else:
            # Если файл создаётся впервые, добавляем заголовок с описанием
            new_content.append("# Конфигурация полей для отображения списка пользователей\n")
            new_content.append("# Формат: поле:заголовок:ширина:позиция\n")
            new_content.append("# Пустые строки и строки начинающиеся с # игнорируются\n")
            new_content.append("\n")
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_content.append(f"# Сохранено: {timestamp}\n")
        
        # Добавляем новые поля
        for field in fields_config:
            new_content.append(f"{field}\n")
        
        # Записываем в файл
        with open(filename, 'w', encoding='utf-8') as f:
            f.writelines(new_content)
        
        logger.info(f"Конфигурация полей ({len(fields_config)} полей) сохранена в файл '{filename}'")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка при сохранении конфигурации полей в файл '{filename}': {e}")
        return False


def interactive_display_users(users_list, settings=None, rows_per_page=20, sort_field='nickname', sort_order='asc', fields_config=None):
    """
    Интерактивная функция-обёртка для управления отображением списка пользователей.
    
    Поддерживает команды для навигации, сортировки и настройки отображения.
    Команды можно вводить полностью или используя первые буквы ключевых слов.
    
    Конфигурация полей загружается с приоритетом:
    1. Из файла (по умолчанию fields_spec.txt, задаётся параметром DISPLAY_USERS_IN_CONSOLE_FIELDS в .env)
    2. Из параметра fields_config (если передан)
    3. Значения по умолчанию
    
    Args:
        users_list: Список пользователей для отображения
        settings: Объект настроек SettingParams (необходим для команды attrib)
        rows_per_page: Начальное количество строк на странице (по умолчанию 20)
        sort_field: Начальное поле для сортировки (по умолчанию 'nickname')
        sort_order: Начальный порядок сортировки - 'asc' или 'desc' (по умолчанию 'asc')
        fields_config: Начальная конфигурация полей для отображения (используется только если нет файла)
    
    Команды (полные и сокращённые):
        ?                                        - Справка (показать все команды и доступные поля)
        page <номер> (p <номер>)                 - Переход на страницу с указанным номером
        page size <размер> (p s <размер>)        - Установить размер страницы
        order by <поле> asc/desc (o b ...)       - Сортировка по указанному полю (asc по умолчанию)
        field add <поле> (f a <поле>)            - Добавить поле для вывода
        field del <имя> (f d <имя>)              - Удалить поле из списка
        attrib <номер> (a <номер>)               - Показать атрибуты пользователя по порядковому номеру
        w, ц, +                                  - Следующая страница
        s, ы, -                                  - Предыдущая страница
        Enter (пустая строка)                    - Выход из режима отображения
    
    Форматы команды field add:
        f a email               -> Добавить поле email (ширина 15, в конце списка)
        f a email:25            -> Добавить поле email шириной 25 (в конце списка)
        f a email:Email:25      -> Добавить поле email с заголовком Email шириной 25 (в конце)
        f a email:Email:25:6    -> Добавить поле email с заголовком Email шириной 25 на позицию 6
    
    Примеры других команд:
        p s 10                  -> page size 10
        p 2                     -> page 2
        o b nickname            -> order by nickname
        f d email               -> field del email
        a 5                     -> attrib 5
    """
    if not users_list:
        print("Список пользователей пуст.")
        return
    
    # Загрузка конфигурации полей из файла (если доступен)
    # Получаем имя файла из settings или используем значение по умолчанию
    fields_file = settings.display_users_fields_file if settings else "fields_spec.txt"
    loaded_fields_config = load_fields_config_from_file(fields_file)
    
    # Инициализация параметров
    current_page = 1
    current_rows_per_page = rows_per_page
    current_sort_field = sort_field
    current_sort_order = sort_order
    
    # Определяем конфигурацию полей с приоритетом:
    # 1. Загруженная из файла
    # 2. Переданная в параметре fields_config
    # 3. Значения по умолчанию
    if loaded_fields_config is not None:
        current_fields_config = loaded_fields_config
        logger.debug(f"Используется конфигурация полей из файла ({len(loaded_fields_config)} полей)")
    elif fields_config is not None:
        current_fields_config = fields_config
        logger.debug("Используется конфигурация полей из параметра функции")
    else:
        current_fields_config = ['nickname:Логин:15:1', 'name.first:Имя:15:2', 'name.last:Фамилия:15:3', 'isAdmin:Админ:8:4', 'isEnabled:Активен:9:5']
        logger.debug("Используется конфигурация полей по умолчанию")
    
    # Вывод справки при первом входе
    help_width = 80
    print("\n" + "="*help_width)
    print("ИНТЕРАКТИВНЫЙ РЕЖИМ ПРОСМОТРА ПОЛЬЗОВАТЕЛЕЙ")
    print("="*help_width)
    
    # Информация об источнике конфигурации
    if loaded_fields_config is not None:
        print(f"\n📄 Конфигурация полей загружена из файла: {fields_file}")
    else:
        print(f"\n📄 Используются поля по умолчанию (файл {fields_file} не найден)")
    
    print("\nДоступные команды:")
    print("  ?                                             - Справка (все команды и поля)")
    print("  page <номер>     (или p <номер>)              - Переход на страницу")
    print("  page size <размер> (или p s <размер>)         - Установить размер страницы")
    print("  order by <поле> asc/desc (или o b ...)        - Сортировка по полю")
    print("  field add <поле> (или f a <поле>)             - Добавить поле (ширина 15)")
    print("  field del <имя> (или f d <имя>)               - Удалить поле")
    print("  attrib <номер> (или a <номер>)                - Атрибуты пользователя")
    print("  w, ц, +                                       - Следующая страница")
    print("  s, ы, -                                       - Предыдущая страница")
    print("  Enter (пустая строка)                         - Выход")
    print("\n  💡 Можно использовать первые буквы команд (p s 10 вместо page size 10)")
    print("  💡 Форматы field add:")
    print("     - email              (ширина 15, в конце)")
    print("     - email:25           (ширина 25, в конце)")
    print("     - email:Email:25     (с заголовком, в конце)")
    print("     - email:Email:25:6   (с заголовком, позиция 6)")
    print(f"  💡 Для сохранения полей создайте файл {fields_file}")
    print("="*help_width + "\n")
    
    # Вычисляем общее количество страниц
    def get_total_pages():
        return max(1, (len(users_list) + current_rows_per_page - 1) // current_rows_per_page)
    
    # Функция для нормализации команды (преобразование сокращений в полные команды)
    def normalize_command(cmd):
        """
        Преобразует сокращённые команды в полные.
        Например: 'p s 10' -> 'page size 10', 'o b nickname' -> 'order by nickname'
        """
        parts = cmd.split()
        if len(parts) == 0:
            return cmd
        
        # Словарь сокращений для первого слова
        first_word_abbreviations = {
            'p': 'page',
            'o': 'order',
            'f': 'field',
            'a': 'attrib'
        }
        
        # Словарь сокращений для второго слова (зависит от первого слова)
        second_word_abbreviations = {
            'page': {'s': 'size'},
            'order': {'b': 'by'},
            'field': {'a': 'add', 'd': 'del'}
        }
        
        # Проверяем первое слово
        first_word = parts[0].lower()
        if first_word in first_word_abbreviations:
            parts[0] = first_word_abbreviations[first_word]
        
        # Проверяем второе слово (если есть) в контексте первого
        if len(parts) >= 2:
            first_normalized = parts[0].lower()
            second_word = parts[1].lower()
            
            if first_normalized in second_word_abbreviations:
                if second_word in second_word_abbreviations[first_normalized]:
                    parts[1] = second_word_abbreviations[first_normalized][second_word]
        
        return ' '.join(parts)
    
    # Функция для проверки существования поля в данных пользователей
    def check_field_exists(field_name):
        """
        Проверяет, существует ли поле в данных пользователей.
        Возвращает (exists, found_in_count, actual_field_name) где:
        - exists: True если поле найдено хотя бы у одного пользователя
        - found_in_count: количество пользователей, у которых найдено поле
        - actual_field_name: имя поля как оно записано в словаре users_list (или None если не найдено)
        
        Сравнение выполняется без учета регистра.
        """
        if not users_list:
            return False, 0, None
        
        def get_nested_value_case_insensitive(obj, field_path):
            """
            Получает значение вложенного поля, например 'name.first'
            Сравнение выполняется без учета регистра.
            Возвращает (value, actual_path) где actual_path - путь с оригинальными именами полей
            """
            keys = field_path.split('.')
            value = obj
            actual_keys = []
            
            for key in keys:
                if isinstance(value, dict):
                    # Ищем ключ без учета регистра
                    found_key = None
                    key_lower = key.lower()
                    for dict_key in value.keys():
                        if dict_key.lower() == key_lower:
                            found_key = dict_key
                            break
                    
                    if found_key:
                        actual_keys.append(found_key)
                        value = value[found_key]
                    else:
                        return None, None
                else:
                    return None, None
            
            actual_path = '.'.join(actual_keys)
            return value, actual_path
        
        found_count = 0
        actual_field_name = None
        
        for user in users_list:
            value, found_field_name = get_nested_value_case_insensitive(user, field_name)
            if value is not None:
                found_count += 1
                # Сохраняем имя поля как оно записано в словаре
                if actual_field_name is None:
                    actual_field_name = found_field_name
        
        return found_count > 0, found_count, actual_field_name
    
    # Функция для получения отсортированного списка пользователей
    def get_sorted_users():
        """Получает отсортированный список пользователей согласно текущим настройкам"""
        def get_nested_value(obj, field_path):
            """Получает значение вложенного поля, например 'name.first'"""
            keys = field_path.split('.')
            value = obj
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key, '')
                else:
                    return ''
            return value if value is not None else ''
        
        def get_sort_value(user):
            value = get_nested_value(user, current_sort_field)
            # Преобразуем значение для корректной сортировки
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, str):
                return value.lower()
            return value if value is not None else ''
        
        try:
            return sorted(users_list, key=get_sort_value, reverse=(current_sort_order.lower() == 'desc'))
        except Exception as e:
            logger.error(f"Ошибка при сортировке по полю '{current_sort_field}': {e}")
            return users_list
    
    # Основной цикл
    while True:
        # Отображение текущей страницы
        display_users_list(
            users_list,
            rows_per_page=current_rows_per_page,
            page_number=current_page,
            sort_field=current_sort_field,
            sort_order=current_sort_order,
            fields_config=current_fields_config
        )
        
        # Запрос команды от пользователя
        try:
            command = input("Команда: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход из режима просмотра.")
            # Сохранение конфигурации полей перед выходом
            save_result = save_fields_config_to_file(fields_file, current_fields_config)
            if save_result:
                print(f"✓ Конфигурация полей сохранена в файл: {fields_file}")
            else:
                print("ℹ️  Конфигурация полей не изменилась")
            break
        
        # Пустая строка - выход
        if not command:
            print("Выход из режима просмотра.")
            # Сохранение конфигурации полей перед выходом
            save_result = save_fields_config_to_file(fields_file, current_fields_config)
            if save_result:
                print(f"✓ Конфигурация полей сохранена в файл: {fields_file}")
            else:
                print("ℹ️  Конфигурация полей не изменилась")
            break
        
        # Команда справки
        if command == '?':
            help_width = 80
            print("\n" + "="*help_width)
            print("СПРАВКА ПО КОМАНДАМ")
            print("="*help_width)
            
            print("\nДоступные команды:")
            print("  ?                                             - Показать эту справку")
            print("  page <номер>     (или p <номер>)              - Переход на страницу")
            print("  page size <размер> (или p s <размер>)         - Установить размер страницы")
            print("  order by <поле> asc/desc (или o b ...)        - Сортировка по полю")
            print("  field add <поле> (или f a <поле>)             - Добавить поле (ширина 15)")
            print("  field del <имя> (или f d <имя>)               - Удалить поле")
            print("  attrib <номер> (или a <номер>)                - Атрибуты пользователя")
            print("  w, ц, +                                       - Следующая страница")
            print("  s, ы, -                                       - Предыдущая страница")
            print("  Enter (пустая строка)                         - Выход")
            
            print("\n  💡 Можно использовать первые буквы команд (p s 10 вместо page size 10)")
            print("  💡 Форматы field add:")
            print("     - email              (ширина 15, в конце)")
            print("     - email:25           (ширина 25, в конце)")
            print("     - email:Email:25     (с заголовком, в конце)")
            print("     - email:Email:25:6   (с заголовком, позиция 6)")
            print(f"  💡 Для сохранения полей создайте файл {fields_file}")
            
            # Сбор всех доступных полей из users_list
            def collect_all_fields(users_list):
                """Собирает все уникальные поля (включая вложенные) из списка пользователей"""
                all_fields = set()
                
                def collect_fields_recursive(obj, prefix=''):
                    """Рекурсивно собирает поля из объекта"""
                    if isinstance(obj, dict):
                        for key, value in obj.items():
                            field_path = f"{prefix}.{key}" if prefix else key
                            all_fields.add(field_path)
                            
                            # Если значение - словарь, рекурсивно обходим его
                            if isinstance(value, dict):
                                collect_fields_recursive(value, field_path)
                            # Если значение - список словарей, обходим первый элемент
                            elif isinstance(value, list) and value and isinstance(value[0], dict):
                                collect_fields_recursive(value[0], field_path)
                
                # Собираем поля из всех пользователей
                for user in users_list:
                    collect_fields_recursive(user)
                
                return sorted(all_fields)
            
            available_fields = collect_all_fields(users_list)
            
            # Проверяем наличие поля isEnabledUpdatedAt и добавляем, если его нет
            if 'isEnabledUpdatedAt' not in available_fields:
                available_fields.append('isEnabledUpdatedAt')
                available_fields.sort()
            
            print("\n" + "="*help_width)
            print("ДОСТУПНЫЕ ПОЛЯ ДЛЯ ИСПОЛЬЗОВАНИЯ")
            print("="*help_width)
            print("\nВсе поля можно использовать в командах order by и field add:")
            print()
            for field in available_fields:
                print(f"  - {field}")
            
            print("\n" + "="*help_width)
            print("Нажмите Enter для продолжения...")
            input()
            continue
        
        # Команды навигации по страницам (одиночные символы) - проверяются ДО нормализации
        if command in ['s', 'ы', '-']:
            # Следующая страница
            total_pages = get_total_pages()
            if current_page < total_pages:
                current_page += 1
            else:
                print(f"⚠️  Вы уже на последней странице ({total_pages}).")
            continue
        
        elif command in ['w', 'ц', '+']:
            # Предыдущая страница
            if current_page > 1:
                current_page -= 1
            else:
                print("⚠️  Вы уже на первой странице.")
            continue
        
        # Нормализация команды (преобразование сокращений в полные команды)
        command = normalize_command(command)
        
        # Обработка команд
        command_lower = command.lower()
        
        # Команда: page size <размер> (проверяется ПЕРЕД командой page)
        if command_lower.startswith('page size '):
            parts = command.split()
            if len(parts) == 3 and parts[2].isdigit():
                size = int(parts[2])
                if size > 0:
                    current_rows_per_page = size
                    current_page = 1  # Сброс на первую страницу
                    print(f"✓ Установлен размер страницы: {size}")
                else:
                    print("⚠️  Размер страницы должен быть больше 0")
            else:
                print("⚠️  Использование: page size <размер>")
            continue
        
        # Команда: page <номер>
        if command_lower.startswith('page '):
            parts = command.split()
            if len(parts) == 2 and parts[1].isdigit():
                page_num = int(parts[1])
                total_pages = get_total_pages()
                if 1 <= page_num <= total_pages:
                    current_page = page_num
                    print(f"✓ Переход на страницу {page_num}")
                else:
                    print(f"⚠️  Некорректный номер страницы. Доступны страницы: 1-{total_pages}")
            else:
                print("⚠️  Использование: page <номер>")
            continue
        
        # Команда: order by <поле> [asc|desc]
        if command_lower.startswith('order by '):
            parts = command.split(maxsplit=2)
            if len(parts) >= 3:
                field_and_order = parts[2].split()
                field = field_and_order[0]
                order = field_and_order[1].lower() if len(field_and_order) > 1 else 'asc'
                
                if order not in ['asc', 'desc']:
                    print("⚠️  Порядок сортировки должен быть 'asc' или 'desc'")
                else:
                    # Проверяем существование поля с учетом регистра
                    field_exists, found_count, actual_field_name = check_field_exists(field)
                    
                    if not field_exists:
                        print(f"⚠️  Поле '{field}' не найдено в данных пользователей")
                    else:
                        # Используем имя поля как оно записано в словаре users_list
                        current_sort_field = actual_field_name
                        current_sort_order = order
                        current_page = 1  # Сброс на первую страницу
                        print(f"✓ Установлена сортировка: {actual_field_name} ({order})")
            else:
                print("⚠️  Использование: order by <поле> [asc|desc]")
            continue
        
        # Команда: field add <поле[:заголовок][:ширина][:порядок]>
        if command_lower.startswith('field add '):
            parts = command.split(maxsplit=2)
            if len(parts) == 3:
                field_spec = parts[2]
                field_parts = field_spec.split(':')
                
                if len(field_parts) >= 1:
                    try:
                        field_name = field_parts[0]
                        field_header = None
                        field_width = None
                        field_order = len(current_fields_config) + 1
                        
                        # Определяем формат на основе количества частей
                        if len(field_parts) == 1:
                            # Формат: поле
                            # Используем значения по умолчанию
                            field_header = field_name
                            field_width = 15
                        elif len(field_parts) == 2:
                            # Пытаемся определить формат: поле:ширина
                            try:
                                field_width = int(field_parts[1])
                                field_header = field_name
                            except ValueError:
                                print("⚠️  Некорректный формат. Второй параметр должен быть числом (ширина).")
                                print("⚠️  Использование: field add <поле> или <поле:ширина> или <поле:заголовок:ширина>")
                                continue
                        elif len(field_parts) == 3:
                            # Формат: поле:ширина:порядок или поле:заголовок:ширина
                            try:
                                field_width = int(field_parts[1])
                                # Если успешно, это формат: поле:ширина:порядок
                                field_header = field_name
                                field_order = int(field_parts[2])
                            except ValueError:
                                # Второй элемент не число, это формат: поле:заголовок:ширина
                                field_header = field_parts[1]
                                field_width = int(field_parts[2])
                        elif len(field_parts) == 4:
                            # Формат: поле:заголовок:ширина:порядок
                            field_header = field_parts[1]
                            field_width = int(field_parts[2])
                            field_order = int(field_parts[3])
                        else:
                            print("⚠️  Слишком много параметров.")
                            print("⚠️  Использование: field add <поле> или <поле:ширина> или <поле:заголовок:ширина[:порядок]>")
                            continue
                        
                        # Проверка существования поля в данных пользователей
                        field_exists, found_count, actual_field_name = check_field_exists(field_name)
                        
                        if not field_exists:
                            print(f"⚠️  Поле '{field_name}' не найдено в данных пользователей и не будет добавлено.")
                        else:
                            # Поиск поля с учетом регистра в текущей конфигурации
                            # Если поле уже есть, удаляем его перед добавлением с новыми параметрами
                            field_name_lower = actual_field_name.lower()
                            existing_field_name = None
                            
                            for field_config in current_fields_config:
                                config_field_name = field_config.split(':')[0]
                                if config_field_name.lower() == field_name_lower:
                                    existing_field_name = config_field_name
                                    break
                            
                            if existing_field_name:
                                # Удаляем существующее поле
                                current_fields_config = [f for f in current_fields_config if f.split(':')[0] != existing_field_name]
                                print(f"⚠️  Поле '{existing_field_name}' уже существовало и будет обновлено")
                            
                            # Используем имя поля как оно записано в словаре users_list
                            # Формируем строку конфигурации в зависимости от наличия заголовка
                            if field_header == field_name:
                                # Формат без заголовка
                                new_field = f"{actual_field_name}:{field_width}:{field_order}"
                            else:
                                # Формат с заголовком
                                new_field = f"{actual_field_name}:{field_header}:{field_width}:{field_order}"
                            
                            current_fields_config.append(new_field)
                            print(f"✓ Добавлено поле: {actual_field_name} (заголовок: {field_header}, ширина: {field_width}, порядок: {field_order})")
                    except ValueError as e:
                        print(f"⚠️  Некорректный формат: {e}")
                        print("⚠️  Использование: field add <поле> или <поле:ширина> или <поле:заголовок:ширина[:порядок]>")
                else:
                    print("⚠️  Некорректный формат. Укажите хотя бы имя поля.")
                    print("⚠️  Использование: field add <поле> или <поле:ширина> или <поле:заголовок:ширина[:порядок]>")
            else:
                print("⚠️  Использование: field add <поле> или <поле:ширина> или <поле:заголовок:ширина[:порядок]>")
            continue
        
        # Команда: field del <имя>
        if command_lower.startswith('field del '):
            parts = command.split(maxsplit=2)
            if len(parts) == 3:
                field_name = parts[2]
                
                # Поиск поля с учетом регистра
                # Извлекаем имена полей из текущей конфигурации и ищем совпадение
                field_name_lower = field_name.lower()
                actual_field_name = None
                
                for field_config in current_fields_config:
                    config_field_name = field_config.split(':')[0]
                    if config_field_name.lower() == field_name_lower:
                        actual_field_name = config_field_name
                        break
                
                if actual_field_name:
                    # Удаляем поле с правильным именем
                    original_count = len(current_fields_config)
                    current_fields_config = [f for f in current_fields_config if f.split(':')[0] != actual_field_name]
                    
                    if len(current_fields_config) < original_count:
                        print(f"✓ Удалено поле: {actual_field_name}")
                    
                    # Проверка, что остался хотя бы один столбец
                    if not current_fields_config:
                        print("⚠️  Невозможно удалить все поля. Восстановлены настройки по умолчанию.")
                        current_fields_config = ['nickname:Логин:15:1', 'name.first:Имя:15:2', 'name.last:Фамилия:15:3', 'isAdmin:Админ:8:4', 'isEnabled:Активен:9:5']
                else:
                    print(f"⚠️  Поле '{field_name}' не найдено в списке")
            else:
                print("⚠️  Использование: field del <имя>")
            continue
        
        # Команда: attrib <номер>
        if command_lower.startswith('attrib '):
            if settings is None:
                print("⚠️  Команда недоступна: не указан объект настроек (settings)")
                continue
            
            parts = command.split()
            if len(parts) == 2 and parts[1].isdigit():
                user_number = int(parts[1])
                
                # Получаем отсортированный список пользователей
                sorted_users = get_sorted_users()
                
                # Проверяем корректность номера (нумерация с 1)
                if 1 <= user_number <= len(sorted_users):
                    user = sorted_users[user_number - 1]
                    user_id = user.get('id')
                    
                    if user_id:
                        print(f"\n{'='*80}")
                        print(f"Получение атрибутов пользователя #{user_number}: {user.get('nickname', 'N/A')}")
                        print(f"{'='*80}\n")
                        
                        # Вызов функции показа атрибутов без сохранения в файл
                        show_one_user_attributes(settings, user_id, departments=None, save_to_file=False)
                        
                        print(f"\n{'='*80}")
                        print("Нажмите Enter для продолжения...")
                        print(f"{'='*80}")
                        input()
                    else:
                        print(f"⚠️  Не удалось получить ID пользователя #{user_number}")
                else:
                    print(f"⚠️  Некорректный номер пользователя. Доступны номера: 1-{len(sorted_users)}")
            else:
                print("⚠️  Использование: attrib <номер>")
            continue
        
        # Неизвестная команда
        print(f"⚠️  Неизвестная команда: '{command}'")
        print("Введите Enter для выхода или используйте доступные команды.")


def find_users_prompt(settings: "SettingParams"):
    print("\n=== Поиск пользователей ===")
    print("\nПоиск всех пользователей: * (звездочка)")
    print("Справка: ? (вопросительный знак)")
    print("Выход: пустая строка (Enter)")
    
    while True:
        double_users_flag = False
        
        answer = input("\nИскать: ")

        # Обработка специальных команд
        if not answer.strip():
            # Пустая строка - выход из цикла
            logger.info("Выход из режима поиска пользователей.")
            return
        
        if answer.strip() == "?":
            # Показать справку и продолжить
            show_search_help()
            continue
        
        if answer.strip() == "*":
            # Поиск всех пользователей
            answer = ""

        users = get_extended_api360_users(settings)
        if not users:
            logger.info("No users found in Y360 organization.")
            print("❌ Пользователи не найдены в организации Y360.")
            continue

        # Определяем тип запроса
        if is_complex_query(answer):
            # Сложный поиск
            logger.info("Обнаружен сложный запрос поиска")
            try:
                users_to_add = execute_complex_query(users, answer, settings)
                if users_to_add:
                    logger.info(f"Найдено пользователей: {len(users_to_add)}")
                    for user in users_to_add:
                        logger.debug(f"User found: {user['nickname']} ({user['id']})")
                else:
                    logger.error("Пользователи не найдены по заданным критериям")
                    continue
            except Exception as e:
                logger.error(f"Ошибка при выполнении сложного запроса: {e}")
                continue
        else:
            # Простой поиск (существующая логика)
            logger.info("Выполняется простой запрос поиска")
            
            # Если пустая строка после обработки "*", получить всех пользователей
            if not answer.strip():
                users_to_add = users
                logger.info(f"Получены все пользователи: {len(users_to_add)}")
            else:
                pattern = r'[;,\s]+'
                search_users = re.split(pattern, answer)
                users_to_add = []

                for searched in search_users:
                    if "@" in searched.strip():
                        searched = searched.split("@")[0]
                    found_flag = False
                    
                    # Проверка на ID - wildcards не поддерживаются
                    searched_cleaned = searched.strip().replace('*', '')
                    if all(char.isdigit() for char in searched_cleaned):
                        # Если в поиске по ID используется wildcard, выводим ошибку
                        if '*' in searched.strip():
                            logger.error(f"Wildcard (*) не может быть использован при поиске по ID: {searched}")
                            continue
                            
                        if len(searched.strip()) == 16 and searched.strip().startswith("113"):
                            for user in users:
                                if user['id'] == searched.strip():
                                    logger.debug(f"User found: {user['nickname']} ({user['id']})")
                                    users_to_add.append(user)
                                    found_flag = True
                                    break

                    else:
                        found_last_name_user = []
                        for user in users:
                            aliases_lower_case = [r.lower() for r in user['aliases']]
                            
                            # Проверка nickname с поддержкой wildcard
                            if wildcard_match(user['nickname'], searched.strip()):
                                logger.debug(f"User found: {user['nickname']} ({user['id']})")
                                users_to_add.append(user)
                                found_flag = True
                                break
                            
                            # Проверка алиасов с поддержкой wildcard
                            for alias in aliases_lower_case:
                                if wildcard_match(alias, searched.strip()):
                                    logger.debug(f"User found: {user['nickname']} ({user['id']})")
                                    users_to_add.append(user)
                                    found_flag = True
                                    break
                            
                            if found_flag:
                                break
                            
                            # Проверка фамилии с поддержкой wildcard
                            if wildcard_contains(user['name']['last'], searched.strip()):
                                found_last_name_user.append(user)
                                
                        if not found_flag and found_last_name_user:
                            if len(found_last_name_user) == 1:
                                logger.debug(f"User found ({searched}): {found_last_name_user[0]['nickname']} ({found_last_name_user[0]['id']}, {found_last_name_user[0]['position']})")
                                users_to_add.append(found_last_name_user[0])
                                found_flag = True
                            else:
                                logger.error(f"User {searched} found more than one user:")
                                for user in found_last_name_user:
                                    logger.error(f" - last name {user['name']['last']}, nickname {user['nickname']} ({user['id']}, {user['position']})")
                                logger.error("Refine your search parameters.")
                                double_users_flag = True
                                break

                    if not found_flag:
                        logger.error(f"User {searched} not found in Y360 organization.")

        # Если были найдены дублирующиеся пользователи, продолжить запрос
        if double_users_flag:
            continue
        
        # Сохранить результаты поиска в файлы
        if users_to_add:
            logger.info("Выгрузка атрибутов найденных пользователей в файл.")
            download_users_attrib_to_file(settings, users_to_add)
            download_users_attrib_to_file_short(settings, users_to_add, answer if answer else "*")
            print(f"✅ Найдено пользователей: {len(users_to_add)}. Результаты сохранены в файл.")
            
            # Отображение списка пользователей в консоли
            interactive_display_users(users_to_add, settings)
        else:
            print("❌ Пользователи не найдены.")
        
        # Продолжить цикл для следующего запроса

def test_add_aliases_prompt(settings: "SettingParams"):
    """
    Интерактивная функция для тестирования добавления алиасов пользователям.
    """
    print("\n=== Тестирование добавления алиасов пользователям ===")
    
    # Получаем логин пользователя
    login = input("Введите логин пользователя: ").strip()
    if not login:
        print("Логин не может быть пустым.")
        return
    
    # Ищем пользователя
    found, user = find_user_by_login(settings, login)
    if not found:
        print(f"Пользователь с логином '{login}' не найден.")
        return
    
    print(f"Найден пользователь: {user['nickname']} (ID: {user['id']})")
    print(f"Текущие алиасы: {user.get('aliases', [])}")
    
    # Получаем алиас для добавления
    alias = input("Введите алиас для добавления: ").strip()
    if not alias:
        print("Алиас не может быть пустым.")
        return
    
    # Валидируем алиас
    is_valid, validated_alias = validate_alias(alias)
    if not is_valid:
        print(f"Некорректный алиас: {validated_alias}")
        return
    
    print(f"Валидация пройдена. Алиас: '{validated_alias}'")
    
    # Подтверждение
    confirm = input(f"Добавить алиас '{validated_alias}' пользователю {user['nickname']}? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes', 'да']:
        print("Операция отменена.")
        return
    
    # Добавляем алиас
    print("Добавляем алиас...")
    success, response_data = create_user_alias_by_api(settings, user['id'], validated_alias)
    
    if success:
        print(f"✅ Алиас '{validated_alias}' успешно добавлен пользователю {user['nickname']}")
        print(f"Ответ API: {response_data}")
    else:
        print(f"❌ Не удалось добавить алиас '{validated_alias}' пользователю {user['nickname']}")

def delete_users_from_list(settings: "SettingParams", users_to_delete: list):
    """
    Удаляет список пользователей через API.
    
    Args:
        settings: Параметры настроек
        users_to_delete: Список пользователей для удаления
    
    Returns:
        tuple: (success_count: int, failed_count: int)
    """
    if not users_to_delete:
        logger.error("Список пользователей для удаления пуст.")
        return 0, 0
    
    logger.info("-" * 100)
    logger.info(f"Начинаем удаление {len(users_to_delete)} пользователей...")
    logger.info("-" * 100)
    
    success_count = 0
    failed_count = 0
    
    for user in users_to_delete:
        user_id = user.get('id')
        nickname = user.get('nickname', 'N/A')
        name = f"{user.get('name', {}).get('last', '')} {user.get('name', {}).get('first', '')}".strip()
        
        logger.info(f"Удаление пользователя: {nickname} ({name}, ID: {user_id})")
        
        success, response_data = delete_user_by_api(settings, user_id)
        
        if success:
            success_count += 1
            logger.info(f"✓ Пользователь {nickname} успешно удален.")
        else:
            failed_count += 1
            logger.error(f"✗ Не удалось удалить пользователя {nickname}.")
        
        # Небольшая пауза между запросами
        time.sleep(SLEEP_TIME_BETWEEN_API_CALLS)
    
    logger.info("-" * 100)
    logger.info(f"Удаление завершено. Успешно: {success_count}, Ошибок: {failed_count}")
    logger.info("-" * 100)
    
    return success_count, failed_count

def delete_users_from_file(settings: "SettingParams", file_name: str):
    """
    Читает данные пользователей из CSV файла и удаляет их.
    
    Args:
        settings: Параметры настроек
        file_name: Имя CSV файла с пользователями для удаления
    
    Returns:
        bool: True если удаление прошло успешно
    """
    if not os.path.exists(file_name):
        full_path = os.path.join(os.path.dirname(__file__), file_name)
        if not os.path.exists(full_path):
            logger.error(f'Ошибка! Файл {file_name} не существует!')
            return False
        else:
            file_name = full_path
    
    logger.info("-" * 100)
    logger.info(f'Чтение пользователей из файла {file_name}')
    logger.info("-" * 100)
    
    users_to_delete = []
    all_api_users = get_all_api360_users(settings, force=False)
    
    if not all_api_users:
        logger.error("Не удалось получить список пользователей из API 360.")
        return False
    
    try:
        with open(file_name, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=';')
            line_number = 0
            
            for row in csv_reader:
                line_number += 1
                
                # Пропускаем строки с комментариями
                if row.get('id', '').startswith('#') or row.get('login', '').startswith('#'):
                    logger.debug(f'Строка #{line_number} начинается с "#". Пропуск.')
                    continue
                
                # Получаем id или login из файла
                user_id = row.get('id', '').strip().strip('"')
                login = row.get('login', '').strip().strip('"')
                
                if not user_id and not login:
                    logger.warning(f'Строка #{line_number}: не указаны ни id, ни login. Пропуск.')
                    continue
                
                # Ищем пользователя в API
                found_user = None
                
                if user_id:
                    # Поиск по ID
                    found_user = next((u for u in all_api_users if u['id'] == user_id), None)
                
                if not found_user and login:
                    # Поиск по логину
                    login_clean = login.split('@')[0] if '@' in login else login
                    found_user = next((u for u in all_api_users if u['nickname'].lower() == login_clean.lower()), None)
                
                if found_user:
                    users_to_delete.append(found_user)
                    logger.debug(f"Строка #{line_number}: найден пользователь {found_user['nickname']} (ID: {found_user['id']})")
                else:
                    logger.warning(f"Строка #{line_number}: пользователь с id={user_id} или login={login} не найден в организации.")
    
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return False
    
    if not users_to_delete:
        logger.warning("Не найдено пользователей для удаления в файле.")
        return False
    
    # Показываем список пользователей для подтверждения
    logger.info("\n")
    logger.info("=" * 100)
    logger.info(f"Найдено {len(users_to_delete)} пользователей для удаления:")
    logger.info("=" * 100)
    
    for idx, user in enumerate(users_to_delete, 1):
        name = f"{user.get('name', {}).get('last', '')} {user.get('name', {}).get('first', '')}".strip()
        logger.info(f"{idx}. {user['nickname']} - {name} (ID: {user['id']})")
    
    logger.info("=" * 100)
    
    # Запрашиваем подтверждение
    if len(users_to_delete) == 1:
        prompt = "одного пользователя"
    elif len(users_to_delete) > 4:
        prompt = f"{len(users_to_delete)} пользователей"
    else:
        prompt = f"{len(users_to_delete)} пользователя"
    confirm = input(f"\nВы уверены, что хотите удалить {prompt}? Введите число удаляемых пользователей для подтверждения: ").strip().lower()
    
    if confirm != str(len(users_to_delete)):
        logger.info("Число удаляемых пользователей не совпадает с введенным числом. Отмена удаления.")
        return False
    
    # Выполняем удаление
    success_count, failed_count = delete_users_from_list(settings, users_to_delete)
    
    return failed_count == 0

def delete_users_prompt(settings: "SettingParams"):
    """
    Интерактивная функция для удаления пользователей.
    Позволяет ввести данные пользователей вручную или загрузить из файла.
    """
    print("\n")
    print("=" * 100)
    print("УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЕЙ")
    print("=" * 100)
    print("\n")
    print("Введите данные пользователей для удаления:")
    print("  - ID, логин, алиас или часть фамилии (несколько значений через запятую)")
    print("  - Или нажмите Enter для загрузки пользователей для удаления из файла")

    use_force_flag = False

    while True:
        print("\n")
        user_input = input("Данные пользователей (или Enter для файла, минус ('-') для выхода): ").strip()
        
        users_to_delete = []

        if user_input == "-":
            break
        
        if not user_input:
            # Пользователь выбрал загрузку из файла
            print("\n")
            print(f"Файл по умолчанию: {settings.users_file}")
            file_input = input(f"Введите имя файла (или Enter для использования '{settings.users_file}'), минус ('-') для выхода: ").strip()
            
            if file_input == "-":
                break
            
            if not file_input:
                file_name = settings.users_file
            else:
                file_name = file_input
            
            # Удаляем из файла
            if delete_users_from_file(settings, file_name):
                break
            else:
                continue
        
        # Пользователь ввел данные вручную
        logger.info("-" * 100)
        logger.info("Поиск пользователей для удаления...")
        logger.info("-" * 100)
        
        # Разбираем ввод
        pattern = r'[;,\s]+'
        search_terms = re.split(pattern, user_input)
        
        all_api_users = get_all_api360_users(settings, force=True)
        
        if not all_api_users:
            logger.error("Не удалось получить список пользователей из API 360.")
            break

        found_multiple = False
        
        while True:
            for search_term in search_terms:
                if not search_term.strip():
                    continue
                
                search_term = search_term.strip()
                found_user = None
                found_by_lastname = []
                
                # Убираем домен если есть
                if "@" in search_term:
                    search_term = search_term.split("@")[0]
                
                # Поиск по ID (16 символов, начинается с 113)
                if all(char.isdigit() for char in search_term):
                    if len(search_term) == 16 and search_term.startswith("113"):
                        found_user = next((u for u in all_api_users if u["id"] == search_term), None)
                        if found_user:
                            logger.debug(f"Пользователь найден по ID: {found_user['nickname']} ({found_user['id']})")
                            if found_user not in users_to_delete:
                                users_to_delete.append(found_user)
                                continue
                            else:
                                continue
                
                # Поиск по логину или алиасу
                if not found_user:
                    for user in all_api_users:
                        aliases_lower = [a.lower() for a in user.get("aliases", [])]
                        if user["nickname"].lower() == search_term.lower() or search_term.lower() in aliases_lower:
                            found_user = user
                            logger.debug(f"Пользователь найден по логину/алиасу: {user['nickname']} ({user['id']})")
                        
                        # Собираем совпадения по фамилии
                        if user["name"]["last"].lower().startswith(search_term.lower()):
                            found_by_lastname.append(user)
                
                # Если не найден по логину/алиасу, проверяем фамилии
                if not found_user and found_by_lastname:
                    if len(found_by_lastname) == 1:
                        found_user = found_by_lastname[0]
                        logger.debug(f"Пользователь найден по фамилии: {found_user['nickname']} ({found_user['id']})")
                    else:
                        logger.error(f"По запросу '{search_term}' найдено несколько пользователей:")
                        for user in found_by_lastname:
                            logger.error(f"  - {user['name']['last']} {user['name']['first']}, {user['nickname']} (ID: {user['id']}, {user.get('position', 'N/A')})")
                        logger.error("Уточните параметры поиска.")
                        found_multiple = True
                        continue
                
                if found_user:
                    if found_user not in users_to_delete:
                        users_to_delete.append(found_user)
                else:
                    if not found_multiple:
                        logger.error(f"Пользователь '{search_term}' не найден в организации.")
            
            if found_multiple:
                logger.error("Найдено несколько совпадений. Удаление отменено.")
                break
            
            if not users_to_delete:
                logger.warning("Не найдено пользователей для удаления.")
                if not use_force_flag:
                    all_api_users = get_all_api360_users(settings, force=True)
                    use_force_flag = True
                else:
                    break
            else:
                break
        
        if users_to_delete:
            # Показываем список пользователей для подтверждения
            logger.info("\n")
            logger.info("=" * 100)
            logger.info(f"Найдено {len(users_to_delete)} пользователей для удаления:")
            logger.info("=" * 100)
            
            for idx, user in enumerate(users_to_delete, 1):
                name = f"{user.get('name', {}).get('last', '')} {user.get('name', {}).get('first', '')}".strip()
                logger.info(f"{idx}. {user['nickname']} - {name} (ID: {user['id']})")
            
            logger.info("=" * 100)
            
            # Запрашиваем подтверждение
            if len(users_to_delete) == 1:
                prompt = "одного пользователя"
            elif len(users_to_delete) > 4:
                prompt = f"{len(users_to_delete)} пользователей"
            else:
                prompt = f"{len(users_to_delete)} пользователя"
            confirm = input(f"\nВы уверены, что хотите удалить {prompt}? Введите число удаляемых пользователей для подтверждения: ").strip().lower()
            
            if confirm != str(len(users_to_delete)):
                logger.info("Число удаляемых пользователей не совпадает с введенным числом. Отмена удаления.")
                continue
            else:
                # Выполняем удаление
                delete_users_from_list(settings, users_to_delete)
                break
        
        return True

def main_menu(settings: "SettingParams"):

    while True:
        print("\n")
        print("Выберите опцию:")
        print("1. Добавить пользователей из файла.")
        print("2. Обновить сотрудников из файла.")
        print("3. Анализировать входной файл для создания пользователей на ошибки.")
        print("4. Поиск подразделения по названию или алиасу.")
        print("5. Показать атрибуты пользователя.")
        print("6. Выгрузить всех или выбранных пользователей в файл.")
        print("7. Создать общие ящики из файла.")
        # print("3. Delete all contacts.")
        # print("4. Output bad records to file")
        print("0. (Ctrl+C) Выход")
        print("\n")
        choice = input("Введите ваш выбор (0-7): ")

        if choice == "0":
            print("До свидания!")
            break
        elif choice == "1":
            print('\n')
            add_users_from_file(settings)
        elif choice == "2":
            print('\n')
            update_users_from_file(settings)
        elif choice == "3":
            print('\n')
            add_users_from_file(settings, analyze_only=True )
        elif choice == "4":
            search_department_prompt(settings)
        elif choice == "5":
            show_user_attributes_prompt(settings)
        elif choice == "6":
            download_users_attrib_to_file_prompt(settings)
        elif choice == "7":
            import_shared_mailboxes_prompt(settings)
        elif choice == "666":
            print('\n')
            delete_users_prompt(settings)
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
    
    