import os
import re
from dotenv import load_dotenv
import logging
import logging.handlers as handlers
import time
import requests
from typing import Any, Dict, Optional
import datetime

LOG_FILE = "add_users.log"

logger = logging.getLogger("users360.log")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
#file_handler = handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30, encoding='utf-8')
file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024,  backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)
logger.addHandler(file_handler)

class API360:
    def __init__(self, org_id, access_token):
        self.url = f"https://api360.yandex.net/directory/v1/org/{org_id}"

        self.headers = {
            "Authorization": f"OAuth {access_token}"
        }

        self.org_id = org_id

        self.per_page = 100

    def post_create_users(self, user_info: dict, dry_run: bool = False):
        """
        Creating the new user with the provided dict
        :param users_info: list of the dicts with the new user info
        :return: displays success or error message
        """
       
        logger.info(f"Creating user {user_info['nickname']}")
        logger.debug(f'Input data: {user_info}')
        if dry_run:
            logger.info(f"DRY_RUN is True. User {user_info['nickname']} was created virtually")
            return True
        
        status, response = self.make_request("POST", f"{self.url}/users", json=user_info) 
        if response.status_code == 200:
            logger.debug(f"Response: {response.text}")
            logger.info(f"User {user_info['nickname']} was created successfully")
        logger.debug("." * 100)

    def make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
        ) -> Any:
        """
        Make HTTP request with retry logic and error handling.

        Args:
            session: Requests session to use
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: URL parameters
            data: Form data
            json: JSON data
            max_retries: Maximum number of retries
            retry_delay: Delay between retries in seconds
            
        Returns:
            Response data as dictionary
            
        Raises:
            APIError: For API-related errors
            AuthenticationError: For authentication failures
            RateLimitError: When rate limited
            ConnectionError: For network issues
        """ 
        retries = 0
        while retries <= max_retries:
            try:
                if method == "GET":
                    response = requests.get(
                        url=url,
                        headers=self.headers
                    )
                elif method == "POST":
                    response = requests.post(
                        url=url,
                        headers=self.headers,
                        json=json
                    )
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    logger.warning(f"Rate limited. Retrying after {retry_after} seconds")
                    time.sleep(retry_after)
                    retries += 1
                    continue

                if response.status_code == 400:
                    logger.error(f"Request failed: {response.text}")
                    return False, None
                    
                # Check for auth issues
                if response.status_code in (401, 403):
                    logger.error(f"Authentication failed {response.status_code}: {response.text}")
                    return False, None               
            
                # Return the JSON response
                return True,response
                
            except Exception as e:
                logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                if retries < max_retries:
                    logger.warning(f"Request failed. Retrying ({retries+1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                    retries += 1
                else:
                    logger.error(f"Request failed after {max_retries} retries: {str(e)}")
                    return False, None

def read_file_data():
    data = []
    with open('users.csv', 'r', encoding='utf-8') as csvfile:
        for line in csvfile:
            data.append(line.strip().split(';'))
    return data

def add_users_from_file(analyze_only=False):
    users_file_name = os.environ.get('USERS_FILE_NAME')
    if not os.path.exists(users_file_name):
        full_path = os.path.join(os.path.dirname(__file__), users_file_name)
        if not os.path.exists(full_path):
            logger.error(f'ERROR! Input file {users_file_name} not exist!')
            return
        else:
            users_file_name = full_path
    
    ## Another way to read file with needed transfromations
    headers = []
    data = []
    try:
        logger.info("-" *100)
        logger.info(f'Reading file {users_file_name}')
        logger.info("-" *100)
        with open(users_file_name, 'r', encoding='utf-8') as csvfile:
            headers = csvfile.readline().replace('"', '').split(";")
            logger.debug(f'Headers: {headers}')
            for line in csvfile:
                logger.debug(f'Reading from file line - {line}')
                fields = line.replace('"','').split(";")
                entry = {}
                for i,value in enumerate(fields):
                    entry[headers[i].strip()] = value.strip()
                data.append(entry)
        logger.info(f'End reading file {users_file_name}')
        logger.info("\n")
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    correct_lines = []
    suspiciose_lines = []
    error_lines = []
    stop_adding = False
    line_number = 0

    logger.info("-" *100)
    logger.info(f'Verifying records.')
    logger.info("-" *100)
    for element in data:
        entry = {}
        correct = True
        stop_adding = False
        line_number += 1
        logger.debug(f'Processing line #{line_number} {element}')
        try:
            temp_login = element["login"].lower()
            if temp_login:
                if '@' in temp_login:
                    temp_login = element["login"].split('@')[0]
                if not validate_login(temp_login):
                    correct = False
                    logger.error(f'Line #{line_number}. Possible incorrect login _"{temp_login}"_')
                entry["login"] = temp_login
            else:
                stop_adding = True
                logger.error(f'Line #{line_number}. Login is empty. Stopping adding users.')

            temp_firest_name = element["first_name"]
            if temp_firest_name:
                if not validate_name(temp_firest_name):
                    correct = False
                    logger.warning(f'Line #{line_number}. Possible incorrect first name _"{temp_firest_name}"_')
                entry["first"] = temp_firest_name
            else:
                stop_adding = True
                logger.error(f'Line #{line_number}. First name is empty. Stopping adding users.')

            temp_last_name = element["last_name"]
            if temp_last_name:
                if not validate_name(temp_last_name):
                    correct = False
                    logger.warning(f'Line #{line_number}. Possible incorrect last name _"{temp_last_name}"_')
                entry["last"] = temp_last_name
            else:
                stop_adding = True
                logger.error(f'Line #{line_number}. Last name is empty. Stopping adding users.')

            temp_middle_name = element["middle_name"]
            if temp_middle_name:
                if not validate_name(temp_middle_name):
                    correct = False
                    logger.warning(f'Line #{line_number}. Possible incorrect middle name _"{temp_middle_name}"_')
            entry["middle"] = temp_middle_name

            temp_password = element["password"]
            if temp_password:
                entry["password"] = temp_password
            else:
                stop_adding = True
                logger.error(f'Line #{line_number}. Password is empty. Stopping adding users.')

            temp_password_change_requered = element["password_change_required"].lower()
            if temp_password_change_requered not in ['true', 'false']:
                stop_adding = True
                logger.error(f'Line #{line_number}. Invalid password change required _"{temp_password_change_requered}"_. Must be true or false. Stopping adding users.')
            entry["password_change_requered"] = temp_password_change_requered

            temp_langusge = element["language"].lower()
            if temp_langusge not in ['ru', 'en']:
                stop_adding = True
                logger.error(f'Line #{line_number}. Invalid language _"{temp_langusge}"_. Must be ru or en. Stopping adding users.')
            entry["language"] = temp_langusge

            temp_gender = element["gender"].lower()
            if temp_gender not in ['male', 'female']:
                stop_adding = True
                logger.error(f'Line #{line_number}. Invalid gender _"{temp_gender}"_. Must be male or female. Stopping adding users.')
            entry["gender"] = temp_gender

            temp_birthday = element["birthday"]
            if temp_birthday:
                check_date, date_value = is_valid_date(temp_birthday)
                if not check_date:
                    stop_adding = True
                    logger.error(f'Line #{line_number}. Invalid birthday _"{temp_birthday}"_ ({date_value}). Stopping adding users.')
                else:
                    entry["birthday"] = date_value.strftime('%Y-%m-%d')

            temp_position = element["position"]

            temp_work_phone = element["work_phone"]
            if temp_work_phone:
                check_phone, phone_value = validate_phone_number(temp_work_phone)
                if not check_phone:
                    stop_adding = True
                    logger.error(f'Line #{line_number}. Invalid work phone _"{temp_work_phone}"_. Stopping adding users.')
            entry["work_phone"] = temp_work_phone

            temp_mobile_phone = element["mobile_phone"]
            if temp_mobile_phone:
                check_phone, phone_value = validate_phone_number(temp_mobile_phone)
                if not check_phone:
                    stop_adding = True
                    logger.error(f'Line #{line_number}. Invalid mobile phone _"{temp_mobile_phone}"_. Stopping adding users.')
            entry["mobile_phone"] = temp_mobile_phone


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

    logger.info(f'End verifying records.')
    logger.info("\n")

    if len(error_lines) > 0:
        logger.error('!' * 100)
        logger.error(f'Bad lines in file. Correct it and try again.')
        logger.error('!' * 100)
        for e in error_lines:
            logger.error(f'Bad line: {e}')
            logger.error("." * 100)
        logger.error(f'Exiting.')
        logger.error('\n')
        return
    
    if len(suspiciose_lines) > 0:
        logger.warning('*' * 100)
        logger.warning(f'There are {len(suspiciose_lines)} suspiciose lines. Check cyrillic letters in this fields: login, first_name, last_name, middle_name')
        logger.warning('*' * 100)
        for element in suspiciose_lines:
            logger.warning(f'login: {element['login']}; first_name: {element['first_name']}; last_name: {element['last_name']}; middle_name: {element['middle_name']}')
            logger.warning("." * 100)
        logger.warning('\n')
        if not analyze_only:
            answer = input("Continue to import? (Y/n): ")
            if answer.upper() not in ["Y", "YES"]:
                return
    
    if analyze_only:
        if len(suspiciose_lines) == 0 and len(error_lines) == 0:
            logger.info('*' * 100)
            logger.info(f'All lines are correct.')
            logger.info('*' * 100)
            return 
    
    logger.info("-" * 100)
    logger.info(f'Adding {len(correct_lines)} users to Y360.')
    logger.info("-" * 100)
    user = {}
    for u in correct_lines:
        user["departmentId"] = u.get('department_id',1) 
        user["name"] = {
            "first": u.get('first'),
            "last": u.get('last'),
            "middle": u.get('middle')
        }
        user["nickname"] = u.get('login')
        user["password"] = u.get('password')
        user["passwordChangeRequired"] = u.get('password_change_requered')
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

        if os.environ.get('DRY_RUN').lower() == 'true':
            dry_run = True  
        else:
            dry_run = False
        organization.post_create_users(user, dry_run=dry_run)
    return
    
    
# Регулярное выражение для проверки фамилии

def validate_name(line):
    pattern = r'^[А-ЯЁ][а-яё]+(-[А-ЯЁ][а-яё]+)?$'
    if re.match(pattern, line):
        return True
    return False

def validate_login(line):
    pattern = r'^[a-z0-9.-]+$'
    if not re.match(pattern, line):
        return False
    if line.startswith('_'):
        return False
    return True

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
    current_date = datetime.date.today()
    for date_format in date_formats:
        try:
            date_obj = datetime.datetime.strptime(date_string, date_format).date()

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



def main_menu():

    while True:
        print(" ")
        print("Select option:")
        print("1. Add users from file.")
        print("2. Analyze input file for errors.")
        # print("3. Delete all contacts.")
        # print("4. Output bad records to file")
        print("0. Exit")

        choice = input("Enter your choice (0-2): ")

        if choice == "0":
            print("Goodbye!")
            break
        elif choice == "1":
            print('\n')
            add_users_from_file()
        elif choice == "2":
            print('\n')
            add_users_from_file( analyze_only=True )
        # elif choice == "3":
        #     delete_all_contacts()
        # elif choice == "4":
        #     analyze_data = add_contacts_from_file(True)
        #     OutputBadRecords(analyze_data)
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    denv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(denv_path):
        load_dotenv(dotenv_path=denv_path,verbose=True, override=True)
    
    organization = API360(os.environ.get('orgId'), os.environ.get('access_token'))
    
    main_menu()