from aiogram import Bot, Dispatcher
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.state import State, StatesGroup
import asyncio
import re
import datetime
import os
import json
import difflib
import traceback
from dotenv import load_dotenv

class QRStates(StatesGroup):
    waiting_for_qr_text = State()

class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"

LOG_DIR = "logs"
QR_CODE_DIR = "qrcodes"
LOG_FILE = os.path.join(LOG_DIR, f"bot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
USER_DATA_FILE = "user_data.json"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(QR_CODE_DIR, exist_ok=True)

def log_message(message_type, user_id=None, username=None, action=None, details=None):
    current_time = datetime.datetime.now().strftime("%H:%M:%S")
    
    color_code = Colors.RESET
    if message_type == "ERROR":
        color_code = Colors.RED
    elif message_type == "WARNING":
        color_code = Colors.YELLOW
    elif message_type == "COMMAND":
        color_code = Colors.GREEN
    elif message_type == "MESSAGE":
        color_code = Colors.CYAN
    elif message_type == "INFO":
        color_code = Colors.WHITE
    elif message_type == "SYSTEM":
        color_code = Colors.MAGENTA
    
    user_str = f"{user_id}" if user_id else "---"
    
    username_str = username or "---"
    
    log_str = f"{current_time} {color_code}{message_type.ljust(7)}{Colors.RESET}"
    log_str += f" | {user_str.ljust(10)} | {username_str.ljust(12)}"
    
    if action:
        log_str += f" | {action}"
    
    if details:
        log_str += f": {details}"
    
    print(log_str)
    
    file_log = {
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "type": message_type,
        "user_id": user_id,
        "username": username,
        "action": action,
        "details": details
    }
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(file_log, ensure_ascii=False) + "\n")

def load_all_user_data():
    global user_data
    try:
        if os.path.exists(USER_DATA_FILE):
            with open(USER_DATA_FILE, "r", encoding="utf-8") as f:
                data_from_file = json.load(f)
                user_data = {int(k): v for k, v in data_from_file.items()}
                for uid in user_data:
                    if 'qr_codes' not in user_data[uid]:
                        user_data[uid]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
                    if 'next_qr_id' not in user_data[uid]['qr_codes']: 
                         user_data[uid]['qr_codes']['next_qr_id'] = 1
                    if 'codes' not in user_data[uid]['qr_codes']:
                         user_data[uid]['qr_codes']['codes'] = []

                log_message("SYSTEM", action="Загрузка данных", details=f"Данные пользователей загружены из {USER_DATA_FILE}")
        else:
            user_data = {}
            log_message("SYSTEM", action="Загрузка данных", details=f"Файл {USER_DATA_FILE} не найден, используется пустая база.")
    except (json.JSONDecodeError, IOError) as e:
        user_data = {}
        log_message("ERROR", action="Загрузка данных", details=f"Ошибка загрузки {USER_DATA_FILE}: {e}. Используется пустая база.")

def save_all_user_data():
    global user_data
    try:
        data_to_save = {str(k): v for k, v in user_data.items()}
        with open(USER_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)
    except IOError as e:
        log_message("ERROR", action="Сохранение данных", details=f"Ошибка сохранения в {USER_DATA_FILE}: {e}")

def log_user_state(user_id):
    if user_id not in user_data:
        log_message("DEBUG", user_id, action="Состояние пользователя", details="Данные отсутствуют")
        return
    
    state = user_data[user_id]
    count = state.get('count', 0)
    values = state.get('values', {})
    
    log_message("DEBUG", user_id, action="Состояние", 
               details=f"Сообщений: {count}, Категорий: {len(values)}")
    
    if values:
        values_str = ", ".join([f"{k}={v}" for k, v in values.items()])
        log_message("DEBUG", user_id, action="Значения", details=values_str)

load_dotenv()

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

user_data = {}

def get_keyboard():
    buttons = [
        [KeyboardButton(text="📝 Новый подсчет")],
        [KeyboardButton(text="🔄 Очистить")],
        [KeyboardButton(text="❓ Инструкция")],
        [KeyboardButton(text="🖼️ QR Коды")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    return keyboard

def get_qr_keyboard():
    buttons = [
        [KeyboardButton(text="➕ Создать QR")],
        [KeyboardButton(text="📋 Список QR")],
        [KeyboardButton(text="🗑️ Удалить QR")],
        [KeyboardButton(text="⬅️ Назад")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    return keyboard

def create_progress_bar(current, total, length=10):
    filled = int(length * current / total)
    return '█' * filled + '▒' * (length - filled)

def parse_line(line):
    if ':' in line:
        parts = line.split(':')
        separator = ':'
    elif '-' in line:
        parts = line.split('-')
        separator = '-'
    else:
        log_message("DEBUG", action="Парсинг строки", 
                  details=f"Не найден разделитель в строке: {line}")
        return None, None

    if len(parts) >= 2:
        original_name = parts[0].strip()
        name = ' '.join(parts[0].split())
        
        if original_name != name:
            log_message("DEBUG", action="Нормализация имени", 
                      details=f"'{original_name}' -> '{name}'")
        
        value_part = parts[1].strip()
        
        try:
            value = int(value_part)
            return name, value
        except ValueError:
            numbers = re.findall(r'-?\d+', value_part)
            if numbers:
                value = int(numbers[-1])
                log_message("DEBUG", action="Извлечение числа из текста", 
                          details=f"Из '{value_part}' получено: {value}")
                return name, value
            else:
                log_message("WARNING", action="Парсинг строки", 
                          details=f"Не удалось извлечь число из: {value_part}")
    else:
        log_message("WARNING", action="Парсинг строки", 
                  details=f"Некорректный формат строки: {line}")
    
    return None, None

def string_similarity(s1, s2):
    return difflib.SequenceMatcher(None, s1, s2).ratio()

def remove_trailing_letters(text):
    return re.sub(r'(\d+[.,]?\d*)[а-яА-Яa-zA-Z]+\b', r'\1', text)

def normalize_category_name(name):
    normalized = ' '.join(name.split())
    normalized = remove_trailing_letters(normalized)
    return normalized

def find_similar_category(name, values, similarity_threshold=0.9):
    normalized_name = normalize_category_name(name)
    
    for existing_name in values.keys():
        if normalize_category_name(existing_name) == normalized_name:
            log_message("DEBUG", action="Поиск похожей категории", details=f"Найдено точное совпадение нормализованных имен: '{name}' -> '{existing_name}'")
            return existing_name
    
    best_match = None
    highest_similarity = 0.0

    for existing_name in values.keys():
        similarity = string_similarity(normalized_name, normalize_category_name(existing_name))
        if similarity >= similarity_threshold and similarity > highest_similarity:
            highest_similarity = similarity
            best_match = existing_name
            
    if best_match:
        log_message("DEBUG", action="Поиск похожей категории", details=f"Найдена похожая категория: '{name}' -> '{best_match}' с схожестью {highest_similarity:.2f}")
        return best_match
    
    log_message("DEBUG", action="Поиск похожей категории", details=f"Похожих категорий для '{name}' не найдено (порог {similarity_threshold}). Создается новая.")
    return name

import handlers

async def main():
    try:
        log_message("SYSTEM", action="Бот запущен", details="Начало работы")
        
        bot_token = os.getenv("BOT_TOKEN")
        if not bot_token:
            log_message("ERROR", action="Ошибка конфигурации", details="BOT_TOKEN не найден в переменных окружения. Убедитесь, что вы создали файл .env с BOT_TOKEN=<ВАШ_ТОКЕН>")
            print(f"{Colors.RED}Ошибка: BOT_TOKEN не найден. Пожалуйста, создайте файл .env и добавьте BOT_TOKEN=<ВАШ_ТОКЕН>{Colors.RESET}")
            return
        
        load_all_user_data()

        log_message("SYSTEM", action="Конфигурация", 
                   details=f"Файл логов: {LOG_FILE}")
        log_message("SYSTEM", action="Лимиты", 
                   details=f"Максимум сообщений в цикле: 6")
        
        await dp.start_polling(bot)
    except Exception as e:
        log_message("ERROR", action="Ошибка в работе бота", details=str(e))
        tb = traceback.format_exc()
        print(f"{Colors.RED}Подробности ошибки:{Colors.RESET}\n{tb}")
    finally:
        log_message("SYSTEM", action="Бот остановлен", details="Завершение работы")
        save_all_user_data()
        await bot.session.close()

if __name__ == '__main__':
    print(f"\n{Colors.BOLD}{Colors.GREEN}==== Бот для подсчета сумм ===={Colors.RESET}")
    print(f"{Colors.CYAN}Запуск: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}\n")
    
    log_message("SYSTEM", action="Инициализация", details="Запуск скрипта")
    asyncio.run(main())