from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
import asyncio
import re
import datetime
import os
import json
import difflib

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
LOG_FILE = os.path.join(LOG_DIR, f"bot_{datetime.datetime.now().strftime('%Y%m%d')}.log")

os.makedirs(LOG_DIR, exist_ok=True)

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
        if len(details) > 30:
            log_str += f": {details[:27]}..."
        else:
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

bot = Bot(token="7813948080:AAGH0qdzgzJdWYl80wYiSp5omPcm95zIOYo")
dp = Dispatcher()

user_data = {}

def get_keyboard():
    buttons = [
        [KeyboardButton(text="📝 Новый подсчет")],
        [KeyboardButton(text="🔄 Очистить")],
        [KeyboardButton(text="❓ Инструкция")]
    ]
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    return keyboard

@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    log_message("COMMAND", user_id, username, action="Выполнена команда /start")
    
    await message.reply(
        "Привет! Я бот для подсчета сумм. Используйте кнопки ниже для управления:\n\n"
        "📝 Новый подсчет - начать новый цикл подсчета\n"
        "🔄 Очистить - удалить последнее сообщение и вычесть его значения из общей суммы\n\n"
        "Отправляйте мне сообщения в формате:\nНазвание - число",
        reply_markup=get_keyboard()
    )

@dp.message(lambda message: message.text == "🔄 Очистить")
async def clear_command(message: types.Message):
    try:
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.first_name
        
        log_message("COMMAND", user_id, username, action="Нажата кнопка 'Очистить'")
        
        if user_id in user_data:
            if user_data[user_id]['count'] > 0:
                user_data[user_id]['count'] -= 1
                
                if 'last_message' in user_data[user_id]:
                    last_message = user_data[user_id]['last_message']
                    lines = last_message.split('\n')
                    
                    log_message("DEBUG", user_id, username, action="Удаление сообщения", 
                               details=f"Разбор {len(lines)} строк")
                    
                    removed_values = []
                    
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if not line:
                            continue
                        
                        log_message("DEBUG", user_id, username, action="Удаление строки", 
                                   details=f"Строка {i+1}: {line}")
                        
                        name, value = parse_line(line)
                        if name and value is not None:
                            similar_category = find_similar_category(name, user_data[user_id]['values'])
                            
                            if similar_category != name:
                                log_message("DEBUG", user_id, username, action="Похожая категория для удаления", 
                                           details=f"'{name}' похожа на '{similar_category}'")
                                name = similar_category
                            
                            if name in user_data[user_id]['values']:
                                old_value = user_data[user_id]['values'][name]
                                
                                user_data[user_id]['values'][name] -= value
                                
                                log_message("DEBUG", user_id, username, action="Вычитание значения", 
                                           details=f"{name}: {old_value} - {value} = {user_data[user_id]['values'][name]}")
                                
                                removed_values.append(f"{name}: {value}")
                            else:
                                log_message("WARNING", user_id, username, action="Пропуск строки при удалении", 
                                           details=f"Не найдено соответствующее значение: {line}")
                        else:
                            log_message("WARNING", user_id, username, action="Пропуск строки при удалении", 
                                       details=f"Не удалось разобрать: {line}")
                    
                    del user_data[user_id]['last_message']
                    
                    log_message("INFO", user_id, username, action="Удалено последнее сообщение", 
                               details=f"Удалены значения: {', '.join(removed_values) if removed_values else 'нет'}")
                    
                    log_user_state(user_id)
                    
                    msg_count = user_data[user_id]['count']
                    if msg_count == 0:
                        user_data[user_id]['values'] = {}
                        await message.reply("Последнее сообщение удалено. История пуста. Начните новый подсчет.", reply_markup=get_keyboard())
                    else:
                        progress_bar = create_progress_bar(msg_count, 6)
                        response = f"{progress_bar} ({msg_count}/6)\n\n"
                        for name, value in user_data[user_id]['values'].items():
                            response += f"{name} - {value}\n"
                        await message.reply(response, reply_markup=get_keyboard())
                else:
                    log_message("INFO", user_id, username, action="Попытка удаления", 
                               details="Нет данных о последнем сообщении")
                    
                    await message.reply("Нет данных о последнем сообщении для удаления.", reply_markup=get_keyboard())
            else:
                log_message("INFO", user_id, username, action="Попытка удаления", 
                           details="История пуста")
                
                await message.reply("История пуста! Нечего удалять.", reply_markup=get_keyboard())
        else:
            log_message("INFO", user_id, username, action="Попытка удаления", 
                       details="Пользователь не найден в базе")
            
            await message.reply("История пуста! Нечего удалять.", reply_markup=get_keyboard())
    
    except Exception as e:
        import traceback
        log_message("ERROR", user_id, username, action="Ошибка при удалении", 
                   details=str(e))
        
        tb = traceback.format_exc()
        print(f"{Colors.RED}Ошибка при очистке данных:{Colors.RESET}\n{tb}")
        
        await message.reply("Произошла ошибка при удалении данных. Пожалуйста, попробуйте еще раз.", 
                           reply_markup=get_keyboard())

@dp.message(lambda message: message.text == "📝 Новый подсчет")
async def new_count(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    log_message("COMMAND", user_id, username, action="Нажата кнопка 'Новый подсчет'")
    
    user_data[user_id] = {
        'count': 0,
        'values': {}
    }
    
    log_message("INFO", user_id, username, action="Начат новый подсчет", 
               details="Данные пользователя сброшены")
    
    log_user_state(user_id)
    
    await message.reply(
        "Начат новый подсчет!\nОтправьте мне данные в формате:\nНазвание - число",
        reply_markup=get_keyboard()
    )

@dp.message(lambda message: message.text == "❓ Инструкция")
async def show_instructions(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    log_message("COMMAND", user_id, username, action="Нажата кнопка 'Инструкция'")
    
    instructions = (
        "Инструкция по использованию бота:\n\n"
        "📝 Новый подсчет - начать новый цикл подсчета\n"
        "🔄 Очистить - удалить последнее сообщение и вычесть его значения из общей суммы\n\n"
        "Отправляйте мне сообщения в формате:\n"
        "Название - число\n\n"
        "Бот автоматически суммирует значения по категориям и умеет распознавать похожие названия "
        "(например, \"АТТ ПБ экзотик 0,25\" и \"АТТ ПБ экзотик 0,25л\" будут считаться одной категорией).\n\n"
        "Нулевые значения сохраняются и отображаются для всех категорий.\n\n"
        "Максимальное количество сообщений в одном цикле - 6."
    )
    await message.reply(instructions, reply_markup=get_keyboard())

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

@dp.message()
async def process_message(message: types.Message):
    try:
        if message.text == "❓ Инструкция":
            return
            
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.first_name
        
        log_message("MESSAGE", user_id, username, action="Получено сообщение", 
                   details=f"Текст: {message.text}")
        
        if user_id not in user_data:
            user_data[user_id] = {
                'count': 0,
                'values': {}
            }
            log_message("INFO", user_id, username, action="Создан новый пользователь", 
                       details="Инициализированы данные")
        
        user_data[user_id]['count'] += 1
        
        user_data[user_id]['last_message'] = message.text
        
        lines = message.text.split('\n')
        log_message("DEBUG", user_id, username, action="Разбор сообщения", 
                   details=f"Количество строк: {len(lines)}")
        
        parsed_values = []
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            log_message("DEBUG", user_id, username, action="Обработка строки", 
                       details=f"Строка {i+1}: {line}")
            
            name, value = parse_line(line)
            if name and value is not None:
                similar_category = find_similar_category(name, user_data[user_id]['values'])
                
                if similar_category != name:
                    log_message("DEBUG", user_id, username, action="Похожая категория", 
                               details=f"'{name}' похожа на '{similar_category}'")
                    name = similar_category
                
                old_value = user_data[user_id]['values'].get(name, 0)
                if name in user_data[user_id]['values']:
                    user_data[user_id]['values'][name] += value
                    log_message("DEBUG", user_id, username, action="Обновление значения", 
                               details=f"{name}: {old_value} + {value} = {user_data[user_id]['values'][name]}")
                else:
                    user_data[user_id]['values'][name] = value
                    log_message("DEBUG", user_id, username, action="Новое значение", 
                               details=f"{name}: {value}")
                parsed_values.append(f"{name}: {value}")
            else:
                log_message("WARNING", user_id, username, action="Пропуск строки", 
                           details=f"Не удалось разобрать: {line}")
        
        if parsed_values:
            log_message("INFO", user_id, username, action="Обработаны значения", 
                       details=", ".join(parsed_values))
        else:
            log_message("WARNING", user_id, username, action="Не удалось обработать сообщение", 
                       details="Неверный формат")
        
        log_user_state(user_id)
        
        msg_count = user_data[user_id]['count']
        
        is_final_message = (msg_count == 6)
        
        if is_final_message:
            response = ""
            for name, value in user_data[user_id]['values'].items():
                response += f"{name} - {value}\n"
        else:
            progress_bar = create_progress_bar(msg_count, 6)
            response = f"{progress_bar} ({msg_count}/6)\n\n"
            for name, value in user_data[user_id]['values'].items():
                response += f"{name} - {value}\n"
        
        if msg_count == 6:
            log_message("INFO", user_id, username, action="Достигнут лимит сообщений", 
                       details="6 из 6")
        
        if msg_count > 6:
            log_message("INFO", user_id, username, action="Превышен лимит сообщений", 
                       details="Начат новый цикл")
            
            user_data[user_id] = {
                'count': 1,
                'values': {}
            }
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                name, value = parse_line(line)
                if name and value is not None:
                    similar_category = find_similar_category(name, user_data[user_id]['values'])
                    
                    if similar_category != name:
                        log_message("DEBUG", user_id, username, action="Похожая категория (новый цикл)", 
                                   details=f"'{name}' похожа на '{similar_category}'")
                        name = similar_category
                    
                    user_data[user_id]['values'][name] = value
                    log_message("DEBUG", user_id, username, action="Новое значение (новый цикл)", 
                               details=f"{name}: {value}")
            
            log_user_state(user_id)
            
            progress_bar = create_progress_bar(1, 6)
            response = f"{progress_bar} (1/6)\n\n"
            for name, value in user_data[user_id]['values'].items():
                response += f"{name} - {value}\n"
        
        await message.reply(response, reply_markup=get_keyboard())
    
    except Exception as e:
        import traceback
        log_message("ERROR", user_id, username, action="Ошибка обработки сообщения", 
                   details=str(e))
        
        tb = traceback.format_exc()
        print(f"{Colors.RED}Ошибка при обработке сообщения:{Colors.RESET}\n{tb}")
        
        await message.reply("Произошла ошибка при обработке вашего сообщения. Пожалуйста, попробуйте еще раз или начните новый подсчет.", 
                           reply_markup=get_keyboard())

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
            return existing_name
    
    for existing_name in values.keys():
        similarity = string_similarity(normalized_name, normalize_category_name(existing_name))
        if similarity >= similarity_threshold:
            if len(existing_name) <= len(name):
                return existing_name
            else:
                values[name] = values[existing_name]
                del values[existing_name]
                return name
    
    return name

async def main():
    try:
        log_message("SYSTEM", action="Бот запущен", details="Начало работы")
        
        log_message("SYSTEM", action="Конфигурация", 
                   details=f"Файл логов: {LOG_FILE}")
        log_message("SYSTEM", action="Лимиты", 
                   details=f"Максимум сообщений в цикле: 6")
        
        await dp.start_polling(bot)
    except Exception as e:
        log_message("ERROR", action="Ошибка в работе бота", details=str(e))
        import traceback
        tb = traceback.format_exc()
        print(f"{Colors.RED}Подробности ошибки:{Colors.RESET}\n{tb}")
    finally:
        log_message("SYSTEM", action="Бот остановлен", details="Завершение работы")
        await bot.session.close()

if __name__ == '__main__':
    print(f"\n{Colors.BOLD}{Colors.GREEN}==== Бот для подсчета сумм ===={Colors.RESET}")
    print(f"{Colors.CYAN}Запуск: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}\n")
    
    log_message("SYSTEM", action="Инициализация", details="Запуск скрипта")
    asyncio.run(main()) 