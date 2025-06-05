from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio
import re
import datetime
import os
import json
import difflib
import qrcode
from io import BytesIO
import traceback

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

bot = Bot(token="7813948080:AAGH0qdzgzJdWYl80wYiSp5omPcm95zIOYo")
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

@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    if user_id not in user_data:
        user_data[user_id] = {
            'count': 0,
            'values': {},
            'qr_codes': {'next_qr_id': 1, 'codes': []}
        }
        log_message("INFO", user_id, username, action="Создан новый пользователь", 
                   details="Инициализированы данные, включая раздел QR")
    elif 'qr_codes' not in user_data[user_id]:
        user_data[user_id]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
        log_message("INFO", user_id, username, action="Обновление пользователя", 
                   details="Добавлен раздел QR для существующего пользователя")

    if 'qr_codes' not in user_data[user_id]:
        user_data[user_id]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
    if 'next_qr_id' not in user_data[user_id]['qr_codes']:
        user_data[user_id]['qr_codes']['next_qr_id'] = 1
    if 'codes' not in user_data[user_id]['qr_codes']:
        user_data[user_id]['qr_codes']['codes'] = []

    save_all_user_data()

    log_message("COMMAND", user_id, username, action="Выполнена команда /start")
    
    await message.reply(
        "Привет! Я бот для помощи в подсчете сумм и QR-кодов. Используйте кнопки ниже для управления:\n\n"
        "📝 Новый подсчет - начать новый цикл подсчета\n"
        "🔄 Очистить - удалить последнее сообщение и вычесть его значения из общей суммы\n\n"
        "🖼️ QR Коды - перейти в раздел управления QR-кодами.\n\n"
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
                    
                    msg_count = user_data[user_id]['count']
                    if msg_count == 0:
                        user_data[user_id]['values'] = {}

                    log_user_state(user_id)
                    save_all_user_data() 
                    if msg_count == 0:
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
        log_message("ERROR", user_id, username, action="Ошибка при удалении", 
                   details=str(e))
        
        print(f"{Colors.RED}Ошибка при очистке данных:{Colors.RESET}\n{tb}")
        
        await message.reply("Произошла ошибка при удалении данных. Пожалуйста, попробуйте еще раз.", 
                           reply_markup=get_keyboard())

@dp.message(lambda message: message.text == "📝 Новый подсчет")
async def new_count(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    log_message("COMMAND", user_id, username, action="Нажата кнопка 'Новый подсчет'")
    
    current_qr_data = {}
    if user_id in user_data and 'qr_codes' in user_data[user_id]:
        current_qr_data = user_data[user_id]['qr_codes']

    user_data[user_id] = {
        'count': 0,
        'values': {},
        'qr_codes': current_qr_data
    }
    
    if not user_data[user_id]['qr_codes']:
        user_data[user_id]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
    if 'next_qr_id' not in user_data[user_id]['qr_codes']:
        user_data[user_id]['qr_codes']['next_qr_id'] = 1
    if 'codes' not in user_data[user_id]['qr_codes']:
        user_data[user_id]['qr_codes']['codes'] = []

    log_message("INFO", user_id, username, action="Начат новый подсчет", 
               details="Данные пользователя сброшены (кроме QR)")
    
    log_user_state(user_id)
    save_all_user_data()
    
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
        "При каждом обновлении бота, нужно будет нажать ➡️ /start\n\n"
        "📝 Новый подсчет - начать новый цикл подсчета (данные QR-кодов сохраняются).\n\n"
        "🔄 Очистить - удалить последнее сообщение и вычесть его значения из общей суммы.\n\n"
        "🖼️ QR Коды - перейти в раздел управления QR-кодами.\n\n"
        "  В разделе QR Кодов:\n"
        "  ➕ Создать QR - сгенерировать новый QR-код по вашему тексту.\n"
        "  📋 Список QR - показать список созданных QR-кодов и отправить выбранный.\n\n"
        "Отправляйте мне сообщения для подсчета в формате:\nНазвание - число\n\n"
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

@dp.message(lambda message: message.text == "🖼️ QR Коды")
async def qr_codes_section(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    log_message("COMMAND", user_id, username, action="Переход в раздел 'QR Коды'")
    await message.reply("Вы в разделе QR-кодов. Выберите действие:", reply_markup=get_qr_keyboard())

@dp.message(lambda message: message.text == "⬅️ Назад")
async def go_back_to_main_menu(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    log_message("COMMAND", user_id, username, action="Возврат в главное меню из QR")
    await message.reply("Возврат в главное меню.", reply_markup=get_keyboard())

@dp.message(lambda message: message.text == "➕ Создать QR")
async def request_qr_text_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    log_message("COMMAND", user_id, username, action="Нажата кнопка 'Создать QR'")
    await state.set_state(QRStates.waiting_for_qr_text)
    await message.reply("Введите текст, который вы хотите преобразовать в QR-код:", reply_markup=types.ReplyKeyboardRemove())

@dp.message(QRStates.waiting_for_qr_text)
async def generate_qr_code_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    qr_text = message.text

    if not qr_text:
        log_message("WARNING", user_id, username, action="Создание QR", details="Пустой текст для QR")
        await message.reply("Текст для QR-кода не может быть пустым. Попробуйте еще раз.", reply_markup=get_qr_keyboard())
        await state.clear()
        return

    log_message("MESSAGE", user_id, username, action="Получен текст для QR", details=qr_text)

    try:
        if user_id not in user_data:
             user_data[user_id] = {'count': 0, 'values': {}, 'qr_codes': {'next_qr_id': 1, 'codes': []}}
        elif 'qr_codes' not in user_data[user_id]:
            user_data[user_id]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
        if 'next_qr_id' not in user_data[user_id]['qr_codes']:
            user_data[user_id]['qr_codes']['next_qr_id'] = 1
        if 'codes' not in user_data[user_id]['qr_codes']:
            user_data[user_id]['qr_codes']['codes'] = []

        existing_qr = None
        for qr_code_item in user_data[user_id]['qr_codes']['codes']:
            if qr_code_item['text'] == qr_text:
                existing_qr = qr_code_item
                break
        
        if existing_qr:
            log_message("INFO", user_id, username, action="Создание QR", details=f"Найден существующий QR с текстом: {qr_text}")
            try:
                filepath = existing_qr['filepath']
                if not os.path.exists(filepath):
                    log_message("WARNING", user_id, username, action="Создание QR", details=f"Файл для существующего QR не найден: {filepath}. Попытка регенерации.")
                    img_regen = qrcode.make(qr_text)
                    img_regen.save(filepath)
                    log_message("INFO", user_id, username, action="QR регенерирован для существующей записи", details=f"Файл: {filepath}")

                with open(filepath, "rb") as qr_file_to_send:
                    qr_image_file = BufferedInputFile(qr_file_to_send.read(), filename=os.path.basename(filepath))
                    await message.reply_photo(photo=qr_image_file, caption=f"У вас уже есть QR-код для текста:\n'{qr_text}'", reply_markup=get_qr_keyboard())
            except Exception as e_send:
                log_message("ERROR", user_id, username, action="Отправка существующего QR", details=str(e_send))
                await message.reply(f"У вас уже есть QR-код для текста:\n'{qr_text}'\nНо произошла ошибка при его отправке. Вы можете найти его в списке.", reply_markup=get_qr_keyboard())
            await state.clear()
            return

        qr_id = user_data[user_id]['qr_codes']['next_qr_id']
        
        img = qrcode.make(qr_text)
        
        filename = f"qr_user{user_id}_id{qr_id}.png"
        filepath = os.path.join(QR_CODE_DIR, filename)
        img.save(filepath)
        log_message("INFO", user_id, username, action="QR-код сохранен в файл", details=f"Путь: {filepath}")

        user_data[user_id]['qr_codes']['codes'].append({'id': qr_id, 'text': qr_text, 'filepath': filepath})
        user_data[user_id]['qr_codes']['next_qr_id'] += 1
        save_all_user_data()
        
        log_message("INFO", user_id, username, action="QR-код создан и информация сохранена", details=f"ID: {qr_id}, Текст: {qr_text}, Файл: {filepath}")

        try:
            with open(filepath, "rb") as qr_file_to_send:
                qr_image_file = BufferedInputFile(qr_file_to_send.read(), filename=filename)
                await message.reply_photo(photo=qr_image_file, caption=f"Ваш QR-код для текста:\n'{qr_text}'", reply_markup=get_qr_keyboard())
        except FileNotFoundError:
            log_message("ERROR", user_id, username, action="Отправка QR", details=f"Файл не найден: {filepath}")
            await message.reply("QR-код был создан и сохранен, но произошла ошибка при его отправке. Попробуйте запросить его из списка.", reply_markup=get_qr_keyboard())
            
    except Exception as e:
        log_message("ERROR", user_id, username, action="Ошибка генерации/сохранения QR", details=str(e))
        await message.reply("Произошла ошибка при создании или сохранении QR-кода. Попробуйте еще раз.", reply_markup=get_qr_keyboard())
    
    await state.clear()

@dp.message(lambda message: message.text == "🗑️ Удалить QR")
async def request_delete_qr_handler(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    log_message("COMMAND", user_id, username, action="Нажата кнопка 'Удалить QR'")

    if user_id in user_data and user_data[user_id]['qr_codes']['codes']:
        qr_list = user_data[user_id]['qr_codes']['codes']
        
        if not qr_list:
            await message.reply("У вас нет QR-кодов для удаления.", reply_markup=get_qr_keyboard())
            return

        inline_buttons = []
        for qr_item in qr_list:
            button_text = qr_item['text'][:20] + "..." if len(qr_item['text']) > 20 else qr_item['text']
            inline_buttons.append([InlineKeyboardButton(text=f"Удалить: {button_text}", callback_data=f"delete_qr_{qr_item['id']}")])
        
        if not inline_buttons:
             await message.reply("Не удалось сформировать список QR-кодов для удаления.", reply_markup=get_qr_keyboard())
             return

        keyboard_inline = InlineKeyboardMarkup(inline_keyboard=inline_buttons)
        await message.reply("Выберите QR-код для удаления (нажмите для подтверждения):", reply_markup=keyboard_inline)
        
    else:
        log_message("INFO", user_id, username, action="Запрос на удаление QR", details="Список пуст")
        await message.reply("У вас нет QR-кодов для удаления.", reply_markup=get_qr_keyboard())

@dp.callback_query(lambda c: c.data and c.data.startswith('delete_qr_'))
async def process_delete_qr_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    username = callback_query.from_user.username or callback_query.from_user.first_name
    qr_id_to_delete = int(callback_query.data.split('_')[-1])

    log_message("CALLBACK", user_id, username, action="Получен колбэк на удаление QR (шаг 1)", details=f"ID QR: {qr_id_to_delete}")

    qr_to_delete = None
    if user_id in user_data and 'qr_codes' in user_data[user_id] and 'codes' in user_data[user_id]['qr_codes']:
        for qr_code in user_data[user_id]['qr_codes']['codes']:
            if qr_code['id'] == qr_id_to_delete:
                qr_to_delete = qr_code
                break

    if qr_to_delete:
        text_preview = qr_to_delete['text'][:30] + "..." if len(qr_to_delete['text']) > 30 else qr_to_delete['text']
        confirm_buttons = [
            [InlineKeyboardButton(text="Да, удалить", callback_data=f"confirm_delete_{qr_id_to_delete}")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_delete")]
        ]
        keyboard_confirm = InlineKeyboardMarkup(inline_keyboard=confirm_buttons)
        await callback_query.message.edit_text(f"Вы уверены, что хотите удалить QR-код с текстом:\n'{text_preview}'?", reply_markup=keyboard_confirm)
    else:
        await callback_query.message.edit_text("QR-код не найден или уже удален.", reply_markup=None)
        await callback_query.answer("Ошибка: QR-код не найден.")
        log_message("ERROR", user_id, username, action="Удаление QR (шаг 1)", details=f"QR с ID {qr_id_to_delete} не найден")

    await callback_query.answer()

@dp.callback_query(lambda c: c.data and (c.data.startswith('confirm_delete_') or c.data == 'cancel_delete'))
async def process_confirm_delete_qr_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    username = callback_query.from_user.username or callback_query.from_user.first_name

    if callback_query.data == 'cancel_delete':
        log_message("CALLBACK", user_id, username, action="Отмена удаления QR")
        await callback_query.message.edit_text("Удаление отменено.", reply_markup=None)
        await callback_query.answer("Удаление отменено.")
        return

    qr_id_to_delete = int(callback_query.data.split('_')[-1])
    log_message("CALLBACK", user_id, username, action="Получен колбэк на подтверждение удаления QR", details=f"ID QR: {qr_id_to_delete}")

    deleted = False
    qr_text_deleted = ""
    if user_id in user_data and 'qr_codes' in user_data[user_id] and 'codes' in user_data[user_id]['qr_codes']:
        qr_codes_list = user_data[user_id]['qr_codes']['codes']
        qr_to_remove_data = None
        for i, qr_code in enumerate(qr_codes_list):
            if qr_code['id'] == qr_id_to_delete:
                qr_to_remove_data = qr_code
                qr_text_deleted = qr_code['text']
                if os.path.exists(qr_code['filepath']):
                    try:
                        os.remove(qr_code['filepath'])
                        log_message("INFO", user_id, username, action="Файл QR удален", details=f"Файл: {qr_code['filepath']}")
                    except OSError as e:
                        log_message("ERROR", user_id, username, action="Ошибка удаления файла QR", details=f"Файл: {qr_code['filepath']}, Ошибка: {e}")
                else:
                    log_message("WARNING", user_id, username, action="Файл QR для удаления не найден", details=f"Файл: {qr_code['filepath']}")
                
                del qr_codes_list[i]
                deleted = True
                break
    
    if deleted:
        save_all_user_data()
        text_preview = qr_text_deleted[:30] + "..." if len(qr_text_deleted) > 30 else qr_text_deleted
        await callback_query.message.edit_text(f"QR-код для текста:\n'{text_preview}'\nуспешно удален.", reply_markup=None)
        await callback_query.answer("QR-код удален!")
        log_message("INFO", user_id, username, action="QR удален из данных", details=f"ID: {qr_id_to_delete}, Текст: {qr_text_deleted}")
    else:
        await callback_query.message.edit_text("Не удалось удалить QR-код. Возможно, он уже был удален ранее.", reply_markup=None)
        await callback_query.answer("Ошибка при удалении.")
        log_message("ERROR", user_id, username, action="Ошибка удаления QR из данных", details=f"ID: {qr_id_to_delete}, QR не найден в списке пользователя.")

@dp.message(lambda message: message.text == "📋 Список QR")
async def list_qr_codes_handler(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    if user_id in user_data and user_data[user_id]['qr_codes']['codes']:
        log_message("COMMAND", user_id, username, action="Запрос списка QR-кодов")
        
        qr_list = user_data[user_id]['qr_codes']['codes']
        
        if not qr_list:
            await message.reply("У вас еще нет сохраненных QR-кодов. Создайте новый!", reply_markup=get_qr_keyboard())
            return

        inline_buttons = []
        for qr_item in qr_list:
            button_text = qr_item['text'][:20] + "..." if len(qr_item['text']) > 20 else qr_item['text']
            inline_buttons.append([InlineKeyboardButton(text=f"QR: {button_text}", callback_data=f"show_qr_{qr_item['id']}")])
        
        if not inline_buttons:
             await message.reply("Не удалось сформировать список QR-кодов.", reply_markup=get_qr_keyboard())
             return

        keyboard_inline = InlineKeyboardMarkup(inline_keyboard=inline_buttons)
        await message.reply("Ваши QR-коды (нажмите, чтобы показать):", reply_markup=keyboard_inline)
        
    else:
        log_message("INFO", user_id, username, action="Запрос списка QR", details="Список пуст")
        await message.reply("У вас еще нет сохраненных QR-кодов. Создайте новый!", reply_markup=get_qr_keyboard())

@dp.callback_query(lambda c: c.data and c.data.startswith('show_qr_'))
async def process_show_qr_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    username = callback_query.from_user.username or callback_query.from_user.first_name
    qr_id_to_show = int(callback_query.data.split('_')[2])

    log_message("CALLBACK", user_id, username, action="Запрос на показ QR", details=f"ID: {qr_id_to_show}")

    qr_code_info = None
    if user_id in user_data and 'qr_codes' in user_data[user_id]:
        for qr in user_data[user_id]['qr_codes']['codes']:
            if qr['id'] == qr_id_to_show:
                qr_code_info = qr
                break
    
    if qr_code_info and 'filepath' in qr_code_info:
        filepath = qr_code_info['filepath']
        text = qr_code_info['text']
        try:
            if not os.path.exists(filepath):
                log_message("WARNING", user_id, username, action="Показ QR из списка", details=f"Файл QR не найден: {filepath}. Попытка регенерации.")
                img = qrcode.make(text)
                img.save(filepath)
                log_message("INFO", user_id, username, action="QR регенерирован", details=f"Файл: {filepath}")

            with open(filepath, "rb") as qr_file_to_send:
                qr_image_file = BufferedInputFile(qr_file_to_send.read(), filename=os.path.basename(filepath))
                await callback_query.message.reply_photo(
                    photo=qr_image_file, 
                    caption=f"QR-код для текста:\n'{text}'",
                    reply_markup=get_qr_keyboard()
                )
            await callback_query.answer()
            log_message("INFO", user_id, username, action="QR-код показан из файла", details=f"ID: {qr_id_to_show}, Файл: {filepath}")

        except FileNotFoundError:
            log_message("ERROR", user_id, username, action="Ошибка показа QR из списка", details=f"Файл не найден: {filepath}")
            await callback_query.message.reply("Файл QR-кода не найден. Возможно, он был удален. Попробуйте создать его заново.", reply_markup=get_qr_keyboard())
            await callback_query.answer("Файл не найден")
        except Exception as e:
            log_message("ERROR", user_id, username, action="Ошибка показа QR из списка", details=str(e))
            await callback_query.message.reply("Произошла ошибка при отображении QR-кода.", reply_markup=get_qr_keyboard())
            await callback_query.answer("Ошибка")
    elif qr_code_info and 'filepath' not in qr_code_info: 
        log_message("WARNING", user_id, username, action="Показ QR из списка", details=f"Информация о файле для QR с ID {qr_id_to_show} отсутствует. Попытка регенерации на лету.")
        try:
            img = qrcode.make(qr_code_info['text'])
            img_byte_arr = BytesIO()
            img.save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            qr_image_file = BufferedInputFile(img_byte_arr.read(), filename=f"qr_code_{qr_id_to_show}.png")
            await callback_query.message.reply_photo(
                photo=qr_image_file, 
                caption=f"QR-код для текста (сгенерирован на лету):\n'{qr_code_info['text']}'",
                reply_markup=get_qr_keyboard()
            )
            await callback_query.answer()
        except Exception as e:
            log_message("ERROR", user_id, username, action="Ошибка регенерации QR на лету", details=str(e))
            await callback_query.message.reply("Произошла ошибка при отображении QR-кода.", reply_markup=get_qr_keyboard())
            await callback_query.answer("Ошибка")
    else:
        log_message("WARNING", user_id, username, action="Показ QR из списка", details=f"QR с ID {qr_id_to_show} не найден")
        await callback_query.message.reply("QR-код не найден.", reply_markup=get_qr_keyboard())
        await callback_query.answer("Не найден")

@dp.message()
async def process_message(message: types.Message):
    try:
        main_menu_buttons = [
            "📝 Новый подсчет",
            "🔄 Очистить",
            "❓ Инструкция",
            "🖼️ QR Коды"
        ]
        if message.text in main_menu_buttons:
            return

        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.first_name
        
        log_message("MESSAGE", user_id, username, action="Получено сообщение", 
                   details=f"Текст: {message.text}")
        
        if user_id not in user_data:
            user_data[user_id] = {
                'count': 0,
                'values': {},
                'qr_codes': {'next_qr_id': 1, 'codes': []}
            }
            log_message("INFO", user_id, username, action="Создан новый пользователь при сообщении", 
                       details="Инициализированы данные, включая раздел QR")
        elif 'qr_codes' not in user_data[user_id]:
            user_data[user_id]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
            log_message("INFO", user_id, username, action="Обновление пользователя при сообщении", 
                       details="Добавлен раздел QR для существующего пользователя")
        
        if 'qr_codes' not in user_data[user_id]:
            user_data[user_id]['qr_codes'] = {'next_qr_id': 1, 'codes': []}
        if 'next_qr_id' not in user_data[user_id]['qr_codes']:
            user_data[user_id]['qr_codes']['next_qr_id'] = 1
        if 'codes' not in user_data[user_id]['qr_codes']:
            user_data[user_id]['qr_codes']['codes'] = []
        
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
        save_all_user_data()

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
            
            current_qr_data = user_data[user_id].get('qr_codes', {'next_qr_id': 1, 'codes': []})
            user_data[user_id] = {
                'count': 1,
                'values': {},
                'qr_codes': current_qr_data 
            }
            
            for line_item in lines: 
                line_item = line_item.strip()
                if not line_item:
                    continue
                name, value = parse_line(line_item)
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
        current_user_id = message.from_user.id if message and message.from_user else None
        current_username = (message.from_user.username or message.from_user.first_name) if message and message.from_user else None
        log_message("ERROR", current_user_id, current_username, action="Ошибка обработки сообщения", 
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

async def main():
    try:
        log_message("SYSTEM", action="Бот запущен", details="Начало работы")
        
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