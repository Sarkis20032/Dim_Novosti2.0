import os
import asyncio
import logging
import sys
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    ReplyKeyboardRemove
)
from aiogram.filters import Command
import psycopg2
from urllib.parse import urlparse

# Настройка event loop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Инициализация бота
API_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Функция для подключения к PostgreSQL
def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        result = urlparse(db_url)
        return psycopg2.connect(
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port
        )
    else:
        return psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'railway'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', ''),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )

# Проверка подключения к БД
async def check_db_connection():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        logger.info("✅ Подключение к PostgreSQL успешно")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
        return False

# Клавиатуры
def make_keyboard(items, row_width=2):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=item) for item in items[i:i+row_width]] 
                 for i in range(0, len(items), row_width)],
        resize_keyboard=True,
        one_time_keyboard=True
    )

GENDER_KEYBOARD = make_keyboard(["Мужской", "Женский"])
AGE_KEYBOARD = make_keyboard(["До 22", "22-30", "Более 30"])
VISIT_KEYBOARD = make_keyboard(["До 3 раз", "3-8 раз", "Более 8 раз"])
YES_NO_KEYBOARD = make_keyboard(["Да", "Нет"])
ADMIN_KEYBOARD = make_keyboard([
    "📊 Отчёт по базе", 
    "👥 Список админов", 
    "➕ Добавить админа",
    "🗑️ Очистить админов",
    "🧹 Очистить базу",
    "📢 Сделать рассылку",
    "💬 Чат с клиентом",
    "📋 Подробный отчёт",
    "🔙 Назад"
])
CANCEL_KEYBOARD = make_keyboard(["❌ Отмена"])

# Состояния
class Questionnaire(StatesGroup):
    WANT_HELP = State()
    CONFIRM_HELP = State()
    APPRECIATE = State()
    DISLIKE = State()
    IMPROVE = State()
    GENDER = State()
    AGE = State()
    VISIT_FREQ = State()

class AdminStates(StatesGroup):
    ADD_ADMIN = State()
    CHAT_WITH_CLIENT = State()
    CONFIRM_CLEAR = State()
    CONFIRM_CLEAR_ADMINS = State()
    ADMIN_CHATTING = State()
    SEND_BROADCAST = State()

# Инициализация базы данных
def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS admins (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            added_by BIGINT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            appreciate TEXT,
            dislike TEXT,
            improve TEXT,
            gender TEXT,
            age_group TEXT,
            visit_freq TEXT,
            is_admin BOOLEAN DEFAULT FALSE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Добавляем основного админа
        cursor.execute('''
        INSERT INTO admins (user_id, username, added_by) 
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
        ''', (641521378, "sarkis_20032", 641521378))
        
        conn.commit()
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Проверка прав администратора
def is_admin(user_id: int) -> bool:
    conn = None
    try:
        if user_id == 641521378:  # Принудительный доступ для основного админа
            return True
            
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM admins WHERE user_id = %s', (user_id,))
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Ошибка проверки админа: {e}")
        return False
    finally:
        if conn:
            conn.close()

# Проверка на главного администратора
def is_super_admin(user_id: int) -> bool:
    return user_id == 641521378

# Уведомление админов
async def notify_admins(text: str, exclude_id=None):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM admins')
        admins = cursor.fetchall()
        
        for admin in admins:
            admin_id = admin[0]
            if exclude_id and admin_id == exclude_id:
                continue
            try:
                await bot.send_message(admin_id, text)
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение админу {admin_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка уведомления админов: {e}")
    finally:
        if conn:
            conn.close()

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command('start'))
async def cmd_start(message: types.Message, state: FSMContext):
    conn = None
    try:
        await state.clear()
        user_id = message.from_user.id
        admin_status = is_admin(user_id)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM clients WHERE user_id = %s', (user_id,))
        
        if cursor.fetchone():
            if not admin_status:
                await message.answer("Вы уже проходили анкету. Хотите пройти её ещё раз?", 
                                  reply_markup=YES_NO_KEYBOARD)
            else:
                await message.answer("Вы уже проходили анкету. Хотите пройти её ещё раз?\n"
                                  "Или перейти в админ-панель: /admin", 
                                  reply_markup=YES_NO_KEYBOARD)
            await state.set_state(Questionnaire.WANT_HELP)
            await state.update_data(is_admin=admin_status)
        else:
            await state.update_data(
                username=message.from_user.username,
                full_name=message.from_user.full_name,
                is_admin=admin_status
            )
            await message.answer(
                "Добрый день, меня зовут Давид👋 я владелец сети магазинов \"Дым\"💨\n"
                "Рад знакомству😊\n\n"
                "Я создал этого бота чтобы дать своим гостям самый лучший сервис и предложение😍\n\n"
                "Вы хотите, чтобы мы стали лучше для вас?",
                reply_markup=YES_NO_KEYBOARD
            )
            await state.set_state(Questionnaire.WANT_HELP)
    except Exception as e:
        logger.error(f"Ошибка в команде /start: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")
    finally:
        if conn:
            conn.close()

@dp.message(Command('admin'))
async def admin_panel(message: types.Message):
    try:
        user_id = message.from_user.id
        
        if not is_admin(user_id):
            await message.answer("⛔ У вас нет прав администратора")
            return
        
        if message.chat.type != 'private':
            await message.answer("🔒 Админ-панель доступна только в личных сообщениях")
            return
            
        await message.answer("👨‍💻 Админ-панель:", reply_markup=ADMIN_KEYBOARD)
    except Exception as e:
        logger.error(f"Ошибка админ-панели: {e}")
        await message.answer("⚠️ Ошибка доступа к админ-панели")

# ========== ОБРАБОТЧИКИ АНКЕТЫ ==========

@dp.message(Questionnaire.WANT_HELP)
async def process_want_help(message: types.Message, state: FSMContext):
    try:
        if message.text.lower() == 'нет':
            await message.answer("Спасибо за ваше время! Возвращайтесь, когда будете готовы помочь.", reply_markup=ReplyKeyboardRemove())
            await state.clear()
            return
        
        await message.answer(
            "Отлично✨\nТут я буду публиковать интересные предложения, розыгрыши и подарки 🎁\n\n"
            "Но самое главное, мы хотим улучшить качество нашей работы\n\n"
            "Сможете нам помочь, ответив на 3 вопроса?",
            reply_markup=YES_NO_KEYBOARD
        )
        await state.set_state(Questionnaire.CONFIRM_HELP)
    except Exception as e:
        logger.error(f"Ошибка в обработке WANT_HELP: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.CONFIRM_HELP)
async def process_confirm_help(message: types.Message, state: FSMContext):
    try:
        if message.text.lower() == 'нет':
            await message.answer("Спасибо за ваше время! Возвращайтесь, когда будете готовы помочь.", reply_markup=ReplyKeyboardRemove())
            await state.clear()
            return
        
        await message.answer(
            "Благодарим за помощь🤝\n"
            "Подскажите, какие 2 вещи в наших магазинах вы цените больше всего?😍",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Questionnaire.APPRECIATE)
    except Exception as e:
        logger.error(f"Ошибка в обработке CONFIRM_HELP: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.APPRECIATE)
async def process_appreciate(message: types.Message, state: FSMContext):
    try:
        await state.update_data(appreciate=message.text)
        await message.answer("Хорошо😊\nИ еще пару вещей которые вам больше всего НЕ нравятся?👿")
        await state.set_state(Questionnaire.DISLIKE)
    except Exception as e:
        logger.error(f"Ошибка в обработке APPRECIATE: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.DISLIKE)
async def process_dislike(message: types.Message, state: FSMContext):
    try:
        await state.update_data(dislike=message.text)
        await message.answer("Отлично и последний вопрос)\nЧто бы вы изменили будучи на моем месте что бы стать лучше?")
        await state.set_state(Questionnaire.IMPROVE)
    except Exception as e:
        logger.error(f"Ошибка в обработке DISLIKE: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.IMPROVE)
async def process_improve(message: types.Message, state: FSMContext):
    try:
        await state.update_data(improve=message.text)
        await message.answer(
            "Спасибо огромное за помощь😊\n"
            "Я учту ваши пожелания и постараюсь приложить усилия что бы это исправить\n\n"
            "Если не сложно подскажите ваш пол:",
            reply_markup=GENDER_KEYBOARD
        )
        await state.set_state(Questionnaire.GENDER)
    except Exception as e:
        logger.error(f"Ошибка в обработке IMPROVE: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.GENDER)
async def process_gender(message: types.Message, state: FSMContext):
    try:
        if message.text not in ["Мужской", "Женский"]:
            await message.answer("Пожалуйста, выберите пол из предложенных вариантов.")
            return
        
        await state.update_data(gender=message.text)
        await message.answer("Ваша возрастная группа:", reply_markup=AGE_KEYBOARD)
        await state.set_state(Questionnaire.AGE)
    except Exception as e:
        logger.error(f"Ошибка в обработке GENDER: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.AGE)
async def process_age(message: types.Message, state: FSMContext):
    try:
        if message.text not in ["До 22", "22-30", "Более 30"]:
            await message.answer("Пожалуйста, выберите возраст из предложенных вариантов.")
            return
        
        await state.update_data(age_group=message.text)
        await message.answer("Как часто вы нас посещаете?", reply_markup=VISIT_KEYBOARD)
        await state.set_state(Questionnaire.VISIT_FREQ)
    except Exception as e:
        logger.error(f"Ошибка в обработке AGE: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")

@dp.message(Questionnaire.VISIT_FREQ)
async def process_visit_freq(message: types.Message, state: FSMContext):
    conn = None
    try:
        if message.text not in ["До 3 раз", "3-8 раз", "Более 8 раз"]:
            await message.answer("Пожалуйста, выберите вариант из предложенных.")
            return
        
        user_data = await state.get_data()
        
        # Сохраняем данные в базу
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO clients (
            user_id, username, full_name, appreciate, dislike, 
            improve, gender, age_group, visit_freq, is_admin
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            full_name = EXCLUDED.full_name,
            appreciate = EXCLUDED.appreciate,
            dislike = EXCLUDED.dislike,
            improve = EXCLUDED.improve,
            gender = EXCLUDED.gender,
            age_group = EXCLUDED.age_group,
            visit_freq = EXCLUDED.visit_freq,
            is_admin = EXCLUDED.is_admin,
            timestamp = CURRENT_TIMESTAMP
        ''', (
            message.from_user.id,
            user_data.get('username'),
            user_data.get('full_name'),
            user_data.get('appreciate'),
            user_data.get('dislike'),
            user_data.get('improve'),
            user_data.get('gender'),
            user_data.get('age_group'),
            message.text,
            user_data.get('is_admin', False)
        ))
        conn.commit()
        
        # Формируем сообщение для админов (только если пользователь не админ)
        if not user_data.get('is_admin', False):
            admin_message = (
                "📝 Новая анкета:\n\n"
                f"👤 Пользователь: @{user_data.get('username', 'без username')} ({user_data.get('full_name', 'без имени')})\n"
                f"🆔 ID: {message.from_user.id}\n"
                f"👍 Что нравится: {user_data.get('appreciate', 'не указано')}\n"
                f"👎 Что не нравятся: {user_data.get('dislike', 'не указано')}\n"
                f"💡 Предложения: {user_data.get('improve', 'не указано')}\n"
                f"🧑‍🤝‍🧑 Пол: {user_data.get('gender', 'не указан')}\n"
                f"📊 Возраст: {user_data.get('age_group', 'не указана')}\n"
                f"🛒 Частота посещений: {message.text}"
            )
            await notify_admins(admin_message)
        
        # Обновленное финальное сообщение
        response = (
            "Благодарю за ваши ответы! 🙏\n\n"
            "📞 Мой номер телефона: 8-918-5567-53-33\n\n"
            "Вы можете:\n"
            "1. Позвонить мне напрямую\n"
            "2. Написать в WhatsApp или Telegram\n"
            "3. Отправить сообщение прямо здесь в чате - я отвечу лично\n\n"
            "Также вы можете присоединиться к нашему чату для обсуждения ассортимента, цен и новостей:\n"
            "👉 https://t.me/+BR14rdoGA91mZjdi"
        )
        
        # Добавляем информацию об админ-панели только для админов
        if user_data.get('is_admin', False):
            response += "\n\nВы можете перейти в админ-панель: /admin"
        
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
            
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в обработке VISIT_FREQ: {e}")
        await message.answer("⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.")
    finally:
        if conn:
            conn.close()

# ========== ОБРАБОТКА СООБЩЕНИЙ ОТ КЛИЕНТОВ ==========

@dp.message()
async def forward_client_message(message: types.Message):
    conn = None
    try:
        # Игнорируем сообщения не из личных чатов и команды
        if message.chat.type != 'private' or message.text.startswith('/'):
            return
            
        user_id = message.from_user.id
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM clients WHERE user_id = %s', (user_id,))
        is_client = cursor.fetchone() is not None
        
        # Если это клиент (не админ) - пересылаем сообщение всем админам
        if is_client and not is_admin(user_id):
            user_info = (
                f"✉️ Новое сообщение от клиента:\n"
                f"👤 Имя: {message.from_user.full_name}\n"
                f"📌 Username: @{message.from_user.username}\n"
                f"🆔 ID: {user_id}\n\n"
                f"📩 Текст сообщения:\n{message.text}"
            )
            
            # Отправляем всем админам (без исключений)
            await notify_admins(user_info)
            
            # Подтверждение клиенту
            await message.answer(
                "✅ Ваше сообщение отправлено администраторам. "
                "Мы ответим вам в ближайшее время.\n\n"
                "Вы можете продолжить общение прямо здесь."
            )
    except Exception as e:
        logger.error(f"Ошибка пересылки сообщения: {e}")
        await message.answer("⚠️ Произошла ошибка при отправке сообщения.")
    finally:
        if conn:
            conn.close()

# ========== ОБРАБОТЧИК ОТВЕТОВ АДМИНОВ ==========

@dp.message(lambda m: is_admin(m.from_user.id))
async def handle_admin_reply(message: types.Message, state: FSMContext):
    # Если админ отвечает на пересланное сообщение
    if message.reply_to_message and "Новое сообщение от клиента" in message.reply_to_message.text:
        try:
            # Парсим ID клиента из пересланного сообщения
            lines = message.reply_to_message.text.split('\n')
            client_id = None
            for line in lines:
                if "🆔 ID:" in line:
                    client_id = int(line.split(':')[1].strip())
                    break
            
            if client_id:
                await bot.send_message(
                    client_id,
                    f"📨 Ответ от администратора:\n\n{message.text}"
                )
                await message.answer("✅ Ваш ответ отправлен клиенту")
            else:
                await message.answer("❌ Не удалось определить ID клиента")
        except Exception as e:
            logger.error(f"Ошибка отправки ответа клиенту: {e}")
            await message.answer("⚠️ Ошибка отправки ответа клиенту")
    else:
        # Если админ не в режиме чата, предлагаем варианты ответа
        current_state = await state.get_state()
        if current_state != AdminStates.ADMIN_CHATTING:
            await message.answer(
                "Вы можете ответить клиенту:\n"
                "1. Ответьте на пересланное сообщение клиента\n"
                "2. Используйте команду /admin и выберите '💬 Чат с клиентом'\n"
                "3. Перешлите мне сообщение клиента и напишите ответ"
            )

# ========== АДМИН-ПАНЕЛЬ ==========

@dp.message(lambda m: m.text == "📊 Отчёт по базе" and is_admin(m.from_user.id))
async def database_report(message: types.Message):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM clients')
        total_clients = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM admins')
        total_admins = cursor.fetchone()[0]
        
        cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM clients')
        first_date, last_date = cursor.fetchone()
        
        cursor.execute('''
        SELECT 
            COUNT(*) as total,
            gender,
            age_group,
            visit_freq
        FROM clients
        GROUP BY gender, age_group, visit_freq
        ''')
        
        stats = cursor.fetchall()
        
        report = (
            "📊 Отчёт по базе:\n"
            f"👥 Всего клиентов: {total_clients}\n"
            f"👨‍💻 Всего админов: {total_admins}\n"
            f"📅 Первая анкета: {first_date}\n"
            f"📅 Последняя анкета: {last_date}\n\n"
            "📈 Статистика по клиентам:\n"
        )
        
        for row in stats:
            report += f"• {row[1]}, {row[2]}, посещает {row[3]}: {row[0]} чел.\n"
        
        await message.answer(report)
    except Exception as e:
        await message.answer(f"⚠️ Ошибка формирования отчёта: {str(e)}")
    finally:
        if conn:
            conn.close()

@dp.message(lambda m: m.text == "👥 Список админов" and is_admin(m.from_user.id))
async def list_admins(message: types.Message):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
        SELECT a.user_id, a.username, u.username as added_by_username, a.added_at
        FROM admins a
        LEFT JOIN admins u ON a.added_by = u.user_id
        ORDER BY a.added_at DESC
        ''')
        admins = cursor.fetchall()
        
        if not admins:
            await message.answer("Нет зарегистрированных админов")
            return
        
        response = "👨‍💻 Список админов:\n\n"
        for admin in admins:
            response += (
                f"🆔 ID: {admin[0]}\n"
                f"👤 @{admin[1]}\n"
                f"➕ Добавил: @{admin[2]}\n"
                f"📅 Дата: {admin[3]}\n\n"
            )
        
        await message.answer(response)
    except Exception as e:
        await message.answer(f"⚠️ Ошибка получения списка админов: {str(e)}")
    finally:
        if conn:
            conn.close()

@dp.message(lambda m: m.text == "➕ Добавить админа" and is_admin(m.from_user.id))
async def add_admin_start(message: types.Message, state: FSMContext):
    try:
        await message.answer(
            "Введите ID пользователя, которого хотите сделать админом:",
            reply_markup=CANCEL_KEYBOARD
        )
        await state.set_state(AdminStates.ADD_ADMIN)
    except Exception as e:
        logger.error(f"Ошибка начала добавления админа: {e}")
        await message.answer("⚠️ Ошибка. Попробуйте снова.")

@dp.message(AdminStates.ADD_ADMIN)
async def add_admin_finish(message: types.Message, state: FSMContext):
    conn = None
    try:
        if message.text == "❌ Отмена":
            await message.answer("Действие отменено", reply_markup=ADMIN_KEYBOARD)
            await state.clear()
            return
        
        try:
            new_admin_id = int(message.text)
        except ValueError:
            await message.answer("Некорректный ID. Введите числовой ID пользователя:")
            return
        
        # Получаем информацию о новом админе
        try:
            new_admin = await bot.get_chat(new_admin_id)
            new_admin_username = new_admin.username if new_admin.username else "без username"
            new_admin_fullname = new_admin.full_name if new_admin.full_name else "без имени"
        except Exception as e:
            logger.error(f"Ошибка получения информации о новом админе: {e}")
            new_admin_username = "неизвестно"
            new_admin_fullname = "неизвестно"
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT 1 FROM admins WHERE user_id = %s', (new_admin_id,))
        if cursor.fetchone():
            await message.answer("Этот пользователь уже является админом", reply_markup=ADMIN_KEYBOARD)
            await state.clear()
            return
        
        cursor.execute('''
        INSERT INTO admins (user_id, username, added_by)
        VALUES (%s, %s, %s)
        ''', (
            new_admin_id,
            new_admin_username,
            message.from_user.id
        ))
        conn.commit()
        
        # Отправляем сообщение новому админу
        try:
            await bot.send_message(
                new_admin_id,
                "🎉 Поздравляем! Вас назначили администратором бота сети магазинов 'Дым'.\n\n"
                "📌 Ваши новые возможности:\n"
                "- Доступ к админ-панели (/admin)\n"
                "- Просмотр статистики и анкет\n"
                "- Общение с клиентами\n"
                "- Рассылка сообщений\n\n"
                "📌 Ограничения:\n"
                "- Вы не можете удалять других администраторов\n"
                "- Очистка базы админов доступна только главному администратору\n\n"
                "📌 Основные обязанности:\n"
                "- Вежливое общение с клиентами\n"
                "- Своевременное рассмотрение анкет\n"
                "- Помощь в решении проблем\n\n"
                "По всем вопросам обращайтесь к @sarkis_20032"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение новому админу: {e}")
        
        # Уведомляем всех админов
        admin_message = (
            "👨‍💻 Новый администратор:\n\n"
            f"🆔 ID: {new_admin_id}\n"
            f"👤 Имя: {new_admin_fullname}\n"
            f"📛 @{new_admin_username}\n"
            f"➕ Добавил: @{message.from_user.username} (ID: {message.from_user.id})\n\n"
            f"ℹ️ Новый админ не имеет прав на удаление других администраторов"
        )
        await notify_admins(admin_message)
        
        await message.answer(
            f"✅ Пользователь @{new_admin_username} добавлен как админ\n"
            "Ему отправлено сообщение с инструкциями и ограничениями",
            reply_markup=ADMIN_KEYBOARD
        )
    except Exception as e:
        logger.error(f"Ошибка добавления админа: {e}")
        await message.answer("⚠️ Ошибка добавления админа. Попробуйте снова.")
    finally:
        if conn:
            conn.close()
        await state.clear()

@dp.message(lambda m: m.text == "🗑️ Очистить админов" and is_admin(m.from_user.id))
async def clear_admins_start(message: types.Message):
    try:
        if not is_super_admin(message.from_user.id):
            await message.answer("⛔ У вас недостаточно прав для этой операции")
            return
            
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, очистить", callback_data="confirm_clear_admins")],
            [InlineKeyboardButton(text="❌ Нет, отменить", callback_data="cancel_clear_admins")]
        ])
        
        await message.answer(
            "⚠️ Вы уверены, что хотите очистить базу админов?\n"
            "Это действие нельзя отменить! Все админы (кроме вас) будут удалены.\n"
            "Вы останетесь единственным администратором.",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Ошибка начала очистки админов: {e}")
        await message.answer("⚠️ Ошибка. Попробуйте снова.")

@dp.callback_query(lambda c: c.data == "confirm_clear_admins" and is_admin(c.from_user.id))
async def confirm_clear_admins(callback: types.CallbackQuery):
    if not is_super_admin(callback.from_user.id):
        await callback.answer("⛔ У вас недостаточно прав", show_alert=True)
        return
        
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Удаляем всех админов, кроме текущего
        cursor.execute('DELETE FROM admins WHERE user_id != %s', (callback.from_user.id,))
        
        # Добавляем текущего админа обратно, если его нет (на всякий случай)
        cursor.execute('INSERT INTO admins (user_id, username, added_by) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO NOTHING',
                      (callback.from_user.id, callback.from_user.username, callback.from_user.id))
        
        conn.commit()
        
        await callback.message.edit_text(
            "✅ База админов очищена. Вы остались единственным администратором.",
            reply_markup=None
        )
    except Exception as e:
        logger.error(f"Ошибка очистки админов: {e}")
        await callback.message.edit_text(
            "⚠️ Ошибка очистки базы админов",
            reply_markup=None
        )
    finally:
        if conn:
            conn.close()
        await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_clear_admins")
async def cancel_clear_admins(callback: types.CallbackQuery):
    try:
        await callback.message.edit_text(
            "❌ Очистка базы админов отменена",
            reply_markup=None
        )
    except Exception as e:
        logger.error(f"Ошибка отмены очистки админов: {e}")
    finally:
        await callback.answer()

@dp.message(lambda m: m.text == "🧹 Очистить базу" and is_admin(m.from_user.id))
async def clear_database_start(message: types.Message):
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, очистить", callback_data="confirm_clear")],
            [InlineKeyboardButton(text="❌ Нет, отменить", callback_data="cancel_clear")]
        ])
        
        await message.answer(
            "⚠️ Вы уверены, что хотите очистить базу клиентов?\n"
            "Это действие нельзя отменить! Все данные клиентов будут удалены.",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Ошибка начала очистки базы: {e}")
        await message.answer("⚠️ Ошибка. Попробуйте снова.")

@dp.callback_query(lambda c: c.data == "confirm_clear")
async def confirm_clear_db(callback: types.CallbackQuery):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM clients')
        conn.commit()
        
        await callback.message.edit_text(
            "✅ База клиентов очищена",
            reply_markup=None
        )
    except Exception as e:
        logger.error(f"Ошибка очистки базы: {e}")
        await callback.message.edit_text(
            "⚠️ Ошибка очистки базы",
            reply_markup=None
        )
    finally:
        if conn:
            conn.close()
        await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_clear")
async def cancel_clear_db(callback: types.CallbackQuery):
    try:
        await callback.message.edit_text(
            "❌ Очистка базы отменена",
            reply_markup=None
        )
    except Exception as e:
        logger.error(f"Ошибка отмены очистки: {e}")
    finally:
        await callback.answer()

@dp.message(lambda m: m.text == "📢 Сделать рассылку" and is_admin(m.from_user.id))
async def start_broadcast(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите сообщение для рассылки всем клиентам:",
        reply_markup=CANCEL_KEYBOARD
    )
    await state.set_state(AdminStates.SEND_BROADCAST)

@dp.message(AdminStates.SEND_BROADCAST)
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await message.answer("Рассылка отменена", reply_markup=ADMIN_KEYBOARD)
        await state.clear()
        return
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM clients WHERE is_admin = FALSE')
        clients = cursor.fetchall()
        
        total = len(clients)
        success = 0
        failed = 0
        
        await message.answer(f"⏳ Начинаю рассылку для {total} клиентов...")
        
        for client in clients:
            try:
                await bot.send_message(
                    client[0],
                    f"📢 Важное сообщение от сети магазинов 'Дым':\n\n{message.text}"
                )
                success += 1
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения клиенту {client[0]}: {e}")
                failed += 1
            await asyncio.sleep(0.1)  # Задержка для избежания ограничений Telegram
        
        report = (
            f"✅ Рассылка завершена:\n"
            f"• Успешно: {success}\n"
            f"• Не удалось: {failed}\n"
            f"• Всего: {total}"
        )
        
        await message.answer(report, reply_markup=ADMIN_KEYBOARD)
        
        # Уведомляем других админов
        await notify_admins(
            f"Администратор @{message.from_user.username} выполнил рассылку:\n\n"
            f"{message.text}\n\n"
            f"{report}"
        )
        
    except Exception as e:
        logger.error(f"Ошибка рассылки: {e}")
        await message.answer("⚠️ Произошла ошибка при рассылке", reply_markup=ADMIN_KEYBOARD)
    finally:
        if conn:
            conn.close()
        await state.clear()

@dp.message(lambda m: m.text == "💬 Чат с клиентом" and is_admin(m.from_user.id))
async def chat_with_client_start(message: types.Message, state: FSMContext):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, full_name FROM clients WHERE is_admin = FALSE ORDER BY timestamp DESC LIMIT 50')
        clients = cursor.fetchall()
        
        if not clients:
            await message.answer("Нет клиентов для чата")
            return
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"{full_name} (ID: {client_id})",
                callback_data=f"admin_chat_{client_id}"
            )] for client_id, full_name in clients
        ])
        
        keyboard.inline_keyboard.append(
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_chat_select")]
        )
        
        await message.answer(
            "Выберите клиента для чата:",
            reply_markup=keyboard
        )
        await state.set_state(AdminStates.CHAT_WITH_CLIENT)
    except Exception as e:
        logger.error(f"Ошибка начала чата с клиентом: {e}")
        await message.answer("⚠️ Ошибка. Попробуйте снова.")
    finally:
        if conn:
            conn.close()

@dp.callback_query(lambda c: c.data.startswith('admin_chat_'))
async def start_client_chat(callback: types.CallbackQuery, state: FSMContext):
    try:
        client_id = int(callback.data.split('_')[2])
        await state.update_data(client_id=client_id)
        
        keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Завершить чат")]], resize_keyboard=True)
        
        await callback.message.answer(
            f"💬 Вы начали чат с клиентом ID: {client_id}\n"
            "Теперь все ваши сообщения будут пересылаться этому клиенту.\n"
            "Для завершения чата нажмите кнопку ниже.",
            reply_markup=keyboard
        )
        await state.set_state(AdminStates.ADMIN_CHATTING)
    except Exception as e:
        logger.error(f"Ошибка начала чата: {e}")
        await callback.message.answer("⚠️ Ошибка начала чата. Попробуйте снова.")
    finally:
        await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_chat_select")
async def cancel_chat_select(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.message.edit_text(
            "❌ Выбор чата отменён",
            reply_markup=None
        )
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка отмены выбора чата: {e}")
    finally:
        await callback.answer()

@dp.message(AdminStates.ADMIN_CHATTING)
async def forward_to_client(message: types.Message, state: FSMContext):
    try:
        if message.text == "❌ Завершить чат":
            await message.answer("Чат с клиентом завершён", reply_markup=ADMIN_KEYBOARD)
            await state.clear()
            return
        
        data = await state.get_data()
        client_id = data['client_id']
        
        try:
            await bot.send_message(
                client_id,
                f"📨 Сообщение от администратора:\n\n{message.text}"
            )
            await message.answer("✅ Сообщение отправлено клиенту")
        except Exception as e:
            await message.answer(f"❌ Не удалось отправить сообщение: {str(e)}")
    except Exception as e:
        logger.error(f"Ошибка пересылки сообщения: {e}")
        await message.answer("⚠️ Ошибка отправки сообщения. Попробуйте снова.")

@dp.message(lambda m: m.text == "📋 Подробный отчёт" and is_admin(m.from_user.id))
async def detailed_clients_report(message: types.Message):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT user_id, username, full_name, timestamp, appreciate, dislike, 
               improve, gender, age_group, visit_freq
        FROM clients
        WHERE is_admin = FALSE
        ORDER BY timestamp DESC
        LIMIT 50
        ''')
        
        clients = cursor.fetchall()
        
        if not clients:
            await message.answer("В базе нет клиентов")
            return
            
        # Формируем отчет
        report_parts = ["📋 Подробный отчёт по клиентам (последние 50)\n"]
        for client in clients:
            client_info = [
                f"👤 {client[2]} (@{client[1]})",
                f"🆔 ID: {client[0]}",
                f"📅 Дата: {client[3]}",
                f"🧑‍🤝‍🧑 Пол: {client[7]}",
                f"📊 Возраст: {client[8]}",
                f"🛒 Посещения: {client[9]}",
                f"👍 Нравится: {client[4]}",
                f"👎 Не нравится: {client[5]}",
                f"💡 Предложения: {client[6]}",
                "="*40
            ]
            report_parts.extend(client_info)
        
        # Разбиваем отчет на части, чтобы избежать ограничения длины сообщения
        current_message = ""
        for part in report_parts:
            if len(current_message) + len(part) > 4000:
                await message.answer(current_message)
                current_message = part + "\n"
            else:
                current_message += part + "\n"
        
        if current_message:
            await message.answer(current_message)

    except Exception as e:
        logger.error(f"Ошибка формирования отчёта: {e}")
        await message.answer("⚠️ Произошла непредвиденная ошибка")
    finally:
        if conn:
            conn.close()

@dp.message(lambda m: m.text == "🔙 Назад" and is_admin(m.from_user.id))
async def back_to_admin_menu(message: types.Message, state: FSMContext):
    try:
        await state.clear()
        await message.answer("Главное меню админ-панели:", reply_markup=ADMIN_KEYBOARD)
    except Exception as e:
        logger.error(f"Ошибка возврата в меню: {e}")

# ========== ДИАГНОСТИКА ==========

@dp.message(Command('debug'))
async def debug_info(message: types.Message):
    conn = None
    try:
        user_id = message.from_user.id
        is_adm = is_admin(user_id)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM admins WHERE user_id = %s', (user_id,))
        admin_data = cursor.fetchone()
        
        cursor.execute('SELECT * FROM clients WHERE user_id = %s', (user_id,))
        client_data = cursor.fetchone()
        
        response = (
            f"🔧 Debug информация:\n"
            f"🆔 Ваш ID: {user_id}\n"
            f"👨‍💻 Вы админ: {'✅ Да' if is_adm else '❌ Нет'}\n"
            f"👑 Вы суперадмин: {'✅ Да' if is_super_admin(user_id) else '❌ Нет'}\n"
            f"📊 Данные в таблице админов: {admin_data}\n"
            f"📝 Данные в таблице клиентов: {client_data}\n\n"
            f"ℹ️ Для доступа к админ-панели используйте /admin"
        )
        
        await message.answer(response)
    except Exception as e:
        logger.error(f"Ошибка debug: {e}")
        await message.answer("⚠️ Ошибка получения debug информации")
    finally:
        if conn:
            conn.close()

# ========== ЗАПУСК БОТА ==========

async def main():
    try:
        # Инициализация базы данных
        init_db()
        
        # Проверка подключения к БД
        if not await check_db_connection():
            logger.critical("Не удалось подключиться к базе данных")
            return
        
        logger.info("Бот запускается...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.critical(f"Ошибка запуска бота: {e}")

if __name__ == '__main__':
    asyncio.run(main())
