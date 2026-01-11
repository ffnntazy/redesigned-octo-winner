import asyncio
import os
import re
from datetime import datetime

import aiosqlite
import pdfplumber
import requests
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove

# === НАСТРОЙКИ ===
TOKEN = "8502329102:AAHEk53e0i9dEqxGlf-USHeTUyFPzEnvTgE"
OWNER_ID = 5355658748
FILE_ID = "1sfUvMb71L_K914WaCLVOmfgnuqc7IRbI"
PDF_PATH = "schedule.pdf"

days_ru = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница"]

# Кэш
cached_tables = None
cached_headers = None
last_update_time = None
UPDATE_INTERVAL = 600  # 10 минут

router = Router()

class AdminStates(StatesGroup):
    broadcast = State()

# === ЗАГРУЗКА И ПАРСИНГ PDF ===
def download_and_parse_pdf(force: bool = False):
    global cached_tables, cached_headers, last_update_time

    now = datetime.now()
    if not force and last_update_time and (now - last_update_time).total_seconds() < UPDATE_INTERVAL:
        return True

    url = f"https://drive.google.com/uc?id={FILE_ID}&export=download"
    session = requests.Session()

    try:
        response = session.get(url, stream=True, timeout=30)
        confirm_token = None
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                confirm_token = value
                break

        if confirm_token:
            url += f"&confirm={confirm_token}"
            response = session.get(url, stream=True, timeout=30)

        if response.status_code != 200:
            return False

        with open(PDF_PATH, 'wb') as f:
            for chunk in response.iter_content(chunk_size=32768):
                if chunk:
                    f.write(chunk)

        with pdfplumber.open(PDF_PATH) as pdf:
            cached_tables = []
            cached_headers = []
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                tables = page.extract_tables()
                cached_tables.append(tables if tables else [])

                date_match = re.search(r'(\d{1,2}\s+[а-яА-Я]+\s+2026\s*г\.)', text)
                day_match = re.search(r'(понедельник|вторник|среда|четверг|пятница)', text, re.I)
                date_str = date_match.group(1).strip() if date_match else ""
                day_str = day_match.group(1).capitalize() if day_match else days_ru[i % 5]
                cached_headers.append((date_str, day_str))

        last_update_time = now
        return True
    except Exception as e:
        print(f"Ошибка загрузки/парсинга: {e}")
        return False

async def get_schedule_for_day(user_class: str, day_index: int):
    normalized_user_class = re.sub(r'\s+', '', user_class.upper())

    if cached_tables is None or len(cached_tables) <= day_index:
        if not download_and_parse_pdf():
            return "❌ Ошибка загрузки расписания. Попробуйте позже."

    page_tables = cached_tables[day_index]
    if not page_tables:
        return f"Расписание на {days_ru[day_index]} не найдено."

    date_str, day_str = cached_headers[day_index]

    header = f"📅 {day_str}"
    if date_str:
        header += f" ({date_str})"
    header += "\n\n"

    for table in page_tables:
        if not table or len(table) < 5:
            continue

        best_row_idx = None
        max_matches = 0
        for r_idx, row in enumerate(table):
            if not row:
                continue
            matches = 0
            for cell in row:
                if cell:
                    norm = re.sub(r'\s+', '', cell.strip().upper())
                    if re.match(r'^\d{1,2}[А-Я]$', norm):
                        matches += 1
            if matches > max_matches:
                max_matches = matches
                best_row_idx = r_idx

        if best_row_idx is None or max_matches < 2:
            continue

        header_row = table[best_row_idx]

        class_to_col = {}
        for c_idx, cell in enumerate(header_row):
            if cell:
                norm = re.sub(r'\s+', '', cell.strip().upper())
                if re.match(r'^\d{1,2}[А-Я]$', norm):
                    class_to_col[norm] = c_idx

        if normalized_user_class not in class_to_col:
            continue

        col = class_to_col[normalized_user_class]

        response = header
        has_lessons = False

        lesson_rows = table[best_row_idx + 1:]

        for row in lesson_rows:
            if not row or len(row) <= col + 1:
                continue

            num_cell = row[0] if len(row) > 0 else ""
            num = num_cell.strip() if num_cell else ""
            if not num.isdigit():
                continue

            time_cell = row[1] if len(row) > 1 else ""
            time_str = time_cell.strip() if time_cell else ""

            subject = (row[col] or "").strip().replace('\n', ' ') if len(row) > col else ""
            cab = (row[col + 1] or "").strip().replace('\n', ' ') if len(row) > col + 1 else "-"

            if not subject or subject in ["-", ""]:
                continue

            has_lessons = True
            response += f"{num}. {time_str} — {subject} (каб. {cab})\n"

        if has_lessons:
            return response.strip()
        else:
            return header.strip() + "\n\nНет уроков в этот день."

    return f"Расписание на {day_str} не найдено для вашего класса."

# === МЕНЮ ===
async def send_main_menu(msg: Message, user_class: str):
    buttons = [
        [KeyboardButton(text="Сегодня"), KeyboardButton(text="Завтра")],
        [KeyboardButton(text="Неделя"), KeyboardButton(text="Сменить класс")],
    ]

    if msg.from_user.id == OWNER_ID:
        buttons.append([KeyboardButton(text="📢 Рассылка")])

    markup = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await msg.answer(f"Ваш класс: {user_class}\n\nВыберите действие:", reply_markup=markup)

# === ОБРАБОТЧИКИ ===
@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    async with aiosqlite.connect("users.db") as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users 
                            (user_id INTEGER PRIMARY KEY, class TEXT)""")
        await db.commit()

        async with db.execute("SELECT class FROM users WHERE user_id = ?", (msg.from_user.id,)) as cursor:
            row = await cursor.fetchone()

    if row and row[0]:
        await send_main_menu(msg, row[0])
    else:
        await msg.answer(
            "Привет! 👋\n\n"
            "Это бот с расписанием уроков вашей школы.\n\n"
            "📌 Введите ваш класс (например: 10Б или 10 Б)"
        )

@router.message(F.text == "📢 Рассылка")
async def admin_broadcast_start(msg: Message, state: FSMContext):
    if msg.from_user.id != OWNER_ID:
        return
    await state.set_state(AdminStates.broadcast)
    await msg.answer("Введите текст для рассылки всем пользователям:", reply_markup=ReplyKeyboardRemove())

@router.message(AdminStates.broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    if msg.from_user.id != OWNER_ID:
        await state.clear()
        return

    broadcast_text = msg.text.strip()
    if not broadcast_text:
        await msg.answer("Текст пустой — рассылка отменена.")
        await state.clear()
        await send_main_menu(msg, saved_class or "Неизвестно")
        return

    await msg.answer("🔄 Начинаю рассылку...")

    async with aiosqlite.connect("users.db") as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            rows = await cursor.fetchall()

    sent = 0
    failed = 0
    total_users = len(rows)

    for row in rows:
        user_id = row[0]
        try:
            await msg.bot.send_message(user_id, broadcast_text)
            sent += 1
        except Exception as e:
            failed += 1
            print(f"Ошибка отправки пользователю {user_id}: {e}")
        await asyncio.sleep(0.05)  # антифлуд

    report = f"✅ Рассылка завершена!\n\n"
    report += f"Всего пользователей в базе: {total_users}\n"
    report += f"Отправлено успешно: {sent}\n"
    report += f"Не доставлено: {failed}"

    if sent == 0 and total_users > 0:
        report += "\n\n⚠️ Возможно, пользователи заблокировали бота или удалили чат."

    await msg.answer(report)
    await state.clear()

    # Возвращаем меню
    async with aiosqlite.connect("users.db") as db:
        async with db.execute("SELECT class FROM users WHERE user_id = ?", (OWNER_ID,)) as cursor:
            row = await cursor.fetchone()
        admin_class = row[0] if row else None

    await send_main_menu(msg, admin_class or "Админ")

@router.message(F.text)
async def handle_message(msg: Message, state: FSMContext):
    text = msg.text.strip()
    user_id = msg.from_user.id

    async with aiosqlite.connect("users.db") as db:
        async with db.execute("SELECT class FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        saved_class = row[0] if row else None

    # Ввод/смена класса
    normalized = re.sub(r'\s+', '', text.upper())
    if re.match(r'^\d{1,2}[А-Я]$', normalized):
        display_class = text.upper().strip()
        async with aiosqlite.connect("users.db") as db:
            await db.execute("REPLACE INTO users (user_id, class) VALUES (?, ?)", (user_id, display_class))
            await db.commit()
        await send_main_menu(msg, display_class)
        return

    if text == "Сменить класс":
        await msg.answer("Введите новый класс:")
        return

    if not saved_class:
        await msg.answer("Пожалуйста, введите класс правильно (пример: 10Б или 10 Б)")
        return

    download_and_parse_pdf()

    today = datetime.now()
    weekday = today.weekday()

    if text == "Сегодня":
        if weekday >= 5:
            await msg.answer("Сегодня выходной — уроков нет.")
            sched = await get_schedule_for_day(saved_class, 0)
            if sched:
                await msg.answer("Ближайшее расписание (Понедельник):\n\n" + sched)
            return
        sched = await get_schedule_for_day(saved_class, weekday)
        await msg.answer(sched or "Расписание на сегодня не найдено.")

    elif text == "Завтра":
        target_index = weekday + 1 if weekday < 4 else 0
        sched = await get_schedule_for_day(saved_class, target_index)
        await msg.answer(sched or "Расписание на завтра не найдено.")

    elif text == "Неделя":
        has_any = False
        for i in range(5):
            sched = await get_schedule_for_day(saved_class, i)
            if sched and "Нет уроков" not in sched and "не найдено" not in sched.lower():
                has_any = True
            await msg.answer(sched)
            await asyncio.sleep(0.3)
        if not has_any:
            await msg.answer("На этой неделе уроков нет или файл не содержит расписание.")

async def main():
    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())