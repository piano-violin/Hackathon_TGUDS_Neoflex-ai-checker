"""
Телеграм-бот для AI-агента проверки тестовых заданий.

Поддерживает два режима работы:
1. Кандидат — регистрация, получение заданий, отправка ответов, получение фидбека
2. Администратор (Учебный центр) — просмотр результатов, выгрузка Excel

Переменные окружения (.env):
    BOT_TOKEN — токен телеграм-бота (от @BotFather)
    ADMIN_PASSWORD — пароль для входа в режим администратора
"""

import os
import logging
import tempfile
from typing import Optional
from datetime import datetime

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode

from core import (
    get_tasks,
    get_tasks_with_ideal_answers,
    register_candidate,
    parse_candidate_file,
    parse_text_answers,
    save_answers,
    evaluate_session,
    get_candidate_result,
    get_school_result,
    get_all_sessions,
    get_session_context,
    export_results_to_excel,
    format_tasks_for_display,
    format_feedback_for_candidate,
    format_result_for_admin,
)

from db import main as initialize_database

# КОНФИГУРАЦИЯ
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "neoflex2026")

# Состояния пользователя
(
    STATE_START,
    STATE_REGISTER_NAME,
    STATE_REGISTER_EMAIL,
    STATE_VIEW_TASKS,
    STATE_SEND_ANSWERS,
    STATE_ADMIN_AUTH,
    STATE_ADMIN_MENU,
) = range(7)

# ХРАНИЛИЩЕ СОСТОЯНИЙ (в памяти)
user_states: dict = {}  # user_id -> state
user_data: dict = {}    # user_id -> {session_id, full_name, email, ...}


def get_user_state(user_id: int) -> int:
    return user_states.get(user_id, STATE_START)


def set_user_state(user_id: int, state: int) -> None:
    user_states[user_id] = state


def get_user_data(user_id: int) -> dict:
    if user_id not in user_data:
        user_data[user_id] = {}
    return user_data[user_id]


def clear_user_data(user_id: int) -> None:
    if user_id in user_data:
        del user_data[user_id]
    if user_id in user_states:
        del user_states[user_id]

# КОМАНДА /start
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает команду /start."""
    user_id = update.effective_user.id
    clear_user_data(user_id)

    keyboard = [
        ["🎓 Я кандидат (хочу пройти тестирование)"],
        ["🔐 Я администратор (Учебный центр)"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)

    await update.message.reply_text(
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Это бот для автоматической проверки тестовых заданий "
        "Учебного центра Неофлекс.\n\n"
        "Выберите ваш роль:",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )

    set_user_state(user_id, STATE_START)

# РЕГИСТРАЦИЯ КАНДИДАТА
async def handle_candidate_registration(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Обрабатывает выбор роли 'Кандидат'."""
    user_id = update.effective_user.id
    data = get_user_data(user_id)

    state = get_user_state(user_id)

    if state == STATE_START:
        # Пользователь выбрал "Я кандидат"
        set_user_state(user_id, STATE_REGISTER_NAME)
        await update.message.reply_text(
            "📝 <b>Регистрация кандидата</b>\n\n"
            "Введите ваше ФИО (полностью):",
            parse_mode=ParseMode.HTML,
            reply_markup=ReplyKeyboardRemove(),
        )

    elif state == STATE_REGISTER_NAME:
        # Получили ФИО
        full_name = update.message.text.strip()
        if len(full_name) < 3:
            await update.message.reply_text(
                "❌ ФИО должно содержать минимум 3 символа. Попробуйте ещё раз:"
            )
            return

        data["full_name"] = full_name
        set_user_state(user_id, STATE_REGISTER_EMAIL)
        await update.message.reply_text(
            f"✅ ФИО: {full_name}\n\n"
            "📧 Теперь введите ваш email:",
            parse_mode=ParseMode.HTML,
        )

    elif state == STATE_REGISTER_EMAIL:
        # Получили email
        email = update.message.text.strip()
        if "@" not in email or "." not in email:
            await update.message.reply_text(
                "❌ Некорректный email. Введите email в формате user@example.com:"
            )
            return

        data["email"] = email

        # Регистрируем кандидата в БД
        try:
            session_id = register_candidate(data["full_name"], data["email"])
            data["session_id"] = session_id
            logger.info(f"Зарегистрирован кандидат: {data['full_name']}, session_id={session_id}")

            # Показываем задания
            tasks = get_tasks()
            data["tasks"] = tasks

            set_user_state(user_id, STATE_VIEW_TASKS)

            keyboard = [
                ["📝 Отправить ответы"],
                ["❌ Отмена"],
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

            await update.message.reply_text(
                f"✅ <b>Регистрация завершена!</b>\n"
                f"👤 {data['full_name']}\n"
                f"📧 {data['email']}\n"
                f"🆔 ID сессии: {session_id}\n\n"
                f"{format_tasks_for_display(tasks)}\n"
                f"Нажмите 'Отправить ответы', когда будете готовы.",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )

        except Exception as e:
            logger.error(f"Ошибка регистрации: {e}")
            await update.message.reply_text(
                f"❌ Ошибка регистрации: {e}\n\n"
                "Попробуйте позже или обратитесь к администратору."
            )
            clear_user_data(user_id)

# ОТПРАВКА ОТВЕТОВ
async def handle_send_answers(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Обрабатывает отправку ответов кандидатом."""
    user_id = update.effective_user.id
    data = get_user_data(user_id)
    state = get_user_state(user_id)

    if state == STATE_VIEW_TASKS and update.message.text == "📝 Отправить ответы":
        set_user_state(user_id, STATE_SEND_ANSWERS)
        await update.message.reply_text(
            "📤 <b>Отправка ответов</b>\n\n"
            "Вы можете отправить ответы одним из способов:\n\n"
            "1️⃣ <b>Текстом</b> — напишите ответы в формате:\n"
            "   <code>1. Ответ на первое задание\n"
            "   2. Ответ на второе задание\n"
            "   ...</code>\n\n"
            "2️⃣ <b>Файлом</b> — прикрепите файл с ответами\n"
            "   (поддерживаются: TXT, PDF, DOCX, XLSX, изображения)\n\n"
            "3️⃣ <b>Отмена</b> — команда /cancel",
            parse_mode=ParseMode.HTML,
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    if state == STATE_SEND_ANSWERS:
        # Обрабатываем полученные ответы
        await process_answers(update, context)


async def process_answers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает полученные ответы (текст или файл)."""
    user_id = update.effective_user.id
    data = get_user_data(user_id)
    tasks = data.get("tasks", get_tasks())

    # Проверяем, есть ли файл
    if update.message.document:
        # Обработка файла
        document = update.message.document
        file_name = document.file_name or "unknown"

        await update.message.reply_text(
            f"📎 Получен файл: <b>{file_name}</b>\n"
            f"⏳ Обрабатываю...",
            parse_mode=ParseMode.HTML,
        )

        try:
            # Скачиваем файл во временную директорию
            file = await context.bot.get_file(document.file_id)

            with tempfile.NamedTemporaryFile(
                delete=False, 
                suffix=os.path.splitext(file_name)[1]
            ) as tmp_file:
                await file.download_to_drive(tmp_file.name)
                tmp_path = tmp_file.name

            # Парсим файл
            result = parse_candidate_file(tmp_path, tasks)

            # Удаляем временный файл
            os.unlink(tmp_path)

            if result["status"] == "error":
                await update.message.reply_text(
                    f"❌ <b>Ошибка обработки файла</b>\n\n"
                    f"{result['message']}\n\n"
                    f"Попробуйте отправить другой файл или введите ответы текстом.",
                    parse_mode=ParseMode.HTML,
                )
                return

            answers = result["answers"]

        except Exception as e:
            logger.error(f"Ошибка обработки файла: {e}")
            await update.message.reply_text(
                f"❌ Ошибка при обработке файла: {e}\n\n"
                "Попробуйте отправить другой файл или введите ответы текстом."
            )
            return

    elif update.message.text:
        # Обработка текста
        text = update.message.text
        result = parse_text_answers(text, tasks)

        if result["status"] == "error":
            await update.message.reply_text(
                f"❌ {result['message']}\n\n"
                "Попробуйте ещё раз."
            )
            return

        answers = result["answers"]

    else:
        await update.message.reply_text(
            "❌ Не распознано. Отправьте текст или файл с ответами."
        )
        return

    # Показываем распознанные ответы
    await update.message.reply_text(
        f"✅ <b>Ответы получены!</b>\n\n"
        f"{result['message']}\n\n"
        f"⏳ Запускаю проверку...",
        parse_mode=ParseMode.HTML,
    )

    # Сохраняем и оцениваем
    try:
        session_id = data["session_id"]

        # Сохраняем ответы
        save_answers(session_id, answers)

        # Оцениваем
        total_score = evaluate_session(session_id)

        # Генерируем фидбек (если модель загружена)
        try:
            from main import generate_and_store_final_feedback
            generate_and_store_final_feedback(session_id)
        except Exception as e:
            logger.warning(f"Не удалось сгенерировать фидбек: {e}")

        # Получаем результат
        result_data = get_candidate_result(session_id)

        keyboard = [["🏠 На главную"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

        if result_data:
            await update.message.reply_text(
                format_feedback_for_candidate(result_data),
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
        else:
            await update.message.reply_text(
                f"✅ <b>Проверка завершена!</b>\n\n"
                f"📊 Ваш итоговый балл: <b>{total_score:.1f}/10</b>\n\n"
                f"Результаты будут дополнительно отправлены на вашу почту.",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )

        clear_user_data(user_id)
        set_user_state(user_id, STATE_START)

    except Exception as e:
        logger.error(f"Ошибка при проверке: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при проверке: {e}\n\n"
            "Обратитесь к администратору."
        )

# АДМИНИСТРАТОР
async def handle_admin_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает вход администратора."""
    user_id = update.effective_user.id
    state = get_user_state(user_id)

    if state == STATE_START and update.message.text == "🔐 Я администратор (Учебный центр)":
        set_user_state(user_id, STATE_ADMIN_AUTH)
        await update.message.reply_text(
            "🔐 <b>Вход в панель администратора</b>\n\n"
            "Введите пароль:",
            parse_mode=ParseMode.HTML,
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    if state == STATE_ADMIN_AUTH:
        password = update.message.text.strip()

        if password == ADMIN_PASSWORD:
            set_user_state(user_id, STATE_ADMIN_MENU)
            await show_admin_menu(update)
        else:
            await update.message.reply_text(
                "❌ Неверный пароль. Попробуйте ещё раз или нажмите /start для выхода."
            )


async def show_admin_menu(update: Update) -> None:
    """Показывает меню администратора."""
    keyboard = [
        ["📊 Список всех кандидатов"],
        ["📥 Выгрузить результаты в Excel"],
        ["🔍 Найти по email"],
        ["🏠 На главную"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "🔐 <b>Панель администратора</b>\n\n"
        "Выберите действие:",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup,
    )


async def handle_admin_actions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обрабатывает действия администратора."""
    user_id = update.effective_user.id
    text = update.message.text

    if get_user_state(user_id) != STATE_ADMIN_MENU:
        return

    if text == "📊 Список всех кандидатов":
        await show_all_candidates(update)

    elif text == "📥 Выгрузить результаты в Excel":
        await export_results(update, context)

    elif text == "🔍 Найти по email":
        await update.message.reply_text(
            "📧 Введите email кандидата для поиска:",
            reply_markup=ReplyKeyboardRemove(),
        )
        # Сохраняем состояние ожидания email
        get_user_data(user_id)["waiting_email"] = True

    elif text == "🏠 На главную":
        clear_user_data(user_id)
        await cmd_start(update, context)


async def show_all_candidates(update: Update) -> None:
    """Показывает список всех кандидатов."""
    sessions = get_all_sessions()

    if not sessions:
        await update.message.reply_text(
            "📭 База данных пуста. Нет зарегистрированных кандидатов."
        )
        return

    lines = ["📊 <b>Список кандидатов:</b>\n"]

    for i, session in enumerate(sessions[:20], 1):  # Показываем последние 20
        feedback_icon = "✅" if session["has_feedback"] else "⏳"
        lines.append(
            f"{i}. {feedback_icon} <b>{session['full_name']}</b>\n"
            f"   📧 {session['email']}\n"
            f"   📊 Балл: {session['total_score']:.1f}/10\n"
            f"   🆔 Сессия: #{session['session_id']}\n"
        )

    if len(sessions) > 20:
        lines.append(f"\n<i>... и ещё {len(sessions) - 20} записей</i>")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


async def export_results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Экспортирует результаты в Excel и отправляет файл."""
    await update.message.reply_text("⏳ Формирую Excel-файл...")

    try:
        # Создаём файл во временной директории
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"neoflex_results_{timestamp}.xlsx"
        filepath = os.path.join(tempfile.gettempdir(), filename)

        result_path = export_results_to_excel(filepath)

        # Отправляем файл
        with open(result_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption="📊 <b>Результаты тестирования</b>\n\n"
                        "Файл содержит информацию о всех кандидатах, "
                        "их баллах и обратную связь для Учебного центра.",
                parse_mode=ParseMode.HTML,
            )

        # Удаляем временный файл
        os.unlink(result_path)

    except Exception as e:
        logger.error(f"Ошибка экспорта: {e}")
        await update.message.reply_text(
            f"❌ Ошибка при создании файла: {e}"
        )

# КОМАНДА /cancel
async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отменяет текущее действие и возвращает в начало."""
    user_id = update.effective_user.id
    clear_user_data(user_id)

    await update.message.reply_text(
        "❌ Действие отменено.",
        reply_markup=ReplyKeyboardRemove(),
    )

    await cmd_start(update, context)

# КОМАНДА /help
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает справку."""
    help_text = """
📚 <b>Справка по боту</b>

<b>Для кандидатов:</b>
1. Нажмите "Я кандидат"
2. Введите ФИО и email
3. Ознакомьтесь с заданиями
4. Отправьте ответы текстом или файлом
5. Получите результат с обратной связью

<b>Поддерживаемые форматы файлов:</b>
• TXT, MD, SQL — текстовые файлы
• PDF — документы PDF
• DOCX, DOC — документы Word
• XLSX — таблицы Excel
• PNG, JPG — изображения (с OCR)

<b>Для администраторов:</b>
1. Нажмите "Я администратор"
2. Введите пароль
3. Используйте панель для просмотра результатов

<b>Команды:</b>
/start — начать заново
/cancel — отменить текущее действие
/help — показать эту справку
"""
    await update.message.reply_text(
        help_text,
        parse_mode=ParseMode.HTML,
    )

# ОБРАБОТЧИК СООБЩЕНИЙ
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Главный обработчик сообщений."""
    user_id = update.effective_user.id
    state = get_user_state(user_id)
    text = update.message.text or ""

    # Проверяем ожидание email для поиска
    if get_user_data(user_id).get("waiting_email"):
        get_user_data(user_id).pop("waiting_email", None)
        await search_by_email(update, text)
        await show_admin_menu(update)
        return

    # Маршрутизация по состояниям
    if state in (STATE_START, STATE_REGISTER_NAME, STATE_REGISTER_EMAIL):
        if "кандидат" in text.lower():
            await handle_candidate_registration(update, context)
        elif "администратор" in text.lower():
            await handle_admin_auth(update, context)
        else:
            await handle_candidate_registration(update, context)

    elif state == STATE_VIEW_TASKS:
        if "отправить" in text.lower() or "ответ" in text.lower():
            await handle_send_answers(update, context)
        elif "отмена" in text.lower():
            await cmd_cancel(update, context)

    elif state == STATE_SEND_ANSWERS:
        await process_answers(update, context)

    elif state == STATE_ADMIN_AUTH:
        await handle_admin_auth(update, context)

    elif state == STATE_ADMIN_MENU:
        await handle_admin_actions(update, context)

    elif text == "🏠 На главную":
        await cmd_start(update, context)


async def search_by_email(update: Update, email: str) -> None:
    """Ищет кандидата по email."""
    sessions = get_all_sessions()
    found = [s for s in sessions if s["email"].lower() == email.lower()]

    if not found:
        await update.message.reply_text(
            f"❌ Кандидат с email <b>{email}</b> не найден.",
            parse_mode=ParseMode.HTML,
        )
        return

    session = found[0]
    result = get_school_result(session["session_id"])

    if result:
        session_data, evaluations = get_session_context(session["session_id"])
        await update.message.reply_text(
            format_result_for_admin(
                {"session_id": session["session_id"], **result},
                evaluations if evaluations else []
            ),
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"👤 <b>{session['full_name']}</b>\n"
            f"📧 {session['email']}\n"
            f"📊 Балл: {session['total_score']:.1f}/10\n\n"
            f"⚠️ Детальный фидбек ещё не сформирован.",
            parse_mode=ParseMode.HTML,
        )

# ЗАПУСК БОТА
def main() -> None:
    """Запускает бота."""
    if not BOT_TOKEN:
        print("❌ Ошибка: не указан BOT_TOKEN в переменных окружения")
        print("Добавьте в .env файл: BOT_TOKEN=your_token_here")
        return

    # Создаём приложение
    application = Application.builder().token(BOT_TOKEN).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(MessageHandler(filters.TEXT | filters.Document.ALL, handle_message))
    
    # Инициализаруем базу даных
    initialize_database()

    # Запускаем бота
    print("🤖 Бот запущен!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
