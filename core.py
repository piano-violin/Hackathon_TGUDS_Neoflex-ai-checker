"""
Ядро бизнес-логики для AI-агента проверки тестовых заданий.

Этот модуль содержит функции, которые используются как в консольном пайплайне (main.py),
так и в телеграм-боте (bot.py). Разделение позволяет переиспользовать код и упрощает тестирование.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from psycopg2.extras import RealDictCursor

from db import (
    get_connection,
    register_candidate as db_register_candidate,
    save_answers as db_save_answers,
    get_candidate_feedback,
    get_school_feedback,
    export_school_results_to_excel,
)
from evaluations import evaluate_session_answers
from file_to_json_converter import convert_to_json


# Константы
INBOX_DIR = "in"
SUPPORTED_INPUT_EXTENSIONS = {
    ".txt", ".md", ".sql", ".pdf", ".docx", ".doc", ".xlsx", ".png", ".jpg", ".jpeg"
}

# РАБОТА С ЗАДАЧАМИ
def get_tasks() -> List[Dict]:
    """
    Получает список задач из БД для отображения кандидату.

    Returns:
        Список словарей с полями: id, title, description
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT id, title, description
            FROM tasks
            ORDER BY id
            """
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def get_tasks_with_ideal_answers() -> List[Dict]:
    """
    Получает задачи с эталонными ответами (для админов).

    Returns:
        Список словарей с полями: id, title, description, ideal_answer, max_score
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT id, title, description, ideal_answer, max_score
            FROM tasks
            ORDER BY id
            """
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

# РЕГИСТРАЦИЯ И СЕССИИ
def register_candidate(full_name: str, email: str) -> int:
    """
    Регистрирует кандидата и создаёт новую сессию.

    Args:
        full_name: ФИО кандидата
        email: Email кандидата

    Returns:
        ID созданной сессии
    """
    return db_register_candidate(full_name=full_name, email=email)


def get_session_context(session_id: int) -> Tuple[Optional[Dict], List[Dict]]:
    """
    Получает данные сессии и детальные оценки по задачам.

    Args:
        session_id: ID сессии

    Returns:
        Кортеж (данные_сессии, список_оценок)
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Данные сессии
        cursor.execute(
            """
            SELECT c.full_name, c.email, s.total_score, s.id as session_id
            FROM sessions s
            JOIN candidates c ON c.id = s.candidate_id
            WHERE s.id = %s
            """,
            (session_id,),
        )
        session_data = cursor.fetchone()

        # Оценки по задачам
        cursor.execute(
            """
            SELECT
                t.id as task_id,
                t.title,
                a.raw_answer,
                e.score,
                e.feedback,
                e.strengths,
                e.weaknesses,
                e.recommendation
            FROM evaluations e
            JOIN answers a ON a.id = e.answer_id
            JOIN tasks t ON t.id = a.task_id
            WHERE a.session_id = %s
            ORDER BY t.id
            """,
            (session_id,),
        )
        evaluations_data = cursor.fetchall()

        return session_data, evaluations_data
    finally:
        cursor.close()
        conn.close()

# ПАРСИНГ ФАЙЛОВ
def parse_candidate_file(file_path: str, tasks: List[Dict]) -> Dict:
    """
    Парсит файл с ответами кандидата.

    Args:
        file_path: Путь к файлу
        tasks: Список задач для сопоставления ответов

    Returns:
        Словарь с результатами парсинга:
        {
            "status": "ok" | "error",
            "message": "...",
            "source_file": "...",
            "answers": [{"task_id": int, "raw_answer": str}, ...]
        }
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return {
            "status": "error",
            "message": f"Файл не найден: {file_path}",
            "answers": []
        }

    parsed = convert_to_json(str(file_path))

    if "error" in parsed:
        return {
            "status": "error",
            "message": f"Ошибка парсера: {parsed['error']}",
            "answers": []
        }

    parsed_answers = parsed.get("answers")
    if not isinstance(parsed_answers, dict):
        return {
            "status": "error",
            "message": "Парсер вернул неожиданный формат",
            "answers": []
        }

    answers: List[Dict] = []
    for idx, task in enumerate(tasks, start=1):
        raw_answer = parsed_answers.get(str(idx), parsed_answers.get(idx, ""))
        answers.append({
            "task_id": task["id"],
            "raw_answer": str(raw_answer or "").strip(),
        })

    non_empty_count = sum(1 for a in answers if a["raw_answer"])

    return {
        "status": "ok",
        "message": f"Распознано непустых ответов: {non_empty_count}/{len(answers)}",
        "source_file": str(file_path),
        "answers": answers,
    }


def parse_text_answers(text: str, tasks: List[Dict]) -> Dict:
    """
    Парсит текст с ответами кандидата (из сообщения в боте).

    Поддерживает форматы:
    - "1. Ответ на первое задание"
    - "Задание 1: Ответ"
    - Просто текст (всё идёт как ответ на первое задание)

    Args:
        text: Текст с ответами
        tasks: Список задач

    Returns:
        Словарь с результатами парсинга
    """
    import re

    if not text or not text.strip():
        return {
            "status": "error",
            "message": "Пустой текст ответов",
            "answers": []
        }

    answers: Dict[str, str] = {}

    # Пытаемся найти структурированные ответы
    # Паттерн 1: "1. текст" или "1) текст"
    pattern1 = r'(?:^|\n)\s*(\d+)\s*[.)\]]\s*(.*?)(?=(?:\n\s*\d+\s*[.)\]])|$)'
    matches1 = re.findall(pattern1, text, re.DOTALL)

    # Паттерн 2: "Задание 1: текст" или "Задание №1 текст"
    pattern2 = r'(?:задание|задача)\s*№?\s*(\d+)\s*[:.)\]]?\s*(.*?)(?=(?:задание|задача)|$)'
    matches2 = re.findall(pattern2, text, re.IGNORECASE | re.DOTALL)

    if matches1:
        for num, answer in matches1:
            num = int(num)
            if 1 <= num <= len(tasks):
                answers[str(num)] = answer.strip()
    elif matches2:
        for num, answer in matches2:
            num = int(num)
            if 1 <= num <= len(tasks):
                answers[str(num)] = answer.strip()
    else:
        # Если структура не найдена, весь текст идёт как ответ на первое задание
        answers["1"] = text.strip()

    # Формируем список ответов
    result_answers: List[Dict] = []
    for idx, task in enumerate(tasks, start=1):
        raw_answer = answers.get(str(idx), "")
        result_answers.append({
            "task_id": task["id"],
            "raw_answer": raw_answer,
        })

    non_empty_count = sum(1 for a in result_answers if a["raw_answer"])

    return {
        "status": "ok",
        "message": f"Обработано ответов: {non_empty_count}/{len(tasks)}",
        "answers": result_answers,
    }

# СОХРАНЕНИЕ И ОЦЕНКА
def save_answers(session_id: int, answers: List[Dict]) -> None:
    """
    Сохраняет ответы кандидата в БД.

    Args:
        session_id: ID сессии
        answers: Список словарей {"task_id": int, "raw_answer": str}
    """
    db_save_answers(session_id=session_id, answers=answers)


def evaluate_session(session_id: int) -> float:
    """
    Оценивает все ответы в сессии и генерирует фидбек.

    Args:
        session_id: ID сессии

    Returns:
        Итоговый балл (0-10)
    """
    evaluate_session_answers(session_id)

    # Получаем итоговый балл
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT total_score FROM sessions WHERE id = %s",
            (session_id,)
        )
        result = cursor.fetchone()
        return float(result[0]) if result and result[0] else 0.0
    finally:
        cursor.close()
        conn.close()

# ГЕНЕРАЦИЯ ФИДБЕКА
def generate_and_store_feedback(session_id: int) -> Tuple[str, str]:
    """
    Генерирует и сохраняет фидбек для кандидата и школы.

    Args:
        session_id: ID сессии

    Returns:
        Кортеж (фидбек_кандидату, фидбек_школе)
    """
    # Импортируем здесь, чтобы избежать циклической зависимости
    from main import generate_and_store_final_feedback
    return generate_and_store_final_feedback(session_id)

# ПОЛУЧЕНИЕ РЕЗУЛЬТАТОВ
def get_candidate_result(session_id: int) -> Optional[Dict]:
    """
    Получает результат для кандидата.

    Args:
        session_id: ID сессии

    Returns:
        Словарь с полями: full_name, total_score, candidate_feedback
    """
    return get_candidate_feedback(session_id)


def get_school_result(session_id: int) -> Optional[Dict]:
    """
    Получает результат для Учебного центра.

    Args:
        session_id: ID сессии

    Returns:
        Словарь с полями: full_name, email, total_score, school_feedback
    """
    return get_school_feedback(session_id)


def get_all_sessions() -> List[Dict]:
    """
    Получает список всех сессий с результатами.

    Returns:
        Список словарей с информацией о сессиях
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT
                s.id as session_id,
                c.full_name,
                c.email,
                s.total_score,
                CASE WHEN f.id IS NOT NULL THEN true ELSE false END as has_feedback
            FROM sessions s
            JOIN candidates c ON c.id = s.candidate_id
            LEFT JOIN feedback f ON f.session_id = s.id
            ORDER BY s.id DESC
            """
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


def export_results_to_excel(filepath: str = "school_results.xlsx") -> str:
    """
    Экспортирует результаты в Excel.

    Args:
        filepath: Путь к файлу

    Returns:
        Путь к созданному файлу
    """
    return export_school_results_to_excel(filepath)

# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
def format_tasks_for_display(tasks: List[Dict]) -> str:
    """
    Форматирует список задач для отображения в боте.

    Args:
        tasks: Список задач

    Returns:
        Отформатированный текст
    """
    lines = ["📚 <b>Тестовые задания:</b>\n"]

    for task in tasks:
        lines.append(f"<b>Задание {task['id']}</b>")
        lines.append(f"<i>{task['title']}</i>\n")
        desc = task['description']
        lines.append(f"{desc}\n")
        lines.append("─" * 40 + "\n")

    return "\n".join(lines)


def format_feedback_for_candidate(data: Dict) -> str:
    """
    Форматирует фидбек для отправки кандидату.

    Args:
        data: Данные из get_candidate_feedback()

    Returns:
        Отформатированный текст для Telegram
    """
    if not data:
        return "❌ Результаты не найдены."

    lines = [
        f"👤 <b>{data['full_name']}</b>",
        f"📊 <b>Итоговый балл: {data['total_score']:.1f}/10</b>\n",
        "─" * 40,
    ]

    feedback = data.get('candidate_feedback', '')
    if feedback:
        # Разбиваем на параграфы
        lines.append(feedback)

    return "\n".join(lines)


def format_result_for_admin(session_data: Dict, evaluations: List[Dict]) -> str:
    """
    Форматирует результат для администратора.

    Args:
        session_data: Данные сессии
        evaluations: Оценки по задачам

    Returns:
        Отформатированный текст для Telegram
    """
    if not session_data:
        return "❌ Сессия не найдена."

    lines = [
        f"📋 <b>Результаты сессии #{session_data['session_id']}</b>",
        f"👤 Кандидат: {session_data['full_name']}",
        f"📧 Email: {session_data['email']}",
        f"📊 Итоговый балл: <b>{session_data['total_score']:.1f}/10</b>\n",
        "─" * 40,
        "\n<b>Детали по заданиям:</b>\n",
    ]

    for eval_data in evaluations:
        lines.append(f"<b>Задание {eval_data['task_id']}: {eval_data['title']}</b>")
        lines.append(f"   Балл: {eval_data['score']}")
        if eval_data.get('feedback'):
            lines.append(f"   📝 {eval_data['feedback'][:200]}...")
        lines.append("")

    return "\n".join(lines)
