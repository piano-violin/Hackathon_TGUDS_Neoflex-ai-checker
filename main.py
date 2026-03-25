# Основной исполняемый файл для запуска всего пайплайна от регистрации кандидата до генерации фидбека и выгрузки результатов.
import json # Импортируем json для сериализации данных при генерации фидбека
import os
import gc
from pathlib import Path
from typing import Dict, List, Tuple # Импортируем типы для аннотаций функций

import torch # Импортируем torch для работы с моделью 
from psycopg2.extras import RealDictCursor # Импортируем RealDictCursor для удобной работы с результатами запросов к БД в виде словарей

# Импортируем функции из модулей db, evaluations, model и prompts для работы с базой данных, оценки ответов, генерации текста и создания промптов
from db import (
    export_school_results_to_excel,
    get_candidate_feedback,
    get_connection,
    get_school_feedback,
    main as initialize_database,
    register_candidate,
    save_answers,
)
from evaluations import evaluate_session_answers
from model import qwen_model, qwen_tokenizer
from file_to_json_converter import convert_to_json
from prompts import (
    evaluate_answer_prompt,
    generate_candidate_final_feedback_prompt,
    generate_school_feedback_prompt,
)

INBOX_DIR = "in"
SUPPORTED_INPUT_EXTENSIONS = {
    ".txt", ".md", ".sql", ".pdf", ".docx", ".doc", ".xlsx", ".png", ".jpg", ".jpeg"
}

def _pick_latest_candidate_file(inbox_dir: str) -> Path:
    inbox_path = Path(inbox_dir)
    files = [
        p for p in inbox_path.iterdir()
        if p.is_file() and p.suffix.lower() in SUPPORTED_INPUT_EXTENSIONS
    ]

    if not files:
        supported = ", ".join(sorted(SUPPORTED_INPUT_EXTENSIONS))
        raise RuntimeError(
            f"В папке {inbox_dir}/ нет поддерживаемых файлов кандидата. "
            f"Поддерживаемые форматы: {supported}"
        )

    return max(files, key=lambda p: p.stat().st_mtime)


def parse_candidate_file_from_inbox(tasks: List[Dict], inbox_dir: str = INBOX_DIR) -> Dict[str, object]:
    """
    Читает самый свежий файл кандидата из папки inbox_dir и
    преобразует его в список ответов для сохранения в БД.
    """
    file_path = _pick_latest_candidate_file(inbox_dir)
    parsed = convert_to_json(str(file_path))

    if "error" in parsed:
        raise RuntimeError(
            f"Ошибка парсера для файла {file_path.name}: {parsed['error']}. "
            "Проверьте, что файл не пустой и содержит текст ответов (а не только скан-изображение)."
        )

    parsed_answers = parsed.get("answers")
    if not isinstance(parsed_answers, dict):
        raise RuntimeError(
            f"Парсер вернул неожиданный формат для {file_path.name}: ожидается объект 'answers'."
        )

    answers: List[Dict] = []
    for idx, task in enumerate(tasks, start=1):
        raw_answer = parsed_answers.get(str(idx), parsed_answers.get(idx, ""))
        answers.append(
            {
                "task_id": task["id"],
                "raw_answer": str(raw_answer or "").strip(),
            }
        )

    non_empty_count = sum(1 for a in answers if a["raw_answer"])
    return {
        "status": "ok",
        "message": (
            f"Файл кандидата обработан: {file_path.name}. "
            f"Распознано непустых ответов: {non_empty_count}/{len(answers)}."
        ),
        "source_file": str(file_path),
        "parsed_json": parsed_answers,
        "answers": answers,
    }


# Получаем задачи из БД для отображения кандидату. Каждая задача содержит id, title и description.
def fetch_tasks() -> List[Dict]:
    """
    Получает список задач из БД для отображения кандидату.
    """
    # Устанавливаем соединение с БД и выполняем запрос на получение задач. 
    # Результат возвращаем в виде списка словарей.
    conn = get_connection()
    # cursor_factory - позволяет получать результаты запросов в виде словарей, а не кортежей.
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

# Получаем данные сессии и оценки по задачам для генерации фидбека. Данные сессии включают имя кандидата, email и итоговый балл.
# Логика: выполняем два запроса - первый для получения данных сессии, второй для получения оценок по задачам. 
# Результаты возвращаем в виде кортежа (словарь с данными сессии, список словарей с оценками).
def fetch_session_context(session_id: int) -> Tuple[Dict, List[Dict]]:
    """
    Достает данные сессии и детальную оценку по задачам.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT c.full_name, c.email, s.total_score
            FROM sessions s
            JOIN candidates c ON c.id = s.candidate_id
            WHERE s.id = %s
            """,
            (session_id,),
        )
        session_data = cursor.fetchone()

        # Получаем оценки по задачам для данной сессии. 
        # Логика запроса: соединяем таблицы evaluations, answers и tasks, чтобы получить 
        # название задачи и связанные с ней оценки и комментарии. Результат сортируем по id задачи.
        cursor.execute(
            """
            SELECT
                t.title,
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


# Генерируем текст через локальную LLM. 
# Логика: сначала токенизируем входной промпт, затем передаем его в модель для генерации текста.
def generate_text_with_model(prompt: list, prefix: str, max_new_tokens: int = 1500) -> str:
    """
    Генерирует текст через локальную LLM.
    """
    # Проверяем, что модель и токенизатор загружены. Если нет, выбрасываем исключение.
    if qwen_model is None or qwen_tokenizer is None:
        raise RuntimeError("Модель не загружена")

    # Очистка перед итерацией
    gc.collect()
    torch.cuda.empty_cache()
    
    # Токенизируем входной промпт. 
    # Устанавливаем параметры для токенизации: возвращаем тензоры PyTorch,
    text = qwen_tokenizer.apply_chat_template(
        prompt, 
        tokenize=False, 
        add_generation_prompt=True
    )
    text += prefix 
    inputs = qwen_tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=4096,
    ).to(qwen_model.device)

    # Генерируем текст с помощью модели. Устанавливаем параметры генерации:
    # - max_new_tokens: максимальное количество новых токенов, которые модель может сгенерировать
    # - temperature: параметр, который влияет на разнообразие генерируемого текста (меньше - более консервативный, больше - более разнообразный)
    with torch.no_grad():
        outputs = qwen_model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            repetition_penalty=1.1,
            pad_token_id=qwen_tokenizer.eos_token_id,
            stop_strings=["<|file_sep|>", "<|fim_middle|>", "<|im_end|>", "###", "```"],
            tokenizer=qwen_tokenizer
        )
    # Отрезаем длину входного промпта от результата
    input_length = inputs.input_ids.shape[1]
    response_ids = outputs[0][input_length:]
    # Декодируем сгенерированные токены обратно в текст.
    generated_text = qwen_tokenizer.decode(response_ids, skip_special_tokens=True).strip()
    # Список мусора для удаления
    garbage_tokens  = ["<|file_sep|>", "<|fim_middle|>", "<|im_end|>", "<|fim_prefix|>", "<|fim_suffix|>"]
    for token in garbage_tokens :
        generated_text = generated_text.replace(token, "")
    generated_text = generated_text.replace("**", "").replace("__", "")
    return (prefix + " " + generated_text).strip()


def _looks_like_bad_feedback(text: str) -> bool:
    if not text or len(text.strip()) < 80:
        return True

    lowered = text.lower()
    
    # Стандартные маркеры плохого качества
    bad_markers = [
        "ваш ответ",
        "ответ:",
        "я здесь, чтобы помочь",
        "и так далее",
        "```",
        "###",
        "**",
    ]
    if any(marker in lowered for marker in bad_markers):
        return True

    # Простой фильтр на бессмысленное многократное повторение уровней
    if lowered.count("junior") >= 3 and lowered.count("middle") >= 2:
        return True
    
    # Проверка на галлюцинации и повторяющиеся фразы
    hallucination_markers = [
        "не является частью тестируемого задания",
        "текстовое сообщение",
        "это текстовое сообщение",
        "требования к тексту",
        "не является частью",
        "просто текстовое сообщение",
    ]
    hallucination_count = sum(1 for m in hallucination_markers if m in lowered)
    if hallucination_count >= 2:
        return True
    
    # Проверка на многократное повторение одной фразы (более 3 раз)
    words = lowered.split()
    if len(words) > 5:
        for phrase_len in range(3, 6):
            phrases = []
            for i in range(len(words) - phrase_len + 1):
                phrase = " ".join(words[i:i + phrase_len])
                phrases.append(phrase)
            for phrase in set(phrases):
                if phrases.count(phrase) > 3:
                    return True

    return False


def _collect_nonempty_fields(evaluations_data: List[Dict], field: str) -> List[str]:
    items: List[str] = []
    seen: set[str] = set()
    for row in evaluations_data:
        value = str(row.get(field, "") or "").strip()
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            items.append(value)
    return items


def _build_candidate_feedback_fallback(session_data: Dict, evaluations_data: List[Dict]) -> str:
    name = session_data["full_name"]
    total_score = session_data.get("total_score") or 0
    strengths = _collect_nonempty_fields(evaluations_data, "strengths")
    weaknesses = _collect_nonempty_fields(evaluations_data, "weaknesses")
    recommendations = _collect_nonempty_fields(evaluations_data, "recommendation")

    strengths_text = "; ".join(strengths[:2]) if strengths else "Есть попытка структурировать решения по заданиям."
    weaknesses_text = "; ".join(weaknesses[:2]) if weaknesses else "По части заданий ответы краткие и неполные."
    rec_text = "; ".join(recommendations[:2]) if recommendations else "Усилить практику SQL и проектирование схем хранения."

    return (
        f"{name}, здравствуйте!\n\n"
        f"Итоговый балл: {total_score} из 10.\n\n"
        "Общая оценка уровня:\n"
        "Уровень ближе к Junior: есть базовое понимание задач, но глубина и детализация ответа нестабильны.\n\n"
        f"Сильные стороны:\n{strengths_text}\n\n"
        f"Зоны роста и рекомендации:\n{weaknesses_text} "
        f"Рекомендуется: {rec_text}"
    )


def _build_school_feedback_fallback(session_data: Dict, evaluations_data: List[Dict]) -> str:
    total_score = float(session_data.get("total_score") or 0)
    level = "Junior" if total_score < 5 else "Strong Junior" if total_score < 7 else "Middle"
    weaknesses = _collect_nonempty_fields(evaluations_data, "weaknesses")
    strengths = _collect_nonempty_fields(evaluations_data, "strengths")
    recommendations = _collect_nonempty_fields(evaluations_data, "recommendation")

    strengths_text = "; ".join(strengths[:2]) if strengths else "Базовая логика решений присутствует."
    weaknesses_text = "; ".join(weaknesses[:2]) if weaknesses else "Нехватка полноты и точности по ряду заданий."
    rec_text = "; ".join(recommendations[:2]) if recommendations else "Нужна дополнительная практика по SQL и моделированию данных."

    final_recommendation = (
        "условно рекомендовать на обучение"
        if total_score >= 4
        else "не рекомендовать на текущем уровне"
    )

    return (
        f"Общий уровень кандидата: {level}. "
        f"Итоговый балл: {total_score:.2f} из 10.\n\n"
        f"Архитектурное мышление: {strengths_text}\n\n"
        f"Работа с SQL: {weaknesses_text}\n\n"
        "Типовые ошибки: неполная аргументация, местами поверхностная проработка требований задачи.\n\n"
        f"Рекомендация: {final_recommendation}. {rec_text}"
    )


# Логика функции insert_feedback_once:
# проверяем, что для сессии еще нет фидбека, и делаем только INSERT.
def insert_feedback_once(session_id: int, candidate_feedback: str, school_feedback: str) -> None:
    """
    Сохраняет фидбек один раз для сессии.
    Если запись уже существует, выбрасывает ошибку.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT id
            FROM feedback
            WHERE session_id = %s
            LIMIT 1
            """,
            (session_id,),
        )
        row = cursor.fetchone()

        if row:
            raise RuntimeError(f"Фидбек для session_id={session_id} уже существует и не подлежит обновлению")

        cursor.execute(
            """
            INSERT INTO feedback (session_id, candidate_feedback, school_feedback)
            VALUES (%s, %s, %s)
            """,
            (session_id, candidate_feedback, school_feedback),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# Генерируем итоговый фидбек кандидату и школе, затем сохраняем в БД.
def generate_and_store_final_feedback(session_id: int) -> Tuple[str, str]:
    """
    Генерирует итоговый фидбек кандидату и школе, затем сохраняет в БД.
    """
    session_data, evaluations_data = fetch_session_context(session_id)
    if not session_data:
        raise RuntimeError(f"Сессия {session_id} не найдена")

    evaluations_json = json.dumps(evaluations_data, ensure_ascii=False, indent=2)

    candidate_prompt = generate_candidate_final_feedback_prompt(
        candidate_name=session_data["full_name"],
        total_score=session_data["total_score"] or 0,
        max_total_score=10,
        evaluations_json=evaluations_json,
    )
    school_prompt = generate_school_feedback_prompt(
        candidate_name=session_data["full_name"],
        email=session_data["email"],
        total_score=session_data["total_score"] or 0,
        max_total_score=10,
        evaluations_json=evaluations_json,
    )

    try:
        cand_prefix = f"{session_data["full_name"]}, здравствуйте!\nИтоговый балл: {session_data["total_score"]} из 10.\n\nОБЩАЯ ОЦЕНКА УРОВНЯ:"
        candidate_feedback_text = generate_text_with_model(candidate_prompt, prefix=cand_prefix, max_new_tokens=700)
        
        school_prefix = f"ОТЧЕТ ПО КАНДИДАТУ: {session_data["full_name"]}\n\n1. ОБЩИЙ УРОВЕНЬ:"
        school_feedback_text = generate_text_with_model(school_prompt, prefix=school_prefix, max_new_tokens=900)
        
        # Проверяем качество
        if _looks_like_bad_feedback(candidate_feedback_text):
            print(f"⚠️ Фидбек кандидату плохой, используем fallback")
            candidate_feedback_text = _build_candidate_feedback_fallback(session_data, evaluations_data)

        if _looks_like_bad_feedback(school_feedback_text):
            print(f"⚠️ Фидбек школе плохой, используем fallback")
            school_feedback_text = _build_school_feedback_fallback(session_data, evaluations_data)
            
    except Exception as exc:
        print(f"❌ Генерация финального фидбека завершилась ошибкой: {exc}. Используем fallback.")
        candidate_feedback_text = _build_candidate_feedback_fallback(session_data, evaluations_data)
        school_feedback_text = _build_school_feedback_fallback(session_data, evaluations_data)

    insert_feedback_once(session_id, candidate_feedback_text, school_feedback_text)
    return candidate_feedback_text, school_feedback_text

# Основная функция для запуска всего пайплайна от регистрации кандидата до генерации фидбека и выгрузки результатов.
def run_pipeline() -> None:
    os.makedirs(INBOX_DIR, exist_ok=True)

    print("Шаг 1/8: Инициализация БД и таблиц...")
    initialize_database()

    print("\nШаг 2/8: Регистрация кандидата")
    full_name = input("Введите ФИО кандидата: ").strip()
    while not full_name:
        full_name = input("ФИО не может быть пустым. Введите ФИО кандидата: ").strip()

    email = input("Введите email кандидата: ").strip()
    while not email or "@" not in email:
        email = input("Некорректный email. Введите email кандидата: ").strip()

    session_id = register_candidate(full_name=full_name, email=email)
    print(f"Сессия создана: {session_id}")

    print("\nШаг 3/8: Загрузка задач")
    tasks = fetch_tasks()
    if not tasks:
        raise RuntimeError("В таблице tasks нет заданий")
    print(f"Найдено задач: {len(tasks)}")

    print("\nШаг 4/8: Парсинг файла кандидата")
    print(f"Папка для входящих файлов кандидатов: {INBOX_DIR}/")
    parser_result = parse_candidate_file_from_inbox(tasks, inbox_dir=INBOX_DIR)
    print(parser_result["message"])
    print(f"Файл-источник: {parser_result['source_file']}")
    print(f"JSON парсера: {parser_result['parsed_json']}")
    answers = parser_result["answers"]

    print("\nШаг 5/8: Ответы получены из файла кандидата")

    print("\nШаг 6/8: Сохранение ответов в БД")
    save_answers(session_id=session_id, answers=answers)

    print("\nШаг 7/8: Оценка ответов моделью")
    evaluate_session_answers(session_id=session_id)

    print("\nШаг 8/8: Генерация итогового фидбека и выгрузка")
    candidate_feedback, school_feedback = generate_and_store_final_feedback(session_id)
    candidate_view = get_candidate_feedback(session_id)
    school_view = get_school_feedback(session_id)

    print("\n" + "#" * 100)
    print("ФИДБЕК КАНДИДАТУ")
    print("#" * 100)
    if candidate_view:
        print(f"ФИО: {candidate_view['full_name']}")
        print(f"Итоговый балл: {candidate_view['total_score']}")
    print(candidate_feedback)

    print("\n" + "#" * 100)
    print("ФИДБЕК ДЛЯ ШКОЛЫ")
    print("#" * 100)
    if school_view:
        print(f"ФИО: {school_view['full_name']}")
        print(f"Email: {school_view['email']}")
        print(f"Итоговый балл: {school_view['total_score']}")
    print(school_feedback)

    output_file = f"school_results_session_{session_id}.xlsx"
    exported_file = export_school_results_to_excel(output_file)
    print(f"\nВыгрузка создана: {exported_file}")


if __name__ == "__main__":
    run_pipeline()
