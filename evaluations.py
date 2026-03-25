# модуль оценки ответа кандидата на задание
import json
import re
import gc
from typing import Dict, Any # для аннотации типов

from db import get_tasks_for_session, save_evaluation, generate_session_feedback
from prompts import evaluate_answer_prompt
from model import qwen_model, qwen_tokenizer
import torch

# Множество обязательных ключей для оценки
# Ожидается, что модель вернет JSON с этими полями. Если какое-то из них отсутствует, оценка считается невалидной.
REQUIRED_EVALUATION_KEYS = {
    "score",
    "feedback",
    "strengths",
    "weaknesses",
    "recommendation",
}

def build_rule_based_evaluation(
    task_id: int,
    task_title: str,
    candidate_answer: str,
    max_score: int
) -> Dict[str, Any]:
    """
    Rule-based fallback по ключевым признакам задачи.
    Используется, когда LLM не вернула валидный JSON.
    """
    text = (candidate_answer or "").strip()
    lower = text.lower()
    if not text:
        return {
            "score": 0,
            "feedback": "Ответ отсутствует.",
            "strengths": "Не выявлены.",
            "weaknesses": "Нет содержимого для оценки.",
            "recommendation": "Предоставьте полный ответ по задаче.",
        }

    ratio = 0.35 if len(text) < 120 else 0.5 if len(text) < 350 else 0.62
    strengths: list[str] = []
    weaknesses: list[str] = []
    recommendation: list[str] = []

    if task_id == 1:
        checks = {
            "keys": any(k in lower for k in ["primary key", "уник", "client_id", "login"]),
            "relations": any(k in lower for k in ["foreign key", "внешн", "city_id", "справочник"]),
            "dob": any(k in lower for k in ["дата рождения", "возраст"]),
            "contacts": any(k in lower for k in ["email", "phone", "контакт"]),
        }
        ratio += 0.08 * sum(checks.values())
        strengths.append("Выделены ключевые проблемы исходной схемы клиентов.")
        if checks["relations"]:
            strengths.append("Учтена ссылочная целостность по city_id.")
        if not checks["contacts"]:
            weaknesses.append("Не хватает детализации по контактным и служебным полям.")
        recommendation.append("Добавить полный DDL с ограничениями и индексами.")
    elif task_id == 2:
        checks = {
            "history": any(k in lower for k in ["effective_date", "expiration_date", "истор", "scd"]),
            "table": any(k in lower for k in ["item_prices", "таблиц", "цены"]),
            "ref": any(k in lower for k in ["item_id", "references", "foreign key"]),
        }
        ratio += 0.1 * sum(checks.values())
        strengths.append("Предложено историческое хранение цен в отдельной таблице.")
        if checks["ref"]:
            strengths.append("Связь цены с товаром описана корректно.")
        else:
            weaknesses.append("Нужно явнее описать ключи и ограничения целостности.")
        recommendation.append("Добавить правила закрытия предыдущего периода цены.")
    elif task_id == 3:
        checks = {
            "is_null": "is null" in lower,
            "group_by": "group by" in lower,
            "having": "having" in lower,
            "join": " join " in lower or "\njoin" in lower,
            "seoul": "seoul" in lower,
        }
        ratio += 0.08 * sum(checks.values())
        strengths.append("Запрос структурирован и близок к рабочему варианту.")
        if checks["having"]:
            strengths.append("Агрегатный фильтр вынесен в HAVING.")
        if not checks["is_null"]:
            weaknesses.append("Проверку отсутствия менеджера нужно делать через IS NULL.")
        recommendation.append("Проверить обработку случая с несколькими LOCATION_ID.")
    else:
        good = any(k in lower for k in ["у стены", "около стены", "вплотную", "стен"])
        ratio += 0.2 if good else 0.0
        strengths.append("Есть попытка логического обоснования ответа.")
        if not good:
            weaknesses.append("Ключевая идея задачи про карандаш у стены раскрыта неточно.")
        recommendation.append("Сформулировать ответ короче, с фокусом на ключевое условие.")

    ratio = max(0.05, min(0.95, ratio))
    score = max(0, min(max_score, int(round(max_score * ratio))))
    return {
        "score": score,
        "feedback": f"Rule-based оценка: {task_title}. Ответ проверен по ключевым критериям задачи.",
        "strengths": "; ".join(strengths[:2]) if strengths else "Ответ по задаче предоставлен.",
        "weaknesses": "; ".join(weaknesses[:2]) if weaknesses else "Критичных ошибок не выявлено, но детализацию можно усилить.",
        "recommendation": "; ".join(recommendation[:2]) if recommendation else "Усилить глубину аргументации.",
    }

# Если LLM недоступна, используем эту функцию
# На момент теста и отладки модели, она будет генерировать фиктивные оценки на основе длины ответа кандидата и других простых эвристик.
def build_mock_evaluation(candidate_answer: str, max_score: int) -> Dict[str, Any]:
    """
    Заглушка оценки, если LLM недоступна.
    Нужна для тестирования пайплайна без установленной модели.
    """
    text = (candidate_answer or "").strip()
    if not text:
        score = 0
        feedback = "Ответ отсутствует."
        strengths = "Не выявлены."
        weaknesses = "Нет содержимого для оценки."
        recommendation = "Предоставьте полный ответ по задаче."
    else:
        length = len(text)
        ratio = 0.45 if length < 80 else 0.6 if length < 300 else 0.72
        score = max(1, min(max_score, int(round(max_score * ratio))))
        feedback = "Оценка сформирована в тестовом режиме без LLM."
        strengths = "Ответ предоставлен, структура читается."
        weaknesses = "Не выполнен полноценный semantic review моделью."
        recommendation = "Запустите с рабочей LLM для точной технической оценки."

    return {
        "score": score,
        "feedback": feedback,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "recommendation": recommendation,
    }


def evaluate_session_answers(session_id: int):
    """
    Основная функция для оценки всех ответов в сессии.
    Получает задачи и ответы из БД, оценивает их моделью, сохраняет оценки в БД.
    """
    model_available = qwen_tokenizer is not None and qwen_model is not None
    if not model_available:
        print("Модель не загружена. Включен fallback: тестовые оценки без LLM.")
    
    # Очистка перед итерацией
    gc.collect()
    torch.cuda.empty_cache()
    
    # Получаем задачи и ответы для сессии
    tasks_with_answers = get_tasks_for_session(session_id)
    
    if not tasks_with_answers:
        print(f"Нет задач для сессии {session_id}")
        return
    
    # Проходим по каждому ответу и оцениваем
    for record in tasks_with_answers:
        task_id = record['task_id']
        task_title = record['title']
        task_description = record['description']
        ideal_answer = record['ideal_answer']
        max_score = record['max_score']
        answer_id = record['answer_id']
        candidate_answer = record['raw_answer']
        
        if not candidate_answer:
            # Пустой ответ должен получать явную оценку 0, а не выпадать из расчета.
            evaluation_result = build_rule_based_evaluation(
                task_id=task_id,
                task_title=task_title,
                candidate_answer="",
                max_score=max_score
            )
            save_evaluation(
                answer_id=answer_id,
                score=evaluation_result.get('score', 0),
                feedback=evaluation_result.get('feedback'),
                strengths=evaluation_result.get('strengths'),
                weaknesses=evaluation_result.get('weaknesses'),
                recommendation=evaluation_result.get('recommendation')
            )
            print(f"Ответ для задачи {task_id} отсутствует; сохранена оценка 0 для ответа {answer_id}.")
            continue
    
        generated_text = ""
        try:
            if model_available:
                # Генерируем промпт для оценки
                prompt = evaluate_answer_prompt(
                    task_title=task_title,
                    task_description=task_description,
                    ideal_answer=ideal_answer,
                    max_score=max_score,
                    candidate_answer=candidate_answer)
                
                # Подготовка для генерации
                text = qwen_tokenizer.apply_chat_template(prompt, tokenize=False, add_generation_prompt=True)
                prefix = '{\n  "score":'
                full_prompt = text + prefix
                inputs = qwen_tokenizer([full_prompt], return_tensors="pt").to(qwen_model.device)
                
                with torch.no_grad():
                    outputs = qwen_model.generate(
                        **inputs,
                        max_new_tokens=1024,
                        do_sample=False,
                        repetition_penalty=1.1,
                        pad_token_id=qwen_tokenizer.eos_token_id,
                        stop_strings=["}", "###", "<|im_end|>"], 
                        tokenizer=qwen_tokenizer 
                    )
                # Отрезаем длину входного промпта от результата
                input_length = inputs.input_ids.shape[1]
                response_ids = outputs[0][input_length:]
                # Декодируем сгенерированные токены обратно в текст.
                generated_text = prefix + qwen_tokenizer.decode(response_ids, skip_special_tokens=True).strip()
                evaluation_result = parse_evaluation_from_response(generated_text)
            else:
                evaluation_result = build_mock_evaluation(candidate_answer, max_score)

            if not evaluation_result:
                # Критично не терять запись по задаче: если LLM вернула невалидный JSON,
                # сохраняем fallback-оценку, чтобы в evaluations была запись для каждого ответа.
                evaluation_result = build_rule_based_evaluation(
                    task_id=task_id,
                    task_title=task_title,
                    candidate_answer=candidate_answer,
                    max_score=max_score
                )
                if model_available:
                    evaluation_result["feedback"] = (
                        "LLM вернула невалидный JSON. "
                        + evaluation_result.get("feedback", "")
                    )
                print(f"Не удалось извлечь валидную оценку для ответа {answer_id}; применен fallback.")

            score = evaluation_result.get('score')
            score = _to_int_or_none(score)
            if score is None:
                score = 0
            score = max(0, min(max_score, score))

            # Сохраняем оценку в БД
            save_evaluation(
                answer_id=answer_id,
                score=score,
                feedback=evaluation_result.get('feedback'),
                strengths=evaluation_result.get('strengths'),
                weaknesses=evaluation_result.get('weaknesses'),
                recommendation=evaluation_result.get('recommendation')
            )
            print(f"Оценка для ответа {answer_id} сохранена.")
                
        except Exception as e:
            print(f"Ошибка при оценке ответа {answer_id}: {e}")
    
    # Получение оценки всех ответов -> генерация общего фидбека для сессии
    try:
        final_score = generate_session_feedback(session_id)
        print(f"Финальная оценка для сессии {session_id}: {final_score}")
    except Exception as e:
        print(f"Ошибка при генерации фидбека для сессии {session_id}: {e}")
        
def parse_evaluation_from_response(response: str) -> Dict[str, Any]:
    """
    Парсит JSON из ответа модели и возвращает словарь с оценкой.
    Ожидаемый формат ответа модели - JSON строка с полями: score, feedback, strengths, weaknesses, recommendation.
    """

    candidates = _extract_json_candidates(response)
    if not candidates:
        print("JSON не найден в ответе модели.")
        return None

    for candidate in candidates:
        normalized = _normalize_json_text(candidate)
        try:
            evaluation_data = json.loads(normalized)
        except json.JSONDecodeError:
            continue

        if not isinstance(evaluation_data, dict):
            continue

        if not REQUIRED_EVALUATION_KEYS.issubset(evaluation_data.keys()):
            continue

        score = _to_int_or_none(evaluation_data.get("score"))
        if score is None:
            continue

        evaluation_data["score"] = score
        evaluation_data["feedback"] = str(evaluation_data.get("feedback", "")).strip()
        evaluation_data["strengths"] = str(evaluation_data.get("strengths", "")).strip()
        evaluation_data["weaknesses"] = str(evaluation_data.get("weaknesses", "")).strip()
        evaluation_data["recommendation"] = str(evaluation_data.get("recommendation", "")).strip()
        return evaluation_data

    print("Не удалось распарсить валидный JSON с обязательными полями.")
    return None


def _to_int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_json_text(text: str) -> str:
    cleaned = text.strip()
    cleaned = cleaned.replace("“", '"').replace("”", '"')
    cleaned = cleaned.replace("’", "'").replace("‘", "'")
    # Убираем запятые перед } или ], которые часто ломают JSON.
    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)
    return cleaned


def _extract_json_candidates(response: str) -> list[str]:
    response = (response or "").strip()
    if not response:
        return []

    candidates: list[str] = []

    for match in re.finditer(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", response, flags=re.IGNORECASE):
        candidates.append(match.group(1))

    candidates.extend(_extract_balanced_braces(response))

    unique: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(key)
    return unique


def _extract_balanced_braces(text: str) -> list[str]:
    results: list[str] = []
    start_indices = [idx for idx, ch in enumerate(text) if ch == "{"][:20]

    for start in start_indices:
        depth = 0
        in_string = False
        escaped = False
        for idx in range(start, len(text)):
            ch = text[idx]

            if in_string:
                if escaped:
                    escaped = False
                    continue
                if ch == "\\":
                    escaped = True
                    continue
                if ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    results.append(text[start:idx + 1])
                    break

    return results
