# промпт для оценки ответа кандидата
def evaluate_answer_prompt(
    task_title: str,
    task_description: str,
    ideal_answer: str,
    max_score: int,
    candidate_answer: str) -> list:
    """
    Формирует структурированный промпт для оценки через Chat Template.
    """
    
    system_content = (
        "Ты — Senior Data Engineer, проводящий технический аудит. "
        "Твоя задача: оценить ответ и вернуть СТРОГО объект JSON. "
        "ЗАПРЕЩЕНО: писать код на Python, создавать функции, писать вводные слова или пояснения вне JSON."
    )
    
    user_content = f"""
Оцени ответ кандидата.
ЗАДАНИЕ: {task_title}
ОПИСАНИЕ: {task_description}
ЭТАЛОН: {ideal_answer}
МАКС. БАЛЛ: {max_score}

ОТВЕТ КАНДИДАТА:
{candidate_answer}

ИНСТРУКЦИЯ:
1. Выставь балл (score) от 0 до {max_score}.
2. Напиши фидбек, сильные и слабые стороны.
3. Верни результат СТРОГО в формате JSON без markdown-разметки.

JSON ФОРМАТ:
{{
  "score": (int),
  "feedback": (str),
  "strengths": (str),
  "weaknesses": (str),
  "recommendation": (str)
}}
"""
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]


# промпт для генерации фидбека для кандидата
def generate_candidate_final_feedback_prompt(
    candidate_name: str,
    total_score: int,
    max_total_score: int,
    evaluations_json: str) -> list:
    """
    Генерация итогового фидбека для кандидата (Friendly/Professional).
    """
    system_content = (
        "Ты — Senior Data Engineer, проводишь ревью тестового задания. "
        "Твоя задача: написать вежливый, но честный фидбек кандидату на русском языке. "
        "Пиши обычным текстом без Markdown-разметки. Пиши содержательно и конкретно."
    )
    
    # Определяем уровень на основе балла
    if total_score >= 7:
        level_hint = "хороший уровень, близкий к Middle"
    elif total_score >= 5:
        level_hint = "средний уровень, Strong Junior"
    elif total_score >= 3:
        level_hint = "начальный уровень, Junior"
    else:
        level_hint = "уровень ниже требуемого, нужна дополнительная подготовка"
    
    user_content = f"""
Напиши персональный фидбек кандидату {candidate_name}.

ИТОГОВЫЙ БАЛЛ: {total_score} из {max_total_score}
ОЦЕНКА УРОВНЯ: {level_hint}

ДЕТАЛЬНЫЕ ОЦЕНКИ ПО ЗАДАНИЯМ:
{evaluations_json}

Напиши фидбек в таком формате:

{candidate_name}, здравствуйте!

Итоговый балл: {total_score} из {max_total_score}.

ОБЩАЯ ОЦЕНКА УРОВНЯ:
[Напиши 2-3 предложения об общем уровне кандидата, опираясь на итоговый балл]

СИЛЬНЫЕ СТОРОНЫ:
[Перечисли 2-3 сильные стороны на основе оценок по заданиям]

ЗОНЫ РОСТА И РЕКОМЕНДАЦИИ:
[Перечисли 2-3 зоны роста и конкретные рекомендации по улучшению]

Всего напиши около 150-200 слов. Пиши конкретно, без общих фраз.
"""
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]


# промпт для генерации фидбека для учебного центра
def generate_school_feedback_prompt(
    candidate_name: str,
    email: str,
    total_score: int,
    max_total_score: int,
    evaluations_json: str) -> list:
    """
    Генерация технического отчёта для школы (Internal Report).
    """
    system_content = (
        "Ты — технический лид. Твоя задача: составить краткий технический отчёт по кандидату. "
        "Пиши сухо, по делу, без лишних слов. Используй обычный текст без Markdown."
    )
    
    # Определяем уровень
    if total_score >= 7:
        level = "Middle"
    elif total_score >= 5:
        level = "Strong Junior"
    elif total_score >= 3:
        level = "Junior"
    else:
        level = "ниже Junior"
    
    user_content = f"""
Составь технический отчёт по кандидату.

ДАННЫЕ КАНДИДАТА:
ФИО: {candidate_name}
Email: {email}
Балл: {total_score} из {max_total_score}
Предварительный уровень: {level}

ДЕТАЛЬНЫЕ ОЦЕНКИ:
{evaluations_json}

Напиши отчёт в таком формате:

ОТЧЁТ ПО КАНДИДАТУ: {candidate_name}

1. ОБЩИЙ УРОВЕНЬ: {level}
[Краткое обоснование уровня]

2. АРХИТЕКТУРНОЕ МЫШЛЕНИЕ:
[Оценка на основе заданий 1-2]

3. РАБОТА С SQL:
[Оценка на основе задания 3]

4. ТИПОВЫЕ ОШИБКИ:
[Основные ошибки кандидата]

5. РЕКОМЕНДАЦИЯ:
[Одно из: рекомендовать / условно рекомендовать / не рекомендовать]

Объём: 100-150 слов. Только факты.
"""
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]
