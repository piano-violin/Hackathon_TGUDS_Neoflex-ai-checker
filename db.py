

# Импортируем необходимые библиотеки
import os # Для работы с переменными окружения
import psycopg2 # Для подключения к PostgreSQL
import pandas as pd
# Импортируем RealDictCursor для получения результатов в виде словаря
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv # Для загрузки переменных окружения из .env файла


# Список с задачами (всего 4 задачи) + эталонные 
# title - название здачи
# description - описание задачи 
# ideal_answer - эталонный ответ
# максимальный балл - поставил 25 чтобы удобно нормировать 
tasks_data = [
    (
        'Задание 1: Анализ таблицы клиентов',
        """Представьте, что вы устроились работать Дата-инженером в некоторый интернет магазин.
            До вас в этой компании уже работал один разработчик, который придумал небольшую
            базу данных для этого магазина и потом неожиданно уволился. Ваша задача провести
            проверку существующей архитектуры и решить корректна ли она илиможно внести
            некоторые доработки.
            Вы обратили внимание на таблицу с информацией о зарегистрировавшихся
            покупателях (клиентах).
            Таблица имеет следующую схему:
            clients (
            client_id number, --уникальный id клиента
            client_name varchar(255), --имя клиента
            client_surname varchar(255), --фамилия клиента
            login varchar(30), --логин, который придумал клиент
            city_id number, --id города, который указал клиент (в интерфейсе
            выбирается название города, а в таблицу сохраняется id)
            age number, --возраст клиента
            reg_date date –дата регистрации на сайте
            );
            Считаете ли вы данный набор и смысл полей корректным? Если нет, то напишите что повашему
            мнению некорректно и какие изменения внесли бы.""",
        """По корректности полей - возраст лучше хранить не числом, а вычислять через дату рождения. Также появляется возможность проведения акций в день рождения
            Отсутвуют атрибуты с конктактной информацией - email, номер телефона, мессенджеры
            Не указаны первичный и внешний ключ. В качестве первичного следует использовать client_id, в качетве внешнего - city_id 
            Возможно следует создать зависимую таблицу с метаданными по клиенту - время сессии, дата последнего подключения, статус аккаунта, язык интерфейса, флаги основных настроек""",
        25
    ),
    (
        'Задание 2: Архитектура таблицы товаров',
        """Представьте, что вы устроились работать Дата-инженером в некоторый интернет магазин.
            До вас в этой компании уже работал один разработчик, который придумал небольшую
            базу данных для этого магазина и потом неожиданно уволился. Ваша задача провести
            проверку существующей архитектуры и решить корректна ли она илиможно внести
            некоторые доработки.
            Вы обратили внимание на таблицу с информацией о товарах магазина.
            Таблица имеет следующую схему:
            items (

            );
            item_id number, --уникальный id товара
            item_name varchar(255), --наименование товара
            item_cost number –стоимость товара

            Глядя на эту таблицу вы вспоминаете, что мы живём в рыночной экономике и цена, если не каждый
            день, то каждый месяц может меняться. К примеру, сезонные повышения цен или наоборот скидки
            (перед началом учебного года или новым годом). Так же вы знаете что у магазина есть отдел
            финансовой отчётности, в котором сотрудникам нужно строить различныотчёты (о доходах
            например) за разные промежутки времени, в том числе и за прошлые месяцы и даже прошлые года.
            Как бы вы доработали архитектуру таблицы, что бы обеспечить историческое хранениестоимости
            товара?""",
        """Для построения отчетности данную таблицу необходимо переработать и дополнить боковыми таблицами 
            Будем исходить из звездно-снежинковой схемы. 
            Саму таблицу items необходимо переименовать в items_prices  и привести к scd2 добавив даты на которые цена была актуальна, 
            при этом убрав из таблицы атрибут item_name в отдельную таблицу item_desc в которой будут храниться бизнес-атрибуты по товару (наименование, вес, срок годности)
            По самим ценам необходимо уточнение является ли эта цена входной или же продажной. 
            В случае если это входная цена, то лучше отказаться от единой таблицы с ценами и декомпозировать ее в разрезе поставщик-заказ-товар и вычислять как средневзвешенную от обьема и стоимости заказа

            В таблицу с ценами возможно следует добавить id валюты - ссылку на отдельную табилицу с валютами 
            также необходима информация по поставщикам, для этого мы можем или добавить id поставщика в таблицу с item_desc, но лучше вывести в отдельную таблицу item_vendor
            в этом случае мы избежим замножения данных по товару если его поставляет сразу несколько поставщиков 
            Помимо этого необходим справочник с категориями товаров, для возможности проводить аналитику не только в разрезе товара но и в разрезе категорий

            итого: резюмируя требуется полная переработка исходной таблицы в зависимости от нужд коллег из финансовой отчетности""",
        25
    ),
    (
        'Задание 3: Исправление SQL-запроса',
        """Требуется проверить запрос на корректность и исправить там ошибки, если они есть.
            Существует учебная схема HR, содержащая таблицы: employees, departments и locations
            Необходимо получить все отделы0, расположенные в Сеуле в которых все сотрудники, не
            имеющие менеджера, зарабатывают в общей сложности более 100000.
            Для решения задачи был написан следующий запрос:
            SELECT DEPARTMENT_ID,
            DEPARTMENT_NAME,
            SUM(SALARY) TOTAL_SALARY
            FROM EMPLOYEE E,
            DEPARTMENTS D
            WHERE E.DEPARTMENT_ID = D.DEPARTMENT_ID
            AND MANAGER_ID = NULL
            AND LOCATION_ID = (SELECT LOCATION_ID
            FROM LOCATIONS
            WHERE CITY = 'SEOUL')
            AND SUM(SALARY) >= 100000
            GROUP BY DEPARTMENT_NAME""",
        """Ошибки:
            1) отсутствие алиасов у атрибутов - атрибут DEPARTMENT_ID присутствует в таблицах EMPLOYEE и DEPARTMENTS
            2) избыточное количество атрибутов в запросе - в задаче указано получить все отделы, не требуется выводить ID отдела и сумму зарплат
            3) Неявный джоин таблиц EMPLOYEE и DEPARTMENTS указанный в FROM ошибкой не является, но лучше укзать его в явном виде как inner join
            4) Таблица EMPLOYEE указана неверно - верно EMPLOYEES
            5) Запись формата "MANAGER_ID = NULL" является не корректной т.к. для сравнения с null используется конструкция is null/is not null
            6)  LOCATION_ID = - не является ошибкой но в случае если подзапрос вернет более одного значения, основной запрос упадет с ошибкой, лучше использовать in, 
            особенно с учетом того что в подзапросе отсутствует дедубликация
            7) WHERE CITY = 'SEOUL' - в связи с тем что у нас нет информации о том как хранится название города, следует или принудительно привести к верхнему регистру через UPPER 
            или использовать like
            8) SUM(SALARY) >= 100000 - условие с агрегацией следует указать в Having
            9) В условии задачи указано "более", а в условии фильтрации ">="
            10) GROUP BY DEPARTMENT_NAME - атрибутивный состав в группировке не соответствует атрибутивному составу в запросе""",
        25
    ),
    (
        'Задание 4: Логическая задача',
        'В некоторой комнате на пол уронили карандаш. Объясните почему вы не можете через него перепрыгнуть?',
        'Карандаш положили вплотную к стене.',
        25
    ),
]


# Загружаем переменные окружения из .env файла
# Параметр override=True позволяет перезаписывать существующие переменные окружения значениями из .env файла
load_dotenv(".env", override=True)
# Получаем значения переменных окружения для подключения к базе данных
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Основная функция для подключения к базе данных и выполнения запроса
def main():
    try:
        if not all([USER, PASSWORD, HOST, PORT, DBNAME]):
            raise RuntimeError("Не все переменные окружения считались из .env") # Проверяем, что все необходимые переменные окружения были считаны
        
        # Подключаемся к базе данных
        conn = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=int(PORT),
            dbname=DBNAME,
            sslmode="require",   # важно для Supabase
        ) 
        print("Connected to the database")
        cursor = conn.cursor() # Создаем курсор для выполнения SQL-запросов
        # Курсор позволяет выполнять SQL-запросы и получать результаты
        
        # -- candidates --
        # таблица candidates для хранения информации о кандидатах
        # IF NOT EXISTS - создает таблицу только если она еще не существует
        # id SERIAL PRIMARY KEY - уникальный идентификатор для каждой записи, который автоматически увеличивается, serial - это тип данных для автоинкрементного целого числа
        # full_name TEXT NOT NULL - имя кандидата, не может быть пустым (из бота)
        # email TEXT UNIQUE - email кандидата, должен быть уникальным (из бота)
        # created_at TIMESTAMP DEFAULT NOW() - дата и время создания записи, по умолчанию устанавливается текущая дата и время
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id SERIAL PRIMARY KEY,
                full_name TEXT NOT NULL,
                email TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT NOW());
                """)
        conn.commit() # Сохраняем изменения в базе данных  
        
        # -- tasks --
        # таблица tasks для хранения информации о задачах
        # id SERIAL PRIMARY KEY - уникальный идентификатор для каждой задачи, который автоматически увеличивается
        # title TEXT NOT NULL UNIQUE - название задачи, не может быть пустым, должно быть уникальным, чтобы избежать дублирования задач с одинаковыми названиями
        # description TEXT NOT NULL - описание задачи, не может быть пустым
        # ideal_answer TEXT NOT NULL - идеальный ответ на задачу, не может быть пустым
        # max_score INTEGER NOT NULL - максимальный балл за выполнение задачи, не может быть пустым
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL,
                ideal_answer TEXT NOT NULL,
                max_score INTEGER NOT NULL);
                """)
        conn.commit() # Сохраняем изменения в базе данных  
        
        # -- INSERT tasks --
        # Вставляем данные о задачах из списка tasks_data в таблицу tasks
        # insert_query - SQL-запрос для вставки данных в таблицу tasks, с использованием параметров для безопасной передачи данных
        # ON CONFLICT (title) DO NOTHING - при попытке вставить запись с уже существующим названием задачи, запрос не будет вставлять дубликат и не вызовет ошибку
        insert_query = """
            INSERT INTO tasks (title, description, ideal_answer, max_score)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (title) DO NOTHING
        """
        # executemany - выполняет один и тот же SQL-запрос для каждого элемента в списке tasks_data, передавая значения из каждого кортежа в качестве параметров запроса
        cursor.executemany(insert_query, tasks_data)
        # Сохраняем изменения в базе данных после вставки данных о задачах
        conn.commit()
        print("Tasks inserted")
           
        
        # -- sessions --   
        # таблица sessions для хранения информации о сессиях кандидатов
        # id SERIAL PRIMARY KEY - уникальный идентификатор для каждой сессии, который автоматически увеличивается
        # candidate_id INTEGER REFERENCES candidates(id) ON DELETE CASCADE - внешний ключ, ссылающийся на таблицу candidates, при удалении кандидата все его сессии будут удалены
        # total_score NUMERIC(5,2) DEFAULT 0 - общий балл за сессию (может быть дробным), по умолчанию 0
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                candidate_id INTEGER REFERENCES candidates(id) ON DELETE CASCADE,
                total_score NUMERIC(5,2) DEFAULT 0); 
            """)
        # Миграция для уже существующей таблицы (если ранее total_score был INTEGER).
        cursor.execute(
            """
            ALTER TABLE sessions
            ALTER COLUMN total_score TYPE NUMERIC(5,2)
            USING total_score::NUMERIC(5,2)
            """
        )
        cursor.execute(
            """
            ALTER TABLE sessions
            ALTER COLUMN total_score SET DEFAULT 0
            """
        )
        # лучше не сохранять total_score в sessions, а вычислять его на лету при запросе, чтобы избежать проблем с синхронизацией данных при обновлении оценок ответов
        # SUM(evaluations.score) FROM answers JOIN evaluations ON answers.id = evaluations.answer_id WHERE answers.session_id = sessions.id
        # важно нормировать чтобы максимум за сесссию было 10 баллов
        # поэтому SUM(evaluations.score) / SUM(tasks.max_score) * 10 AS total_score
        # теперь при запросе сессий можно будет получать актуальный total_score без необходимости обновлять его при каждой оценке ответа
        # оценка ограничивается 10 баллами, а не суммой всех баллов за задачи
        conn.commit() # Сохраняем изменения в базе данных   
        
        # -- answers --
        # таблица answers для хранения ответов кандидатов на задачи
        # id SERIAL PRIMARY KEY - уникальный идентификатор для каждого ответа, который автоматически увеличивается
        # session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE - внешний ключ, ссылающийся на таблицу sessions, при удалении сессии все ее ответы будут удалены
        # task_id INTEGER REFERENCES tasks(id) ON DELETE CASCADE - внешний ключ, ссылающийся на таблицу tasks, при удалении задачи все связанные с ней ответы будут удалены
        # raw_answer TEXT - текстовый ответ кандидата, может быть пустым
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS answers (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                task_id INTEGER REFERENCES tasks(id) ON DELETE CASCADE,
                raw_answer TEXT);
                """)
        conn.commit() # Сохраняем изменения в базе данных   
        
        # -- evaluations --
        # таблица evaluations для хранения оценок ответов кандидатов
        # id SERIAL PRIMARY KEY - уникальный идентификатор для каждой оценки, который автоматически увеличивается
        # answer_id INTEGER REFERENCES answers(id) ON DELETE CASCADE - внешний ключ, ссылающийся на таблицу answers, при удалении ответа все связанные с ним оценки будут удалены
        # score INTEGER - балл за ответ, может быть пустым
        # feedback TEXT - текстовый отзыв по ответу, может быть пустым
        # strengths TEXT - сильные стороны ответа, может быть пустым
        # weaknesses TEXT - слабые стороны ответа, может быть пустым
        # recommendation TEXT - рекомендация по ответу, может быть пустым
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS evaluations (
                id SERIAL PRIMARY KEY,
                answer_id INTEGER REFERENCES answers(id) ON DELETE CASCADE,
                score INTEGER,
                feedback TEXT,
                strengths TEXT,
                weaknesses TEXT,
                recommendation TEXT);
                """)
        conn.commit() # Сохраняем изменения в базе данных   
        
        # -- feedback --
        # таблица feedback для хранения обратной связи по сессиям кандидатов
        # id SERIAL PRIMARY KEY - уникальный идентификатор для каждой обратной связи, который автоматически увеличивается
        # session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE - внешний ключ, ссылающийся на таблицу sessions, при удалении сессии все связанные с ней обратные связи будут удалены
        # candidate_feedback TEXT - обратная связь для кандидата 
        # school_feedback TEXT - обратная связь для учебного центра
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                candidate_feedback TEXT,
                school_feedback TEXT);
                """)
        conn.commit() # Сохраняем изменения в базе данных

        cursor.close() # Закрываем курсор
        conn.close() # Закрываем соединение с базой данных
        print("Connection closed")

    # Обрабатываем возможные исключения и выводим ошибку
    except Exception as e:
        print("Error:", e)
        
        
# Функция для подключения к базе данных
def get_connection():
    """Возвращает соединение с БД"""
    return psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=int(PORT),
        dbname=DBNAME,
        sslmode="require",
    )
    

# Функция для регистрации кандидата и создания сессии
def register_candidate(full_name: str, email: str) -> int:
    """
    Регистрирует кандидата и создает новую сессию.
    Возвращает id созданной сессии.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # пытаемся вставить нового кандидата, если email уже существует, то ничего не делаем и получаем его id
        cursor.execute("""
            INSERT INTO candidates (full_name, email)
            VALUES (%s, %s)
            ON CONFLICT (email) DO NOTHING
            RETURNING id;
        """, (full_name, email))
        # если кандидат был вставлен, то получаем его id, иначе выполняем запрос на получение id существующего кандидата
        candidate_id = cursor.fetchone()
        if candidate_id is None:
            # кандидат уже есть, получаем его id
            # SELECT id FROM candidates WHERE email = %s - выполняем запрос на получение id кандидата по email
            # (email,) - передаем email как параметр запроса, важно передавать его в виде кортежа, даже если он один, чтобы избежать ошибок при выполнении запроса
            cursor.execute("SELECT id FROM candidates WHERE email = %s", (email,))
            candidate_id = cursor.fetchone()[0]
        else:
            candidate_id = candidate_id[0]

        # создаем новую сессию
        cursor.execute("""
            INSERT INTO sessions (candidate_id)
            VALUES (%s)
            RETURNING id;
        """, (candidate_id,))
        session_id = cursor.fetchone()[0]

        conn.commit()
        # возвращаем id созданной сессии
        return session_id

    # finally блок гарантирует, что курсор и соединение будут закрыты, даже если возникнет ошибка в процессе выполнения запросов
    finally:
        cursor.close()
        conn.close()
        

# Функция сохранения ответов кандидата 
def save_answers(session_id: int, answers: list):
    """
    answers: список словарей {'task_id': int, 'raw_answer': str}
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        insert_query = """
            INSERT INTO answers (session_id, task_id, raw_answer)
            VALUES (%s, %s, %s)
        """
        # формируем данные для вставки в виде списка кортежей (session_id, task_id, raw_answer) для каждого ответа из списка answers
        data = [(session_id, a['task_id'], a['raw_answer']) for a in answers]
        # выполняем вставку данных в таблицу answers с помощью executemany, который позволяет вставить несколько записей за один запрос
        cursor.executemany(insert_query, data)
        conn.commit()
    # finally гарантирует, что курсор и соединение будут закрыты, даже если возникнет ошибка в процессе выполнения запросов
    finally:
        cursor.close()
        conn.close()
        

# функция для получения задач из базы данных для оценки ответов кандидата
def get_tasks_for_session(session_id: int):
    """
    Возвращает список задач с эталонными ответами и ответами кандидата для сессии
    """
    conn = get_connection()
    # RealDictCursor - получать результаты в виде словаря, удобнее для работы с данными
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    # выполняем запрос, который объединяет таблицы tasks и answers, чтобы получить информацию о задачах и ответах кандидата для данной сессии
    try:
        cursor.execute("""
            SELECT t.id AS task_id, t.title, t.description, t.ideal_answer, t.max_score,
                   a.id AS answer_id, a.raw_answer
            FROM tasks t
            JOIN answers a ON a.task_id = t.id
            WHERE a.session_id = %s
        """, (session_id,))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
        

# функция для сохранения оценки по задачам
def save_evaluation(answer_id: int, score: int = None,
                    feedback: str = None, strengths: str = None,
                    weaknesses: str = None, recommendation: str = None):
    """ Сохраняет оценку по задаче
    в таблице evaluations, связывая ее с конкретным ответом кандидата через answer_id"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Если оценка по answer_id уже существует, обновляем ее.
        # Иначе создаем новую запись.
        cursor.execute(
            """
            UPDATE evaluations
            SET
                score = %s,
                feedback = %s,
                strengths = %s,
                weaknesses = %s,
                recommendation = %s
            WHERE answer_id = %s
            """,
            (score, feedback, strengths, weaknesses, recommendation, answer_id),
        )

        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO evaluations (answer_id, score, feedback, strengths, weaknesses, recommendation)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (answer_id, score, feedback, strengths, weaknesses, recommendation),
            )
        conn.commit()
    finally:
        cursor.close()
        conn.close()
        

# функциия для форматирования и нормирования оценки кандидата по сессии + фидбек для кандидата и учебного центра
def generate_session_feedback(session_id: int):
    """
    Подсчет суммарного балла и формирование итогового фидбека
    Обновляет sessions.total_score
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Получаем суммарный балл
        # логика:
        # 1) объединяем таблицы evaluations, answers и tasks, чтобы получить информацию о баллах за ответы кандидата для данной сессии, а также максимальных баллах за задачи для нормирования итоговой оценки
        # 2) суммируем баллы за ответы кандидата и максимальные баллы за задачи
        # 3) нормируем итоговую оценку по формуле (сумма баллов за ответы / сумма максимальных баллов за задачи) * 10, чтобы итоговая оценка была в диапазоне от 0 до 10
        # 4) округляем итоговую оценку до 2 знаков после запятой для удобства отображения
        # 5) обновляем таблицу sessions, устанавливая рассчитанную итоговую оценку для данной сессии
        cursor.execute("""
            SELECT SUM(score) AS total, SUM(t.max_score) AS max_total
            FROM evaluations e
            JOIN answers a ON a.id = e.answer_id
            JOIN tasks t ON t.id = a.task_id
            WHERE a.session_id = %s
        """, (session_id,))
        # получаем результат запроса, который содержит суммарный балл за ответы кандидата (total) и суммарный максимальный балл за задачи (max_total)
        row = cursor.fetchone()
        total_score = row[0] or 0 # если total_score равен None, то устанавливаем его в 0
        max_score = row[1] or 1 # если max_score равен None, то устанавливаем его в 1, чтобы избежать деления на ноль при нормировании оценки
        # нормируем итоговую оценку по формуле (сумма баллов за ответы / сумма максимальных баллов за задачи) * 10, чтобы итоговая оценка была в диапазоне от 0 до 10
        normalized_score = round(total_score / max_score * 10, 2)

        # Обновляем sessions
        cursor.execute("""
            UPDATE sessions
            SET total_score = %s
            WHERE id = %s
        """, (normalized_score, session_id))

        conn.commit()
        # возвращаем нормированную итоговую оценку для данной сессии, которая может быть использована для отображения результатов кандидата
        return normalized_score
    finally:
        cursor.close()
        conn.close()
        

# функция получения фидбека для пользователя (для бота)
def get_candidate_feedback(session_id: int):
    """
    Возвращает данные для кандидата:
    - ФИО
    - итоговый балл
    - текст фидбека кандидату
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT 
                c.full_name,
                s.total_score,
                f.candidate_feedback
            FROM sessions s
            JOIN candidates c ON c.id = s.candidate_id
            JOIN feedback f ON f.session_id = s.id
            WHERE s.id = %s
        """, (session_id,))
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()
        
        
# функция получения фидбека для учебного центра 
def get_school_feedback(session_id: int):
    """
    Возвращает расширенный фидбек для учебного центра:
    - ФИО
    - email
    - итоговый балл
    - текст фидбека для школы
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT 
                c.full_name,
                c.email,
                s.total_score,
                f.school_feedback
            FROM sessions s
            JOIN candidates c ON c.id = s.candidate_id
            JOIN feedback f ON f.session_id = s.id
            WHERE s.id = %s
        """, (session_id,))
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()
        
        
# Функция для выгрузки всех результатов школой
def get_all_school_results_df():
    """
    Возвращает DataFrame для выгрузки школе
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT 
                c.full_name      AS "ФИО",
                c.email          AS "Email",
                s.id             AS "Session ID",
                s.total_score    AS "Итоговый балл",
                f.school_feedback AS "Фидбек для школы"
            FROM sessions s
            JOIN candidates c ON c.id = s.candidate_id
            LEFT JOIN feedback f ON f.session_id = s.id
            ORDER BY s.id DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()
        conn.close()
        
# Функция для выгрузки всех результатов школой в excel   
def export_school_results_to_excel(filepath="school_results.xlsx"):
    """
    Пытается экспортировать в Excel.
    Если openpyxl не установлен, делает fallback в CSV и возвращает путь к созданному файлу.
    """
    df = get_all_school_results_df()
    try:
        df.to_excel(filepath, index=False)
        return filepath
    except ModuleNotFoundError as exc:
        if exc.name != "openpyxl":
            raise
        csv_path = os.path.splitext(filepath)[0] + ".csv"
        df.to_csv(csv_path, index=False)
        print("openpyxl не установлен. Создан CSV вместо Excel.")
        return csv_path
