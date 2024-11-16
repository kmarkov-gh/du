import os
import sqlite3
import time
import argparse

# Функция для обработки командной строки
def parse_arguments():
    parser = argparse.ArgumentParser(description="Track directory sizes over time.")
    parser.add_argument(
        'directory', nargs='?', default=os.getcwd(),
        help="Directory to track (default is current working directory)"
    )
    return parser.parse_args()

# Чтение конфигурационного файла в формате key=value
def read_config(config_file='config.txt'):
    config = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file {config_file} not found.")
        exit(1)

# Определяем файловую систему корневой директории
def get_root_dev(path):
    return os.stat(path).st_dev

def initialize_database(db_path):
    """Создает таблицу в базе данных для хранения информации о размерах (выполняется один раз)."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS directory_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT,
                size INTEGER,
                timestamp INTEGER
            );
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_path_timestamp ON directory_snapshot (path, timestamp);')
        conn.commit()

def get_size(path):
    """Возвращает размер директории. Работает только в пределах одной файловой системы."""
    total_size = 0
    for dirpath, _, _ in os.walk(path):
        if os.stat(dirpath).st_dev != root_dev:
            continue
        for filename in os.listdir(dirpath):
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    return total_size

def get_last_recorded_size(cursor, dirpath):
    """
    Возвращает последний сохранённый размер для директории.
    Если директория отсутствует в базе, возвращает None.
    """
    cursor.execute('''
        SELECT size FROM directory_snapshot
        WHERE path = ?
        ORDER BY timestamp DESC
        LIMIT 1;
    ''', (dirpath,))
    row = cursor.fetchone()
    return row[0] if row else None

def record_sizes(target_directory, db_path):
    """
    Записывает размеры только тех директорий, которые отсутствуют или изменились в базе данных.
    """
    updated_directories = []  # Список директорий с изменениями
    timestamp = int(time.time())
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        for dirpath, dirnames, _ in os.walk(target_directory):
            if os.stat(dirpath).st_dev != root_dev:
                continue
            dir_size = get_size(dirpath)
            last_size = get_last_recorded_size(cursor, dirpath)
            # Проверяем, нужно ли обновить базу данных
            if last_size is None or last_size != dir_size:
                cursor.execute('INSERT INTO directory_snapshot (path, size, timestamp) VALUES (?, ?, ?)',
                               (dirpath, dir_size, timestamp))
                updated_directories.append((dirpath, dir_size))  # Добавляем в список изменений
        conn.commit()

    # Выводим список директорий, у которых были изменения
    for dirpath, dir_size in updated_directories:
        print(f"Updated: {dirpath}, Size: {dir_size}")

if __name__ == "__main__":
    args = parse_arguments()
    TARGET_DIRECTORY = args.directory

    config = read_config()
    DB_PATH = config.get('DB_PATH')

    if not DB_PATH:
        print("Error: DB_PATH is not defined in the configuration file.")
        exit(1)

    root_dev = get_root_dev(TARGET_DIRECTORY)

    initialize_database(DB_PATH)
    record_sizes(TARGET_DIRECTORY, DB_PATH)
