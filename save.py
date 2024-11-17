import os
import sqlite3
import time
import argparse
import psutil
import sys
from collections import deque

# Функция для обработки командной строки
def parse_arguments():
    parser = argparse.ArgumentParser(description="Track directory sizes over time.")
    parser.add_argument(
        'directories', nargs='+', default=[os.getcwd()],
        help="Directories to track (default is current working directory)"
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

# Определение, выполняется ли программа в cron
def is_running_in_cron():
    return os.getenv('X_CRON') is not None or not sys.stdout.isatty()

# Определяем файловую систему корневой директории
def get_root_dev(path):
    return os.stat(path).st_dev

def initialize_database(db_path):
    """
    Создает таблицу в базе данных для хранения информации о размерах (выполняется один раз).
    """
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

def get_size(path, root_dev, excluded_mounts):
    """
    Возвращает размер директории. Работает только в пределах одной файловой системы и игнорирует примонтированные поддиректории.
    """
    total_size = 0
    stack = deque([path])

    while stack:
        current_path = stack.pop()
        try:
            with os.scandir(current_path) as it:
                for entry in it:
                    entry_path = entry.path
                    entry_dev = entry.stat(follow_symlinks=False).st_dev
                    # Игнорируем примонтированные файловые системы
                    if entry_dev != root_dev or any(entry_path.startswith(mount) for mount in excluded_mounts):
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry_path)
                    elif entry.is_file(follow_symlinks=False):
                        total_size += entry.stat(follow_symlinks=False).st_size
        except (PermissionError, FileNotFoundError):
            # Игнорируем недоступные директории и файлы
            continue

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

def log_query(query, params):
    with open("query_log.txt", "a") as log_file:
        log_file.write(f"Query: {query}\nParameters: {params}\n\n")

def record_sizes(target_directories, db_path):
    """
    Записывает размеры только тех директорий, которые отсутствуют или изменились в базе данных.
    """
    updated_directories = []  # Список директорий с изменениями
    timestamp = int(time.time())
    partitions = psutil.disk_partitions(all=True)
    excluded_mounts = {partition.mountpoint for partition in partitions if "loop" in partition.opts or partition.fstype in {"proc", "sysfs", "tmpfs", "devpts", "cgroup", "squashfs", "devtmpfs", "overlay", "fusectl", "fuse.sshfs"}}

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        for target_directory in target_directories:
            # Пропускаем целевые директории, которые являются псевдо-файловыми системами
            if target_directory in excluded_mounts:
                print(f"Skipping pseudo-filesystem directory: {target_directory}")
                continue

            root_dev = get_root_dev(target_directory)
            dir_size = get_size(target_directory, root_dev, excluded_mounts)
            last_size = get_last_recorded_size(cursor, target_directory)
            # Проверяем, нужно ли обновить базу данных для основного каталога
            if last_size is None or last_size != dir_size:
                query = 'INSERT INTO directory_snapshot (path, size, timestamp) VALUES (?, ?, ?)'
                params = (target_directory, dir_size, timestamp)
                log_query(query, params)
                cursor.execute(query, params)
                updated_directories.append((target_directory, dir_size))  # Добавляем в список изменений
            # Обход всех поддиректорий
            for dirpath, dirnames, _ in os.walk(target_directory, topdown=True):
                # Пропускаем примонтированные поддиректории
                dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) not in excluded_mounts and not os.path.ismount(os.path.join(dirpath, d))]
                dir_size = get_size(dirpath, root_dev, excluded_mounts)
                last_size = get_last_recorded_size(cursor, dirpath)
                # Проверяем, нужно ли обновить базу данных
                if last_size is None or last_size != dir_size:
                    query = 'INSERT INTO directory_snapshot (path, size, timestamp) VALUES (?, ?, ?)'
                    params = (dirpath, dir_size, timestamp)
                    log_query(query, params)
                    cursor.execute(query, params)
                    updated_directories.append((dirpath, dir_size))  # Добавляем в список изменений
        conn.commit()
    

    # Выводим список директорий, у которых были изменения
    if not is_running_in_cron():
        for dirpath, dir_size in updated_directories:
            print(f"Updated: {dirpath}, Size: {dir_size}")

if __name__ == "__main__":
    args = parse_arguments()
    TARGET_DIRECTORIES = args.directories

    config = read_config()
    DB_PATH = config.get('DB_PATH')

    if not DB_PATH:
        print("Error: DB_PATH is not defined in the configuration file.")
        exit(1)

    initialize_database(DB_PATH)
    record_sizes(TARGET_DIRECTORIES, DB_PATH)
