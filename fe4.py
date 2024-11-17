import os
import curses
import argparse
import sqlite3
import datetime
import logging
from itertools import cycle

# Настраиваем логирование для записи запросов в базу данных и прочих событий
# Логирование настраивается в зависимости от аргументов командной строки
size_cache = {}

# Функция для загрузки конфигурации из файла
# Она загружает параметры, такие как путь к базе данных
# Ожидается, что конфигурационный файл находится в текущей директории и называется 'config.txt'
def load_config(config_file='config.txt'):
    config = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue  # Игнорируем пустые строки и комментарии
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_file}' not found.")
    except Exception as e:
        print(f"Error reading configuration file: {e}")
    return config

# Загружаем конфигурацию и получаем путь к базе данных
config = load_config()
DB_PATH = config.get("DB_PATH")

# Если путь к базе данных не задан, то программа не сможет работать дальше
if not DB_PATH:
    raise ValueError("DB_PATH not set in configuration file.")

# Функция для обработки аргументов командной строки
# Она позволяет указать директорию, которую нужно отобразить
# По умолчанию будет использоваться текущая рабочая директория
# Также позволяет включить или выключить логирование отладочной информации
def parse_arguments():
    parser = argparse.ArgumentParser(description="Display directory list with ncurses.")
    parser.add_argument(
        'directory', nargs='?', default=os.getcwd(),
        help="Directory to display (default is current working directory)"
    )
    parser.add_argument(
        '--debug', action='store_true',
        help="Enable debug logging"
    )
    return parser.parse_args()

# Обработка аргументов командной строки
args = parse_arguments()

# Настройка логирования в зависимости от аргументов командной строки
if args.debug:
    logging.basicConfig(filename='sql_queries.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.debug("Debug logging is enabled.")
else:
    logging.basicConfig(level=logging.CRITICAL)  # Отключение логирования, если флаг --debug не задан

# Функция для получения данных по размеру директории из базы данных
# Сначала проверяем кеш, если данные в нем есть, используем их
# Если данных нет, выполняем запрос в базу данных
def get_directory_size_data(directory):
    # Приводим путь к нормализованному абсолютному пути для улучшения кеширования
    directory = os.path.abspath(directory)
    if directory in size_cache:
        logging.info(f"Cache hit for directory data: {directory}")
        return size_cache[directory]

    # Подключение к базе данных
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # SQL-запрос для получения размеров директорий
    query = """
        SELECT timestamp, size FROM directory_snapshot
        WHERE path = ?
        ORDER BY timestamp
    """
    logging.info(f"{query.strip()}-- parameters: '{directory}'")
    cursor.execute(query, (directory,))

    data = cursor.fetchall()
    conn.close()

    # Сохраняем данные в кеш для дальнейшего использования
    if data:
        size_cache[directory] = list(data)
    else:
        size_cache[directory] = []
    return data

# Функция для форматирования размера в человекочитаемый вид
# Например, 1024 будет отображено как 1.0K, 1048576 как 1.0M и так далее
def format_size(size):
    """
    Форматирует размер в человекочитаемый формат (байты, КБ, МБ, ГБ).
    """
    for unit in ['B', 'K', 'M', 'G', 'T']:
        if size < 1024:
            if unit == 'B':
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}P"  # На случай, если размер слишком велик

# Функция для получения последнего размера директории из базы данных
# Используется кеш для ускорения работы, если данные уже были запрошены ранее
def get_last_snapshot_size(directory):
    # Приводим путь к нормализованному абсолютному пути для улучшения кеширования
    directory = os.path.abspath(directory)
    cache_key = f"snapshot_{directory}"
    if cache_key in size_cache and size_cache[cache_key]:
        logging.info(f"Cache hit for directory snapshot with key: {cache_key}")
        return size_cache[cache_key]
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # SQL-запрос для получения последних размеров всех поддиректорий
    query = """
        SELECT path, MAX(timestamp) AS latest_time, size
        FROM directory_snapshot
        WHERE path LIKE ?
        GROUP BY path
    """
    logging.info(f"{query.strip()} -- parameters: '{directory}%' ")
    cursor.execute(query, (f"{directory}%",))

    data = cursor.fetchall()
    conn.close()

    # Сохраняем данные в кеш
    if data:
        size_cache[cache_key] = {row[0]: row[2] for row in data if len(row) >= 3}
    else:
        size_cache[cache_key] = {}
    return size_cache[cache_key]

# Функция для отображения диаграммы размеров директорий с использованием символов #
# Она отображает исторические данные о размере директории в виде столбиков
# Также отображает дату и время для выбранной директории
def draw_bar_chart(stdscr, size_data, start_row, max_height, max_width, bar_offset, selected_bar, target_directory, current_unit):
    if not size_data:
        return

    max_size = max(size for _, size in size_data)

    if max_size == 0:
        return  # Нечего отображать, все размеры равны нулю

    num_bars = min(len(size_data) - bar_offset, max_width - 2)
    bar_width = max(1, max_width // num_bars)

    for i, (timestamp, size) in enumerate(size_data[bar_offset:bar_offset + num_bars]):
        bar_height = int((size / max_size) * max_height)
        bar_x = i * bar_width + 1
        color = curses.A_REVERSE if i == selected_bar else curses.A_NORMAL

        for j in range(bar_height):
            stdscr.addstr(start_row - j, bar_x, '#', color)
        
        # Отображаем дату, время и размер под выделенным столбцом
        if i == selected_bar:
            date_time_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            size_str = format_size_by_unit(size, current_unit)
            stdscr.addstr(start_row + 1, 0, f"Date/Time: {date_time_str}, Size ({current_unit}): {size_str}")

# Список доступных единиц измерения размера
# 'HR' означает "человекочитаемый" (Human Readable)
size_units = ['HR', 'TB', 'GB', 'MB', 'KB', 'B']
size_format_cyclers = {}  # хранение текущего состояния единиц измерения для каждой директории

# Функция для получения следующей единицы измерения размера
# Используется для переключения между различными единицами измерения
def get_next_unit(size, units, current_index):
    # Всегда переключаем на следующую единицу измерения, даже если размер равен 0 или меньше 0.01
    next_index = (current_index + 1)% len(units)
    logging.info(f"Next index: {next_index}")
    for i in range(next_index, len(units)):
        next_unit = units[i]
        if next_unit == 'HR' or format_size_in_unit(size, next_unit) >= 0.01:
            return i, next_unit
    return 0, 'HR'

# Функция для обновления списка единиц измерения размера
# Она исключает те единицы, которые меньше 0.01 в текущем размере
def update_size_units(size):
    sizes = ['TB', 'GB', 'MB', 'KB', 'B']
    valid_units = ['HR']
    for unit in sizes:
        if format_size_in_unit(size, unit) >= 0.01:
            valid_units.append(unit)
    return valid_units

# Функция для преобразования размера в определенную единицу измерения
def format_size_in_unit(size, unit):
    unit_multipliers = {
        'TB': 1024**4,
        'GB': 1024**3,
        'MB': 1024**2,
        'KB': 1024,
        'B': 1,
    }
    return size / unit_multipliers[unit]

# Функция для форматирования размера в заданной единице
# Если единица 'HR', используется форматирование в человекочитаемый вид
def format_size_by_unit(size, unit):
    if unit == 'HR':
        return format_size(size)
    elif unit == 'B':
        return f"{int(size)} {unit}"
    else:
        value = format_size_in_unit(size, unit)
        return f"{value:.2f} {unit}"
    

# Основная функция для отображения списка директорий с использованием ncurses
# Пользователь может взаимодействовать с интерфейсом, используя клавиши вверх/вниз, влево/вправо, 'b' для смены единицы измерения, Enter для выбора директории
# Отображается список директорий и размеры каждой директории
def display_directories(stdscr, target_directory):
    previous_directory = None
    bar_offset = 0
    selected_bar = 0

    while True:
        # Получаем список директорий в целевой директории
        directories = [d for d in os.listdir(target_directory) if os.path.isdir(os.path.join(target_directory, d))]
        directories_sizes = get_last_snapshot_size(target_directory)

        # Сортируем директории по размеру (по убыванию)
        directories = sorted(directories, key=lambda d: directories_sizes.get(os.path.join(target_directory, d), 0), reverse=True)

        # Форматируем список директорий и их размеров
        directories_with_sizes = [
            (d, directories_sizes.get(os.path.join(target_directory, d), 0)) for d in directories
        ]
        if target_directory not in size_format_cyclers:
            initial_size = sum(directories_sizes.values()) if directories_sizes else 0
            size_format_cyclers[target_directory] = {
                'units': update_size_units(initial_size),
                'index': 0
            }
        current_unit = size_format_cyclers[target_directory]['units'][size_format_cyclers[target_directory]['index']]

        # Добавляем родительскую директорию, если возможно (для возможности навигации вверх)
        parent_directory = os.path.abspath(os.path.join(target_directory, ".."))
        if parent_directory != target_directory:
            directories_with_sizes.insert(0, ("..", 0)) 

        # Очищаем экран и готовимся к отображению списка директорий
        curses.curs_set(0)
        stdscr.clear()
        height, width = stdscr.getmaxyx()

        # Определяем максимальную длину имени поддиректорий и позицию для отображения размеров
        max_dir_name_length = max(len(d) for d, _ in directories_with_sizes) + 2
        size_column_start = max_dir_name_length + 2

        half_height = height // 2
        maxlines = half_height - 2 - 1
        selected_idx = 0
        scroll_start = 0
        scroll_end = scroll_start + maxlines - 1

        # Если мы возвращаемся в предыдущую директорию, пытаемся восстановить выбранную директорию
        if previous_directory:
            try:
                selected_idx = [d for d, _ in directories_with_sizes].index(os.path.basename(previous_directory))
            except ValueError:
                selected_idx = 0
            scroll_start = max(0, selected_idx - maxlines // 2)
            scroll_end = scroll_start + maxlines - 1

        while True:
            # Отображаем заголовок и список директорий с размерами
            stdscr.addstr(0, 0, f"Directory listing for: {target_directory}")
            stdscr.addstr(1, 0, "-" * width)

            for idx, (dir_name, size) in enumerate(directories_with_sizes[scroll_start:scroll_end + 1]):
                stdscr.addstr(2 + idx, 0, " " * width)
                global_idx = scroll_start + idx
                display_name = f"{dir_name}"
                size_display = f"{format_size(size)}"
                stdscr.addstr(2 + idx, 0, f"> {display_name}" if global_idx == selected_idx else f"  {display_name}")
                stdscr.addstr(
                    2 + idx,
                    size_column_start,
                    size_display,
                    curses.A_REVERSE if global_idx == selected_idx else 0
                )

            stdscr.addstr(half_height - 1, 0, "-" * width)

            # Отображаем текущую выбранную директорию
            selected_dir = directories_with_sizes[selected_idx][0]
            logging.info(f"Current selected directory: {selected_dir}")
            stdscr.addstr(half_height, 0, f"Current selection: {selected_dir}")

            # Получаем данные о размерах для выбранной директории
            size_data = get_directory_size_data(os.path.join(target_directory, selected_dir))

            # Очищаем область диаграммы
            for row in range(half_height, height - 2):
                stdscr.addstr(row, 0, " " * width)

            # Отображаем диаграмму для выбранной директории
            draw_bar_chart(
                stdscr, size_data,
                start_row=height - 5,
                max_height=height - half_height - 6,
                max_width=width,
                bar_offset=bar_offset,
                selected_bar=selected_bar,
                target_directory=target_directory,
                current_unit=current_unit
            )

            # Инструкция для пользователя
            stdscr.addstr(height - 1, 0, "Press 'q' to quit, Enter to select")
            stdscr.refresh()

            # Обработка ввода от пользователя
            key = stdscr.getch()
            if key == curses.KEY_HOME or key == 126:  # Добавляем обработку клавиши Home (код 126)
                selected_idx = 0
                scroll_start = 0
                scroll_end = min(maxlines - 1, len(directories_with_sizes) - 1)
            if key == curses.KEY_HOME:
                selected_idx = 0
                scroll_start = 0
                scroll_end = min(maxlines - 1, len(directories_with_sizes) - 1)
            if key == ord('q'):
                return
            if key == curses.KEY_LEFT:
                if bar_offset > 0:
                    bar_offset -= 1
                if selected_bar > 0:
                    selected_bar -= 1
            elif key == curses.KEY_RIGHT:
                if size_data and bar_offset < len(size_data) - (width // max(1, width // min(len(size_data), max(width - 2, 1)))):
                    bar_offset += 1
                if size_data and selected_bar < min(len(size_data) - bar_offset, width // max(1, width // min(len(size_data), max(width - 2, 1)))) - 1:
                    selected_bar += 1
            elif key == curses.KEY_UP:
                if selected_idx > 0:
                    selected_idx -= 1
                elif scroll_start > 0:
                    scroll_start -= 1
                # Обновляем выбранную диаграмму для новой директории
                size_data = get_directory_size_data(os.path.join(target_directory, directories_with_sizes[selected_idx][0]))
                selected_bar = len(size_data) - 1 if size_data else 0
            elif key == curses.KEY_DOWN:
                if selected_idx < len(directories_with_sizes) - 1:
                    selected_idx += 1
                # Обновляем выбранную диаграмму для новой директории
                size_data = get_directory_size_data(os.path.join(target_directory, directories_with_sizes[selected_idx][0]))
                selected_bar = len(size_data) - 1 if size_data else 0
            elif key == ord('b'):
                logging.info(f"'b' key pressed to change size unit for selected directory.")
                # Обновляем индекс единицы измерения, переключаясь на следующую
                size_format_cyclers[target_directory]['index'] = (size_format_cyclers[target_directory]['index'] ) % len(size_format_cyclers[target_directory]['units'])
                current_unit = size_format_cyclers[target_directory]['units'][size_format_cyclers[target_directory]['index']]
                # Обновляем единицу измерения для текущей директории
                size_format_cyclers[target_directory]['index'], current_unit = get_next_unit(
                    directories_with_sizes[selected_idx][1],
                    size_format_cyclers[target_directory]['units'],
                    size_format_cyclers[target_directory]['index']
                )
                logging.info(f"Updated size unit to: {current_unit}")
                selected_size = directories_with_sizes[selected_idx][1]
                size_str = format_size_by_unit(selected_size, current_unit)
                stdscr.addstr(half_height + 1, 0, f"Current selection: {selected_dir}, Size ({current_unit}): {size_str}")
                stdscr.refresh()
            elif key == curses.KEY_ENTER or key == 10 or key == 13:
                # Переход в выбранную директорию или возврат на уровень вверх
                selected_dir = directories_with_sizes[selected_idx][0]
                if selected_dir == "..":
                    previous_directory = target_directory
                    target_directory = os.path.abspath(os.path.join(target_directory, ".."))
                else:
                    target_directory = os.path.join(target_directory, selected_dir)
                    previous_directory = None
                break

            # Обновляем окно прокрутки в случае, если выбранный элемент выходит за пределы текущего экрана
            if selected_idx < scroll_start:
                scroll_start = selected_idx
            elif selected_idx > scroll_end:
                scroll_start = selected_idx - maxlines + 1
            scroll_end = scroll_start + maxlines - 1

# Основная функция для запуска программы
# Использует ncurses для управления пользовательским интерфейсом
import signal

def signal_handler(sig, frame):
    print("Program terminated gracefully.")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)

def main():
    target_directory = args.directory

    # Запуск ncurses-оболочки с функцией display_directories
    curses.wrapper(display_directories, target_directory)

if __name__ == "__main__":
    main()
