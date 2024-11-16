import os
import curses
import argparse
import sqlite3
import datetime


# Функция для загрузки конфигурации из файла
def load_config(config_file='config.txt'):
    config = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                # Игнорируем пустые строки и комментарии
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Разбиваем строку на ключ и значение
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

if not DB_PATH:
    raise ValueError("DB_PATH not set in configuration file.")

# Функция для обработки командной строки
def parse_arguments():
    parser = argparse.ArgumentParser(description="Display directory list with ncurses.")
    parser.add_argument(
        'directory', nargs='?', default=os.getcwd(),
        help="Directory to display (default is current working directory)"
    )
    return parser.parse_args()

# Функция для получения данных по размеру директории
def get_directory_size_data(directory):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Убираем ограничение по времени и извлекаем все данные
    cursor.execute("""
        SELECT timestamp, size FROM directory_snapshot
        WHERE path = ?
        ORDER BY timestamp
    """, (directory,))

    data = cursor.fetchall()
    conn.close()
    return data

# Функция для форматирования размера в человекочитаемый вид
def format_size(size):
    """Форматирует размер в человекочитаемый формат (байты, КБ, МБ, ГБ)."""
    for unit in ['B', 'K', 'M', 'G', 'T']:
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}P"  # На случай, если размер слишком велик

# Функция для получения последнего размера директории
def get_last_snapshot_size(directory):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем последний снапшот размера для каждой директории
    cursor.execute("""
        SELECT path, MAX(timestamp) AS latest_time, size
        FROM directory_snapshot
        WHERE path LIKE ?
        GROUP BY path
    """, (f"{directory}%",))
    
    data = cursor.fetchall()
    conn.close()

    # Возвращаем словарь {путь: размер}
    return {row[0]: row[2] for row in data}

# Функция для отображения диаграммы с символом #
def draw_bar_chart(stdscr, size_data, start_row, max_height, max_width):
    if not size_data:
        return

    max_size = max(size for _, size in size_data)

    if max_size == 0:
        return  # Нечего отображать, все размеры равны нулю

    num_bars = min(len(size_data), max_width - 2)
    bar_width = max(1, max_width // num_bars)

    for i, (_, size) in enumerate(size_data[:num_bars]):
        bar_height = int((size / max_size) * max_height)
        bar_x = i * bar_width + 1

        for j in range(bar_height):
            stdscr.addstr(start_row - j, bar_x, '#')

# Функция для отображения списка директорий с использованием ncurses
def display_directories(stdscr, target_directory):
    previous_directory = None

    while True:
        # Получаем список директорий и их размеры
        directories = [d for d in os.listdir(target_directory) if os.path.isdir(os.path.join(target_directory, d))]
        directories_sizes = get_last_snapshot_size(target_directory)

        # Сортируем директории по размеру (по убыванию)
        directories = sorted(directories, key=lambda d: directories_sizes.get(os.path.join(target_directory, d), 0), reverse=True)

        # Форматируем список с размерами
        directories_with_sizes = [
            (d, directories_sizes.get(os.path.join(target_directory, d), 0)) for d in directories
        ]

        parent_directory = os.path.abspath(os.path.join(target_directory, ".."))
        if parent_directory != target_directory:
            directories_with_sizes.insert(0, ("..", 0))  # Добавляем родительскую директорию

        curses.curs_set(0)
        stdscr.clear()
        height, width = stdscr.getmaxyx()

        half_height = height // 2
        maxlines = half_height - 2 - 1
        selected_idx = 0
        scroll_start = 0
        scroll_end = scroll_start + maxlines - 1

        if previous_directory:
            try:
                selected_idx = [d for d, _ in directories_with_sizes].index(os.path.basename(previous_directory))
            except ValueError:
                selected_idx = 0
            scroll_start = max(0, selected_idx - maxlines // 2)
            scroll_end = scroll_start + maxlines - 1

        while True:
            stdscr.addstr(0, 0, f"Directory listing for: {target_directory}")
            stdscr.addstr(1, 0, "-" * width)

            for idx, (dir_name, size) in enumerate(directories_with_sizes[scroll_start:scroll_end + 1]):
                stdscr.addstr(2 + idx, 0, " " * width)
                global_idx = scroll_start + idx
                display_name = f"{dir_name} ({format_size(size)})"
                if global_idx == selected_idx:
                    stdscr.addstr(2 + idx, 0, f"> {display_name}", curses.A_REVERSE)
                else:
                    stdscr.addstr(2 + idx, 0, f"  {display_name}")

            stdscr.addstr(half_height - 1, 0, "-" * width)

            selected_dir = directories_with_sizes[selected_idx][0]
            stdscr.addstr(half_height, 0, f"Current selection: {selected_dir}")

            size_data = get_directory_size_data(os.path.join(target_directory, selected_dir))

            # Очищаем область диаграммы
            for row in range(half_height, height - 2):
                stdscr.addstr(row, 0, " " * width)

            # Отображаем отладочный вывод и диаграмму
            debug_info = f"Debug size_data: {size_data}"
            stdscr.addstr(height - 3, 0, debug_info[:width])
            draw_bar_chart(
                stdscr, size_data,
                start_row=height - 5,
                max_height=height - half_height - 6,
                max_width=width
            )

            stdscr.addstr(height - 1, 0, "Press 'q' to quit, Enter to select")
            stdscr.refresh()

            key = stdscr.getch()
            if key == ord('q'):
                return
            elif key == curses.KEY_UP:
                if selected_idx > 0:
                    selected_idx -= 1
                elif scroll_start > 0:
                    scroll_start -= 1
            elif key == curses.KEY_DOWN:
                if selected_idx < len(directories_with_sizes) - 1:
                    selected_idx += 1
            elif key == curses.KEY_ENTER or key == 10 or key == 13:
                selected_dir = directories_with_sizes[selected_idx][0]
                if selected_dir == "..":
                    previous_directory = target_directory
                    target_directory = os.path.abspath(os.path.join(target_directory, ".."))
                else:
                    target_directory = os.path.join(target_directory, selected_dir)
                    previous_directory = None
                break

            if selected_idx < scroll_start:
                scroll_start = selected_idx
            elif selected_idx > scroll_end:
                scroll_start = selected_idx - maxlines + 1
            scroll_end = scroll_start + maxlines - 1
# Основная функция
def main():
    # Получаем путь из аргументов командной строки или используем текущую директорию
    args = parse_arguments()
    target_directory = args.directory

    # Используем ncurses для отображения
    curses.wrapper(display_directories, target_directory)

if __name__ == "__main__":
    main()