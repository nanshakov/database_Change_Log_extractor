import os
import yaml
from pathlib import Path
import re


def extract_date_from_filename(filename):
    """Извлекает дату из имени файла в формате YYYYMMDD-HHMM-..."""
    pattern = r'(\d{8}-\d{4})'
    match = re.search(pattern, filename)
    if match:
        date_str = match.group(1)
        try:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            hour = date_str[9:11]
            minute = date_str[11:13]
            return f"{year}-{month}-{day} {hour}:{minute}"
        except:
            return date_str
    return None


def resolve_relative_path(relative_path, base_file_path):
    """Разрешает относительные пути с ../ относительно базового файла"""
    base_dir = os.path.dirname(os.path.abspath(base_file_path))
    absolute_path = os.path.normpath(os.path.join(base_dir, relative_path))
    return absolute_path


def is_schema_path(file_path):
    """Проверяет, является ли путь путем к схеме (начинается с src/main/adgp/databases/dwh/schemas/)"""
    return file_path.startswith('src/main/adgp/databases/dwh/schemas/')


def get_schema_folder(schema_path):
    """Извлекает путь к папке схемы (убирает имя файла)"""
    return os.path.dirname(schema_path)


def process_yaml_file(file_path, source_master, nested_file=None):
    """Обрабатывает YAML файл и извлекает пути к схемам"""
    schema_files = []

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)

        if data and 'databaseChangeLog' in data:
            for item in data['databaseChangeLog']:
                if isinstance(item, dict):
                    # Обрабатываем include с file
                    if 'include' in item and 'file' in item['include']:
                        included_file = item['include']['file']

                        # Если это путь к схеме, добавляем его
                        if is_schema_path(included_file):
                            schema_files.append({
                                'schema_file': included_file,
                                'schema_folder': get_schema_folder(included_file),
                                'source_master': source_master,
                                'nested_file': nested_file,
                                'nested_file_date': extract_date_from_filename(
                                    os.path.basename(nested_file)) if nested_file else None,
                                'type': 'direct_schema'
                            })
                        else:
                            # Если это не путь к схеме, но содержит ../, разрешаем и проверяем
                            if '../' in included_file:
                                resolved_path = resolve_relative_path(included_file, file_path)

                                # Если разрешенный путь ведет к YAML файлу, рекурсивно обрабатываем
                                if os.path.exists(resolved_path) and (
                                        resolved_path.endswith('.yaml') or resolved_path.endswith('.yml')):
                                    print(f"  Рекурсивно обрабатываем: {included_file} -> {resolved_path}")
                                    nested_schemas = process_yaml_file(
                                        resolved_path,
                                        source_master,
                                        file_path
                                    )
                                    schema_files.extend(nested_schemas)

                    # Обрабатываем includeAll с path
                    elif 'includeAll' in item and 'path' in item['includeAll']:
                        include_path = item['includeAll']['path']

                        # Если путь относительный, разрешаем его
                        if '../' in include_path:
                            include_path = resolve_relative_path(include_path, file_path)

                        if os.path.exists(include_path):
                            print(f"  Обрабатываем includeAll: {include_path}")
                            nested_schemas = process_include_all_directory(include_path, source_master)
                            schema_files.extend(nested_schemas)

    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")

    return schema_files


def process_include_all_directory(directory_path, master_source):
    """Обрабатывает директорию из includeAll и извлекает пути к схемам из всех YAML файлов"""
    schema_files = []

    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.yaml') or file.endswith('.yml'):
                    file_path = os.path.join(root, file)
                    print(f"    Обрабатываем файл из includeAll: {file_path}")
                    nested_schemas = process_yaml_file(file_path, master_source, file_path)
                    schema_files.extend(nested_schemas)
    except Exception as e:
        print(f"Ошибка при обработке директории {directory_path}: {e}")

    return schema_files


def find_all_master_files(base_path):
    """Находит все master.yaml файлы в структуре папок"""
    master_files = []

    for root, dirs, files in os.walk(base_path):
        if 'master.yaml' in files:
            master_files.append(os.path.join(root, 'master.yaml'))

    return master_files


def get_latest_files_from_report(all_schema_files):
    """Выбирает последние файлы из полного отчета (без сортировки)"""
    # Группируем по папкам
    folder_files = {}

    for schema_info in all_schema_files:
        folder = schema_info['schema_folder']

        if folder not in folder_files:
            folder_files[folder] = []

        folder_files[folder].append(schema_info)

    # Для каждой папки берем последний элемент из списка (как он был добавлен)
    latest_files = []
    for folder, files in folder_files.items():
        # Берем последний файл из списка (без сортировки)
        latest_file = files[-1]
        latest_files.append(latest_file)

    return latest_files


def main():
    base_path = "src/main/adgp/databases/dwh/changelogs"

    if not os.path.exists(base_path):
        print(f"Путь {base_path} не существует!")
        return

    # Находим все master.yaml файлы
    master_files = find_all_master_files(base_path)
    print(f"Найдено master.yaml файлов: {len(master_files)}")

    # Собираем все пути к схемам
    all_schema_files = []

    for master_file in master_files:
        print(f"\nОбрабатываем master: {master_file}")
        schema_files = process_yaml_file(master_file, master_file)
        all_schema_files.extend(schema_files)
        print(f"  Найдено путей к схемам: {len(schema_files)}")

    # Получаем последние файлы из отчета (без сортировки)
    latest_schema_files = get_latest_files_from_report(all_schema_files)

    # ВЫВОД 1: Полный отчет со всеми файлами
    print(f"\n{'=' * 80}")
    print(f"ПОЛНЫЙ ОТЧЕТ - ВСЕ ПУТИ К СХЕМАМ: {len(all_schema_files)}")
    print(f"{'=' * 80}\n")

    for i, schema_info in enumerate(all_schema_files, 1):
        print(f"{i:3d}. Схема: {schema_info['schema_file']}")
        print(f"     Папка: {schema_info['schema_folder']}")
        print(f"     Master: {schema_info['source_master']}")
        if schema_info['nested_file']:
            print(f"     Вложенный файл: {schema_info['nested_file']}")
            if schema_info.get('nested_file_date'):
                print(f"     Дата файла: {schema_info['nested_file_date']}")
        print(f"     Тип: {schema_info['type']}")
        print()

    # ВЫВОД 2: Отчет только с последними файлами из каждой папки (из отчета)
    print(f"\n{'=' * 80}")
    print(f"ОТЧЕТ ПО ПОСЛЕДНИМ ФАЙЛАМ ИЗ ОТЧЕТА: {len(latest_schema_files)}")
    print(f"(выбрано по последнему вхождению в каждой папке без сортировки)")
    print(f"{'=' * 80}\n")

    for i, schema_info in enumerate(latest_schema_files, 1):
        print(f"{i:3d}. Схема: {schema_info['schema_file']}")
        print(f"     Папка: {schema_info['schema_folder']}")
        print(f"     Master: {schema_info['source_master']}")
        if schema_info['nested_file']:
            print(f"     Вложенный файл: {schema_info['nested_file']}")
            if schema_info.get('nested_file_date'):
                print(f"     Дата файла: {schema_info['nested_file_date']}")
        print(f"     Тип: {schema_info['type']}")
        print()

    # Сохраняем полный отчет в файл
    with open('all_schema_paths_report.txt', 'w', encoding='utf-8') as f:
        f.write("ПОЛНЫЙ ОТЧЕТ - ВСЕ ПУТИ К СХЕМАМ\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"Всего найдено путей: {len(all_schema_files)}\n\n")

        for i, schema_info in enumerate(all_schema_files, 1):
            f.write(f"{i:3d}. Схема: {schema_info['schema_file']}\n")
            f.write(f"     Папка: {schema_info['schema_folder']}\n")
            f.write(f"     Master: {schema_info['source_master']}\n")
            if schema_info['nested_file']:
                f.write(f"     Вложенный файл: {schema_info['nested_file']}\n")
                if schema_info.get('nested_file_date'):
                    f.write(f"     Дата файла: {schema_info['nested_file_date']}\n")
            f.write(f"     Тип: {schema_info['type']}\n")
            f.write("\n")

    # Сохраняем отчет по последним файлам в отдельный файл
    with open('latest_schema_files_report.txt', 'w', encoding='utf-8') as f:
        f.write("ОТЧЕТ ПО ПОСЛЕДНИМ ФАЙЛАМ ИЗ ОТЧЕТА\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"Всего папок: {len(latest_schema_files)}\n")
        f.write(f"(выбрано по последнему вхождению в каждой папке без сортировки)\n\n")

        for i, schema_info in enumerate(latest_schema_files, 1):
            f.write(f"{i:3d}. Схема: {schema_info['schema_file']}\n")
            f.write(f"     Папка: {schema_info['schema_folder']}\n")
            f.write(f"     Master: {schema_info['source_master']}\n")
            if schema_info['nested_file']:
                f.write(f"     Вложенный файл: {schema_info['nested_file']}\n")
                if schema_info.get('nested_file_date'):
                    f.write(f"     Дата файла: {schema_info['nested_file_date']}\n")
            f.write(f"     Тип: {schema_info['type']}\n")
            f.write("\n")

    # Сохраняем в CSV
    with open('all_schema_paths_report.csv', 'w', encoding='utf-8') as f:
        f.write("schema_file;schema_folder;source_master;nested_file;nested_file_date;type\n")
        for schema_info in all_schema_files:
            nested_file = schema_info.get('nested_file', '')
            nested_file_date = schema_info.get('nested_file_date', '')
            f.write(
                f"{schema_info['schema_file']};{schema_info['schema_folder']};{schema_info['source_master']};{nested_file};{nested_file_date};{schema_info['type']}\n")

    with open('latest_schema_files_report.csv', 'w', encoding='utf-8') as f:
        f.write("schema_file;schema_folder;source_master;nested_file;nested_file_date;type\n")
        for schema_info in latest_schema_files:
            nested_file = schema_info.get('nested_file', '')
            nested_file_date = schema_info.get('nested_file_date', '')
            f.write(
                f"{schema_info['schema_file']};{schema_info['schema_folder']};{schema_info['source_master']};{nested_file};{nested_file_date};{schema_info['type']}\n")

    print(f"\nРезультат сохранен в файлы:")
    print(f"  ПОЛНЫЙ ОТЧЕТ:")
    print(f"    - all_schema_paths_report.txt")
    print(f"    - all_schema_paths_report.csv")
    print(f"  ОТЧЕТ ПО ПОСЛЕДНИМ ФАЙЛАМ ИЗ ОТЧЕТА:")
    print(f"    - latest_schema_files_report.txt")
    print(f"    - latest_schema_files_report.csv")


if __name__ == "__main__":
    main()