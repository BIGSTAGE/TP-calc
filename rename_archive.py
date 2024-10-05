import os
import re

# Путь к директории с файлами
data_dir = r'F:\cr\trading\Binance_archive\September_24_1min'

def rename_files(data_dir):
    # Проходим по всем файлам в директории
    for filename in os.listdir(data_dir):
        # Проверяем, что файл имеет формат .csv
        if filename.endswith('.csv'):
            # Используем регулярное выражение, чтобы извлечь тикер
            match = re.match(r'(.+?)_(USDT)_(.+?)\.csv', filename)
            if match:
                ticker = match.group(1)  # Тикер
                # Формируем новое имя файла
                new_filename = f"{ticker}{match.group(2)}_{match.group(2)}_september_2024.csv"
                # Путь к старому и новому файлу
                old_file_path = os.path.join(data_dir, filename)
                new_file_path = os.path.join(data_dir, new_filename)
                # Переименовываем файл
                os.rename(old_file_path, new_file_path)
                print(f"Переименован: {filename} -> {new_filename}")
            else:
                print(f"Не удалось распознать файл: {filename}")

# Запускаем переименование
rename_files(data_dir)
print("Переименование файлов завершено.")
