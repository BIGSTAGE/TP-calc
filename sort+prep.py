import pandas as pd
import random
import string
import os

# Функция для генерации случайного двухзначного буквенно-цифрового обозначения
def generate_random_suffix():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=2))

# Шаг 1: Загрузка данных и сортировка
file_path = r'F:\cr\trading\Coinlegs archive\september_api_data_1w.xlsx'
df = pd.read_excel(file_path)

# Преобразуем временные метки, убираем временную зону
if 'SignalDate' in df.columns:
    df['SignalDate'] = pd.to_datetime(df['SignalDate']).dt.tz_localize(None)
if 'RecordDate' in df.columns:
    df['RecordDate'] = pd.to_datetime(df['RecordDate']).dt.tz_localize(None)
if 'SignalDateUTCString' in df.columns:
    df['SignalDateUTCString'] = pd.to_datetime(df['SignalDateUTCString']).dt.tz_localize(None)

# Сортируем данные
df_sorted = df.sort_values(by=['SignalDate', 'MarketName'], ascending=[False, True])

# Удаляем дубликаты по указанным столбцам
df_sorted = df_sorted.drop_duplicates(subset=['Name', 'DisplayName', 'ShortMarketName', 'SignalDate', 'Price'])

# Сохранение отсортированной таблицы без дубликатов (можно пропустить этот шаг, если не нужен промежуточный файл)
sorted_output_file = r'F:\cr\trading\Coinlegs archive\sorted\sorted_september_1w_v2.xlsx'
df_sorted.to_excel(sorted_output_file, index=False)
print("Таблица успешно отсортирована и сохранена без дубликатов.")

# Шаг 2: Извлечение нужных столбцов
df_selected = df_sorted[['Signal', 'ShortMarketName', 'SignalDate', 'Price', 'Name']].copy()  # Добавляем 'Name'

# Шаг 3: Преобразование Signal в Type
df_selected.loc[:, 'Type'] = df_selected['Signal'].map({1: 'Buy', -1: 'Sell'})

# Шаг 4: Преобразование формата даты SignalDate
df_selected.loc[:, 'Datetime'] = pd.to_datetime(df_selected['SignalDate'], errors='coerce').dt.strftime('%d %b %Y %H:%M')

# Шаг 5: Приведение 'Open Price' к нужному формату (числовой)
df_selected['Price'] = df_selected['Price'].astype(str).str.replace(',', '.').astype(float)

# Шаг 6: Создание новой таблицы с нужными столбцами
df_final = df_selected[['Type', 'ShortMarketName', 'Datetime', 'Price', 'Name']].rename(columns={
    'ShortMarketName': 'Ticker',
    'Price': 'Open Price'
})

# Шаг 7: Генерация случайного суффикса и сохранение новой таблицы
random_suffix = generate_random_suffix()
output_file = f'F:/cr/trading/Trades/trades_{random_suffix}.xlsx'

# Сохраняем результат в файл Excel
df_final.to_excel(output_file, index=False)

print(f"Таблица успешно создана и сохранена как {output_file}.")
