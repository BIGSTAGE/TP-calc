import pandas as pd
from datetime import datetime
import os

# Путь к директории с данными (обновленный на минутный таймфрейм)
data_dir = r'F:\cr\trading\Binance_archive\September_24'
# Путь к файлу Excel с данными сделок
excel_file_path = r'F:\cr\trading\trades\trades_4h.xlsx'

TAKE_PROFIT_PERCENTAGE = 1.2  # Значение Take Profit в процентах

def load_data_from_csv(ticker):
    try:
        csv_file_name = f"{ticker.replace('/', '_')}_september_2024.csv"
        csv_file_path = os.path.join(data_dir, csv_file_name)
        if not os.path.isfile(csv_file_path):
            print(f"Файл {csv_file_path} не найден.")
            return pd.DataFrame()

        # Чтение данных из CSV с указанием разделителя и формата
        df = pd.read_csv(csv_file_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)  # Преобразуем в UTC
        return df
    except Exception as e:
        print(f"Ошибка при загрузке данных из {ticker}: {e}")
        return pd.DataFrame()

def calculate_take_profit(df, open_price, trade_type):
    if df.empty:
        return None, None

    # Устанавливаем целевую цену для Take Profit в зависимости от типа торговли
    if trade_type == 'Long':
        target_price = open_price * (1 + TAKE_PROFIT_PERCENTAGE / 100)
    else:  # trade_type == 'Short'
        target_price = open_price * (1 - TAKE_PROFIT_PERCENTAGE / 100)

    max_opposite_deviation = 0  # Здесь мы будем хранить максимальное отклонение
    tp_time = None  # Время достижения тейк-профита

    for _, row in df.iterrows():
        if trade_type == 'Long':
            # Отклонение для Long сделки: ищем минимальную цену на свече
            deviation = ((open_price - row['low']) / open_price) * 100
            max_opposite_deviation = max(max_opposite_deviation, deviation)

            # Если целевая цена достигнута
            if row['high'] >= target_price:
                tp_time = row['timestamp']
                break  # Завершаем цикл, так как TP достигнут

        elif trade_type == 'Short':
            # Отклонение для Short сделки: ищем максимальную цену на свече
            deviation = ((row['high'] - open_price) / open_price) * 100
            max_opposite_deviation = max(max_opposite_deviation, deviation)

            # Если целевая цена достигнута
            if row['low'] <= target_price:
                tp_time = row['timestamp']
                break  # Завершаем цикл, так как TP достигнут

    return tp_time, max_opposite_deviation

def read_trades_from_excel(file_path):
    df = pd.read_excel(file_path, decimal=',')  # Указываем разделитель десятичной части
    trades = []
    
    for _, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        if not ticker.endswith('USDT'):
            ticker += 'USDT'
        
        trade_type = str(row['Type']).strip().lower() if pd.notna(row['Type']) else ''
        
        if trade_type == 'buy':  
            trade_type = "Long"
        elif trade_type == 'sell':
            trade_type = "Short"
        else:
            continue  # Пропускаем строку, если тип не валиден

        trades.append({
            "ticker": ticker,
            "datetime": row['Datetime'],
            "open_price": row['Open Price'],
            "type": trade_type,
            "name": row['Name']
        })
    
    return trades

trades = read_trades_from_excel(excel_file_path)

results = []

# Добавляем столбец 'Open Price' в таблицу results
for index, trade in enumerate(trades, start=1):
    ticker = trade['ticker']
    open_datetime_str = trade['datetime']
    open_price = trade['open_price']
    trade_type = trade['type']
    name = trade['name']

    # Конвертация времени сделки из Excel в UTC и добавление временной зоны
    open_datetime_utc = pd.to_datetime(open_datetime_str, utc=True)

    # Загружаем данные из CSV для этого тикера
    df = load_data_from_csv(ticker)

    if df.empty:
        results.append([ticker, open_datetime_str, open_price, "Error: No data", trade_type, "N/A", name])
        print(f"Не удалось получить данные для {ticker}. Пропускаем сделку.")
        continue

    # Отфильтруем свечи, начиная с момента открытия сделки, включая свечу открытия
    df = df[df['timestamp'] >= open_datetime_utc]

    # Проверяем, чтобы включить первую свечу открытия
    open_row = df.loc[df['timestamp'] == open_datetime_utc]
    if open_row.empty:
        # Если свеча открытия отсутствует, добавляем ее в DataFrame
        open_row = df[df['timestamp'] < open_datetime_utc].iloc[-1:]  # Последняя свеча перед открытием
        df = pd.concat([open_row, df])

    if df.empty:
        results.append([ticker, open_datetime_str, open_price, "No data after opening", trade_type, "N/A", name])
        print(f"Нет данных для {ticker} после времени открытия сделки. Пропускаем.")
        continue

    # Рассчитываем время до достижения TP и максимальное отклонение
    tp_time, max_opposite_deviation = calculate_take_profit(df, open_price, trade_type)

    if tp_time is not None:
        # Рассчитываем время до достижения TP в часах
        time_to_tp = (tp_time - open_datetime_utc).total_seconds() / 3600
        results.append([
            ticker,
            open_datetime_str,
            open_price,  # Добавляем цену открытия
            round(time_to_tp, 2),  # Числовое значение для Duration
            trade_type,
            round(max_opposite_deviation, 2),  # Числовое значение для Max Deviation
            name
        ])

    else:
        # Если TP не достигнут
        results.append([
            ticker,
            open_datetime_str,
            open_price,  # Добавляем цену открытия
            "TP not reached",
            trade_type,
            round(max_opposite_deviation, 2),
            name
        ])

    print(f"Обработан {index} тикер(ы) из {len(trades)}")

# Извлекаем последнюю часть имени файла (например, _4h)
base_name = os.path.basename(excel_file_path)
file_name, file_ext = os.path.splitext(base_name)
suffix = file_name.split('_')[-1]  # Извлекаем последнюю часть

# Формируем строку с процентом TP для добавления в имя файла
tp_percentage_str = str(TAKE_PROFIT_PERCENTAGE).replace('.', ',')  # Преобразуем и заменяем точку на запятую

# Формируем путь для сохранения файла с результатами
output_file_path = os.path.join(r'F:\cr\trading\Results', f'results_local_{suffix}_{tp_percentage_str}%.xlsx')

# Сохраняем результаты в Excel
results_df = pd.DataFrame(results, columns=["Ticker", "Datetime", "Open Price", "Duration", "Type", "Max Deviation", "Name"])
results_df.to_excel(output_file_path, index=False)

print(f"Результаты сохранены в файл: {output_file_path}")

