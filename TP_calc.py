import ccxt
import pandas as pd
from datetime import datetime
import pytz

# Настройка клиента Binance через ccxt
binance = ccxt.binance({
    'options': {
        'defaultType': 'future'  # Важно для работы с фьючерсами
    }
})

# Установим часовой пояс UTC и UTC+3
utc = pytz.utc
utc_plus_3 = pytz.timezone('Europe/Moscow')  # UTC+3 временная зона

# Переменная для желаемого Take Profit в процентах
TAKE_PROFIT_PERCENTAGE = 1.2  # Значение Take Profit в процентах

# Функция для получения исторических данных по тикеру
# Функция для получения исторических данных по тикеру
# Функция для получения исторических данных по тикеру
def fetch_ohlcv(ticker, since):
    try:
        data = []
        since_timestamp = since  # У нас уже есть значение в миллисекундах

        while True:
            batch = binance.fetch_ohlcv(ticker, timeframe='15m', since=since_timestamp)
            if not batch:
                break  # Если данных больше нет, выходим из цикла
            
            # Добавляем данные в общий список
            data.extend(batch)

            # Обновляем временную метку для следующего запроса
            since_timestamp = batch[-1][0] + 900000  # Переход к следующей минуте

        # Если нет данных, возвращаем пустой DataFrame
        if len(data) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Преобразуем timestamp в UTC и затем конвертируем в UTC+3
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.tz_convert(utc_plus_3)

        return df
    except Exception as e:
        print(f"Ошибка при получении данных для {ticker}: {e}")
        return pd.DataFrame()


# Функция для расчета Take-Profit и максимального отклонения
def calculate_take_profit(df, open_price, trade_type):
    if df.empty:
        return None, None

    if trade_type == 'Long':  # Long
        target_price = open_price * (1 + TAKE_PROFIT_PERCENTAGE / 100)  # +1.2%
        max_opposite_deviation = 0  # Максимальное падение для Long
    else:  # Short
        target_price = open_price * (1 - TAKE_PROFIT_PERCENTAGE / 100)  # -1.2%
        max_opposite_deviation = 0  # Максимальный рост для Short

    for index, row in df.iterrows():
        if trade_type == 'Long':
            # Для Long: считаем отклонение по минимуму свечи
            deviation = ((open_price - row['low']) / open_price) * 100
            max_opposite_deviation = max(max_opposite_deviation, deviation)
            
            if row['high'] >= target_price:
                return row['timestamp'], max_opposite_deviation  # Возвращаем время TP и максимальное отклонение
        else:
            # Для Short: считаем отклонение по максимуму свечи
            deviation = ((row['high'] - open_price) / open_price) * 100
            max_opposite_deviation = max(max_opposite_deviation, deviation)
            
            if row['low'] <= target_price:
                return row['timestamp'], max_opposite_deviation  # Возвращаем время TP и максимальное отклонение

    return None, max_opposite_deviation  # TP не был достигнут, возвращаем максимальное отклонение

# Считываем данные сделок из Excel
def read_trades_from_excel(file_path):
    df = pd.read_excel(file_path)
    trades = []
    for _, row in df.iterrows():
        ticker = row['Ticker'].strip()  # Удаляем пробелы
        if not ticker.endswith('USDT'):
            ticker += 'USDT'  # Добавляем 'USDT', если его нет
        trades.append({
            "ticker": ticker,
            "datetime": row['Datetime'],
            "open_price": row['Open Price'],
            "type": row['Type'].strip().lower()  # Приводим тип сделки к нижнему регистру
        })
    return trades

# Путь к файлу Excel с данными сделок
excel_file_path = 'trades.xlsx'  # Укажите путь к вашему файлу

# Считываем данные сделок
trades = read_trades_from_excel(excel_file_path)

# Создаем список для результатов
results = []

# Проход по каждой сделке и расчет времени срабатывания TP
for index, trade in enumerate(trades, start=1):
    ticker = trade['ticker']
    open_datetime_str = trade['datetime']
    open_price = trade['open_price']
    trade_type = "Long" if trade['type'] == 'b' else "Short"  # Определяем полный тип сделки

    # Преобразуем время открытия сделки из UTC+3 в UTC
    open_datetime_utc_plus_3 = utc_plus_3.localize(datetime.strptime(open_datetime_str, "%d %b %Y %H:%M"))
    open_datetime_utc = open_datetime_utc_plus_3.astimezone(utc)

    # Конвертация времени сделки в миллисекунды для API
    since = int(open_datetime_utc.timestamp() * 1000)

    # Получение исторических данных по тикеру
    df = fetch_ohlcv(ticker, since)

    # Проверка, были ли получены данные
    if df.empty:
        results.append([ticker, open_datetime_str, "Error: No data", trade_type, "N/A"])  # Добавляем ошибку в результаты
        print(f"Не удалось получить данные для {ticker}. Пропускаем сделку.")
        continue  # Переходим к следующей сделке

    # Убираем строки, которые идут до момента открытия сделки
    df = df[df['timestamp'] >= open_datetime_utc_plus_3]

    # Проверяем, есть ли данные после фильтрации
    if df.empty:
        results.append([ticker, open_datetime_str, "No data after opening", trade_type, "N/A"])  # Добавляем ошибку в результаты
        print(f"Нет данных для {ticker} после времени открытия сделки. Пропускаем.")
        continue  # Переходим к следующей сделке

    # Расчет времени срабатывания Take-Profit и максимального отклонения
    tp_time, max_opposite_deviation = calculate_take_profit(df, open_price, trade_type)

    # Если TP не достигнут, устанавливаем tp_time на None для дальнейшей обработки
    if tp_time is not None:
        # Вычисляем время в часах между открытием сделки и достижением TP
        time_to_tp = (tp_time - open_datetime_utc_plus_3).total_seconds() / 3600
        results.append([ticker, open_datetime_str, f"{time_to_tp:.2f}", trade_type, f"{max_opposite_deviation:.2f}"])  # Без символа процента
    else:
        results.append([ticker, open_datetime_str, "TP not reached", trade_type, f"{max_opposite_deviation:.2f}"])  # Без символа процента

    # Выводим прогресс
    print(f"Обработан {index} тикер(ы) из {len(trades)}")

# Создаем DataFrame из результатов
results_df = pd.DataFrame(results, columns=["Ticker", "Datetime", "Duration", "Type", "Max Deviation"])

# Путь для сохранения результатов в Excel
output_file_path = 'results.xlsx'
results_df.to_excel(output_file_path, index=False)

print(f"Результаты сохранены в файл: {output_file_path}")
