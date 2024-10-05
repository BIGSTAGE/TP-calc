import ccxt
import pandas as pd
import os
from datetime import datetime, timezone

# Инициализация клиента Binance для фьючерсного рынка
binance = ccxt.binance({
    'options': {
        'defaultType': 'future'  # Используем фьючерсный рынок
    }
})

# Путь для сохранения данных
data_dir = r'F:\cr\trading\Binance_archive\September_24'  # Путь к вашей папке

# Создаем папку, если она не существует
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Получаем список всех тикеров с Binance
markets = binance.load_markets()
tickers = [market for market in markets if '/USDT' in market and markets[market]['active']]

# Диапазон времени для загрузки данных
start_date = '2024-09-01 00:00:00'
end_date = '2024-09-30 23:59:59'
since = int(datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
until = int(datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)

# Функция для загрузки исторических данных
def fetch_data_for_ticker(ticker):
    data = []
    since_timestamp = since
    
    while since_timestamp < until:
        try:
            batch = binance.fetch_ohlcv(ticker, timeframe='1m', since=since_timestamp, limit=1000)
            if not batch:
                print(f"Нет данных для {ticker} на временной метке {since_timestamp}.")
                break
            
            data.extend(batch)
            since_timestamp = batch[-1][0] + 60000  # Переход к следующему 1-минутному интервалу
            
            # Если получено меньше 1000 записей, значит, данных больше нет
            if len(batch) < 1000:
                break

        except Exception as e:
            print(f"Ошибка при запросе для {ticker} на временной метке {since_timestamp}: {e}")
            break

    if data:
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    else:
        return pd.DataFrame()  # Возвращаем пустой DataFrame, если данных нет

# Функция проверки, пуст ли файл
def is_file_empty(file_path):
    return os.path.exists(file_path) and os.stat(file_path).st_size == 0

# Функция для сохранения данных в CSV файл
def save_ohlcv_data(df, ticker):
    # Изменяем формат имени файла
    ticker_base = ticker.replace('/', '')
    csv_file = os.path.join(data_dir, f"{ticker_base}_september_2024.csv")
    df.to_csv(csv_file, index=False)
    print(f"Данные для {ticker} сохранены в {csv_file}.")

# Функция для проверки наличия пустого файла и отсутствия CSV файла
def download_missing_data():
    # Перебираем все файлы в директории
    for filename in os.listdir(data_dir):
        # Если файл имеет формат без расширения .csv (например, APT_USDT)
        if '_' in filename and not filename.endswith('.csv'):
            ticker = filename
            csv_file_name = f"{ticker}_september_2024.csv"
            csv_file_path = os.path.join(data_dir, csv_file_name)
            empty_file_path = os.path.join(data_dir, filename)
            
            # Проверяем, если парного CSV файла нет или он пустой
            if not os.path.exists(csv_file_path) and is_file_empty(empty_file_path):
                print(f"Обнаружен пустой файл и отсутствует CSV с данными для тикера {ticker}. Загружаем данные...")

                # Скачиваем данные заново
                df = fetch_data_for_ticker(ticker.replace('_', '/'))
                if not df.empty:
                    save_ohlcv_data(df, ticker)
                else:
                    print(f"Нет данных для {ticker}, повторная загрузка не удалась.")

# Функция для удаления пустых файлов
def remove_empty_files():
    for filename in os.listdir(data_dir):
        file_path = os.path.join(data_dir, filename)
        if is_file_empty(file_path):
            try:
                os.remove(file_path)
                print(f"Удален пустой файл: {filename}")
            except Exception as e:
                print(f"Ошибка при удалении файла {filename}: {e}")

# Основной цикл для загрузки данных для всех тикеров
for ticker in tickers:
    print(f"Загружаем данные для {ticker}...")
    df = fetch_data_for_ticker(ticker)
    
    if not df.empty:
        save_ohlcv_data(df, ticker)
    else:
        print(f"Нет данных для {ticker}, файл не будет создан.")

# После завершения основного процесса загружаем данные для отсутствующих или пустых файлов
download_missing_data()

# Удаление пустых файлов
remove_empty_files()

print("Процесс завершен.")
