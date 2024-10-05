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
            # Загружаем данные по часовому таймфрейму
            batch = binance.fetch_ohlcv(ticker, timeframe='1m', since=since_timestamp, limit=1000)
            if not batch:
                print(f"Нет данных для {ticker} на временной метке {since_timestamp}.")
                break
            
            data.extend(batch)
            since_timestamp = batch[-1][0] + 60000  # Переход к следующему 1-часовому интервалу
            
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

# Функция для проверки и обработки пустых файлов
def process_empty_files():
    empty_files = []  # Список пустых файлов для удаления после успешной загрузки данных

    # Перебираем все файлы в директории
    for filename in os.listdir(data_dir):
        # Если файл имеет формат без расширения .csv (например, 1000FLOKIUSDT)
        if '_' not in filename and not filename.endswith('.csv'):
            ticker = filename  # Тикер в формате 1000FLOKIUSDT
            csv_file_name = f"{ticker}_september_2024.csv"
            csv_file_path = os.path.join(data_dir, csv_file_name)
            empty_file_path = os.path.join(data_dir, filename)
            
            # Проверяем, если CSV файла нет, а пустой файл присутствует
            if not os.path.exists(csv_file_path) and is_file_empty(empty_file_path):
                print(f"Обнаружен пустой файл и отсутствует CSV с данными для тикера {ticker}. Загружаем данные...")

                # Скачиваем данные для тикера
                df = fetch_data_for_ticker(ticker)
                if not df.empty:
                    # Сохраняем данные
                    save_ohlcv_data(df, ticker)
                    # Добавляем пустой файл в список для удаления, так как данные успешно загружены
                    empty_files.append(empty_file_path)
                else:
                    print(f"Нет данных для {ticker}, повторная загрузка не удалась.")

    # Удаляем пустые файлы, для которых данные были успешно загружены
    for empty_file in empty_files:
        try:
            os.remove(empty_file)
            print(f"Удален пустой файл: {empty_file}")
        except Exception as e:
            print(f"Ошибка при удалении пустого файла {empty_file}: {e}")

# Основная функция для запуска процесса
def main():
    # Обрабатываем пустые файлы и загружаем недостающие данные
    process_empty_files()

    print("Процесс завершен.")

# Запуск основного процесса
if __name__ == "__main__":
    main()
