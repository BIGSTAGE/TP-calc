import requests
import pandas as pd
import time

# Функция для отправки запроса на API
def fetch_data_from_api(start_date, end_date, page):
    url = 'https://www.coinlegs.com/api//Exchange/SelectDetections'
    headers = {
        'Content-Type': 'application/json'
    }

    data = {
        "DetectionIds": [14, 23, 12, 18, 41, 47, 45, 27, 13, 24, 25, 40, 3, 4, 1, 5, 2, 43, 16, 6, 9, 8, 44, 17, 10, 22, 48, 29, 37, 28, 33, 46, 42, 11, 26, 31, 39, 30, 35, 15, 7, 19, 20],
        "Periods": ["1h"],
        "MarketName": "",
        "StartDate": start_date,
        "EndDate": end_date,
        "Exchg": "Binance Futures",
        "Market": "USDT",
        "IncludeBuySignal": True,
        "IncludeSellSignal": True,
        "IncludeNeutralSignal": True,
        "Page": page,
        "Sorting": {}
    }

    try:
        # Отправляем POST запрос
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()  # Проверяем успешность запроса
        return response.json()  # Возвращаем данные в формате JSON
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе данных: {e}")
        return None

# Функция для отправки запроса с повторами в случае ошибки
def fetch_data_with_retries(start_date, end_date, page, max_retries=3, retry_delay=5):
    retries = 0
    while retries < max_retries:
        response = fetch_data_from_api(start_date, end_date, page)
        if response and response.get('success'):
            return response  # Успешный запрос
        else:
            retries += 1
            print(f"Не удалось получить данные для страницы {page}. Попытка {retries} из {max_retries}. Ожидание {retry_delay} секунд.")
            time.sleep(retry_delay)  # Задержка перед новой попыткой
    print(f"Превышено количество попыток для страницы {page}. Пропускаем.")
    return None

# Функция для сбора данных за весь сентябрь
def collect_september_data():
    all_data = []
    start_date = "2024-09-01T21:00:00.000Z"  # Начало сентября
    end_date = "2024-09-30T20:59:59.999Z"  # Конец сентября

    # Получаем информацию о количестве страниц и total detections для всех страниц
    initial_response = fetch_data_with_retries(start_date, end_date, 0)
    
    if initial_response and initial_response.get('success'):
        total_detections = initial_response.get('TotalDetections', 0)
        max_page = initial_response.get('MaxPage', 1)  # Максимальное количество страниц
        print(f"Total Detections: {total_detections}, Max Pages: {max_page}")
    else:
        print(f"Не удалось получить данные с {start_date} до {end_date}")
        return []

    successful_pages = 0  # Счетчик успешно обработанных страниц
    total_signals = 0  # Счетчик успешно обработанных детекций

    # Запрашиваем данные для всех страниц
    for current_page in range(max_page):
        print(f"Запрашиваем данные для страницы {current_page}")
        response = fetch_data_with_retries(start_date, end_date, current_page)

        if response and response.get('success'):
            signals = response.get('Signals', [])
            all_data.extend(signals)
            successful_pages += 1  # Увеличиваем счетчик успешных страниц
            total_signals += len(signals)  # Увеличиваем счетчик детекций
        else:
            print(f"Не удалось получить данные для страницы {current_page}. Пропускаем.")
            # Переход к следующей итерации

        # Добавляем задержку между запросами, чтобы не перегружать сервер
        time.sleep(1)

    # Выводим информацию о количестве успешно обработанных страниц и детекций
    print(f"Успешно обработано страниц: {successful_pages}, Детекций: {total_signals}")

    return all_data

# Функция для сохранения данных в Excel
def save_data_to_excel(data, filename=r'F:\cr\trading\Coinlegs archive\september_api_data_1h.xlsx'):
    if data:
        df = pd.DataFrame(data)  # Конвертируем данные в DataFrame для удобства
        df.to_excel(filename, index=False)  # Сохраняем в Excel
        print(f"Данные успешно сохранены в файл: {filename}")
    else:
        print("Нет данных для сохранения.")

# Основной запуск программы
if __name__ == "__main__":
    september_data = collect_september_data()  # Собираем данные за сентябрь
    save_data_to_excel(september_data)  # Сохраняем данные в Excel
