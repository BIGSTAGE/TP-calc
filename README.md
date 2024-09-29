В trades.xlsx добавляем сделки под шаблону
Ticker	Datetime	Open Price	Type

Копипастить сделки можно с Coinlegs, b и s ставим сами ручками
![image](https://github.com/user-attachments/assets/7c2cb4e0-3cc5-4c02-bda0-e63bef4bf502)


s=Short
b=Long

Желаемый процент TP меняем в переменной TAKE_PROFIT_PERCENTAGE

Данные берутся по 15-минутным отрезкам, чтобы изменить указываем здесь since_timestamp = batch[-1][0] + 900000 число миллисекунд в нужном таймфрейме, 1 минута = 60000мс

