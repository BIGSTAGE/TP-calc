В trades.xlsx добавляем сделки согласно шаблону
Ticker	Datetime	Open Price	Type

s=Short
b=Long

Желаемый процент TP меняем в переменной TAKE_PROFIT_PERCENTAGE

Данные берутся по 15-минутным отрезкам, чтобы изменить указываем здесь since_timestamp = batch[-1][0] + 900000 число миллисекунд в нужном таймфрейме, 1 минута = 60000мс
