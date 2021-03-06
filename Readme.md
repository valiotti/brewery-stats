# CraftBeerData

Сервис, который позволяет оценить реальное состояние оценок в Untappd на рынке российского крафта.

## Стартовый экран
* Общая динамика оценок за последине 12 месяцев по числу чекинов (с возможностью выбора /ABV )
* Средняя ежемесячная оценка крафтового пива (методика расчета среднего дана в Untappd)
* Топ-5 пивоварен по оценке за последний месяц
* Топ-5 пивоварен по числу чекинов за последний месяц
* Топ-5 сортов пива по числу чекинов за последний месяц
* Топ-5 городов РФ по числу чекинов за месяц / количество уникальных биргиков

## Базовая аналитика, доступная всем
Четыре раздела:

1. Биргики

* Топ-100 биргиков за всю историю по российскому крафту (количество чекинов, количество уникальных сортов, количество уникальных русских пивоварен)
* Топ-100 биргиков за текущий месяц и место vs предыдущий месяц (количество чекинов, количество уникальных сортов, количество уникальных русских пивоварен)
* Распределение количества оценок на биргика в месяц (boxplot)

2. Пиво
* Корреляция оценки и ABV за все время
* Корреляция оценки и изображения пива
* Степень похожести этикеток между пивоварнями
* Кластеризация сортов пива
* Факторы влияющие на высокий рейтинга (версия PRO для пивоварни)

3. Локации
* Карта России с числом чекинов, биргиков, уникальными сортами

4. Пивоварни
* Топ-пивоварен по чекинам за месяц / всю историю (чеки, уникальные оценки)
* Карта пивоварен: X - количество сортов, Y - средняя оценка, Z - количество чекинов (размер круга) -- динамика с начала (интерактивныый chart)

## Сервисы

### Биргики PRO
* На основе своих оценок получить рекомендации по сортам пива (необходима авторизация) (рекомендательная система)
* Поиск похожих на вас биргиков по оценкам (рекмеондательная система)

### Бары PRO
* Наилучшие сорта / наиболее интересные сорта в вашем регионе (планирование закупок)
* Наиболее интересные пивоварни в регионе 

### Пивоварни PRO
* Выявление накруток рейтинга Untappd у конкурентов (собрать информацию о пользователях и их чекинах)
* Какие сорта / изображения / получает лучшие отзывы
* Семантический анализ текста чекинов и выявление узких мест для дальнейшей фиксации
* Топ-лояльных антаппдеров
