5.1 Есть данные о посещения пользователями страниц:	`userid, timestamp, url, browser`,
где `browser` – это Chrome, Firefox, MSIE, Safari и т.д. При этом один пользователь может приходить из нескольких браузеров. 

Соберите статистику посещений доменов для всех пользователей, которые хотя бы раз появлялись с браузером “Chrome” в виде:
`userid, domain, count`

Напишите соответствующий Hive запрос.

----------
<P style="page-break-before: always">

5.2 Есть данные о посещениях страниц – events: 

`userid, timestamp, url, date`

Постройте рейтинг популярных страниц (url’ов) за каждый день по числу уникальных пользователей, результат сгруппируйте по url в виде: 

`url, date, position_in_rating`

Напишите соответствующий Hive запрос. (Указание: используйте оконные функции.)

----------
<P style="page-break-before: always">

5.3 Есть данные о посещениях страниц – events: 

`userid, timestamp, url, date`

Вычислите долю посещений и уникальных посетителей для каждого урла от общего числа посещений и посетителей на домене этого url’а за каждый день: 

`url, date, pageviews/domain_pageviews, users/domain_users`

Напишите соответствующий Hive запрос.

----------
<P style="page-break-before: always">
 
5.4 Таблицы a и b партиционированы по дням. Делаем LEFT JOIN двух партиций (так можно получить, например, какие записи из a отсутствуют в b) . Какое решение правильное и почему:

	SELECT a.val, b.val FROM a LEFT OUTER JOIN b
	ON (a.key=b.key)
	WHERE a.date='2009-07-07' AND b.date='2009-07-07';
 
	SELECT a.val, b.val FROM a LEFT OUTER JOIN b
	ON (a.key=b.key AND b.date='2009-07-07' AND a.date='2009-07-07');

----------
<P style="page-break-before: always">

5.5 Из документации Hive:

If all but one of the tables being joined are small, the join can be performed as a map only job. The query

	SELECT /*+ MAPJOIN(b) */ a.key, a.value
	FROM a JOIN b ON a.key = b.key

does not need a reducer. For every mapper of A, B is read completely. The restriction is that a FULL/RIGHT OUTER JOIN b cannot be performed.

Объясните последнюю фразу – откуда такое ограничение?

----------
<P style="page-break-before: always">

5.6 Исходные данные – посещения пользователями страниц: `userid, timestamp, url, date`

Нужно отфильтровать за два дня – d1 и d2 такие url’ы, на которых было > 100 посещений в день: U1 и U2. Далее за d2 отобрать только новые: U2\U1. На этом множестве отобрать все события за d1. (Т.е. таким образом, мы отбираем пользователей, которые посещали страницы, которые стали популярными лишь на следующий день). 

Напишите соответствующий Hive запрос.

----------
<P style="page-break-before: always">

5.7 Есть данные о посещениях страниц – events: 

`userid, timestamp, url, date`

и данные о новостных кластерах (страницы, которые на разных сайтах содержат одну и ту же новость образуют новостной кластер) – news: 

`url, cluster_id`

Постройте гистограмму: количества кластеров от количества посещений (т.е. n1 кластеров с одним визитом, n2 с двумя и т.д.). Напишите соответствующий Hive запрос.

----------
<P style="page-break-before: always">

5.8 Есть Hive запрос:

	SELECT a.year, a.quarter, b.origin, b.originstate, b.origintype count(*) cn 
	FROM (
		SELECT id, YEAR(date) year, QUARTER(date) quarter, origin, originstate
		FROM air_travel_booking
	) a
	JOIN (
		SELECT id, origin, originstate, origintype
		FROM air_travel_origins
	) b
	ON (
		a.origin = b.origin AND a.orignstate = b.originstate
	)
	GROUP BY a.year, a.quarter, b.origin, b.originstate, b.origintype;

Есть маршруты и бронирования по этим маршрутам. Хотим узнать для каждого квартала количество перелетов по маршруту каждого типа.

Опишите, какие MapReduce задачи реализуют этот запрос.

----------
