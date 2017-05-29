9.1 Исходные данные описывают историю посещений пользователями страниц:

`user_id, timestamp, url, diff_time`

`user_id` – идентификатор пользователя, `timestamp` – время в секундах, `url` – адрес страницы, `diff_time` – время, проведенное на странице, в секундах.

Спроектируйте таблицу в HBase для запроса количества хитов (просмотров страниц) за указанный интервал времени в разбивке по часам/дням и по доменам.

----------
<P style="page-break-before: always">

9.2 Исходные данные описывают историю посещений пользователями страниц:

`user_id, timestamp, url, diff_time`

`user_id` – идентификатор пользователя, `timestamp` – время в секундах, `url` – адрес страницы, `diff_time` – время, проведенное на странице, в секундах.

Спроектируйте таблицу в HBase для запроса  распределения страниц по дням недели за указанный интервал времени, т.е. результат: день недели, страница, доля просмотров страницы от общего количества просмотров страниц в этот день недели.

----------
<P style="page-break-before: always">

9.3 Исходные данные описывают историю посещений пользователями страниц:

`user_id, timestamp, url, diff_time`

`user_id` – идентификатор пользователя, `timestamp` – время в секундах, `url` – адрес страницы, `diff_time` – время, проведенное на странице, в секундах.

Спроектируйте таблицу в HBase для запроса TOP10 наиболее посещаемых страниц за указанный интервал времени и доли их запросов в указанный интервал.

----------
<P style="page-break-before: always">

9.4 Исходные данные описывают историю посещений пользователями страниц:

`user_id, timestamp, url, diff_time`

`user_id` – идентификатор пользователя, `timestamp` – время в секундах, `url` – адрес страницы, `diff_time` – время, проведенное на странице, в секундах.

Спроектируйте таблицу в HBase для запроса доли количества хитов (просмотров страниц) для каждого домена за указанный интервал времени.