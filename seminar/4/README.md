## Семинар 3.
### Знакомство с HBase.

На данном семинаре будут рассмотрены:

- Работа с CLI HBase.
- Web интерфейс HBase.
- Bulk load в HBase.
- Работа в MR c HBase
- Schema design в HBase

После данного семинара студент должен уметь:

- подключаться к HBase в терминальном режиме
- получать справку по командам HBase cli
- проверять доступность регионов и статус HBase
- создавать изменять, удалять таблицы в HBase через cli
- изменять версионность таблицы, задавать начальное разбиение на регионы
- читать и писать данные по ключу через HBase cli
- иметь понимание о возможности массовой загрузку данные в HBase из данных любого - формата, а также уметь массово загружать данные в таблицу HBase через утилиту `importtsv`
- обращаться к данным в HBase из mapper (reducer) в MR задаче
- подбирать схему разложения данных по ключу для удобства выбора данных из HBase
- как создать табличку (hbase-cli), дропнуть, посмотреть статистики; потюнить количество versions to keep;
- показать web-интерфейс;
- через cli -- записать по ключу, прочитать по ключу, показать работу с timestamp (чтение старых данных);
- lookup из маппера с помощью happybase;
- bulk load через CLI;
- schema design по https://hbase.apache.org/book.html#schema

#### Материалы семинара
1. Для подключения к Hbase в терминальном режиме необходимо запустить команду hbase shell. Запустится командный интерпретатор Hbase.

        hbase(main):003:0>
Дальнейшие действия будем выполнять в этом командном интерпретаторе.
Для получения справки по командам необходимо выполнить

        hbase(main):003:0> help
Для получения статуса по hbase необходимо выполнить команду

        hbase(main):003:0> status
Для получения списка таблиц в hbase необходимо выполнить команду

        hbase(main):003:0> list
Для создания namespace в hbase необходимо выполнить команду

        hbase(main):003:0> create_namespace ‘your_namespace’
Для создания таблицы в hbase необходимо выполнить команду

        hbase(main):003:0> create ‘your_namespace:your_tablename’ , ‘cf’
Для получения информации по таблице в hbase необходимо выполнить команду

        hbase(main):003:0> describe ‘your_namespace:your_tablename’
Для создания таблицы с заданным количеством регионов необходимо выполнить команду

        hbase org.apache.hadoop.hbase.util.RegionSplitter ‘your_namespace:your_tablename’ UniformSplit -c 256 -f cf
Для изменения column family таблицы необходимо выполнить команду

        hbase(main):003:0>  alter ‘your_namespace:your_tablename’, {NAME=>'cf', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION=>'SNAPPY', VERSIONS => '3'}
Для внесения данных в таблицу необходимо выполнить команду

        hbase(main):003:0> put ‘your_namespace:your_tablename’, ‘row1’, ‘cf:c1’, ‘value’
Для просмотра данных в таблицы необходимо выполнить или get или scan

        hbase(main):003:0> get ‘your_namespace:your_tablename’, ‘row1’
        hbase(main):003:0> get ‘your_namespace:your_tablename’, ‘row1’, {COLUMN => ‘cf:c1’, TIMERANGE => [ts1, ts2], VERSIONS => 3}
        hbase(main):003:0> get ‘your_namespace:your_tablename’, ‘row1’, {COLUMN => ‘cf:c1’, TIMERANGE => [ts1, ts2], VERSIONS => 3}
        hbase(main):003:0>  scan ‘your_namespace:your_tablename’, {COLUMNS => [‘cf:c1’], LIMIT => 10, STARTROW => ‘value’}
Для удаления таблицы необходимо вначале ее деактивировать, а потом удалить

        hbase(main):003:0> disable ‘your_namespace:your_tablename’
        hbase(main):003:0> is_disabled ‘your_namespace:your_tablename’
        hbase(main):003:0> drop ‘your_namespace:your_tablename’

<!--
note1: $ echo "describe 'test'" | ./hbase shell -n > /dev/null 2>&1
note2: $ (Table, RowKey, Family, Column, Timestamp) → Value
note3: $ SortedMap<RowKey, List<SortedMap<Column, List<Value, Timestamp>>>>      -->

2. Для открытия web-интерфейса Hbase необходимо “пробросить” через опцию -L порт 60010 на машину кластера hadoop2-10.yandex.ru

    $ ssh user@hadoop2.yandex.ru -L 60010:hadoop2-10.yandex.ru:60010

3. Для массовой загрузки данных в таблицу Hbase возможно написать MR задачу, в которой реализовать создание hfile (https://blog.cloudera.com/blog/2013/09/how-to-use-hbase-bulk-loading-and-why/). Простой реализацией данного подхода является утилита `importtsv` (http://hbase.apache.org/0.94/book/ops_mgt.html#importtsv)

    Usage: importtsv -Dimporttsv.columns=a,b,c <tablename> <inputdir>




