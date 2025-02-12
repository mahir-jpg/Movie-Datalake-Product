# Movie-Datalake-Product
Datalake and ingestion Project

```bash
python build/unpack_to_raw.py --bucket raw --endpoint http://localhost:4566
```

```bash
mysql -h 127.0.0.1 -P 3306 -u user -ppassword < "sql scripts"/init.sql
```


```bash
mysql -h 127.0.0.1 -P 3306 -u user -ppassword
```