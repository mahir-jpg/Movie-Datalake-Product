# Movie-Datalake-Product
Datalake and ingestion Project
AWS CLI is required
mysql-client is required

1. Start the containers
```bash
docker-compose up -d
```

2. Run the script.sh file to setup AWS:
```bash
bash scripts/script.sh
```

3. To extract the data to the Raw bucket:
```bash
python build/unpack_to_raw.py --bucket raw --endpoint http://localhost:4566
```

```bash
aws --endpoint-url=http://localhost:4566 s3 rm s3://raw --recursive
```

4. Setup the database
```bash
mysql -h 127.0.0.1 -P 3306 -u user -ppassword < "sql scripts"/init.sql
```

5. Extract from raw to staging

```bash
mysql -h 127.0.0.1 -P 3306 -u user -ppassword
```

```bash
Use staging_db;
Select * from movies limit 100;
```

