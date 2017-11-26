# bitcointalk

parses bitcointalk ann forum and sends you notifications to telegram

## installation

you'll need python 3.6, postgresql and telegram

1. clone/download this repo
2. run `pip install -r requirements.txt`
3. create `config.yml` from `config.example.yml`
4. create a database in postgres
5. create a new bot in telegram
6. add this script to cron with 5 min interval
