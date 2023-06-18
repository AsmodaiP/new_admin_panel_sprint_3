# Инструкция по запуску
1. Скачать репозиторий
 ```bash
 git clone https://github.com/AsmodaiP/new_admin_panel_sprint_3
 ```

2. Установить зависимости
```bash
  pip install poetry
  poetry install
```
3. Создать файл .env и заполнить его по примеру .env.example

4. Запустить процесс
```bash
   poetry run python etl/main.py
```