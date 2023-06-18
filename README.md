# Инструкция по запуску

1. Скачайте репозиторий
    ```bash
    git clone https://github.com/AsmodaiP/new_admin_panel_sprint_3
    cd new_admin_panel_sprint_3
    ```

2. Установите зависимости 
```bash
  pip install poetry
  cd postgres_to_es
  poetry install
```
3. Создатe файл .env и заполнитe его по примеру .env.example

4. Запуститe процесс
```bash
   poetry run python etl/main.py
```


# Запуск через docker-compose
1. Создайте файл .env и заполните его по примеру .env.example
2. Запустите docker-compose
```bash
    docker-compose up
```

