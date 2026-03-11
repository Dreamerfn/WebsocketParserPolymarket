# Deploy Guide (Upload to GitHub)

Ниже шаги для залива проекта в репозиторий:
- `https://github.com/Dreamerfn/WebsocketParserPolymarket`

## 1) Перейти в каталог проекта

```bash
cd ~/Desktop/Vibe-Projects/WebSocketParser
```

## 2) Инициализация git (если еще не сделано)

```bash
git init -b main
```

## 3) Привязать remote

```bash
git remote add origin https://github.com/Dreamerfn/WebsocketParserPolymarket.git
```

Если remote уже есть:

```bash
git remote set-url origin https://github.com/Dreamerfn/WebsocketParserPolymarket.git
```

## 4) Проверить что не включаются тяжелые артефакты

```bash
git status --short
```

Убедитесь, что в индекс не попали:
- `.venv*`
- `data/`
- `*.csv`

## 5) Commit

```bash
git add .
git commit -m "Initial import: websocket parser for Polymarket/Binance + docs"
```

## 6) Push

```bash
git push -u origin main
```

## 7) Проверка

Откройте:
- `https://github.com/Dreamerfn/WebsocketParserPolymarket`

Проверьте, что отображаются:
- `collector/`
- `tests/`
- `docs/`
- `README.md`
