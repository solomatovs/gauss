# Makefile для автоматической установки окружения и Poetry
# Поддерживаются macOS и Linux

# -----------------------------
# Настройки
# -----------------------------
EXTRAS := all  # имя группы optional extras (можно использовать все extras)

# -----------------------------
# Определяем ОС
# -----------------------------
UNAME_S := $(shell uname -s)

# -----------------------------
# Определение Python
# -----------------------------
PYTHON_BIN := $(shell \
	if command -v python3 >/dev/null 2>&1; then \
		command -v python3; \
	elif command -v python >/dev/null 2>&1; then \
		command -v python; \
	elif [ -x "/usr/bin/python3" ]; then \
		echo "/usr/bin/python3"; \
	elif [ -x "/opt/homebrew/bin/python3" ]; then \
		echo "/opt/homebrew/bin/python3"; \
	else \
		echo "Python не найден. Устанавливаем..."; \
		if [ "$$(uname -s)" = "Darwin" ]; then \
			echo "==> macOS detected"; \
			brew install python || true; \
		elif [ -f "/etc/debian_version" ]; then \
			sudo apt update >/dev/null 2>&1 && sudo apt install -y python3 >/dev/null 2>&1; \
		elif [ -f "/etc/redhat-release" ]; then \
			sudo yum install -y python3 >/dev/null 2>&1; \
		else \
			echo "Неизвестная ОС. Установите Python вручную." >&2; \
			exit 1; \
		fi; \
		command -v python3; \
	fi \
)

# -----------------------------
# Переменная POETRY_BIN
# -----------------------------
POETRY_BIN := $(shell \
	if command -v poetry >/dev/null 2>&1; then \
		command -v poetry; \
	elif [ -f "$$HOME/.local/bin/poetry" ]; then \
		echo "$$HOME/.local/bin/poetry"; \
	else \
		if [ "$(UNAME_S)" = "Darwin" ]; then \
			"/Applications/Python 3.11/Install Certificates.command" >/dev/null 2>&1 || true; \
		fi; \
		curl -sSL https://install.python-poetry.org | python3 - >/dev/null 2>&1; \
		POETRY_BIN="$$HOME/.local/bin/poetry"; \
		echo $$POETRY_BIN; \
	fi \
)

# -----------------------------
# Основные команды
# -----------------------------
.PHONY: help
help:
	@echo "Makefile команды:"
	@echo "  make show-poetry.  - Показать где лежит python"
	@echo "  make show-poetry.  - Показать где лежит poetry"
	@echo "  make setup         - Установить все зависимости (dev + extras) в .venv"
	@echo "  make clean         - Очистить .venv, lock файлы и .poetry"
	@echo "  make format        - Отформатировать код (black)"
	@echo "  make lint          - Проверить код линтером (ruff)"
	@echo "  make test          - Запустить тесты (pytest)"


.PHONY: show-python
show-python:
	@echo "$(PYTHON_BIN)"

.PHONY: show-poetry
show-poetry:
	@echo "$(POETRY_BIN)"

# -----------------------------
# Настройка проекта
# -----------------------------
.PHONY: setup
setup:
	@echo "==> Проверяем наличие виртуального окружения .venv"
	@POETRY_ENV_PATH=$$($(POETRY_BIN) env info --path 2>/dev/null || true) && \
	if [ -n "$$POETRY_ENV_PATH" ] && [ -d "$$POETRY_ENV_PATH" ]; then \
		echo ".venv уже существует"; \
	else \
		echo "Создаём виртуальное окружение в $(VENV_DIR)"; \
		$(POETRY_BIN) env use $(PYTHON_BIN); \
	fi
	@echo "==> Выполняем poetry lock"
	$(POETRY_BIN) lock
	@echo "==> Устанавливаем зависимости проекта (dev + extras)"
	$(POETRY_BIN) install --with dev -E $(EXTRAS)

# -----------------------------
# Очистка окружения
# -----------------------------
.PHONY: clean
clean:
	@echo "==> Удаляем артефакты"
	@rm -f poetry.lock
	@rm -rf .poetry
	@rm -rf .pytest_cache
	@rm -rf .ruff_cache
	@echo "==> Удаляем виртуальное окружение Poetry"
	@POETRY_ENV_PATH=$$($(POETRY_BIN) env info -p 2>/dev/null || true) && \
	if [ -n "$$POETRY_ENV_PATH" ] && [ -d "$$POETRY_ENV_PATH" ]; then \
		echo "Удаляем окружение: $$POETRY_ENV_PATH"; \
		rm -rf "$$POETRY_ENV_PATH" || true; \
	else \
		echo "Виртуальное окружение не найдено"; \
	fi
# -----------------------------
# Утилиты разработки
# -----------------------------
.PHONY: format
format: setup
	@echo "==> Форматируем код через black"
	$(POETRY_BIN) run black .

.PHONY: lint
lint: setup
	@echo "==> Линтим код через ruff"
	$(POETRY_BIN) run ruff check .

.PHONY: test
test: setup
	@echo "==> Запускаем тесты через pytest"
	@if [ -n "$(t)" ]; then \
		echo "==> Запуск конкретного теста: $(t)"; \
		$(POETRY_BIN) run pytest -v -k "$(t)"; \
	else \
		$(POETRY_BIN) run pytest -v; \
	fi