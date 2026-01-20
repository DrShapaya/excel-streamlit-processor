import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import re
import warnings
import zipfile
import tempfile
import logging
import threading
from typing import Optional, Tuple, Dict, List
import shutil

# ==========================
# КОНСТАНТЫ И НАСТРОЙКИ
# ==========================

CONFIG = {
    "KEY_COLUMN": "ФИО",
    "MAX_COLUMNS": 50,
    "MONTHS": {
        "ЯНВАРЬ": 1, "ФЕВРАЛЬ": 2, "МАРТ": 3, "АПРЕЛЬ": 4,
        "МАЙ": 5, "ИЮНЬ": 6, "ИЮЛЬ": 7, "АВГУСТ": 8,
        "СЕНТЯБРЬ": 9, "ОКТЯБРЬ": 10, "НОЯБРЬ": 11, "ДЕКАБРЬ": 12
    },
    "ALLOWED_EXTENSIONS": [".xlsx", ".xls"],
    "MAX_FILE_SIZE_MB": 50
}

# Современная цветовая схема
COLORS = {
    "primary": "#2563eb",  # Синий
    "secondary": "#64748b",  # Серый
    "accent": "#3b82f6",  # Светло-синий
    "success": "#10b981",  # Зеленый
    "warning": "#f59e0b",  # Оранжевый
    "danger": "#ef4444",  # Красный
    "light": "#f8fafc",  # Светло-серый
    "dark": "#1e293b",  # Темно-синий
    "text": "#334155",  # Текст
    "bg": "#ffffff",  # Белый фон
    "card": "#f1f5f9",  # Карточки
    "border": "#e2e8f0"  # Границы
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Игнорируем предупреждения
warnings.filterwarnings("ignore", message="Could not infer format")
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# ==========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================

def find_year_folders(base_dir: Path) -> List[str]:
    """Поиск папок с годами (рекурсивно)"""
    try:
        if not base_dir or not base_dir.exists():
            return []

        year_folders = []

        # Ищем папки с годами рекурсивно
        for year_folder in base_dir.rglob("*"):
            if year_folder.is_dir():
                name = year_folder.name
                # Проверяем, является ли имя папки годом
                if (name.isdigit() and len(name) == 4
                        and 2000 <= int(name) <= 2100):
                    # Проверяем, есть ли в папке Excel файлы
                    has_excel_files = False
                    for ext in CONFIG["ALLOWED_EXTENSIONS"]:
                        if list(year_folder.glob(f"*{ext}")):
                            has_excel_files = True
                            break

                    if has_excel_files:
                        year_folders.append(str(year_folder.relative_to(base_dir)))

        # Удаляем дубликаты и сортируем
        unique_years = sorted(
            set(year_folders),
            key=lambda x: (x.split('\\')[-1] if '\\' in x else x),
            reverse=True
        )

        return unique_years

    except Exception as e:
        logger.error(f"Ошибка при поиске папок с годами: {e}")
        return []

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от пустых и ненужных столбцов"""
    if df.empty:
        return df

    original_columns = len(df.columns)

    # Удаляем полностью пустые столбцы
    df = df.dropna(axis=1, how='all')

    # Удаляем Unnamed столбцы
    unnamed_pattern = r'^Unnamed:|^unnamed'
    df = df.loc[:, ~df.columns.astype(str).str.contains(unnamed_pattern, na=False, regex=True)]

    # Удаляем столбцы с пустыми строками
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        if df[col].astype(str).str.strip().eq('').all():
            df = df.drop(columns=[col], errors='ignore')

    # Ограничиваем количество столбцов
    if len(df.columns) > CONFIG["MAX_COLUMNS"]:
        logger.warning(f"Превышено максимальное количество столбцов: {len(df.columns)}")
        df = df.iloc[:, :CONFIG["MAX_COLUMNS"]]

    # Удаляем полностью пустые строки
    df = df.dropna(how="all")

    return df


def find_fio_column(df: pd.DataFrame) -> Optional[str]:
    """Поиск столбца с ФИО"""
    if CONFIG["KEY_COLUMN"] in df.columns:
        return CONFIG["KEY_COLUMN"]

    fio_patterns_priority = [
        (r'фио\b', 1), (r'ф\.и\.о\.?', 2), (r'фам(илия)?\b', 3),
        (r'фамилия и.?о.?', 4), (r'full.?name', 5), (r'name', 6)
    ]

    found_columns = []
    for pattern, priority in fio_patterns_priority:
        matches = [
            col for col in df.columns
            if re.search(pattern, str(col), re.IGNORECASE)
        ]
        for match in matches:
            found_columns.append((match, priority))

    if found_columns:
        return min(found_columns, key=lambda x: x[1])[0]

    return None


def validate_excel_file(file_path: Path) -> Tuple[bool, str]:
    """Проверка валидности Excel файла"""
    try:
        if not file_path.exists():
            return False, "Файл не существует"

        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        if file_size_mb > CONFIG["MAX_FILE_SIZE_MB"]:
            return False, f"Файл слишком большой ({file_size_mb:.1f} MB)"

        if file_path.suffix.lower() not in CONFIG["ALLOWED_EXTENSIONS"]:
            return False, f"Неподдерживаемое расширение файла: {file_path.suffix}"

        if file_path.suffix.lower() == '.xlsx':
            with open(file_path, 'rb') as f:
                header = f.read(4)
                if header != b'PK\x03\x04':
                    return False, "Неверный формат Excel файла"

        return True, "OK"

    except Exception as e:
        return False, f"Ошибка проверки: {str(e)}"


def extract_zip_to_temp(zip_path: str) -> Optional[Path]:
    """Распаковка ZIP архива во временную директорию"""
    try:
        zip_file_path = Path(zip_path)

        if not zip_file_path.exists():
            raise FileNotFoundError(f"ZIP файл не найден: {zip_path}")

        if not zipfile.is_zipfile(zip_file_path):
            raise ValueError(f"Файл не является ZIP архивом: {zip_path}")

        # Создаем уникальную временную директорию
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_dir = Path(tempfile.gettempdir()) / f"excel_processor_{timestamp}"
        temp_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Распаковка ZIP архива в: {temp_dir}")

        # Распаковываем архив
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        logger.info(f"ZIP архив успешно распакован. Содержимое:")

        # Логируем содержимое распакованной директории
        for item in temp_dir.rglob("*"):
            if item.is_file():
                logger.info(f"  Файл: {item.relative_to(temp_dir)}")
            elif item.is_dir():
                logger.info(f"  Папка: {item.relative_to(temp_dir)}")

        return temp_dir

    except zipfile.BadZipFile:
        logger.error(f"Поврежденный ZIP архив: {zip_path}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при распаковке ZIP архива {zip_path}: {e}")
        raise


def cleanup_temp_dir(temp_dir: Path):
    """Очистка временной директории"""
    try:
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir)
            logger.info(f"Временная директория удалена: {temp_dir}")
    except Exception as e:
        logger.warning(f"Не удалось удалить временную директорию {temp_dir}: {e}")


# ==========================
# КЛАСС ДЛЯ ОБРАБОТКИ ФАЙЛОВ
# ==========================

class ExcelFileProcessor:
    """Класс для обработки Excel файлов"""

    def __init__(self, year_dir: Path, year: int, progress_callback=None, log_callback=None):
        self.year_dir = Path(year_dir)
        self.year = year
        self.month_pattern = re.compile("|".join(CONFIG["MONTHS"].keys()))
        self.progress_callback = progress_callback
        self.log_callback = log_callback
        self.processed_files = []

    def log_message(self, message: str, level: str = "info"):
        """Отправка сообщения в лог"""
        if self.log_callback:
            self.log_callback(message, level)
        else:
            print(f"{level.upper()}: {message}")

    def update_progress(self, value: int, max_value: int = 100, message: str = ""):
        """Обновление прогресса"""
        if self.progress_callback:
            self.progress_callback(value, max_value, message)

    def extract_month_from_filename(self, filename: str) -> Optional[Tuple[str, int]]:
        """Извлечение месяца из названия файла"""
        try:
            name_clean = Path(filename).stem.split('(')[0].strip().upper()
            match = self.month_pattern.search(name_clean)

            if match:
                month_name = match.group()
                month_num = CONFIG["MONTHS"][month_name]
                return month_name, month_num

            return None
        except Exception as e:
            self.log_message(f"Ошибка при извлечении месяца из {filename}: {e}", "error")
            return None

    def process_sheet(self, sheet_data: pd.DataFrame, day: int, month_num: int) -> Optional[pd.DataFrame]:
        """Обработка одного листа Excel"""
        try:
            date_str = datetime(self.year, month_num, day).strftime("%d.%m.%Y")
            df = sheet_data.copy()
            df = clean_dataframe(df)

            if df.empty:
                return None

            fio_column = find_fio_column(df)
            if not fio_column:
                self.log_message(f"Не найден столбец ФИО в листе дня {day}", "warning")
                return None

            df[fio_column] = (
                df[fio_column]
                .astype(str)
                .str.strip()
                .replace(['nan', 'NaN', 'None', 'null', 'NULL', ''], pd.NA)
            )

            # УДАЛЕНО: Вывод сообщений об удалении пустых строк
            df = df.dropna(subset=[fio_column])

            if len(df) == 0:
                return None

            birth_date_columns = [col for col in df.columns if 'рожд' in str(col).lower()]
            for col in birth_date_columns:
                try:
                    df[col] = pd.to_datetime(
                        df[col],
                        errors='coerce',
                        dayfirst=True
                    ).dt.strftime("%d.%m.%Y")
                except Exception:
                    pass

            df["Дата"] = date_str
            return df

        except Exception as e:
            self.log_message(f"Ошибка обработки листа дня {day}: {e}", "error")
            return None

    def process_month_file(self, file_path: Path) -> Optional[Tuple[pd.DataFrame, str]]:
        """Обработка одного файла месяца"""
        try:
            is_valid, message = validate_excel_file(file_path)
            if not is_valid:
                self.log_message(f"Пропущен файл {file_path.name}: {message}", "warning")
                return None

            month_info = self.extract_month_from_filename(file_path.name)
            if not month_info:
                self.log_message(f"Не удалось определить месяц из названия: {file_path.name}", "warning")
                return None

            month_name, month_num = month_info

            try:
                excel = pd.ExcelFile(file_path, engine='openpyxl')
            except Exception as e:
                self.log_message(f"Ошибка чтения Excel файла {file_path.name}: {e}", "error")
                return None

            monthly_data = []
            skipped_sheets = []

            for sheet_name in excel.sheet_names:
                if not sheet_name.isdigit():
                    skipped_sheets.append(sheet_name)
                    continue

                try:
                    day = int(sheet_name)
                    if not (1 <= day <= 31):
                        skipped_sheets.append(sheet_name)
                        continue
                except ValueError:
                    skipped_sheets.append(sheet_name)
                    continue

                try:
                    sheet_df = pd.read_excel(
                        excel,
                        sheet_name=sheet_name,
                        dtype=str,
                        na_values=['', ' ', 'nan', 'NaN', 'None', 'null']
                    )
                except Exception as e:
                    self.log_message(f"Ошибка чтения листа {sheet_name} в {file_path.name}: {e}", "warning")
                    skipped_sheets.append(sheet_name)
                    continue

                processed_df = self.process_sheet(sheet_df, day, month_num)
                if processed_df is not None:
                    monthly_data.append(processed_df)
                else:
                    skipped_sheets.append(sheet_name)

            if skipped_sheets:
                self.log_message(f"Пропущено листов в {file_path.name}: {len(skipped_sheets)}", "info")

            if monthly_data:
                result_df = pd.concat(monthly_data, ignore_index=True)
                self.log_message(f"Обработан файл {file_path.name}: {len(result_df)} строк", "info")
                return result_df, month_name
            else:
                self.log_message(f"Нет данных для обработки в файле {file_path.name}", "warning")
                return None

        except Exception as e:
            self.log_message(f"Критическая ошибка при обработке {file_path.name}: {e}", "error")
            return None

    def process_all_months(self) -> Dict[str, Path]:
        """Обработка всех файлов месяцев в директории"""
        all_files = []
        for ext in CONFIG["ALLOWED_EXTENSIONS"]:
            all_files.extend(list(self.year_dir.glob(f"*{ext}")))
            all_files.extend(list(self.year_dir.glob(f"*{ext.upper()}")))

        if not all_files:
            self.log_message(f"В папке {self.year_dir} не найдено Excel файлов", "warning")
            return {}

        month_to_file = {}
        for file_path in all_files:
            month_info = self.extract_month_from_filename(file_path.name)
            if not month_info:
                continue

            month_name, _ = month_info
            if month_name in month_to_file:
                existing_file = month_to_file[month_name]
                existing_ext = existing_file.suffix.lower()
                current_ext = file_path.suffix.lower()

                if (current_ext == '.xlsx' and existing_ext == '.xls') or \
                        (current_ext == existing_ext and
                         file_path.stat().st_mtime > existing_file.stat().st_mtime):
                    self.log_message(f"Выбран {file_path.name} вместо {existing_file.name} для месяца {month_name}",
                                     "info")
                    month_to_file[month_name] = file_path
            else:
                month_to_file[month_name] = file_path

        month_order = list(CONFIG["MONTHS"].keys())
        sorted_files = []
        for month in month_order:
            if month in month_to_file:
                sorted_files.append(month_to_file[month])

        if not sorted_files:
            self.log_message(f"Не найдено файлов с названиями месяцев", "warning")
            return {}

        output_dir = self.year_dir / str(self.year)
        output_dir.mkdir(exist_ok=True)

        results = {}
        processed_count = 0
        failed_count = 0

        for i, file_path in enumerate(sorted_files):
            self.update_progress(i + 1, len(sorted_files), f"Обработка: {file_path.name}")

            month_info = self.extract_month_from_filename(file_path.name)
            if not month_info:
                continue

            month_name, _ = month_info
            result = self.process_month_file(file_path)

            if result:
                result_df, processed_month_name = result

                if month_name != processed_month_name:
                    self.log_message(f"Несоответствие месяцев: {month_name} != {processed_month_name}", "warning")

                output_file = output_dir / f"Результат_{month_name}.xlsx"
                try:
                    result_df.to_excel(output_file, index=False, engine='openpyxl')
                    results[month_name] = output_file
                    processed_count += 1
                    self.log_message(f"✅ Сохранен: {output_file.name}", "success")
                except Exception as e:
                    self.log_message(f"Ошибка сохранения {output_file}: {e}", "error")
                    failed_count += 1
            else:
                failed_count += 1
                self.log_message(f"⚠️ Не удалось обработать: {file_path.name}", "warning")

        self.processed_files = list(results.values())

        if processed_count > 0:
            expected_months = 12
            if processed_count != expected_months:
                self.log_message(f"⚠️ Обработано {processed_count} месяцев вместо {expected_months}", "warning")

            self.log_message(f"""
            🎉 **Обработка завершена!**
            - ✅ Успешно обработано: **{processed_count}** файлов
            - ⚠️ Не обработано: **{failed_count}** файлов
            - 📁 Результаты сохранены в: `{output_dir}`
            """, "success")
        else:
            self.log_message("❌ Не удалось обработать ни одного файла", "error")

        return results

    def create_final_report(self) -> Optional[Path]:
        """Создание итогового отчета за год"""
        result_dir = self.year_dir / str(self.year)

        if not result_dir.exists():
            self.log_message(f"Папка с результатами не найдена: {result_dir}", "error")
            return None

        files = sorted(result_dir.glob("Результат_*.xlsx"))

        if not files:
            self.log_message(f"Нет файлов Результат_*.xlsx в папке: {result_dir}", "warning")
            return None

        self.log_message(f"Найдено {len(files)} файлов для объединения", "info")
        all_data = []

        for i, file in enumerate(files):
            self.update_progress(i + 1, len(files), f"Загрузка: {file.name}")
            try:
                df = pd.read_excel(file, engine='openpyxl')
                fio_col = find_fio_column(df)
                if fio_col and "Дата" in df.columns:
                    all_data.append(df)
                    self.log_message(f"✓ Загружен: {file.name} ({len(df)} строк)", "info")
                else:
                    self.log_message(f"⚠️ Пропущен {file.name}: отсутствуют необходимые колонки", "warning")
            except Exception as e:
                self.log_message(f"❌ Ошибка чтения {file.name}: {e}", "error")

        if not all_data:
            self.log_message("Не удалось загрузить данные из файлов", "error")
            return None

        try:
            self.update_progress(0, 0, "🔄 Объединение данных...")
            final_df = pd.concat(all_data, ignore_index=True, sort=False)

            if "Дата" in final_df.columns:
                final_df["Дата_сорт"] = pd.to_datetime(
                    final_df["Дата"],
                    format="%d.%m.%Y",
                    errors='coerce'
                )
                final_df = final_df.sort_values("Дата_сорт", na_position='first')
                final_df = final_df.drop(columns=["Дата_сорт"])

            output_file = result_dir / f"ИТОГ_{self.year}.xlsx"
            final_df.to_excel(output_file, index=False, engine='openpyxl')

            self.log_message(f"✅ Итоговый отчет создан: {output_file}", "success")
            return output_file

        except Exception as e:
            self.log_message(f"❌ Ошибка при объединении данных: {e}", "error")
            return None


# ==========================
# ПРИЛОЖЕНИЕ Tkinter
# ==========================

class ExcelProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("📊 Excel Processor Pro")
        self.root.geometry("1200x850")  # Увеличенная ширина
        self.root.minsize(1000, 700)  # Минимальные размеры

        # Настройка иконки
        try:
            self.root.iconbitmap(default="icon.ico")
        except:
            pass

        # Установка современного стиля
        self.setup_modern_style()

        self.base_dir = None
        self.temp_dir = None
        self.processor = None

        self.setup_ui()

        # Привязка события выбора года
        self.year_combo.bind("<<ComboboxSelected>>", self.on_year_selected)

        # Конфигурация колонок для адаптивности
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(0, weight=1)

    def setup_modern_style(self):
        """Настройка современного стиля"""
        style = ttk.Style()
        style.theme_use('clam')

        # Стиль для кнопок
        style.configure(
            "Primary.TButton",
            font=("Segoe UI", 10),
            background=COLORS["primary"],
            foreground="white",
            borderwidth=0,
            padding=10
        )

        style.map(
            "Primary.TButton",
            background=[('active', COLORS["accent"]), ('!disabled', COLORS["primary"])],
            foreground=[('!disabled', "white")]
        )

        style.configure(
            "Success.TButton",
            font=("Segoe UI", 10),
            background=COLORS["success"],
            foreground="white",
            borderwidth=0,
            padding=10
        )

        style.map(
            "Success.TButton",
            background=[('active', "#059669"), ('!disabled', COLORS["success"])],
            foreground=[('!disabled', "white")]
        )

        style.configure(
            "Secondary.TButton",
            font=("Segoe UI", 10),
            background=COLORS["light"],
            foreground=COLORS["dark"],
            borderwidth=1
        )

        # Стиль для полей ввода
        style.configure(
            "TEntry",
            fieldbackground="white",
            borderwidth=1,
            padding=5
        )

        # Стиль для комбобоксов
        style.configure(
            "TCombobox",
            fieldbackground="white",
            borderwidth=1,
            padding=5
        )

        # Стиль для фреймов
        style.configure(
            "Card.TLabelframe",
            background=COLORS["card"],
            borderwidth=1,
            padding=10
        )

        style.configure(
            "Card.TLabelframe.Label",
            font=("Segoe UI", 11, "bold"),
            foreground=COLORS["primary"],
            background=COLORS["card"]
        )

        # Стиль для прогресс-бара
        style.configure(
            "Custom.Horizontal.TProgressbar",
            thickness=20,
            troughcolor=COLORS["light"],
            background=COLORS["primary"],
            borderwidth=0
        )

    def setup_ui(self):
        """Настройка пользовательского интерфейса"""
        # Главный контейнер
        main_frame = tk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # Конфигурация адаптивной сетки
        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_rowconfigure(2, weight=1)  # Лог получает оставшееся пространство

        # Заголовок
        header_frame = tk.Frame(main_frame)
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        header_frame.grid_columnconfigure(0, weight=1)

        title_label = tk.Label(
            header_frame,
            text="📊 Excel Processor v3",
            font=("Segoe UI", 24, "bold"),
            fg=COLORS["primary"],
            bg=COLORS["bg"]
        )
        title_label.grid(row=0, column=0, sticky="w")

        # 1. Выбор источника данных
        source_frame = ttk.LabelFrame(
            main_frame,
            text="📁 Выбор источника данных",
            style="Card.TLabelframe",
            padding=15
        )
        source_frame.grid(row=1, column=0, sticky="ew", pady=(0, 15))
        source_frame.grid_columnconfigure(1, weight=1)  # Поле ввода получает оставшееся пространство

        # Режимы работы
        mode_frame = tk.Frame(source_frame)
        mode_frame.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 10))

        self.mode_var = tk.StringVar(value="local")

        tk.Radiobutton(
            mode_frame,
            text="🖥 Локальная директория",
            variable=self.mode_var,
            value="local",
            command=self.on_mode_change
        ).pack(side=tk.LEFT, padx=(0, 20))

        tk.Radiobutton(
            mode_frame,
            text="📦 ZIP архив",
            variable=self.mode_var,
            value="zip",
            command=self.on_mode_change
        ).pack(side=tk.LEFT)

        # Поле пути
        tk.Label(source_frame, text="Путь:", font=("Segoe UI", 10, "bold")).grid(
            row=1, column=0, sticky="w", padx=(0, 10), pady=(10, 5)
        )

        self.dir_var = tk.StringVar(value="")

        self.dir_entry = tk.Entry(
            source_frame,
            textvariable=self.dir_var,
            font=("Segoe UI", 10)
        )
        self.dir_entry.grid(row=1, column=1, sticky="ew", padx=(0, 10), pady=(10, 5))

        # Кнопки действий и статус в одной строке
        action_frame = tk.Frame(source_frame)
        action_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(5, 0))

        # Левая часть: кнопки
        button_frame = tk.Frame(action_frame)
        button_frame.pack(side=tk.LEFT, fill=tk.Y)

        self.browse_button = ttk.Button(
            button_frame,
            text="📂 Обзор директории",
            command=self.browse_directory,
            style="Secondary.TButton"
        )
        self.browse_button.pack(side=tk.LEFT, padx=(0, 10))

        self.zip_button = ttk.Button(
            button_frame,
            text="📥 Загрузить ZIP",
            command=self.load_zip,
            style="Secondary.TButton"
        )
        self.zip_button.pack(side=tk.LEFT)

        # Правая часть: статус загрузки (в той же строке)
        self.status_label = tk.Label(
            action_frame,
            text="",
            font=("Segoe UI", 9),
            foreground=COLORS["success"]
        )
        self.status_label.pack(side=tk.RIGHT, padx=(10, 0))

        # 2. Выбор года
        self.year_frame = ttk.LabelFrame(
            main_frame,
            text="📅 Выбор года",
            style="Card.TLabelframe",
            padding=15
        )
        self.year_frame.grid(row=2, column=0, sticky="ew", pady=(0, 15))
        self.year_frame.grid_columnconfigure(1, weight=1)  # Поле информации получает пространство

        year_selection_frame = tk.Frame(self.year_frame)
        year_selection_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        tk.Label(
            year_selection_frame,
            text="Год:",
            font=("Segoe UI", 10, "bold")
        ).pack(side=tk.LEFT, padx=(0, 10))

        self.year_combo = ttk.Combobox(
            year_selection_frame,
            state="readonly",
            font=("Segoe UI", 10),
            width=30
        )
        self.year_combo.pack(side=tk.LEFT)

        # Информация о выбранном годе
        self.year_info_text = scrolledtext.ScrolledText(
            self.year_frame,
            height=5,
            font=("Consolas", 9),
            bg=COLORS["light"],
            relief=tk.FLAT,
            wrap=tk.WORD
        )
        self.year_info_text.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 0))

        # 3. Обработка данных
        process_frame = ttk.LabelFrame(
            main_frame,
            text="⚙️ Обработка данных",
            style="Card.TLabelframe",
            padding=15
        )
        process_frame.grid(row=3, column=0, sticky="ew", pady=(0, 15))
        process_frame.grid_columnconfigure(0, weight=1)
        process_frame.grid_columnconfigure(1, weight=1)

        # Кнопки обработки в сетке
        self.process_button = ttk.Button(
            process_frame,
            text="📊 Подготовить файлы месяцев",
            command=self.process_months,
            style="Primary.TButton",
            state="disabled"
        )
        self.process_button.grid(row=0, column=0, padx=(0, 10), sticky="ew")

        self.final_button = ttk.Button(
            process_frame,
            text="📈 Собрать итоговый отчет",
            command=self.create_final_report,
            style="Success.TButton",
            state="disabled"
        )
        self.final_button.grid(row=0, column=1, sticky="ew")

        # Прогресс бар
        progress_frame = tk.Frame(main_frame)
        progress_frame.grid(row=4, column=0, sticky="ew", pady=(0, 15))
        progress_frame.grid_columnconfigure(0, weight=1)

        self.progress = ttk.Progressbar(
            progress_frame,
            mode='determinate',
            style="Custom.Horizontal.TProgressbar"
        )
        self.progress.grid(row=0, column=0, sticky="ew", pady=(0, 5))

        self.progress_label = ttk.Label(
            progress_frame,
            text="Готов к работе",
            font=("Segoe UI", 9),
            foreground=COLORS["secondary"]
        )
        self.progress_label.grid(row=1, column=0, sticky="w")

        # 4. Лог выполнения - СВОРАЧИВАЕМАЯ секция
        self.log_frame_visible = True

        log_header_frame = ttk.Frame(main_frame)
        log_header_frame.grid(row=5, column=0, sticky="ew", pady=(0, 5))
        log_header_frame.grid_columnconfigure(1, weight=1)

        # Кнопка для сворачивания/разворачивания
        self.toggle_log_btn = ttk.Button(
            log_header_frame,
            text="▼ Свернуть лог",  # Показываем "Свернуть" изначально
            command=self.toggle_log_frame,
            style="Secondary.TButton",
            width=15
        )
        self.toggle_log_btn.grid(row=0, column=0, sticky="w")

        # Заголовок секции логов
        log_title = ttk.Label(
            log_header_frame,
            text="📝 Лог выполнения",
            font=("Segoe UI", 11, "bold"),
            foreground=COLORS["primary"]
        )
        log_title.grid(row=0, column=1, sticky="w", padx=(10, 0))

        # Фрейм для содержимого лога (будет сворачиваться)
        self.log_content_frame = ttk.Frame(
            main_frame,
            style="Card.TLabelframe",
            padding=15
        )
        self.log_content_frame.grid(row=6, column=0, sticky="nsew", pady=(0, 10))

        # Конфигурация сетки для лога
        main_frame.grid_rowconfigure(6, weight=1)
        self.log_content_frame.grid_columnconfigure(0, weight=1)
        self.log_content_frame.grid_rowconfigure(1, weight=1)

        # Панель инструментов лога
        log_toolbar = ttk.Frame(self.log_content_frame)
        log_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 10))

        ttk.Button(
            log_toolbar,
            text="🗑️ Очистить лог",
            command=self.clear_log,
            style="Secondary.TButton"
        ).pack(side=tk.LEFT)

        ttk.Button(
            log_toolbar,
            text="💾 Сохранить лог",
            command=self.save_log,
            style="Secondary.TButton"
        ).pack(side=tk.LEFT, padx=(10, 0))

        ttk.Button(
            log_toolbar,
            text="📄 Открыть лог-файл",
            command=self.open_log_file,
            style="Secondary.TButton"
        ).pack(side=tk.LEFT, padx=(10, 0))

        # Текстовое поле лога
        self.log_text = scrolledtext.ScrolledText(
            self.log_content_frame,
            font=("Consolas", 9),
            bg="#1e293b",
            fg="#e2e8f0",
            insertbackground="white",
            wrap=tk.WORD
        )
        self.log_text.grid(row=1, column=0, sticky="nsew")

        # Настройка цветов для разных уровней лога
        self.log_text.tag_config("info", foreground="#94a3b8")
        self.log_text.tag_config("success", foreground="#10b981")
        self.log_text.tag_config("warning", foreground="#f59e0b")
        self.log_text.tag_config("error", foreground="#ef4444")

        # Статус бар
        self.status_bar = ttk.Frame(main_frame)
        self.status_bar.grid(row=7, column=0, sticky="ew", pady=(5, 0))

        self.status_var = tk.StringVar(value="✅ Готов к работе")
        status_label = ttk.Label(
            self.status_bar,
            textvariable=self.status_var,
            font=("Segoe UI", 9),
            foreground=COLORS["secondary"],
            relief=tk.SUNKEN,
            anchor=tk.W,
            padding=(10, 5)
        )
        status_label.pack(fill=tk.X)

        # Сворачиваем лог после полной инициализации окна
        self.root.after(100, self.initialize_log_state)  # Используем задержку

    def initialize_log_state(self):
        """Инициализация состояния лога (вызывается после полной загрузки UI)"""
        # Сворачиваем лог и обновляем состояние флага
        self.log_frame_visible = True  # Сейчас он видим
        self.toggle_log_frame()  # Сворачиваем

    def toggle_log_frame(self):
        """Свернуть/развернуть секцию логов"""
        if self.log_frame_visible:
            # Сворачиваем
            self.log_content_frame.grid_remove()  # Скрываем фрейм
            self.toggle_log_btn.config(text="▶ Развернуть лог")
            # Освобождаем место для других элементов
            self.root.grid_rowconfigure(6, weight=0)
            self.log_frame_visible = False
        else:
            # Разворачиваем
            self.log_content_frame.grid()  # Показываем фрейм
            self.toggle_log_btn.config(text="▼ Свернуть лог")
            # Возвращаем место для лога
            self.root.grid_rowconfigure(6, weight=1)
            self.log_frame_visible = True

    def on_mode_change(self):
        """Обработка смены режима"""
        mode = self.mode_var.get()

        if mode == "local":
            self.dir_entry.config(state="normal")
            self.browse_button.config(state="normal")
            self.zip_button.config(state="disabled")
            self.load_directory()
        else:
            self.dir_entry.config(state="disabled")
            self.browse_button.config(state="disabled")
            self.zip_button.config(state="normal")
            self.base_dir = None
            self.temp_dir = None
            self.update_year_list([])
            self.update_year_info("")
            self.status_label.config(text="")

    def browse_directory(self):
        """Выбор директории"""
        directory = filedialog.askdirectory(
            title="Выберите директорию с данными",
            initialdir=self.dir_var.get() if os.path.exists(self.dir_var.get()) else "."
        )
        if directory:
            self.dir_var.set(directory)
            self.load_directory()

    def load_zip(self):
        """Загрузка и распаковка ZIP архива"""
        # Очищаем предыдущую временную директорию
        if hasattr(self, 'temp_dir') and self.temp_dir and self.temp_dir.exists():
            cleanup_temp_dir(self.temp_dir)

        zip_path = filedialog.askopenfilename(
            title="Выберите ZIP архив",
            filetypes=[("ZIP архивы", "*.zip"), ("Все файлы", "*.*")],
            initialdir="."
        )

        if not zip_path:
            return

        try:
            self.log_message("📦 Начинаю распаковку ZIP архива...", "info")
            self.update_progress(0, 0, "Распаковка ZIP архива...")
            self.status_var.set("⏳ Распаковка ZIP архива...")

            # Распаковываем архив
            self.temp_dir = extract_zip_to_temp(zip_path)

            if self.temp_dir:
                # Автоматически выбираем распакованную директорию
                self.dir_var.set(str(self.temp_dir))

                # Автоматически переключаемся в режим локальной директории
                self.mode_var.set("local")
                self.on_mode_change()

                # Принудительно загружаем содержимое
                self.load_directory()

                # Дополнительное логирование содержимого
                self.log_zip_contents(self.temp_dir)

                self.log_message(f"✅ ZIP архив успешно распакован: {os.path.basename(zip_path)}", "success")
                self.status_label.config(text=f"✅ Архив распакован: {os.path.basename(zip_path)}")
                self.status_var.set("✅ ZIP архив распакован")

        except zipfile.BadZipFile:
            error_msg = f"❌ Ошибка: Файл не является валидным ZIP архивом: {zip_path}"
            self.log_message(error_msg, "error")
            messagebox.showerror("Ошибка", error_msg)
            self.status_var.set("❌ Ошибка распаковки ZIP")
        except Exception as e:
            error_msg = f"❌ Ошибка при распаковке ZIP архива: {str(e)}"
            self.log_message(error_msg, "error")
            messagebox.showerror("Ошибка", error_msg)
            self.status_var.set("❌ Ошибка распаковки ZIP")

    def log_zip_contents(self, temp_dir: Path):
        """Логирование содержимого распакованного ZIP архива"""
        try:
            self.log_message("📂 Содержимое распакованного архива:", "info")

            # Счетчики
            dir_count = 0
            file_count = 0
            year_folders = []

            # Рекурсивный обход
            for item in temp_dir.rglob("*"):
                if item.is_dir():
                    dir_count += 1
                    # Проверяем, является ли папка годом
                    if item.name.isdigit() and len(item.name) == 4 and 2000 <= int(item.name) <= 2100:
                        year_folders.append(item.relative_to(temp_dir))
                    if dir_count <= 10:  # Ограничиваем вывод
                        self.log_message(f"  📁 Папка: {item.relative_to(temp_dir)}", "info")
                elif item.is_file():
                    file_count += 1
                    if file_count <= 10:  # Ограничиваем вывод
                        self.log_message(f"  📄 Файл: {item.relative_to(temp_dir)}", "info")

            # Сводная информация
            self.log_message(f"📊 Итого в архиве:", "info")
            self.log_message(f"  • Папок: {dir_count}", "info")
            self.log_message(f"  • Файлов: {file_count}", "info")

            if year_folders:
                self.log_message(f"  📅 Найдено папок с годами:", "success")
                for year_folder in year_folders:
                    self.log_message(f"    • {year_folder}", "success")
            else:
                self.log_message(f"  ⚠️ Папки с годами не найдены (ожидается формат YYYY)", "warning")

        except Exception as e:
            self.log_message(f"❌ Ошибка при анализе содержимого: {e}", "error")

    def load_directory(self):
        """Загрузка содержимого директории и обновление списка годов"""
        directory = self.dir_var.get().strip()

        if not directory:
            self.base_dir = None
            self.update_year_list([])
            self.update_year_info("")
            return

        if not os.path.exists(directory):
            self.log_message(f"❌ Директория не существует: {directory}", "error")
            self.base_dir = None
            self.update_year_list([])
            self.update_year_info("")
            self.status_label.config(text="❌ Директория не существует")
            return

        try:
            self.base_dir = Path(directory)
            years = find_year_folders(self.base_dir)

            if not years:
                self.log_message(f"ℹ️ В директории не найдено папок с годами", "warning")
                self.update_year_list([])
                self.update_year_info(f"Директория: {directory}\n\nНе найдено папок с годами (формат: YYYY)")
                self.status_label.config(text="⚠️ Не найдено папок с годами")
            else:
                self.update_year_list(years)
                self.status_label.config(text=f"✅ Найдено годов: {len(years)}")

                # Автоматически обновляем информацию о выбранном годе
                self.on_year_selected()

        except Exception as e:
            self.log_message(f"❌ Ошибка загрузки директории: {e}", "error")
            self.base_dir = None
            self.update_year_list([])
            self.update_year_info("")
            self.status_label.config(text="❌ Ошибка загрузки")

    def update_year_list(self, years):
        """Обновление списка годов"""
        self.year_combo['values'] = years

        if years:
            self.year_combo.set(years[0])
            self.year_combo.config(state="readonly")
            self.process_button.config(state="normal")
            self.final_button.config(state="normal")

            # Автоматически вызываем обновление информации
            self.root.after(100, self.on_year_selected)
        else:
            self.year_combo.set("")
            self.year_combo.config(state="disabled")
            self.process_button.config(state="disabled")
            self.final_button.config(state="disabled")
            self.update_year_info("")

    def on_year_selected(self, event=None):
        """Обработка выбора года"""
        selected_year = self.year_combo.get()
        if not selected_year or not self.base_dir:
            self.update_year_info("")
            return

        # Полный путь к выбранному году
        if '\\' in selected_year or '/' in selected_year:
            # Если это относительный путь
            year_dir = self.base_dir / selected_year
        else:
            # Если это просто имя папки
            year_dir = self.base_dir / selected_year

        if not year_dir.exists():
            # Пробуем найти рекурсивно
            for found_dir in self.base_dir.rglob(selected_year):
                if found_dir.is_dir():
                    year_dir = found_dir
                    break

            if not year_dir.exists():
                self.update_year_info(f"Папка года не найдена: {selected_year}")
                return

        # Получаем информацию о содержимом папки года
        info = self.get_year_directory_info(year_dir)
        self.update_year_info(info)

    def get_year_directory_info(self, year_dir: Path) -> str:
        """Получение информации о содержимом папки года"""
        try:
            # Если year_dir - это строка с относительным путем, преобразуем
            if isinstance(year_dir, str):
                if self.base_dir:
                    year_dir = self.base_dir / year_dir
                else:
                    year_dir = Path(year_dir)

            if not year_dir.exists():
                return f"❌ Папка не найдена: {year_dir}"

            excel_files = []
            for ext in CONFIG["ALLOWED_EXTENSIONS"]:
                excel_files.extend(list(year_dir.glob(f"*{ext}")))
                excel_files.extend(list(year_dir.glob(f"*{ext.upper()}")))

            info = f"📂 Директория: {year_dir}\n"
            info += f"📅 Год: {year_dir.name}\n\n"

            if excel_files:
                info += f"📊 Найдено Excel файлов: {len(excel_files)}\n\n"

                # Группируем файлы по месяцам
                months_found = {}
                for file in excel_files:
                    month_info = self.extract_month_from_filename_ui(file.name)
                    if month_info:
                        month_name, _ = month_info
                        if month_name not in months_found:
                            months_found[month_name] = []
                        months_found[month_name].append(file.name)

                if months_found:
                    info += "📅 Определенные месяцы:\n"
                    for month_name in CONFIG["MONTHS"].keys():
                        if month_name in months_found:
                            files = months_found[month_name]
                            info += f"  ✅ {month_name}: {len(files)} файл(ов)\n"
                        else:
                            info += f"  ❌ {month_name}: не найден\n"

                    # Файлы без определенного месяца
                    other_files = [f for f in excel_files
                                   if not self.extract_month_from_filename_ui(f.name)]
                    if other_files:
                        info += f"\n📁 Прочие файлы ({len(other_files)}):\n"
                        for file in other_files[:3]:  # Показываем только первые 3
                            info += f"  • {file.name}\n"
                        if len(other_files) > 3:
                            info += f"  ... и еще {len(other_files) - 3} файл(ов)\n"
                else:
                    info += "⚠️ Не найдено файлов с названиями месяцев\n"
            else:
                info += "📭 Excel файлы не найдены\n"

            return info

        except Exception as e:
            return f"❌ Ошибка чтения директории: {str(e)}"

    def extract_month_from_filename_ui(self, filename: str) -> Optional[Tuple[str, int]]:
        """Извлечение месяца из названия файла (для UI)"""
        try:
            name_clean = Path(filename).stem.split('(')[0].strip().upper()
            month_pattern = re.compile("|".join(CONFIG["MONTHS"].keys()))
            match = month_pattern.search(name_clean)

            if match:
                month_name = match.group()
                month_num = CONFIG["MONTHS"][month_name]
                return month_name, month_num

            return None
        except:
            return None

    def update_year_info(self, text: str):
        """Обновление информации о выбранном годе"""
        self.year_info_text.config(state=tk.NORMAL)
        self.year_info_text.delete(1.0, tk.END)
        self.year_info_text.insert(1.0, text)
        self.year_info_text.config(state=tk.DISABLED)

    def process_months(self):
        """Обработка файлов месяцев в отдельном потоке"""
        if not self.base_dir or not self.year_combo.get():
            return

        year_dir = self.base_dir / self.year_combo.get()
        if not year_dir.exists():
            messagebox.showerror("Ошибка", f"Папка года не найдена:\n{year_dir}")
            return

        # Отключение кнопок на время обработки
        self.set_buttons_state("disabled")
        self.status_var.set("⏳ Обработка месяцев...")

        # Создание процессора
        self.processor = ExcelFileProcessor(
            year_dir,
            int(self.year_combo.get()),
            progress_callback=self.update_progress,
            log_callback=self.log_message
        )

        # Запуск в отдельном потоке
        thread = threading.Thread(target=self._process_months_thread)
        thread.daemon = True
        thread.start()

    def _process_months_thread(self):
        """Поток обработки месяцев"""
        try:
            self.processor.process_all_months()
        except Exception as e:
            self.log_message(f"❌ Критическая ошибка при обработке: {e}", "error")
        finally:
            # Включение кнопок после завершения
            self.root.after(0, lambda: self.set_buttons_state("normal"))
            self.root.after(0, lambda: self.status_var.set("✅ Обработка завершена"))

    def create_final_report(self):
        """Создание итогового отчета в отдельном потоке"""
        if not self.processor:
            year_dir = self.base_dir / self.year_combo.get()
            self.processor = ExcelFileProcessor(
                year_dir,
                int(self.year_combo.get()),
                progress_callback=self.update_progress,
                log_callback=self.log_message
            )

        # Отключение кнопок на время обработки
        self.set_buttons_state("disabled")
        self.status_var.set("⏳ Создание итогового отчета...")

        # Запуск в отдельном потоке
        thread = threading.Thread(target=self._create_final_report_thread)
        thread.daemon = True
        thread.start()

    def _create_final_report_thread(self):
        """Поток создания итогового отчета"""
        try:
            report_path = self.processor.create_final_report()
            if report_path:
                self.log_message(f"✅ Итоговый отчет создан: {report_path}", "success")

                # Показываем сообщение
                self.root.after(0, lambda: messagebox.showinfo(
                    "Готово",
                    f"✅ Итоговый отчет успешно создан!\n\n"
                    f"📁 Файл: {report_path.name}\n"
                    f"📍 Путь: {report_path.parent}"
                ))
            else:
                self.log_message("❌ Не удалось создать итоговый отчет", "error")
        except Exception as e:
            self.log_message(f"❌ Ошибка при создании отчета: {e}", "error")
        finally:
            # Включение кнопок после завершения
            self.root.after(0, lambda: self.set_buttons_state("normal"))
            self.root.after(0, lambda: self.status_var.set("✅ Готово"))

    def set_buttons_state(self, state):
        """Установка состояния кнопок"""
        self.process_button.config(state=state)
        self.final_button.config(state=state)
        self.browse_button.config(state=state if self.mode_var.get() == "local" else "disabled")
        self.zip_button.config(state=state if self.mode_var.get() == "zip" else "disabled")

    def update_progress(self, value: int, max_value: int = 100, message: str = ""):
        """Обновление прогресса"""
        self.root.after(0, lambda: self._update_progress_ui(value, max_value, message))

    def _update_progress_ui(self, value: int, max_value: int, message: str):
        """Обновление UI прогресса"""
        if max_value > 0:
            percentage = int((value / max_value) * 100)
            self.progress['value'] = percentage
        else:
            self.progress['value'] = 0

        self.progress_label.config(text=message)

    def log_message(self, message: str, level: str = "info"):
        """Запись сообщения в лог"""
        # Фильтрация ненужных сообщений об удалении строк
        if "Удалено" in message and "строк с пустыми ФИО" in message:
            return  # Не выводим эти сообщения

        self.root.after(0, lambda: self._log_message_ui(message, level))

    def _log_message_ui(self, message: str, level: str):
        """Запись сообщения в UI лога"""
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Добавляем отступы для многострочных сообщений
        lines = message.strip().split('\n')
        if len(lines) > 1:
            formatted_message = f"[{timestamp}] {lines[0]}\n"
            for line in lines[1:]:
                formatted_message += f"          {line}\n"
        else:
            formatted_message = f"[{timestamp}] {message}\n"

        self.log_text.insert(tk.END, formatted_message, level)
        self.log_text.see(tk.END)

        # Обновляем статус бар для важных сообщений
        if level in ["success", "error"]:
            clean_msg = message.split('\n')[0].strip('✅❌⚠️ ')
            self.status_var.set(f"{'✅' if level == 'success' else '❌'} {clean_msg}")

    def clear_log(self):
        """Очистка лога"""
        self.log_text.delete(1.0, tk.END)
        self.log_message("Лог очищен", "info")

    def save_log(self):
        """Сохранение лога в файл"""
        file_path = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Лог файлы", "*.log"), ("Текстовые файлы", "*.txt"), ("Все файлы", "*.*")]
        )

        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.log_text.get(1.0, tk.END))
                self.log_message(f"✅ Лог сохранен: {file_path}", "success")
            except Exception as e:
                self.log_message(f"❌ Ошибка сохранения лога: {e}", "error")

    def open_log_file(self):
        """Открытие файла лога"""
        file_path = "excel_processor.log"
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Создаем окно для просмотра лога
                log_window = tk.Toplevel(self.root)
                log_window.title("Лог файл - Excel Processor")
                log_window.geometry("900x600")
                log_window.configure(bg=COLORS["bg"])

                # Заголовок
                tk.Label(
                    log_window,
                    text="📝 Лог файл приложения",
                    font=("Segoe UI", 14, "bold"),
                    bg=COLORS["bg"],
                    fg=COLORS["primary"]
                ).pack(pady=10)

                # Текстовое поле
                text = scrolledtext.ScrolledText(
                    log_window,
                    font=("Consolas", 9),
                    bg="#1e293b",
                    fg="#e2e8f0",
                    wrap=tk.WORD
                )
                text.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
                text.insert(1.0, content)
                text.config(state=tk.DISABLED)

            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось открыть лог файл: {e}")
        else:
            messagebox.showinfo("Информация", "Лог файл не найден. Лог будет создан после первой операции.")

    def on_closing(self):
        """Обработка закрытия приложения"""
        # Очищаем временную директорию
        if hasattr(self, 'temp_dir') and self.temp_dir:
            cleanup_temp_dir(self.temp_dir)

        self.root.destroy()


# ==========================
# ЗАПУСК ПРИЛОЖЕНИЯ
# ==========================

def main():
    root = tk.Tk()
    app = ExcelProcessorApp(root)

    # Обработка закрытия окна
    root.protocol("WM_DELETE_WINDOW", app.on_closing)

    # Центрирование окна
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    root.mainloop()


if __name__ == "__main__":
    main()