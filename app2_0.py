"""
📊 Excel Processor Application
Обработка Excel-файлов с месячными данными и их консолидация в годовой отчет
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import warnings
import zipfile
import tempfile
import os
import logging
from typing import List, Optional, Tuple, Dict
from contextlib import contextmanager

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


# Инициализация логгера
def setup_logging() -> logging.Logger:
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('excel_processor.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()

# Игнорируем предупреждения
warnings.filterwarnings("ignore", message="Could not infer format")
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


# ==========================
# КЛАССЫ ИСКЛЮЧЕНИЙ
# ==========================

class DataValidationError(Exception):
    """Класс для ошибок валидации данных"""
    pass


class FileProcessingError(Exception):
    """Класс для ошибок обработки файлов"""
    pass


# ==========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================

@contextmanager
def st_progress_context(label: str, total: int = 100):
    """Контекстный менеджер для отображения прогресса"""
    progress_bar = st.progress(0, text=label)
    try:
        yield lambda current, text=None: progress_bar.progress(
            current / total if total > 0 else 0,
            text=text or label
        )
    finally:
        progress_bar.empty()


def validate_directory(base_dir_input: str) -> Path:
    """Валидация входной директории"""
    if not base_dir_input or not base_dir_input.strip():
        st.error("🚫 Путь к директории не может быть пустым")
        st.stop()

    base_dir = Path(base_dir_input.strip())

    if not base_dir.exists():
        st.error(f"📂 Директория не существует:\n{base_dir}")
        st.stop()

    if not base_dir.is_dir():
        st.error(f"❌ Указанный путь не является директорией:\n{base_dir}")
        st.stop()

    return base_dir


def extract_zip_to_temp(uploaded_zip) -> Optional[Path]:
    """Распаковка ZIP архива"""
    if uploaded_zip is None:
        return None

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / uploaded_zip.name

        try:
            # Сохраняем ZIP файл
            with open(zip_path, "wb") as f:
                f.write(uploaded_zip.getbuffer())

            # Проверяем валидность ZIP
            if not zipfile.is_zipfile(zip_path):
                st.error("❌ Загруженный файл не является валидным ZIP-архивом")
                return None

            # Распаковываем
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            logger.info(f"ZIP архив распакован: {uploaded_zip.name}")
            return Path(temp_dir)

        except zipfile.BadZipFile:
            st.error("❌ Ошибка: поврежденный ZIP-архив")
            return None
        except Exception as e:
            st.error(f"❌ Ошибка при распаковке ZIP: {e}")
            return None


def find_year_folders(base_dir: Path) -> List[str]:
    """Поиск папок с годами"""
    try:
        years = [
            p.name for p in base_dir.iterdir()
            if p.is_dir() and p.name.isdigit() and len(p.name) == 4
               and 2000 <= int(p.name) <= 2100  # Реалистичный диапазон лет
        ]

        if not years:
            st.warning("📂 Не найдено папок с годами (формат: YYYY)")
            st.info("**Пример правильной структуры:**\n"
                    "```\n"
                    "📁 Ваша_папка/\n"
                    "  ├── 📁 2024/\n"
                    "  ├── 📁 2025/\n"
                    "  └── 📁 2026/\n"
                    "```")
            st.stop()

        return sorted(years, reverse=True)  # Сначала новые года

    except Exception as e:
        st.error(f"❌ Ошибка при поиске папок с годами: {e}")
        st.stop()


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от пустых и ненужных столбцов"""
    if df.empty:
        return df

    # Сохраняем исходные колонки для логов
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

    # Логирование изменений
    if original_columns != len(df.columns):
        logger.info(f"Очистка DataFrame: {original_columns} -> {len(df.columns)} колонок")

    return df


def find_fio_column(df: pd.DataFrame) -> Optional[str]:
    """Поиск столбца с ФИО"""
    # Прямое совпадение
    if CONFIG["KEY_COLUMN"] in df.columns:
        return CONFIG["KEY_COLUMN"]

    # Поиск по различным вариантам написания (с приоритетом)
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
        # Возвращаем столбец с наивысшим приоритетом
        return min(found_columns, key=lambda x: x[1])[0]

    return None


def validate_excel_file(file_path: Path) -> Tuple[bool, str]:
    """Проверка валидности Excel файла"""
    try:
        # Проверка существования файла
        if not file_path.exists():
            return False, "Файл не существует"

        # Проверка размера файла
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        if file_size_mb > CONFIG["MAX_FILE_SIZE_MB"]:
            return False, f"Файл слишком большой ({file_size_mb:.1f} MB > {CONFIG['MAX_FILE_SIZE_MB']} MB)"

        # Проверка расширения
        if file_path.suffix.lower() not in CONFIG["ALLOWED_EXTENSIONS"]:
            return False, f"Неподдерживаемое расширение файла: {file_path.suffix}"

        # Быстрая проверка формата (только для .xlsx)
        if file_path.suffix.lower() == '.xlsx':
            with open(file_path, 'rb') as f:
                header = f.read(4)
                if header != b'PK\x03\x04':
                    return False, "Неверный формат Excel файла"

        return True, "OK"

    except Exception as e:
        return False, f"Ошибка проверки: {str(e)}"


# ==========================
# КЛАСС ДЛЯ ОБРАБОТКИ ФАЙЛОВ
# ==========================

class ExcelFileProcessor:
    """Класс для обработки Excel файлов"""

    def __init__(self, year_dir: Path, year: int):
        self.year_dir = Path(year_dir)
        self.year = year
        self.month_pattern = re.compile("|".join(CONFIG["MONTHS"].keys()))

        # Инициализация session state
        if 'processed_files' not in st.session_state:
            st.session_state.processed_files = []
        if 'final_report' not in st.session_state:
            st.session_state.final_report = None

    def extract_month_from_filename(self, filename: str) -> Optional[Tuple[str, int]]:
        """Извлечение месяца из названия файла"""
        try:
            # Очищаем имя файла
            name_clean = Path(filename).stem.split('(')[0].strip().upper()
            match = self.month_pattern.search(name_clean)

            if match:
                month_name = match.group()
                month_num = CONFIG["MONTHS"][month_name]
                return month_name, month_num

            return None
        except Exception as e:
            logger.error(f"Ошибка при извлечении месяца из {filename}: {e}")
            return None

    def process_sheet(self, sheet_data: pd.DataFrame, day: int, month_num: int) -> Optional[pd.DataFrame]:
        """Обработка одного листа Excel"""
        try:
            # Формируем дату
            date_str = datetime(self.year, month_num, day).strftime("%d.%m.%Y")

            # Клонируем данные для безопасности
            df = sheet_data.copy()

            # Очищаем DataFrame
            df = clean_dataframe(df)

            if df.empty:
                return None

            # Находим столбец ФИО
            fio_column = find_fio_column(df)
            if not fio_column:
                logger.warning(f"Не найден столбец ФИО в листе дня {day}")
                return None

            # Очистка и фильтрация ФИО
            df[fio_column] = (
                df[fio_column]
                .astype(str)
                .str.strip()
                .replace(['nan', 'NaN', 'None', 'null', 'NULL', ''], pd.NA)
            )

            # Удаляем строки с пустыми ФИО
            before_count = len(df)
            df = df.dropna(subset=[fio_column])
            after_count = len(df)

            if after_count == 0:
                return None

            if before_count != after_count:
                logger.info(f"Удалено {before_count - after_count} строк с пустыми ФИО")

            # Обработка даты рождения
            birth_date_columns = [col for col in df.columns if 'рожд' in str(col).lower()]
            for col in birth_date_columns:
                try:
                    df[col] = pd.to_datetime(
                        df[col],
                        errors='coerce',
                        dayfirst=True
                    ).dt.strftime("%d.%m.%Y")
                except Exception:
                    pass  # Игнорируем ошибки преобразования

            # Добавляем дату
            df["Дата"] = date_str

            return df

        except Exception as e:
            logger.error(f"Ошибка обработки листа дня {day}: {e}")
            return None

    def process_month_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Обработка одного файла месяца"""
        try:
            # Проверка файла
            is_valid, message = validate_excel_file(file_path)
            if not is_valid:
                logger.warning(f"Пропущен файл {file_path.name}: {message}")
                return None

            # Определяем месяц
            month_info = self.extract_month_from_filename(file_path.name)
            if not month_info:
                logger.warning(f"Не удалось определить месяц из названия: {file_path.name}")
                return None

            month_name, month_num = month_info

            # Читаем Excel файл
            try:
                excel = pd.ExcelFile(file_path, engine='openpyxl')
            except Exception as e:
                logger.error(f"Ошибка чтения Excel файла {file_path.name}: {e}")
                return None

            monthly_data = []
            skipped_sheets = []

            # Обрабатываем каждый лист
            for sheet_name in excel.sheet_names:
                # Проверяем, является ли имя листа числом (днем месяца)
                if not sheet_name.isdigit():
                    skipped_sheets.append(sheet_name)
                    continue

                try:
                    day = int(sheet_name)
                    # Проверяем валидность дня для данного месяца
                    if not (1 <= day <= 31):
                        skipped_sheets.append(sheet_name)
                        continue
                except ValueError:
                    skipped_sheets.append(sheet_name)
                    continue

                # Читаем данные листа
                try:
                    sheet_df = pd.read_excel(
                        excel,
                        sheet_name=sheet_name,
                        dtype=str,  # Читаем все как строки для сохранения форматов
                        na_values=['', ' ', 'nan', 'NaN', 'None', 'null']
                    )
                except Exception as e:
                    logger.warning(f"Ошибка чтения листа {sheet_name} в {file_path.name}: {e}")
                    skipped_sheets.append(sheet_name)
                    continue

                # Обрабатываем лист
                processed_df = self.process_sheet(sheet_df, day, month_num)
                if processed_df is not None:
                    monthly_data.append(processed_df)
                else:
                    skipped_sheets.append(sheet_name)

            # Логируем пропущенные листы
            if skipped_sheets:
                logger.info(f"Пропущено листов в {file_path.name}: {len(skipped_sheets)}")

            # Объединяем все данные за месяц
            if monthly_data:
                result_df = pd.concat(monthly_data, ignore_index=True)
                logger.info(f"Обработан файл {file_path.name}: {len(result_df)} строк")
                return result_df, month_name
            else:
                logger.warning(f"Нет данных для обработки в файле {file_path.name}")
                return None

        except Exception as e:
            logger.error(f"Критическая ошибка при обработке {file_path.name}: {e}")
            return None

    def process_all_months(self) -> Dict[str, Path]:
        """Обработка всех файлов месяцев в директории"""
        # Собираем все возможные файлы
        all_files = []
        for ext in CONFIG["ALLOWED_EXTENSIONS"]:
            all_files.extend(list(self.year_dir.glob(f"*{ext}")))
            all_files.extend(list(self.year_dir.glob(f"*{ext.upper()}")))

        if not all_files:
            st.warning(f"📭 В папке {self.year_dir} не найдено Excel файлов")
            return {}

        # Группируем файлы по месяцам (берем только один файл на месяц)
        month_to_file = {}
        skipped_files = []

        for file_path in all_files:
            # Извлекаем месяц из названия файла
            month_info = self.extract_month_from_filename(file_path.name)

            if not month_info:
                skipped_files.append(file_path.name)
                continue

            month_name, _ = month_info

            if month_name in month_to_file:
                existing_file = month_to_file[month_name]
                existing_ext = existing_file.suffix.lower()
                current_ext = file_path.suffix.lower()


                if (current_ext == '.xlsx' and existing_ext == '.xls') or \
                        (current_ext == existing_ext and
                         file_path.stat().st_mtime > existing_file.stat().st_mtime):
                    logger.info(f"Выбран {file_path.name} вместо {existing_file.name} для месяца {month_name}")
                    month_to_file[month_name] = file_path
            else:
                month_to_file[month_name] = file_path

        # Логируем пропущенные файлы
        if skipped_files:
            logger.info(f"Пропущено файлов (не удалось определить месяц): {len(skipped_files)}")
            if len(skipped_files) <= 10:
                for f in skipped_files:
                    logger.debug(f"  - {f}")

        # Сортируем месяцы в правильном порядке
        month_order = list(CONFIG["MONTHS"].keys())
        sorted_files = []
        for month in month_order:
            if month in month_to_file:
                sorted_files.append(month_to_file[month])

        if not sorted_files:
            st.warning(f"📭 Не найдено файлов с названиями месяцев")
            return {}

        # Создаем папку для результатов
        output_dir = self.year_dir / str(self.year)
        output_dir.mkdir(exist_ok=True)

        st.info(f"📁 Папка для результатов: `{output_dir}`")
        st.info(f"📊 Найдено файлов для обработки: {len(sorted_files)} из {len(all_files)}")

        # Статистика
        processed_count = 0
        failed_count = 0
        results = {}

        # Прогресс-бар
        with st_progress_context("📊 Обработка файлов...", len(sorted_files)) as update_progress:
            for i, file_path in enumerate(sorted_files):
                # Получаем название месяца для отображения
                month_info = self.extract_month_from_filename(file_path.name)
                if not month_info:
                    continue

                month_name, _ = month_info

                # Обновляем прогресс
                update_progress(i + 1, f"Обработка: {file_path.name}")

                # Обрабатываем файл
                result = self.process_month_file(file_path)

                if result:
                    result_df, processed_month_name = result

                    # Проверяем, что месяц совпадает
                    if month_name != processed_month_name:
                        logger.warning(f"Несоответствие месяцев: {month_name} != {processed_month_name}")

                    # Сохраняем результат
                    output_file = output_dir / f"Результат_{month_name}.xlsx"
                    try:
                        result_df.to_excel(output_file, index=False, engine='openpyxl')
                        results[month_name] = output_file
                        processed_count += 1

                        # Показываем мини-отчет
                        with st.expander(f"✅ {file_path.name} → {month_name}", expanded=False):
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Строк", len(result_df))
                            with col2:
                                st.metric("Столбцов", len(result_df.columns))
                            with col3:
                                if "Дата" in result_df.columns:
                                    dates = result_df["Дата"].unique()
                                    st.metric("Дней", len(dates))

                    except Exception as e:
                        logger.error(f"Ошибка сохранения {output_file}: {e}")
                        st.error(f"❌ Ошибка сохранения {file_path.name}")
                        failed_count += 1
                else:
                    failed_count += 1
                    st.warning(f"⚠️ Не удалось обработать: {file_path.name}")

        # Сохраняем статистику в session state
        st.session_state.processed_files = list(results.values())

        # Показываем итоговую статистику
        if processed_count > 0:
            # Проверяем, что обработано 12 месяцев
            expected_months = 12
            if processed_count != expected_months:
                st.warning(f"⚠️ Обработано {processed_count} месяцев вместо {expected_months}")

            st.success(f"""
            🎉 **Обработка завершена!**
            - ✅ Успешно обработано: **{processed_count}** файлов
            - ⚠️ Не обработано: **{failed_count}** файлов
            - 📁 Результаты сохранены в: `{output_dir}`
            """)

            # Показываем список созданных файлов в порядке месяцев
            with st.expander("📋 Список созданных файлов", expanded=False):
                month_order = list(CONFIG["MONTHS"].keys())
                for month_name in month_order:
                    if month_name in results:
                        file_path = results[month_name]
                        st.write(f"- **{month_name}**: `{file_path.name}`")

            # Показываем статистику по месяцам
            with st.expander("📊 Статистика по месяцам", expanded=False):
                stats_data = []
                for month_name, file_path in results.items():
                    try:
                        df = pd.read_excel(file_path, engine='openpyxl')
                        stats_data.append({
                            "Месяц": month_name,
                            "Строк": len(df),
                            "Столбцов": len(df.columns),
                            "Дней": df["Дата"].nunique() if "Дата" in df.columns else 0,
                            "Размер файла": f"{file_path.stat().st_size / 1024:.1f} KB"
                        })
                    except:
                        stats_data.append({
                            "Месяц": month_name,
                            "Строк": "Ошибка",
                            "Столбцов": "Ошибка",
                            "Дней": "Ошибка",
                            "Размер файла": "Ошибка"
                        })

                if stats_data:
                    stats_df = pd.DataFrame(stats_data)
                    st.dataframe(stats_df, use_container_width=True, hide_index=True)

        else:
            st.error("❌ Не удалось обработать ни одного файла")
            # Показываем список файлов, которые были найдены
            with st.expander("🔍 Найденные файлы", expanded=False):
                for file_path in sorted_files:
                    month_info = self.extract_month_from_filename(file_path.name)
                    if month_info:
                        month_name, _ = month_info
                        st.write(f"- `{file_path.name}` → {month_name}")
                    else:
                        st.write(f"- `{file_path.name}` (месяц не определен)")

        return results

    def create_final_report(self) -> Optional[Path]:
        """Создание итогового отчета за год"""
        # Проверяем наличие папки с результатами
        result_dir = self.year_dir / str(self.year)

        if not result_dir.exists():
            st.error(f"📂 Папка с результатами не найдена:\n`{result_dir}`")
            st.info("Сначала выполните обработку месяцев (кнопка 'Подготовить файлы месяцев')")
            return None

        # Ищем файлы результатов
        files = sorted(result_dir.glob("Результат_*.xlsx"))

        if not files:
            st.warning(f"📭 Нет файлов Результат_*.xlsx в папке:\n`{result_dir}`")
            return None

        st.info(f"🔍 Найдено {len(files)} файлов для объединения")

        all_data = []

        # Читаем и объединяем файлы
        with st_progress_context("📥 Загрузка файлов...", len(files)) as update_progress:
            for i, file in enumerate(files):
                try:
                    df = pd.read_excel(file, engine='openpyxl')

                    # Проверяем наличие необходимых колонок
                    fio_col = find_fio_column(df)
                    if fio_col and "Дата" in df.columns:
                        all_data.append(df)
                        st.write(f"✓ Загружен: `{file.name}` ({len(df)} строк)")
                    else:
                        st.warning(f"⚠️ Пропущен {file.name}: отсутствуют необходимые колонки")
                except Exception as e:
                    st.error(f"❌ Ошибка чтения {file.name}: {e}")

                update_progress(i + 1, f"Загружено {i + 1}/{len(files)} файлов")

        if not all_data:
            st.error("📭 Не удалось загрузить данные из файлов")
            return None

        # Объединяем все данные
        with st.spinner("🔄 Объединение данных..."):
            try:
                final_df = pd.concat(all_data, ignore_index=True, sort=False)

                # Сортируем по дате
                if "Дата" in final_df.columns:
                    # Создаем временную колонку для сортировки
                    final_df["Дата_сорт"] = pd.to_datetime(
                        final_df["Дата"],
                        format="%d.%m.%Y",
                        errors='coerce'
                    )
                    final_df = final_df.sort_values("Дата_сорт", na_position='first')
                    final_df = final_df.drop(columns=["Дата_сорт"])

                # Сохраняем итоговый файл
                output_file = result_dir / f"ИТОГ_{self.year}.xlsx"
                final_df.to_excel(output_file, index=False, engine='openpyxl')

                # Сохраняем в session state
                st.session_state.final_report = output_file

                return output_file

            except Exception as e:
                st.error(f"❌ Ошибка при объединении данных: {e}")
                logger.error(f"Ошибка создания финального отчета: {e}")
                return None

    def display_report_statistics(self, report_path: Path):
        """Отображение статистики отчета"""
        try:
            df = pd.read_excel(report_path, engine='openpyxl')

            st.success(f"""
            🎊 **Итоговый отчет создан!**
            - 📄 Файл: `{report_path.name}`
            - 📁 Путь: `{report_path.parent}`
            """)

            # Детальная статистика
            with st.expander("📊 Детальная статистика отчета", expanded=True):
                # Основные метрики
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("📈 Всего строк", f"{len(df):,}")

                with col2:
                    st.metric("📊 Столбцов", len(df.columns))

                with col3:
                    if "Дата" in df.columns:
                        unique_dates = df["Дата"].nunique()
                        st.metric("📅 Уникальных дат", unique_dates)
                    else:
                        st.metric("📅 Даты", "Нет данных")

                with col4:
                    fio_col = find_fio_column(df)
                    if fio_col:
                        unique_fio = df[fio_col].nunique()
                        st.metric("👥 Уникальных ФИО", unique_fio)
                    else:
                        st.metric("👥 ФИО", "Не найден")

                # Информация о периоде
                if "Дата" in df.columns:
                    st.subheader("📅 Период данных")
                    dates = pd.to_datetime(df["Дата"], format="%d.%m.%Y", errors='coerce')
                    valid_dates = dates.dropna()

                    if not valid_dates.empty:
                        col_start, col_end, col_days = st.columns(3)

                        with col_start:
                            st.metric(
                                "Начало",
                                valid_dates.min().strftime("%d.%m.%Y")
                            )

                        with col_end:
                            st.metric(
                                "Конец",
                                valid_dates.max().strftime("%d.%m.%Y")
                            )

                        with col_days:
                            total_days = (valid_dates.max() - valid_dates.min()).days + 1
                            st.metric("Всего дней", total_days)

                # Столбцы с количеством заполненных значений
                st.subheader("📋 Заполненность столбцов")
                completeness_data = []
                for col in df.columns:
                    non_null = df[col].notna().sum()
                    total = len(df)
                    percentage = (non_null / total * 100) if total > 0 else 0
                    completeness_data.append({
                        "Столбец": col,
                        "Заполнено": non_null,
                        "Всего": total,
                        "%": f"{percentage:.1f}%"
                    })

                completeness_df = pd.DataFrame(completeness_data)
                st.dataframe(
                    completeness_df,
                    use_container_width=True,
                    hide_index=True
                )

            # Кнопка скачивания
            with open(report_path, "rb") as f:
                st.download_button(
                    label="📥 Скачать итоговый файл",
                    data=f,
                    file_name=report_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True
                )

        except Exception as e:
            st.error(f"❌ Ошибка при отображении статистики: {e}")


# ==========================
# ОСНОВНОЕ ПРИЛОЖЕНИЕ
# ==========================

def main():
    """Основная функция приложения"""

    # Настройка страницы
    st.set_page_config(
        page_title="📊 Обработка Excel-файлов (Месяцы → Год)",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Заголовок
    st.title("📊 Обработка Excel-файлов (Месяцы → Год)")
    st.markdown("---")

    # ==========================
    # БОКОВАЯ ПАНЕЛЬ
    # ==========================
    with st.sidebar:
        st.header("⚙️ Настройки")

        # Выбор режима загрузки
        mode = st.radio(
            "**Выберите источник данных:**",
            ["🖥 Локальная директория", "☁️ Загрузка ZIP-архива"],
            index=0
        )

        st.markdown("---")

        # Информация о приложении
        with st.expander("ℹ️ О приложении"):
            st.markdown("""
            **Excel Processor v2.0**

            *Функции:*
            - 📁 Обработка месячных Excel файлов
            - 🔄 Объединение данных по дням
            - 📊 Создание годовых отчетов
            - 📤 Поддержка ZIP архивов

            *Формат файлов:*
            - Название файла должно содержать месяц
            - Листы должны быть названы числами (дни месяца)
            - Поддерживаются .xlsx и .xls форматы
            """)

        # Контакты/помощь
        with st.expander("🆘 Помощь"):
            st.markdown("""
            **Проблемы?**

            1. Проверьте структуру директории
            2. Убедитесь в правильности имен файлов
            3. Проверьте формат Excel файлов

            Для дополнительной помощи:
            - Проверьте логи в `excel_processor.log`
            - Убедитесь, что файлы не повреждены
            """)

    # ==========================
    # ВЫБОР ИСТОЧНИКА ДАННЫХ
    # ==========================
    st.header("1. Выбор источника данных")

    base_dir = None
    temp_dir = None

    if mode == "🖥 Локальная директория":
        col1, col2 = st.columns([3, 1])

        with col1:
            base_dir_input = st.text_input(
                "**Путь к директории с годами:**",
                value=r"C:\Users\isaev\Downloads\Профотбор\Реестр профотбор",
                help="Путь должен содержать папки с годами (например: 2024, 2025)"
            )

        with col2:
            st.markdown("###")
            if st.button("🔍 Проверить", use_container_width=True):
                if base_dir_input:
                    test_dir = Path(base_dir_input.strip())
                    if test_dir.exists():
                        st.success("✅ Директория существует")
                    else:
                        st.error("❌ Директория не найдена")

        if base_dir_input:
            base_dir = validate_directory(base_dir_input)

    else:  # ZIP архив
        uploaded_zip = st.file_uploader(
            "**Загрузите ZIP-архив с папками годов**",
            type=["zip"],
            help="Архив должен содержать папки с годами (например: 2024/, 2025/)"
        )

        if uploaded_zip:
            with st.spinner("📦 Распаковка архива..."):
                temp_dir = extract_zip_to_temp(uploaded_zip)

            if temp_dir:
                base_dir = temp_dir
                st.success(f"✅ Архив распакован: `{uploaded_zip.name}`")

    # Если источник не выбран, останавливаем выполнение
    if base_dir is None:
        st.info("👆 Выберите источник данных для продолжения")
        return

    # ==========================
    # ВЫБОР ГОДА
    # ==========================
    st.header("2. Выбор года")

    try:
        years = find_year_folders(base_dir)
    except Exception as e:
        st.error(f"❌ Ошибка при получении списка годов: {e}")
        return

    # Отображение найденных годов
    col1, col2 = st.columns([2, 1])

    with col1:
        year_selected = st.selectbox(
            "**Выберите год для обработки:**",
            years,
            index=0
        )

    with col2:
        st.metric("📅 Найдено годов", len(years))

    # Информация о выбранном годе
    YEAR_DIR = base_dir / year_selected

    if not YEAR_DIR.exists():
        st.error(f"❌ Папка года не найдена: `{YEAR_DIR}`")
        return

    with st.expander(f"📂 Содержимое папки {year_selected}", expanded=False):
        try:
            # Список Excel файлов
            excel_files = []
            for ext in CONFIG["ALLOWED_EXTENSIONS"]:
                excel_files.extend(list(YEAR_DIR.glob(f"*{ext}")))

            if excel_files:
                st.write(f"**Найдено Excel файлов:** {len(excel_files)}")

                # Таблица с файлами
                files_data = []
                for file in sorted(excel_files)[:50]:  # Ограничиваем вывод
                    files_data.append({
                        "Файл": file.name,
                        "Размер": f"{file.stat().st_size / 1024:.1f} KB",
                        "Дата изменения": datetime.fromtimestamp(file.stat().st_mtime).strftime("%d.%m.%Y %H:%M")
                    })

                if files_data:
                    st.dataframe(
                        pd.DataFrame(files_data),
                        use_container_width=True,
                        hide_index=True
                    )

                    if len(excel_files) > 50:
                        st.info(f"... и еще {len(excel_files) - 50} файлов")
            else:
                st.warning("📭 В папке не найдено Excel файлов")

        except Exception as e:
            st.error(f"❌ Ошибка при чтении содержимого папки: {e}")

    # ==========================
    # ОБРАБОТКА ДАННЫХ
    # ==========================
    st.header("3. Обработка данных")
    st.markdown("---")

    # Создаем экземпляр процессора
    try:
        processor = ExcelFileProcessor(YEAR_DIR, int(year_selected))
    except ValueError:
        st.error("❌ Некорректный год!")
        return

    # Кнопки обработки
    col1, col2 = st.columns(2)

    with col1:
        if st.button(
                "📁 Подготовить файлы месяцев",
                type="primary",
                use_container_width=True,
                help="Обработка всех Excel файлов в папке года"
        ):
            with st.spinner("🔄 Запуск обработки месяцев..."):
                processor.process_all_months()

    with col2:
        if st.button(
                "📊 Собрать итоговый файл за год",
                type="secondary",
                use_container_width=True,
                help="Объединение обработанных файлов в единый отчет"
        ):
            with st.spinner("🔄 Создание итогового отчета..."):
                report_path = processor.create_final_report()

                if report_path:
                    processor.display_report_statistics(report_path)

    # ==========================
    # ИНСТРУКЦИЯ
    # ==========================
    st.markdown("---")
    st.header("📘 Инструкция по использованию")

    with st.expander("Подробная инструкция", expanded=False):
        st.markdown("""
        ### 🚀 Быстрый старт

        1. **Выберите источник данных**
           - 🖥 *Локальная директория*: укажите путь к папке с годами
           - ☁️ *ZIP архив*: загрузите архив с папками годов

        2. **Выберите год**
           - Приложение автоматически найдет все папки с годами
           - Выберите нужный год из списка

        3. **Обработка данных**
           - 📁 **Подготовить файлы месяцев**: 
             - Обрабатывает все Excel файлы в папке года
             - Определяет месяц из названия файла
             - Объединяет данные со всех листов (по дням)
             - Сохраняет по одному файлу на каждый месяц
           - 📊 **Собрать итоговый файл за год**:
             - Объединяет все файлы месяцев
             - Сортирует данные по дате
             - Создает итоговый отчет за год

        ### 📁 Требования к структуре данных

        ```
        Основная_папка/
        ├── 📁 2024/                    # Папка с годом
        │   ├── 📄 Январь_2024.xlsx    # Файл содержит листы: 1, 2, 3, ... 31
        │   ├── 📄 Февраль_2024.xlsx
        │   └── ... (остальные месяцы)
        ├── 📁 2025/
        └── ...
        ```

        ### ⚠️ Важные моменты

        - **Названия файлов** должны содержать название месяца (ЯНВАРЬ, ФЕВРАЛЬ и т.д.)
        - **Листы в файлах** должны быть названы числами (1, 2, 3, ..., 31)
        - **Столбец ФИО** может называться: "ФИО", "фио", "Ф.И.О.", "Фамилия"
        - **Максимальный размер файла**: 50 MB
        - **Поддерживаемые форматы**: .xlsx, .xls

        ### 🔧 Дополнительные функции

        - **Логирование**: все действия записываются в `excel_processor.log`
        - **Валидация**: проверка файлов перед обработкой
        - **Кэширование**: быстрая повторная обработка
        - **Статистика**: детальная информация о результатах

        ### 🆘 Поиск и устранение неисправностей

        1. **"Не найдено папок с годами"**
           - Проверьте, что в указанной директории есть папки с названиями годов (2024, 2025 и т.д.)
           - Убедитесь, что у вас есть права на чтение директории

        2. **"Не удалось определить месяц из названия файла"**
           - Убедитесь, что в названии файла есть русское название месяца
           - Пример правильных названий: "Январь_2024.xlsx", "Отчет_за_ФЕВРАЛЬ.xls"

        3. **"Ошибка чтения Excel файла"**
           - Убедитесь, что файл не поврежден
           - Попробуйте открыть файл в Excel
           - Проверьте, что файл не защищен паролем

        4. **"Не найден столбец ФИО"**
           - Проверьте наличие столбца с ФИО в данных
           - Столбец может называться: "ФИО", "фио", "Ф.И.О.", "Фамилия", "ФИО сотрудника"
        """)

    # ==========================
    # ДЕБАГ ИНФОРМАЦИЯ
    # ==========================
    if st.sidebar.checkbox("🐛 Отладочная информация", value=False):
        st.sidebar.markdown("---")
        st.sidebar.subheader("Отладочная информация")

        st.sidebar.write("**Текущие параметры:**")
        st.sidebar.json({
            "base_dir": str(base_dir) if base_dir else None,
            "year_selected": year_selected,
            "year_dir": str(YEAR_DIR) if YEAR_DIR else None,
            "year_dir_exists": YEAR_DIR.exists() if YEAR_DIR else False,
            "processed_files_count": len(st.session_state.get('processed_files', [])),
            "final_report_exists": st.session_state.get('final_report') is not None
        })

        if YEAR_DIR and YEAR_DIR.exists():
            st.sidebar.write("**Содержимое YEAR_DIR:**")
            try:
                items = list(YEAR_DIR.iterdir())
                for item in items[:10]:  # Ограничиваем вывод
                    st.sidebar.write(f"- {item.name} ({'📁' if item.is_dir() else '📄'})")
                if len(items) > 10:
                    st.sidebar.write(f"... и еще {len(items) - 10} элементов")
            except Exception as e:
                st.sidebar.error(f"Ошибка чтения: {e}")

    # ==========================
    # ФУТЕР
    # ==========================
    st.markdown("---")
    st.caption(
        "📊 Excel Processor v2.0 | "
        "Обработка месячных данных в годовые отчеты | "
        f"© {datetime.now().year}"
    )

    # Очистка временных файлов (если использовался ZIP)
    if temp_dir and temp_dir.exists():
        try:
            import shutil
            shutil.rmtree(temp_dir)
            logger.info("Временные файлы очищены")
        except Exception as e:
            logger.warning(f"Не удалось очистить временные файлы: {e}")


# ==========================
# ЗАПУСК ПРИЛОЖЕНИЯ
# ==========================

if __name__ == "__main__":
    main()