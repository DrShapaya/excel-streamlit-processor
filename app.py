import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import warnings
import zipfile
import tempfile
import os


st.set_page_config(page_title="Excel Processor", layout="wide")
st.title("📊 Обработка Excel-файлов (Месяцы → Год)")

# ==========================
# ВЫБОР БАЗОВОЙ ДИРЕКТОРИИ
# ==========================
st.subheader("1. Выбор директории")

st.subheader("1. Источник данных")

mode = st.radio(
    "Выберите способ загрузки данных",
    ["🖥 Локальная директория", "☁️ Загрузка ZIP-архива"]
)

base_dir = None
temp_dir = None

if mode == "🖥 Локальная директория":
    base_dir_input = st.text_input(
        "Введите путь к основной директории с годами",
        r"C:\Users\isaev\Downloads\Профотбор\Реестр профотбор",
        help="Путь должен содержать папки с годами (например: 2024, 2025)"
    )

    if not base_dir_input:
        st.info("Введите путь к директории")
        st.stop()

    base_dir = Path(base_dir_input)

    if not base_dir.exists():
        st.error("Указанная директория не существует!")
        st.stop()

else:
    uploaded_zip = st.file_uploader(
        "Загрузите ZIP-архив с папками годов (например: 2024/2025)",
        type=["zip"]
    )

    if not uploaded_zip:
        st.info("Загрузите ZIP-архив для продолжения")
        st.stop()

    temp_dir = Path(tempfile.mkdtemp())
    zip_path = temp_dir / uploaded_zip.name

    with open(zip_path, "wb") as f:
        f.write(uploaded_zip.read())

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
    except zipfile.BadZipFile:
        st.error("Ошибка: загружен невалидный ZIP-архив")
        st.stop()

    base_dir = temp_dir
    st.success("ZIP-архив успешно загружен и распакован")

KEY_COLUMN = "ФИО"

MONTHS = {
    "ЯНВАРЬ": 1, "ФЕВРАЛЬ": 2, "МАРТ": 3, "АПРЕЛЬ": 4,
    "МАЙ": 5, "ИЮНЬ": 6, "ИЮЛЬ": 7, "АВГУСТ": 8,
    "СЕНТЯБРЬ": 9, "ОКТЯБРЬ": 10, "НОЯБРЬ": 11, "ДЕКАБРЬ": 12
}
MONTH_PATTERN = re.compile("|".join(MONTHS.keys()))

# Инициализируем переменные
years = []
YEAR_DIR = None

# Проверяем, определена ли базовая директория
if base_dir is None:
    st.info("Пожалуйста, выберите источник данных")
    st.stop()

if not base_dir.exists():
    st.error(f"Директория {base_dir} не существует!")
    st.stop()

# Ищем папки с годами
years = [p.name for p in base_dir.iterdir()
         if p.is_dir() and p.name.isdigit() and len(p.name) == 4]

if not years:
    st.warning("В указанной директории не найдено папок с годами (формат: YYYY)")
    st.info("Проверьте, что директория содержит папки с названиями годов, например: 2024, 2025")
    st.stop()

st.success(f"Найдено {len(years)} года(ов): {', '.join(sorted(years))}")

# Выбор года
year_selected = st.selectbox("Выберите год для обработки", sorted(years))

# Создаем путь к выбранной папке года
YEAR_DIR = base_dir / year_selected

if not YEAR_DIR.exists():
    st.error(f"Папка {YEAR_DIR} не найдена!")
    st.stop()

# ==========================
# ИНФОРМАЦИЯ О ВЫБРАННОЙ ДИРЕКТОРИИ
# ==========================
st.subheader("2. Информация о выбранном годе")
st.write(f"**Директория года:** {YEAR_DIR}")

# Проверяем файлы в директории
excel_files = list(YEAR_DIR.glob("*.xlsx"))
st.write(f"**Найдено Excel-файлов:** {len(excel_files)}")
if excel_files:
    st.write("**Список файлов:**")
    for file in excel_files[:10]:  # Показываем первые 10 файлов
        st.write(f"- {file.name}")
    if len(excel_files) > 10:
        st.write(f"... и еще {len(excel_files) - 10} файлов")

# ==========================
# КНОПКА 1 — МЕСЯЦЫ
# ==========================
st.subheader("3. Обработка")
col1, col2 = st.columns(2)

with col1:
    if st.button("📁 Подготовить файлы месяцев", type="primary", use_container_width=True):
        if not excel_files:
            st.warning("В папке года нет Excel-файлов для обработки")
            st.stop()

        with st.spinner("Обработка файлов месяцев..."):
            warnings.filterwarnings("ignore", message="Could not infer format")

            try:
                year = int(year_selected)
            except ValueError:
                st.error("Некорректный год!")
                st.stop()

            # Создаем подпапку с названием года
            output_dir = YEAR_DIR / year_selected
            output_dir.mkdir(exist_ok=True)

            st.info(f"Создана папка для результатов: {output_dir}")

            progress_bar = st.progress(0)
            status_text = st.empty()

            processed_files = 0
            skipped_files = 0

            for i, file_path in enumerate(excel_files):
                # Обновляем статус
                status_text.text(f"Обработка {i + 1}/{len(excel_files)}: {file_path.name}")

                # Очищаем имя файла для поиска месяца
                name_clean = file_path.stem.split('(')[0].strip().upper()
                match = MONTH_PATTERN.search(name_clean)

                if not match:
                    skipped_files += 1
                    continue

                month_name = match.group()
                month = MONTHS[month_name]

                # Открываем Excel файл
                try:
                    excel = pd.ExcelFile(file_path)
                except Exception as e:
                    st.warning(f"Ошибка чтения файла {file_path.name}: {e}")
                    skipped_files += 1
                    continue

                dfs = []
                skipped_sheets = []

                for sheet in excel.sheet_names:
                    # Пропускаем нечисловые листы
                    if not sheet.isdigit():
                        skipped_sheets.append(sheet)
                        continue

                    try:
                        day = int(sheet)
                    except ValueError:
                        skipped_sheets.append(sheet)
                        continue

                    # Формируем дату
                    try:
                        date_value = datetime(year, month, day).strftime("%d.%m.%Y")
                    except ValueError:
                        skipped_sheets.append(sheet)
                        continue

                    # Читаем данные листа
                    try:
                        df = pd.read_excel(excel, sheet_name=sheet)
                    except Exception:
                        skipped_sheets.append(sheet)
                        continue

                    # Удаляем полностью пустые столбцы
                    df = df.dropna(axis=1, how='all')

                    # Удаляем столбцы с названиями типа "Unnamed"
                    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]

                    # Еще раз удаляем полностью пустые столбцы
                    df = df.dropna(axis=1, how='all')

                    # Удаляем столбцы, где все значения - пустые строки
                    for col in df.columns:
                        if df[col].dtype == object:
                            if df[col].astype(str).str.strip().eq('').all():
                                df = df.drop(columns=[col], errors='ignore')

                    # Ограничение на максимальное количество столбцов
                    MAX_COLUMNS = 50  # Реалистичное количество для таких данных
                    if len(df.columns) > MAX_COLUMNS:
                        # Оставляем только первые MAX_COLUMNS столбцов
                        df = df.iloc[:, :MAX_COLUMNS]

                    # Удаляем полностью пустые строки
                    df = df.dropna(how="all")

                    if df.empty:
                        continue

                    # Обработка ключевого столбца ФИО
                    if KEY_COLUMN in df.columns:
                        df[KEY_COLUMN] = df[KEY_COLUMN].astype(str).str.strip()
                        df = df[~df[KEY_COLUMN].isin(['', 'nan', 'NaN', 'None'])]
                    else:
                        # Поиск похожих столбцов
                        possible_fio_columns = [col for col in df.columns
                                                if 'фио' in str(col).lower() or 'ф.и.о' in str(col).lower()]
                        if possible_fio_columns:
                            KEY_COLUMN_ACTUAL = possible_fio_columns[0]
                            df[KEY_COLUMN_ACTUAL] = df[KEY_COLUMN_ACTUAL].astype(str).str.strip()
                            df = df[~df[KEY_COLUMN_ACTUAL].isin(['', 'nan', 'NaN', 'None'])]
                        else:
                            continue

                    # Обработка даты рождения
                    if "Год рождения" in df.columns:
                        try:
                            df["Год рождения"] = pd.to_datetime(
                                df["Год рождения"],
                                errors="coerce",
                                dayfirst=True
                            ).dt.strftime("%d.%m.%Y")
                        except Exception:
                            pass

                    # Добавляем дату
                    df["Дата"] = date_value
                    dfs.append(df)

                # Сохранение результата по месяцу
                if dfs:
                    result_df = pd.concat(dfs, ignore_index=True)
                    out_file = output_dir / f"Результат_{month_name}.xlsx"

                    try:
                        result_df.to_excel(out_file, index=False)
                        processed_files += 1
                        st.success(f"✅ Обработан: {file_path.name} → {out_file.name}")
                    except Exception as e:
                        st.error(f"❌ Ошибка сохранения {out_file.name}: {e}")

                # Обновляем прогресс
                progress_bar.progress((i + 1) / len(excel_files))

            # Очищаем статус
            status_text.empty()
            progress_bar.empty()

            st.success(f"""
            ✅ Обработка завершена!
            - Обработано файлов: {processed_files}
            - Пропущено файлов: {skipped_files}
            - Результаты сохранены в: {output_dir}
            """)

with col2:
    if st.button("📊 Собрать итоговый файл за год", type="secondary", use_container_width=True):
        # Проверяем наличие папки с результатами
        result_dir = YEAR_DIR / year_selected

        if not result_dir.exists():
            st.error(f"Папка с результатами не найдена: {result_dir}")
            st.info("Сначала выполните обработку месяцев (кнопка слева)")
            st.stop()

        # Ищем файлы результатов
        files = sorted(result_dir.glob("Результат_*.xlsx"))

        if not files:
            st.warning("Нет файлов Результат_*.xlsx для объединения")
            st.stop()

        st.info(f"Найдено файлов для объединения: {len(files)}")

        with st.spinner("Объединение файлов..."):
            dfs = []
            progress_bar = st.progress(0)

            for i, file in enumerate(files):
                try:
                    df = pd.read_excel(file)
                    dfs.append(df)
                    st.write(f"✓ Загружен: {file.name} ({len(df)} строк)")
                except Exception as e:
                    st.warning(f"⚠ Ошибка чтения {file.name}: {e}")

                progress_bar.progress((i + 1) / len(files))

            if not dfs:
                st.error("Не удалось загрузить ни один файл")
                st.stop()

            # Объединяем все данные
            result_df = pd.concat(dfs, ignore_index=True)

            # Сортировка по дате
            if "Дата" in result_df.columns:
                try:
                    result_df["Дата"] = pd.to_datetime(
                        result_df["Дата"],
                        format="%d.%m.%Y",
                        errors="coerce"
                    )
                    result_df = result_df.sort_values("Дата")
                    result_df["Дата"] = result_df["Дата"].dt.strftime("%d.%m.%Y")
                except Exception as e:
                    st.warning(f"⚠ Ошибка сортировки по дате: {e}")

            # Сохраняем итоговый файл
            output_file = result_dir / f"ИТОГ_{year_selected}.xlsx"
            try:
                result_df.to_excel(output_file, index=False)
                progress_bar.empty()

                # Показываем статистику
                st.success(f"""
                ✅ Итоговый файл создан!
                - Файл: {output_file}
                - Всего строк: {len(result_df):,}
                - Всего столбцов: {len(result_df.columns)}
                - Период: {result_df['Дата'].min() if 'Дата' in result_df.columns else 'N/A'} - {result_df['Дата'].max() if 'Дата' in result_df.columns else 'N/A'}
                """)

                # Кнопка для скачивания
                with open(output_file, "rb") as f:
                    st.download_button(
                        label="📥 Скачать итоговый файл",
                        data=f,
                        file_name=f"ИТОГ_{year_selected}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

            except Exception as e:
                st.error(f"❌ Ошибка сохранения итогового файла: {e}")

# ==========================
# ИНСТРУКЦИЯ
# ==========================
st.subheader("📘 Инструкция")
with st.expander("Как использовать приложение"):
    st.markdown("""
    1. **Введите путь к основной директории** - папке, содержащей папки с годами (например: `C:\Профотбор\Реестр`)
    2. **Выберите год** из списка доступных
    3. **Нажмите "Подготовить файлы месяцев"** - приложение:
        - Создаст папку с названием года внутри выбранной папки
        - Обработает все Excel-файлы в папке года
        - Определит месяц из названия файла
        - Объединит данные из всех листов (по дням)
        - Сохранит по одному файлу на каждый месяц
    4. **Нажмите "Собрать итоговый файл за год"** - приложение:
        - Объединит все файлы месяцев
        - Отсортирует по дате
        - Создаст итоговый файл за год
    5. **Скачайте итоговый файл** используя кнопку загрузки
    """)

# ==========================
# ДЕБАГ ИНФОРМАЦИЯ
# ==========================
if st.checkbox("Показать отладочную информацию"):
    st.subheader("Отладочная информация")
    st.write(f"YEAR_DIR: {YEAR_DIR}")
    st.write(f"Exists: {YEAR_DIR.exists() if YEAR_DIR else 'None'}")
    if YEAR_DIR and YEAR_DIR.exists():
        st.write(f"Files: {[f.name for f in YEAR_DIR.iterdir()]}")