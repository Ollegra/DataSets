import sys
import pandas as pd
import sqlite3
import matplotlib

# matplotlib.use("Qt5Agg")  # Принудительно устанавливаем backend
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import seaborn as sns
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QPushButton,
    QTextEdit,
    QTableWidget,
    QTableWidgetItem,
    QFileDialog,
    QMessageBox,
    QComboBox,
    QLabel,
    QLineEdit,
    QTabWidget,
    QSplitter,
    QGroupBox,
    QGridLayout,
    QCheckBox,
    QStatusBar,
    QMenuBar,
    QScrollArea,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QIcon
import os
from sqlalchemy import create_engine, text
import pymysql
import pyodbc
import warnings

warnings.filterwarnings("ignore")

try:
    from dbfread import DBF

    DBF_AVAILABLE = True
except ImportError:
    DBF_AVAILABLE = False


class DatabaseConnection:
    """Класс для управления подключениями к различным базам данных"""

    def __init__(self):
        self.external_engine = None  # Внешняя БД
        self.internal_engine = None  # Внутренняя БД для файлов
        self.connection_type = None
        self.current_table_name = None
        self.setup_internal_db()

    def setup_internal_db(self):
        """Создание внутренней SQLite базы данных в памяти"""
        try:
            self.internal_engine = create_engine("sqlite:///:memory:")
            self.connection_type = "internal"
        except Exception as e:
            print(f"Ошибка создания внутренней БД: {e}")

    def connect_sqlite(self, db_path):
        """Подключение к внешней SQLite"""
        try:
            self.external_engine = create_engine(f"sqlite:///{db_path}")
            self.connection_type = "external_sqlite"
            return True, "Успешно подключено к внешней SQLite"
        except Exception as e:
            return False, f"Ошибка подключения к SQLite: {str(e)}"

    def connect_mysql(self, host, port, user, password, database):
        """Подключение к внешней MySQL"""
        try:
            connection_string = (
                f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            )
            self.external_engine = create_engine(connection_string)
            self.connection_type = "external_mysql"
            return True, "Успешно подключено к внешней MySQL"
        except Exception as e:
            return False, f"Ошибка подключения к MySQL: {str(e)}"

    def connect_sqlserver(
        self, server, database, user=None, password=None, trusted_connection=True
    ):
        """Подключение к SQL Server"""
        try:
            if trusted_connection:
                # Windows Authentication
                connection_string = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            else:
                # SQL Server Authentication
                connection_string = f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

            self.external_engine = create_engine(connection_string)
            self.connection_type = "external_sqlserver"
            return True, "Успешно подключено к SQL Server"
        except Exception as e:
            return False, f"Ошибка подключения к SQL Server: {str(e)}"

    def disconnect_external_db(self):
        """Отключение от внешней базы данных"""
        if self.external_engine:
            self.external_engine.dispose()
            self.external_engine = None
            self.connection_type = "internal"
            return True, "Отключено от внешней базы данных"
        return False, "Нет активного подключения к внешней БД"

    def load_data_to_internal_db(self, data, table_name="dataset"):
        """Загрузка данных во внутреннюю базу данных"""
        try:
            if self.internal_engine is None:
                self.setup_internal_db()

            # Создаем копию данных для обработки
            processed_data = data.copy()

            # Обрабатываем колонки со сложными типами данных
            import json

            print(
                f"Обрабатываем данные: {len(processed_data)} строк, {len(processed_data.columns)} колонок"
            )

            for column in processed_data.columns:
                print(f"Обрабатываем колонку: {column}")

                # Проверяем ВСЕ значения в колонке на наличие сложных объектов
                needs_conversion = False

                # Проверяем каждое значение в колонке
                for idx in processed_data.index:
                    value = processed_data.loc[idx, column]
                    if isinstance(value, (list, dict)):
                        needs_conversion = True
                        print(
                            f"Найден сложный объект в колонке {column}, строка {idx}: {type(value)}"
                        )
                        break

                # Если нужна конвертация, обрабатываем всю колонку
                if needs_conversion:
                    print(f"Конвертируем колонку {column}")

                    def convert_to_json_string(x):
                        if isinstance(x, (list, dict)):
                            try:
                                return json.dumps(
                                    x, ensure_ascii=False, separators=(",", ":")
                                )
                            except Exception as e:
                                print(f"Ошибка конвертации значения {x}: {e}")
                                return str(x)
                        elif pd.isna(x):
                            return None
                        else:
                            return str(x)

                    try:
                        processed_data[column] = processed_data[column].apply(
                            convert_to_json_string
                        )
                        print(f"Колонка {column} успешно конвертирована")
                    except Exception as e:
                        print(f"Ошибка при конвертации колонки {column}: {e}")
                        # В крайнем случае, конвертируем все в строки
                        processed_data[column] = processed_data[column].astype(str)

            print("Начинаем сохранение в БД...")

            # Дополнительная проверка перед сохранением - убеждаемся, что нет сложных объектов
            for column in processed_data.columns:
                for idx in processed_data.index:
                    value = processed_data.loc[idx, column]
                    if isinstance(value, (list, dict)):
                        print(
                            f"ВНИМАНИЕ: Все еще есть сложный объект в {column}, строка {idx}"
                        )
                        # Принудительно конвертируем
                        processed_data.loc[idx, column] = json.dumps(
                            value, ensure_ascii=False, separators=(",", ":")
                        )

            # Сохраняем обработанные данные в таблицу
            processed_data.to_sql(
                table_name, self.internal_engine, if_exists="replace", index=False
            )
            self.current_table_name = table_name
            print("Данные успешно сохранены в БД")
            return True, f"Данные загружены в таблицу '{table_name}'"
        except Exception as e:
            print(f"Полная ошибка: {str(e)}")
            return False, f"Ошибка загрузки данных в БД: {str(e)}"

    def clear_internal_data(self):
        """Очистка внутренней базы данных"""
        try:
            if self.internal_engine:
                # Пересоздаем внутреннюю БД
                self.internal_engine.dispose()
                self.setup_internal_db()
                self.current_table_name = None
                return True, "Внутренняя база данных очищена"
            return False, "Внутренняя база данных не инициализирована"
        except Exception as e:
            return False, f"Ошибка очистки БД: {str(e)}"

    def get_table_names(self):
        """Получение списка таблиц в активной базе данных"""
        try:
            if self.connection_type.startswith("external") and self.external_engine:
                with self.external_engine.connect() as conn:
                    if self.connection_type == "external_mysql":
                        result = conn.execute(text("SHOW TABLES"))
                        return [row[0] for row in result]
                    elif self.connection_type == "external_sqlserver":
                        result = conn.execute(
                            text(
                                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
                            )
                        )
                        return [row[0] for row in result]
                    else:  # SQLite
                        result = conn.execute(
                            text("SELECT name FROM sqlite_master WHERE type='table'")
                        )
                        return [row[0] for row in result]
            elif self.internal_engine:
                with self.internal_engine.connect() as conn:
                    result = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    )
                    return [row[0] for row in result]
            return []
        except Exception as e:
            print(f"Ошибка получения списка таблиц: {e}")
            return []

    def execute_query(self, query):
        """Выполнение SQL запроса"""
        try:
            # Определяем, какую базу использовать
            if self.connection_type.startswith("external") and self.external_engine:
                engine = self.external_engine
            elif self.internal_engine:
                engine = self.internal_engine
            else:
                return False, "Нет доступной базы данных"

            with engine.connect() as conn:
                result = pd.read_sql(text(query), conn)
            return True, result
        except Exception as e:
            return False, f"Ошибка выполнения запроса: {str(e)}"

    def export_data_to_external_db(self, data, table_name, if_exists="replace"):
        """Экспорт данных во внешнюю базу данных"""
        try:
            if (
                not self.connection_type.startswith("external")
                or not self.external_engine
            ):
                return False, "Нет подключения к внешней базе данных"

            data.to_sql(
                table_name, self.external_engine, if_exists=if_exists, index=False
            )
            return True, f"Данные экспортированы в таблицу '{table_name}'"
        except Exception as e:
            return False, f"Ошибка экспорта данных: {str(e)}"


class PlotCanvas(FigureCanvas):
    """Виджет для отображения графиков"""

    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.setParent(parent)

    def plot_histogram(self, data, column, bins=30):
        """Построение гистограммы"""
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.hist(data[column].dropna(), bins=bins, alpha=0.7, edgecolor="black")
        ax.set_title(f"Гистограмма: {column}")
        ax.set_xlabel(column)
        ax.set_ylabel("Частота")
        ax.grid(True, alpha=0.3)
        self.draw()

    def plot_line(self, data, x_col, y_col):
        """Построение линейного графика"""
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.plot(data[x_col], data[y_col], marker="o", linestyle="-", markersize=4)
        ax.set_title(f"{y_col} от {x_col}")
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.grid(True, alpha=0.3)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.fig.tight_layout()
        self.draw()

    def plot_scatter(self, data, x_col, y_col):
        """Построение точечного графика"""
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.scatter(data[x_col], data[y_col], alpha=0.6)
        ax.set_title(f"{y_col} vs {x_col}")
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.grid(True, alpha=0.3)
        self.draw()

    def plot_bar(self, data, x_col, y_col):
        """Построение столбчатого графика"""
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        ax.bar(data[x_col], data[y_col])
        ax.set_title(f"{y_col} по {x_col}")
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.grid(True, alpha=0.3)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        self.fig.tight_layout()
        self.draw()


class DatasetAnalyzer(QMainWindow):
    """Главное окно приложения"""

    def __init__(self):
        super().__init__()
        self.db_connection = DatabaseConnection()
        self.current_data = None
        self.init_ui()

    def init_ui(self):
        """Инициализация пользовательского интерфейса"""
        self.setWindowTitle("Универсальный анализатор датасетов")
        self.setGeometry(100, 100, 1400, 900)
        self.setStyleSheet("""
            QTextEdit {
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                      stop: 0 #E0E0E0, stop: 1 #FFFFFF);
                border: 2px solid gray;
                border-radius: 5px;
            }
            QTableWidget {
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                      stop: 0 #E0E0E0, stop: 1 #FFFFFF);
                border: 2px solid gray;
                border-radius: 5px;
            }
            QStatusBar {
                border: 2px solid gray;
                border-radius: 5px;
            }
            QGroupBox {
                background-color: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                      stop: 0 #E0E0E0, stop: 1 #FFFFFF);
                border: 2px solid gray;
                border-radius: 5px;
                margin-top: 1ex; /* leave space at the top for the title */
                padding-top: 5px;
                font-weight: bold;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                padding: 0 3px;
                color: #808080;
                font-weight: bold;
                font-size: 12px;
            }
            QToolTip {
                background-color: #CCF0FE;
                color: blue;
                border: 2px dashed #FF0000;
                border-radius: 7px;
                padding: 2px;
                font: 10pt "Segoe UI";
            }
            QPushButton {
                color: #0000FF;
                font-weight: bold;
            }
            QLabel {
                color: #808080;
                font-weight: bold;
            }
            QTabWidget::pane { /* The tab widget frame */
                border-top: 2px solid #C2C7CB;
            }
            QTabBar::tab {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                stop: 0 #E1E1E1, stop: 0.4 #DDDDDD,
                                stop: 0.5 #D8D8D8, stop: 1.0 #D3D3D3);
                border: 2px solid #C4C4C3;
                border-bottom-color: #C2C7CB; /* same as the pane color */
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
                min-width: 8ex;
                padding: 2px;
            }
            QTabBar::tab:selected, QTabBar::tab:hover {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                                stop: 0 #fafafa, stop: 0.4 #f4f4f4,
                                stop: 0.5 #e7e7e7, stop: 1.0 #fafafa);
            }
            QTabBar::tab:selected {
                border-color: #9B9B9B;
                border-bottom-color: #C2C7CB; /* same as pane color */
            }
            QTabBar::tab:!selected {
                margin-top: 2px; /* make non-selected tabs look smaller */
            }
        """)

        # Создание статус бара
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Готов к работе")

        # Центральный виджет
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Основной layout
        main_layout = QHBoxLayout(central_widget)

        # Создание вкладок
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Вкладка загрузки данных
        self.create_data_tab()

        # Вкладка SQL запросов
        self.create_sql_tab()

        # Вкладка графиков
        self.create_plot_tab()

        # Вкладка помощи
        self.create_help_tab()

        # Создание меню
        self.create_menu()

    def create_data_tab(self):
        """Создание вкладки для загрузки данных"""
        data_widget = QWidget()
        layout = QVBoxLayout(data_widget)

        # Горизонтальный контейнер для file_group и db_group
        top_container = QHBoxLayout()

        # Группа загрузки файлов
        file_group = QGroupBox("Загрузка файлов")
        file_layout = QVBoxLayout(file_group)

        # Кнопки для разных типов файлов (вертикально)
        btn_csv = QPushButton(
            QIcon(os.path.join("images", "opencsv.png")), " Загрузить CSV"
        )
        btn_csv.clicked.connect(self.load_csv)
        file_layout.addWidget(btn_csv)

        btn_excel = QPushButton(
            QIcon(os.path.join("images", "openxls.png")), " Загрузить Excel"
        )
        btn_excel.clicked.connect(self.load_excel)
        file_layout.addWidget(btn_excel)

        if DBF_AVAILABLE:
            btn_dbf = QPushButton(
                QIcon(os.path.join("images", "tabledbf.png")), " Загрузить DBF"
            )
            btn_dbf.clicked.connect(self.load_dbf)
            file_layout.addWidget(btn_dbf)
        
        btn_json = QPushButton(
            QIcon(os.path.join("images", "open2json.png")), "Загрузить JSON"
        )
        btn_json.clicked.connect(self.load_json)
        file_layout.addWidget(btn_json)

        # Кнопка очистки данных в группе файлов
        btn_clear = QPushButton(
            QIcon(os.path.join("images", "clear.png")), " Очистить внутренние данные"
        )
        btn_clear.clicked.connect(self.clear_internal_data)
        btn_clear.setStyleSheet("color: #8B0000; font-weight: bold;")
        file_layout.addWidget(btn_clear)

        top_container.addWidget(file_group)

        # Группа подключения к БД
        db_group = QGroupBox("Подключение к базе данных")
        db_layout = QGridLayout(db_group)

        # Управление данными
        btn_disconnect = QPushButton(
            QIcon(os.path.join("images", "dataoff_.png")), " Отключить внешнюю БД"
        )
        btn_disconnect.clicked.connect(self.disconnect_external_db)
        btn_disconnect.setStyleSheet("color: #8B0000; font-weight: bold;")
        db_layout.addWidget(btn_disconnect, 0, 0, 1, 2)

        # SQLite
        btn_sqlite = QPushButton(
            QIcon(os.path.join("images", "dataon.png")), " Подключить SQLite"
        )
        btn_sqlite.clicked.connect(self.connect_sqlite)
        db_layout.addWidget(btn_sqlite, 0, 3)

        # MySQL
        db_layout.addWidget(QLabel("MySQL Host:"), 2, 0)
        self.mysql_host = QLineEdit("localhost")
        db_layout.addWidget(self.mysql_host, 2, 1)

        db_layout.addWidget(QLabel("Port:"), 2, 2)
        self.mysql_port = QLineEdit("3306")
        db_layout.addWidget(self.mysql_port, 2, 3)

        db_layout.addWidget(QLabel("User:"), 3, 0)
        self.mysql_user = QLineEdit()
        db_layout.addWidget(self.mysql_user, 3, 1)

        db_layout.addWidget(QLabel("Password:"), 3, 2)
        self.mysql_password = QLineEdit()
        self.mysql_password.setEchoMode(QLineEdit.EchoMode.Password)
        db_layout.addWidget(self.mysql_password, 3, 3)

        db_layout.addWidget(QLabel("Database:"), 4, 0)
        self.mysql_database = QLineEdit()
        db_layout.addWidget(self.mysql_database, 4, 1)

        btn_mysql = QPushButton(
            QIcon(os.path.join("images", "dataon.png")), " Подключить MySQL"
        )
        btn_mysql.clicked.connect(self.connect_mysql)
        db_layout.addWidget(btn_mysql, 4, 3)

        # SQL Server
        db_layout.addWidget(QLabel("SQL Server:"), 5, 0)
        self.sqlserver_server = QLineEdit("localhost")
        db_layout.addWidget(self.sqlserver_server, 5, 1)

        db_layout.addWidget(QLabel("Database:"), 5, 2)
        self.sqlserver_database = QLineEdit()
        db_layout.addWidget(self.sqlserver_database, 5, 3)

        # Чекбокс для выбора типа аутентификации
        self.sqlserver_trusted = QCheckBox("Windows Authentication")
        self.sqlserver_trusted.setChecked(True)
        self.sqlserver_trusted.toggled.connect(self.toggle_sqlserver_auth)
        db_layout.addWidget(self.sqlserver_trusted, 6, 0, 1, 2)

        # Поля для SQL Server Authentication (изначально скрыты)
        db_layout.addWidget(QLabel("User:"), 7, 0)
        self.sqlserver_user = QLineEdit()
        self.sqlserver_user.setEnabled(False)
        db_layout.addWidget(self.sqlserver_user, 7, 1)

        db_layout.addWidget(QLabel("Password:"), 7, 2)
        self.sqlserver_password = QLineEdit()
        self.sqlserver_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.sqlserver_password.setEnabled(False)
        db_layout.addWidget(self.sqlserver_password, 7, 3)

        btn_sqlserver = QPushButton(
            QIcon(os.path.join("images", "dataon.png")), " Подключить SQL Server"
        )
        btn_sqlserver.clicked.connect(self.connect_sqlserver)
        db_layout.addWidget(btn_sqlserver, 8, 0, 1, 2)

        # Информация о подключении
        self.db_status = QLabel("Статус: Внутренняя БД готова")
        self.db_status.setStyleSheet("color: #FF0000; font-weight: bold;")
        db_layout.addWidget(self.db_status, 8, 3, 1, 4)

        top_container.addWidget(db_group)

        # Добавляем горизонтальный контейнер в основной layout
        layout.addLayout(top_container)

        # Группа работы с таблицами БД
        table_group = QGroupBox("Работа с таблицами базы данных")
        table_layout = QGridLayout(table_group)

        table_layout.addWidget(QLabel("Выберите таблицу:"), 0, 0)
        self.table_selector = QComboBox()
        self.table_selector.setEnabled(False)
        table_layout.addWidget(self.table_selector, 0, 1)

        btn_refresh_tables = QPushButton(
            QIcon(os.path.join("images", "reload.png")), " Обновить список"
        )
        btn_refresh_tables.clicked.connect(self.refresh_table_list)
        table_layout.addWidget(btn_refresh_tables, 0, 2)

        btn_view_table = QPushButton(
            QIcon(os.path.join("images", "taleview.png")), " Просмотреть таблицу"
        )
        btn_view_table.clicked.connect(self.view_selected_table)
        table_layout.addWidget(btn_view_table, 0, 3)

        layout.addWidget(table_group)

        # Группа экспорта данных в БД
        export_group = QGroupBox("Экспорт данных в базу данных")
        export_layout = QGridLayout(export_group)

        export_layout.addWidget(QLabel("Имя таблицы:"), 0, 0)
        self.export_table_name = QLineEdit("exported_data")
        export_layout.addWidget(self.export_table_name, 0, 1)

        export_layout.addWidget(QLabel("Если таблица существует:"), 0, 2)
        self.export_if_exists = QComboBox()
        self.export_if_exists.addItems(["replace", "append", "fail"])
        export_layout.addWidget(self.export_if_exists, 0, 3)

        btn_export_current = QPushButton(
            QIcon(os.path.join("images", "export.png")), " Экспорт текущих данных"
        )
        btn_export_current.clicked.connect(self.export_current_data)
        export_layout.addWidget(btn_export_current, 1, 0, 1, 2)

        btn_export_sql_result = QPushButton(
            QIcon(os.path.join("images", "export.png")), " Экспорт результата SQL"
        )
        btn_export_sql_result.clicked.connect(self.export_sql_result)
        export_layout.addWidget(btn_export_sql_result, 1, 2, 1, 2)

        layout.addWidget(export_group)

        # Таблица для отображения данных
        self.data_table = QTableWidget()
        self.data_table.verticalHeader().setDefaultSectionSize(10)
        layout.addWidget(self.data_table)

        # Информация о данных
        self.data_info = QTextEdit()
        self.data_info.setMaximumHeight(100)
        layout.addWidget(self.data_info)

        self.tabs.addTab(data_widget, "Данные")

    def create_sql_tab(self):
        """Создание вкладки для SQL запросов"""
        sql_widget = QWidget()
        layout = QVBoxLayout(sql_widget)

        # Информация о доступных таблицах
        self.tables_info = QLabel("Доступные таблицы: ")
        layout.addWidget(self.tables_info)

        # Поле для ввода SQL запроса
        layout.addWidget(QLabel("SQL Запрос (Основная таблица: 'dataset'):"))
        self.sql_input = QTextEdit()
        self.sql_input.setMaximumHeight(150)
        self.sql_input.setFont(QFont("Courier", 10))
        self.sql_input.setPlaceholderText(
            "Пример: SELECT * FROM dataset WHERE колонка > 100"
        )
        layout.addWidget(self.sql_input)

        # Кнопки управления
        btn_layout = QHBoxLayout()

        btn_execute = QPushButton(
            QIcon(os.path.join("images", "sqlz.png")), " Выполнить запрос"
        )
        btn_execute.clicked.connect(self.execute_sql)
        btn_layout.addWidget(btn_execute)

        btn_export_csv = QPushButton(
            QIcon(os.path.join("images", "export.png")), " Экспорт в CSV"
        )
        btn_export_csv.clicked.connect(self.export_sql_result_to_csv)
        btn_layout.addWidget(btn_export_csv)

        btn_clear_sql = QPushButton(
            QIcon(os.path.join("images", "clear.png")), " Очистить результат"
        )
        btn_clear_sql.clicked.connect(self.clear_sql_result)
        btn_clear_sql.setStyleSheet("color: #8B0000; font-weight: bold;")
        btn_layout.addWidget(btn_clear_sql)

        layout.addLayout(btn_layout)

        # Таблица результатов
        self.sql_result_table = QTableWidget()
        self.sql_result_table.verticalHeader().setDefaultSectionSize(10)
        layout.addWidget(self.sql_result_table)

        self.tabs.addTab(sql_widget, "SQL")

        # Сохраняем последний результат SQL запроса для экспорта
        self.last_sql_result = None

    def create_plot_tab(self):
        """Создание вкладки для графиков"""
        plot_widget = QWidget()
        layout = QHBoxLayout(plot_widget)

        # Левая панель управления
        control_panel = QWidget()
        control_layout = QVBoxLayout(control_panel)
        control_panel.setMaximumWidth(300)

        # Выбор типа графика
        control_layout.addWidget(QLabel("Тип графика:"))
        self.plot_type = QComboBox()
        self.plot_type.addItems(["Гистограмма", "Линейный", "Точечный", "Столбчатый"])
        control_layout.addWidget(self.plot_type)

        # Выбор колонок
        control_layout.addWidget(QLabel("X колонка:"))
        self.x_column = QComboBox()
        control_layout.addWidget(self.x_column)

        control_layout.addWidget(QLabel("Y колонка:"))
        self.y_column = QComboBox()
        control_layout.addWidget(self.y_column)

        # Кнопка построения графика
        btn_plot = QPushButton(
            QIcon(os.path.join("images", "plot.png")), " Построить график"
        )
        btn_plot.clicked.connect(self.create_plot)
        control_layout.addWidget(btn_plot)

        # Кнопка очистки графика
        btn_clear_plot = QPushButton(
            QIcon(os.path.join("images", "clear.png")), "Очистить график"
        )
        btn_clear_plot.clicked.connect(self.clear_plot)
        btn_clear_plot.setStyleSheet("color: #8B0000; font-weight: bold;")
        control_layout.addWidget(btn_clear_plot)

        control_layout.addStretch()

        # Canvas для графика
        self.plot_canvas = PlotCanvas(self, width=8, height=6)

        layout.addWidget(control_panel)
        layout.addWidget(self.plot_canvas)

        self.tabs.addTab(plot_widget, "Графики")

    def create_help_tab(self):
        """Создание вкладки помощи"""
        help_widget = QWidget()
        layout = QVBoxLayout(help_widget)

        # Создаем область прокрутки
        scroll_area = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)

        # Создаем текст справки
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        help_text.setHtml(self.get_help_content())

        scroll_layout.addWidget(help_text)
        scroll_area.setWidget(scroll_widget)
        scroll_area.setWidgetResizable(True)

        layout.addWidget(scroll_area)

        self.tabs.addTab(help_widget, "Справка")

    def create_menu(self):
        """Создание меню"""
        menubar = self.menuBar()

        # Меню "Справка"
        help_menu = menubar.addMenu("Справка")

        # Действие "О программе"
        about_action = help_menu.addAction("О программе")
        about_action.triggered.connect(self.show_about)

        # Действие "Быстрая помощь"
        quick_help_action = help_menu.addAction("Быстрая помощь")
        quick_help_action.triggered.connect(self.show_quick_help)

    def get_help_content(self):
        """Получение содержимого справки"""
        return """
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; margin: 20px; }
                h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
                h2 { color: #34495e; margin-top: 30px; }
                h3 { color: #7f8c8d; margin-top: 20px; }
                .feature { background-color: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; }
                .code { background-color: #2c3e50; color: #ecf0f1; padding: 10px; border-radius: 3px; font-family: 'Courier New'; }
                .warning { color: #D11616; font-weight: bold; padding: 10px; border-radius: 3px; }
                .tip { color: #008000; font-weight: bold; padding: 10px; border-radius: 3px; }
                ul { margin-left: 20px; }
                li { margin-bottom: 5px; }
            </style>
        </head>
        <body>
            <h1>📊 Универсальный анализатор датасетов</h1>
            <p>Мощное приложение для работы с данными, поддерживающее множество форматов файлов и различные типы баз данных.</p>
            
            <h2>🗂️ Вкладка "Данные"</h2>
            
            <h3>📁 Загрузка файлов</h3>
            <div class="feature">
                <ul>
                    <li><strong>Загрузить CSV</strong> - импорт данных из CSV файлов с автоматическим определением разделителей</li>
                    <li><strong>Загрузить Excel</strong> - поддержка форматов .xlsx и .xls</li>
                    <li><strong>Загрузить DBF</strong> - импорт данных из dBase файлов (требует библиотеку dbfread)</li>
                    <li><strong>Очистить внутренние данные</strong> - удаление всех загруженных данных из памяти</li>
                </ul>
            </div>
            
            <h3>🗄️ Подключение к базам данных</h3>
            <div class="feature">
                <h4>SQLite</h4>
                <ul>
                    <li>Подключение к локальным файлам базы данных SQLite (.db, .sqlite, .sqlite3)</li>
                    <li>Автоматическое сканирование структуры БД</li>
                </ul>
                
                <h4>MySQL</h4>
                <ul>
                    <li>Настройка хоста, порта, пользователя и пароля</li>
                    <li>Поддержка удаленных подключений</li>
                    <li>Автоматическое определение доступных таблиц</li>
                </ul>
                
                <h4>SQL Server</h4>
                <ul>
                    <li><strong>Windows Authentication</strong> - использование учетных данных Windows</li>
                    <li><strong>SQL Server Authentication</strong> - использование логина и пароля SQL Server</li>
                    <li>Поддержка локальных и удаленных серверов</li>
                </ul>
            </div>
            
            <h3>📋 Работа с таблицами базы данных</h3>
            <div class="feature">
                <ul>
                    <li><strong>Выбор таблицы</strong> - выпадающий список всех доступных таблиц</li>
                    <li><strong>Обновить список</strong> - обновление списка таблиц после изменений в БД</li>
                    <li><strong>Просмотреть таблицу</strong> - загрузка данных из выбранной таблицы (до 1000 записей)</li>
                </ul>
            </div>
            
            <h3>📤 Экспорт данных в базу данных</h3>
            <div class="feature">
                <ul>
                    <li><strong>Экспорт текущих данных</strong> - сохранение загруженного файла в БД</li>
                    <li><strong>Экспорт результата SQL</strong> - сохранение результатов SQL запроса в новую таблицу</li>
                    <li><strong>Настройки экспорта:</strong>
                        <ul>
                            <li><em>replace</em> - заменить существующую таблицу</li>
                            <li><em>append</em> - добавить данные к существующей таблице</li>
                            <li><em>fail</em> - прервать операцию, если таблица существует</li>
                        </ul>
                    </li>
                </ul>
            </div>
            
            <h2>🔍 Вкладка "SQL"</h2>
            
            <h3>📝 Выполнение SQL запросов</h3>
            <div class="feature">
                <ul>
                    <li><strong>Поле ввода запроса</strong> - многострочный редактор с подсветкой синтаксиса</li>
                    <li><strong>Выполнить запрос</strong> - исполнение SQL команд</li>
                    <li><strong>Экспорт в CSV</strong> - сохранение результатов в CSV файл</li>
                    <li><strong>Очистить результат</strong> - очистка таблицы результатов</li>
                </ul>
            </div>
            
            <h3>💡 Примеры SQL запросов</h3>
            <div class="code">
-- Базовая выборка данных<br>
SELECT * FROM dataset LIMIT 100;<br><br>

-- Фильтрация данных<br>
SELECT * FROM dataset WHERE column_name > 50;<br><br>

-- Группировка и агрегация<br>
SELECT category, COUNT(*), AVG(price)<br>
FROM dataset<br>
GROUP BY category;<br><br>

-- Сортировка результатов<br>
SELECT * FROM dataset<br>
ORDER BY date_column DESC;<br><br>

-- Объединение условий<br>
SELECT * FROM dataset<br>
WHERE status = 'active' AND price BETWEEN 100 AND 500;
            </div>
            
            <h2>📊 Вкладка "Графики"</h2>
            
            <h3>📈 Типы графиков</h3>
            <div class="feature">
                <ul>
                    <li><strong>Гистограмма</strong> - распределение значений в одной колонке</li>
                    <li><strong>Линейный график</strong> - зависимость между двумя переменными во времени</li>
                    <li><strong>Точечный график</strong> - корреляция между двумя переменными</li>
                    <li><strong>Столбчатый график</strong> - сравнение категориальных данных</li>
                </ul>
            </div>
            
            <h3>⚙️ Настройки графиков</h3>
            <div class="feature">
                <ul>
                    <li><strong>X колонка</strong> - выбор данных для горизонтальной оси</li>
                    <li><strong>Y колонка</strong> - выбор данных для вертикальной оси (не требуется для гистограммы)</li>
                    <li><strong>Построить график</strong> - создание визуализации</li>
                    <li><strong>Очистить график</strong> - очистка области построения</li>
                </ul>
            </div>
            
            <h2>📋 Статус бар</h2>
            
            <div class="feature">
                <p>В нижней части окна отображается актуальная информация о состоянии программы:</p>
                <ul>
                    <li>Результаты операций загрузки данных</li>
                    <li>Статус подключения к базам данных</li>
                    <li>Количество обработанных записей</li>
                    <li>Сообщения об ошибках и предупреждения</li>
                </ul>
            </div>
            
            <h2>🔧 Системные требования</h2>
            
            <div class="feature">
                <h4>Обязательные зависимости:</h4>
                <div class="code">
pip install pandas PyQt6 matplotlib seaborn sqlalchemy pymysql
                </div>
                
                <h4>Дополнительные зависимости:</h4>
                <div class="code">
pip install dbfread  # Для работы с DBF файлами<br>
pip install pyodbc   # Для подключения к SQL Server
                </div>
            </div>
            
            <h2>⚠️ Важные замечания</h2>
            
            <div class="warning">
                <ul>
                    <li>Для больших файлов отображается только первые 1000 строк</li>
                    <li>При работе с SQL Server может потребоваться установка ODBC Driver 17</li>
                    <li>Всегда делайте резервные копии важных данных перед экспортом</li>
                </ul>
            </div>
            
            <h2>💡 Полезные советы</h2>
            
            <div class="tip">
                <ul>
                    <li>Используйте LIMIT в SQL запросах для ограничения количества результатов</li>
                    <li>При экспорте больших данных выбирайте режим 'append' для добавления порциями</li>
                    <li>Проверяйте типы данных колонок перед построением графиков</li>
                    <li>Используйте группировку данных для столбчатых диаграмм с большим количеством записей</li>
                </ul>
            </div>
            
            <h2>🆘 Решение проблем</h2>
            
            <div class="feature">
                <h4>Не удается подключиться к MySQL:</h4>
                <ul>
                    <li>Проверьте правильность хоста, порта и учетных данных</li>
                    <li>Убедитесь, что MySQL сервер запущен</li>
                    <li>Проверьте настройки брандмауэра</li>
                </ul>
                
                <h4>Ошибка при загрузке файла:</h4>
                <ul>
                    <li>Проверьте кодировку файла (рекомендуется UTF-8)</li>
                    <li>Убедитесь, что файл не открыт в другой программе</li>
                    <li>Проверьте целостность файла</li>
                </ul>
                
                <h4>График не отображается:</h4>
                <ul>
                    <li>Убедитесь, что выбранные колонки содержат числовые данные</li>
                    <li>Проверьте наличие данных в выбранных колонках</li>
                    <li>Очистите график и попробуйте построить заново</li>
                </ul>
            </div>
            
            <hr>
            <p><em>Версия: 2.0 | Последнее обновление: 2025</em></p>
        </body>
        </html>
        """

    def show_about(self):
        """Показ информации о программе"""
        about_text = """
        <h2>📊 Универсальный анализатор датасетов</h2>
        <p><strong>Версия:</strong> 2.0</p>
        <p><strong>Автор:</strong> Разработано с использованием PyQt6</p>
        <p><strong>Описание:</strong> Мощное приложение для анализа данных с поддержкой множества форматов файлов и баз данных.</p>
        
        <h3>🔧 Основные возможности:</h3>
        <ul>
            <li>Загрузка CSV, Excel, DBF файлов</li>
            <li>Подключение к SQLite, MySQL, SQL Server</li>
            <li>Выполнение SQL запросов</li>
            <li>Построение графиков и диаграмм</li>
            <li>Экспорт данных в различные форматы</li>
        </ul>
        
        <h3>📚 Используемые библиотеки:</h3>
        <ul>
            <li>PyQt6 - графический интерфейс</li>
            <li>Pandas - обработка данных</li>
            <li>Matplotlib/Seaborn - визуализация</li>
            <li>SQLAlchemy - работа с базами данных</li>
        </ul>
        """

        msg = QMessageBox()
        msg.setWindowTitle("О программе")
        msg.setTextFormat(Qt.TextFormat.RichText)
        msg.setText(about_text)
        msg.exec()

    def show_quick_help(self):
        """Показ быстрой справки"""
        quick_help_text = """
        <h2>🚀 Быстрый старт</h2>
        
        <h3>1️⃣ Загрузка данных:</h3>
        <ul>
            <li>Нажмите <strong>"Загрузить CSV/Excel/DBF"</strong></li>
            <li>Или подключитесь к базе данных</li>
        </ul>
        
        <h3>2️⃣ Анализ данных:</h3>
        <ul>
            <li>Перейдите на вкладку <strong>"SQL"</strong></li>
            <li>Напишите запрос: <code>SELECT * FROM dataset</code></li>
            <li>Нажмите <strong>"Выполнить запрос"</strong></li>
        </ul>
        
        <h3>3️⃣ Визуализация:</h3>
        <ul>
            <li>Перейдите на вкладку <strong>"Графики"</strong></li>
            <li>Выберите тип графика и колонки</li>
            <li>Нажмите <strong>"Построить график"</strong></li>
        </ul>
        
        <h3>4️⃣ Экспорт результатов:</h3>
        <ul>
            <li>На вкладке "SQL": <strong>"Экспорт в CSV"</strong></li>
            <li>На вкладке "Данные": экспорт в базу данных</li>
        </ul>
        
        <p><strong>💡 Совет:</strong> Следите за сообщениями в статус баре внизу окна!</p>
        """

        msg = QMessageBox()
        msg.setWindowTitle("Быстрая помощь")
        msg.setTextFormat(Qt.TextFormat.RichText)
        msg.setText(quick_help_text)
        msg.exec()

    def load_csv(self):
        """Загрузка CSV файла"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите CSV файл", "", "CSV Files (*.csv)"
        )
        if file_path:
            try:
                self.current_data = pd.read_csv(file_path)
                # Загружаем данные во внутреннюю БД
                success, message = self.db_connection.load_data_to_internal_db(
                    self.current_data, "dataset"
                )
                if success:
                    self.display_data(self.current_data)
                    self.update_column_selectors()
                    self.update_tables_info()
                    self.show_message(
                        f"CSV файл загружен: {len(self.current_data)} строк. {message}"
                    )
                    self.show_status_message(
                        f"CSV загружен: {len(self.current_data)} строк"
                    )
                else:
                    self.show_error(message)
            except Exception as e:
                self.show_error(f"Ошибка загрузки CSV: {str(e)}")

    def load_excel(self):
        """Загрузка Excel файла"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите Excel файл", "", "Excel Files (*.xlsx *.xls)"
        )
        if file_path:
            try:
                self.current_data = pd.read_excel(file_path)
                # Загружаем данные во внутреннюю БД
                success, message = self.db_connection.load_data_to_internal_db(
                    self.current_data, "dataset"
                )
                if success:
                    self.display_data(self.current_data)
                    self.update_column_selectors()
                    self.update_tables_info()
                    self.show_message(
                        f"Excel файл загружен: {len(self.current_data)} строк. {message}"
                    )
                    self.show_status_message(
                        f"Excel загружен: {len(self.current_data)} строк"
                    )
                else:
                    self.show_error(message)
            except Exception as e:
                self.show_error(f"Ошибка загрузки Excel: {str(e)}")

    def load_dbf(self):
        """Загрузка DBF файла"""
        if not DBF_AVAILABLE:
            self.show_error("Библиотека dbfread не установлена")
            return

        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите DBF файл", "", "DBF Files (*.dbf)"
        )
        if file_path:
            try:
                dbf = DBF(file_path)
                self.current_data = pd.DataFrame(iter(dbf))
                # Загружаем данные во внутреннюю БД
                success, message = self.db_connection.load_data_to_internal_db(
                    self.current_data, "dataset"
                )
                if success:
                    self.display_data(self.current_data)
                    self.update_column_selectors()
                    self.update_tables_info()
                    self.show_message(
                        f"DBF файл загружен: {len(self.current_data)} строк. {message}"
                    )
                    self.show_status_message(
                        f"DBF загружен: {len(self.current_data)} строк"
                    )
                else:
                    self.show_error(message)
            except Exception as e:
                self.show_error(f"Ошибка загрузки DBF: {str(e)}")
    
    def load_json(self):
        """Загрузка JSON файла"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите JSON файл", "", "JSON Files (*.json)"
        )
        if file_path:
            print(f"Начинаем загрузку JSON файла: {file_path}")
            try:
                # Пробуем различные способы загрузки JSON
                try:
                    # Сначала пробуем загрузить как массив объектов
                    print("Пробуем загрузить JSON с помощью pd.read_json()")
                    self.current_data = pd.read_json(file_path)
                    print(
                        f"Успешно загружено с pd.read_json(): {self.current_data.shape}"
                    )
                except ValueError as e:
                    print(f"pd.read_json() не сработал: {e}")
                    # Если не получилось, пробуем загрузить как обычный JSON и нормализовать
                    import json

                    print("Пробуем загрузить как обычный JSON файл")
                    with open(file_path, "r", encoding="utf-8") as f:
                        json_data = json.load(f)

                    print(f"JSON загружен, тип данных: {type(json_data)}")

                    # Проверяем структуру данных
                    if isinstance(json_data, dict):
                        print("JSON является словарем, нормализуем")
                        # Если это словарь, пробуем нормализовать
                        self.current_data = pd.json_normalize(json_data)
                        print(f"После нормализации словаря: {self.current_data.shape}")
                    elif isinstance(json_data, list):
                        if len(json_data) > 0 and isinstance(json_data[0], dict):
                            print("JSON является списком словарей, нормализуем")
                            # Если это список словарей, нормализуем каждый элемент
                            self.current_data = pd.json_normalize(json_data)
                            print(
                                f"После нормализации списка словарей: {self.current_data.shape}"
                            )
                        else:
                            print("JSON является простым списком")
                            # Если это простой список, создаем DataFrame с одной колонкой
                            self.current_data = pd.DataFrame({"values": json_data})
                            print(
                                f"После создания DataFrame из списка: {self.current_data.shape}"
                            )
                    else:
                        print("JSON является простым значением")
                        # Если это простое значение, создаем DataFrame
                        self.current_data = pd.DataFrame({"value": [json_data]})
                        print(
                            f"После создания DataFrame из значения: {self.current_data.shape}"
                        )

                # Проверяем, что данные загружены успешно
                if self.current_data.empty:
                    print("ОШИБКА: DataFrame пуст после загрузки JSON")
                    self.show_error(
                        "JSON файл не содержит данных или имеет неподдерживаемую структуру"
                    )
                    return

                print(
                    f"Данные успешно преобразованы в DataFrame: {self.current_data.shape}"
                )
                print(f"Колонки: {list(self.current_data.columns)}")

                # Показываем типы данных в колонках
                for col in self.current_data.columns:
                    sample_vals = self.current_data[col].head(3).tolist()
                    print(f"Колонка '{col}': типы = {[type(v) for v in sample_vals]}")

                # Загружаем данные во внутреннюю БД
                print("Начинаем загрузку в БД...")
                success, message = self.db_connection.load_data_to_internal_db(
                    self.current_data, "dataset"
                )
                if success:
                    print("Данные успешно загружены в БД")
                    self.display_data(self.current_data)
                    self.update_column_selectors()
                    self.update_tables_info()
                    self.show_message(
                        f"JSON файл загружен: {len(self.current_data)} строк, {len(self.current_data.columns)} колонок. {message}"
                    )
                    self.show_status_message(
                        f"JSON загружен: {len(self.current_data)} строк"
                    )
                else:
                    print(f"ОШИБКА загрузки в БД: {message}")
                    self.show_error(message)
            except Exception as e:
                print(f"ОБЩАЯ ОШИБКА при загрузке JSON: {str(e)}")
                import traceback

                traceback.print_exc()
                self.show_error(f"Ошибка загрузки JSON: {str(e)}")

    def clear_internal_data(self):
        """Очистка внутренних данных"""
        success, message = self.db_connection.clear_internal_data()
        if success:
            self.current_data = None
            self.data_table.setRowCount(0)
            self.data_table.setColumnCount(0)
            self.data_info.clear()
            self.update_column_selectors()
            self.update_tables_info()
            self.update_db_status()
            self.show_message(message)
            self.show_status_message("Подключено к SQLite")
        else:
            self.show_error(message)

    def connect_sqlserver(self):
        """Подключение к SQL Server"""
        server = self.sqlserver_server.text()
        database = self.sqlserver_database.text()
        trusted = self.sqlserver_trusted.isChecked()

        if not all([server, database]):
            self.show_error(
                "Заполните поля Server и Database для подключения к SQL Server"
            )
            return

        if trusted:
            success, message = self.db_connection.connect_sqlserver(
                server, database, trusted_connection=True
            )
        else:
            user = self.sqlserver_user.text()
            password = self.sqlserver_password.text()
            if not all([user, password]):
                self.show_error(
                    "Заполните поля User и Password для SQL Server Authentication"
                )
                return
            success, message = self.db_connection.connect_sqlserver(
                server, database, user, password, trusted_connection=False
            )

        if success:
            self.update_tables_info()
            self.update_db_status()
            self.show_message(message)
            self.show_status_message("Подключено к SQL Server")
        else:
            self.show_error(message)

    def toggle_sqlserver_auth(self, checked):
        """Переключение типа аутентификации SQL Server"""
        self.sqlserver_user.setEnabled(not checked)
        self.sqlserver_password.setEnabled(not checked)

    def export_current_data(self):
        """Экспорт текущих данных в БД"""
        if self.current_data is None:
            self.show_error("Нет данных для экспорта")
            return

        if not self.db_connection.connection_type.startswith("external"):
            self.show_error("Подключитесь к внешней базе данных для экспорта")
            return

        table_name = self.export_table_name.text().strip()
        if not table_name:
            self.show_error("Введите имя таблицы для экспорта")
            return

        if_exists = self.export_if_exists.currentText()

        success, message = self.db_connection.export_data_to_external_db(
            self.current_data, table_name, if_exists
        )

        if success:
            self.show_message(message)
            self.show_status_message(f"Данные экспортированы в таблицу '{table_name}'")
            # Обновляем список таблиц
            self.refresh_table_list()
        else:
            self.show_error(message)

    def export_sql_result(self):
        """Экспорт результата SQL запроса в БД"""
        if self.last_sql_result is None:
            self.show_error("Нет результата SQL запроса для экспорта")
            return

        if not self.db_connection.connection_type.startswith("external"):
            self.show_error("Подключитесь к внешней базе данных для экспорта")
            return

        table_name = self.export_table_name.text().strip()
        if not table_name:
            self.show_error("Введите имя таблицы для экспорта")
            return

        if_exists = self.export_if_exists.currentText()

        success, message = self.db_connection.export_data_to_external_db(
            self.last_sql_result, table_name, if_exists
        )

        if success:
            self.show_message(message)
            self.show_status_message(
                f"Результат SQL запроса экспортирован в таблицу '{table_name}'"
            )
            # Обновляем список таблиц
            self.refresh_table_list()
        else:
            self.show_error(message)

    def export_sql_result_to_csv(self):
        """Экспорт результата SQL запроса в CSV файл"""
        if self.last_sql_result is None:
            self.show_error("Нет результата SQL запроса для экспорта")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "Сохранить как CSV", "", "CSV Files (*.csv)"
        )

        if file_path:
            try:
                self.last_sql_result.to_csv(
                    file_path, index=False, encoding="utf-8-sig"
                )
                self.show_message(f"Данные экспортированы в файл: {file_path}")
                self.show_status_message(
                    f"Экспорт в CSV выполнен: {len(self.last_sql_result)} строк"
                )
            except Exception as e:
                self.show_error(f"Ошибка экспорта в CSV: {str(e)}")

    def disconnect_external_db(self):
        """Отключение от внешней БД"""
        success, message = self.db_connection.disconnect_external_db()
        if success:
            self.update_tables_info()
            self.update_db_status()
            self.show_message(message)
            self.show_status_message("Отключено от внешней БД")
        else:
            self.show_error(message)

    def update_tables_info(self):
        """Обновление информации о доступных таблицах"""
        tables = self.db_connection.get_table_names()
        if tables:
            self.tables_info.setText(f"Доступные таблицы: {', '.join(tables)}")
        else:
            self.tables_info.setText("Доступные таблицы: нет данных")

    def update_db_status(self):
        """Обновление статуса подключения к БД"""
        if hasattr(self, "db_status"):
            if self.db_connection.connection_type.startswith("external"):
                self.db_status.setText(
                    f"Статус: Подключено к {self.db_connection.connection_type}"
                )
                self.db_status.setStyleSheet("color: blue; font-weight: bold;")
                # Активируем селектор таблиц
                if hasattr(self, "table_selector"):
                    self.table_selector.setEnabled(True)
                    self.refresh_table_list()
            else:
                self.db_status.setText("Статус: Внутренняя БД")
                self.db_status.setStyleSheet("color: green; font-weight: bold;")
                # Деактивируем селектор таблиц
                if hasattr(self, "table_selector"):
                    self.table_selector.setEnabled(False)
                    self.table_selector.clear()

    def refresh_table_list(self):
        """Обновление списка таблиц в селекторе"""
        if hasattr(self, "table_selector"):
            self.table_selector.clear()
            tables = self.db_connection.get_table_names()
            if tables:
                self.table_selector.addItems(tables)
                self.show_status_message(f"Найдено таблиц: {len(tables)}")
            else:
                self.show_status_message("Таблицы не найдены")

    def view_selected_table(self):
        """Просмотр выбранной таблицы из БД"""
        if not hasattr(self, "table_selector") or not self.table_selector.currentText():
            self.show_error("Выберите таблицу для просмотра")
            return

        table_name = self.table_selector.currentText()

        # Выполняем запрос для получения данных таблицы
        query = f"SELECT * FROM {table_name} LIMIT 1000"  # Ограничиваем для производительности

        success, result = self.db_connection.execute_query(query)
        if success:
            self.current_data = result
            self.display_data(result)
            self.update_column_selectors()
            self.show_status_message(
                f"Загружена таблица '{table_name}': {len(result)} строк"
            )

            # Переключаемся на вкладку данных
            self.tabs.setCurrentIndex(0)
        else:
            self.show_error(f"Ошибка загрузки таблицы: {result}")

    def connect_sqlite(self):
        """Подключение к SQLite"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите SQLite файл", "", "SQLite Files (*.db *.sqlite *.sqlite3)"
        )
        if file_path:
            success, message = self.db_connection.connect_sqlite(file_path)
            if success:
                self.update_tables_info()
                self.update_db_status()
                self.show_message(message)
            else:
                self.show_error(message)

    def connect_mysql(self):
        """Подключение к MySQL"""
        host = self.mysql_host.text()
        port = self.mysql_port.text()
        user = self.mysql_user.text()
        password = self.mysql_password.text()
        database = self.mysql_database.text()

        if not all([host, port, user, database]):
            self.show_error("Заполните все поля для подключения к MySQL")
            return

        success, message = self.db_connection.connect_mysql(
            host, port, user, password, database
        )
        if success:
            self.update_tables_info()
            self.update_db_status()
            self.show_message(message)
            self.show_status_message("Подключено к MySQL")
        else:
            self.show_error(message)

    def execute_sql(self):
        """Выполнение SQL запроса"""
        query = self.sql_input.toPlainText().strip()
        if not query:
            self.show_error("Введите SQL запрос")
            return

        success, result = self.db_connection.execute_query(query)
        if success:
            # Сохраняем результат для возможного экспорта
            self.last_sql_result = result

            # Обновляем текущие данные только если это SELECT запрос
            if query.strip().upper().startswith("SELECT"):
                self.current_data = result
                self.update_column_selectors()

            self.display_data_in_table(result, self.sql_result_table)
            self.show_message(f"Запрос выполнен: {len(result)} строк")
            self.show_status_message(f"SQL запрос выполнен: {len(result)} строк")
        else:
            self.show_error(result)
            self.show_status_message("Ошибка выполнения SQL запроса")

    def display_data(self, data):
        """Отображение данных в таблице"""
        self.display_data_in_table(data, self.data_table)

        # Показ информации о данных
        info = f"Размер: {data.shape[0]} строк, {data.shape[1]} колонок\n"
        info += f"Колонки: {', '.join(data.columns.tolist())}\n"
        info += f"Типы данных:\n{data.dtypes.to_string()}"
        self.data_info.setText(info)

    def display_data_in_table(self, data, table_widget):
        """Отображение данных в указанной таблице"""
        table_widget.setRowCount(
            min(len(data), 1000)
        )  # Ограничиваем до 1000 строк для производительности
        table_widget.setColumnCount(len(data.columns))
        table_widget.setHorizontalHeaderLabels(data.columns.tolist())

        for i in range(min(len(data), 1000)):
            for j, column in enumerate(data.columns):
                item = QTableWidgetItem(str(data.iloc[i, j]))
                table_widget.setItem(i, j, item)

        # Автоматическая настройка ширины колонок
        table_widget.resizeColumnsToContents()

        # Дополнительная настройка: устанавливаем минимальную и максимальную ширину
        header = table_widget.horizontalHeader()
        for i in range(table_widget.columnCount()):
            # Получаем текущую ширину после resizeColumnsToContents
            current_width = table_widget.columnWidth(i)

            # Устанавливаем минимальную ширину (80 пикселей)
            min_width = 80
            # Устанавливаем максимальную ширину (300 пикселей для больших данных)
            max_width = 300

            # Применяем ограничения
            if current_width < min_width:
                table_widget.setColumnWidth(i, min_width)
            elif current_width > max_width:
                table_widget.setColumnWidth(i, max_width)

        # Настраиваем поведение заголовков колонок
        header.setSectionResizeMode(header.ResizeMode.Interactive)

    def update_column_selectors(self):
        """Обновление списков колонок для графиков"""
        if self.current_data is not None:
            columns = self.current_data.columns.tolist()

            self.x_column.clear()
            self.y_column.clear()

            self.x_column.addItems(columns)
            self.y_column.addItems(columns)

    def create_plot(self):
        """Создание графика"""
        if self.current_data is None:
            self.show_error("Сначала загрузите данные")
            return

        plot_type = self.plot_type.currentText()
        x_col = self.x_column.currentText()
        y_col = self.y_column.currentText()

        if not x_col:
            self.show_error("Выберите колонку для оси X")
            return

        try:
            if plot_type == "Гистограмма":
                self.plot_canvas.plot_histogram(self.current_data, x_col)
            elif plot_type == "Линейный":
                if not y_col:
                    self.show_error("Выберите колонку для оси Y")
                    return
                self.plot_canvas.plot_line(self.current_data, x_col, y_col)
            elif plot_type == "Точечный":
                if not y_col:
                    self.show_error("Выберите колонку для оси Y")
                    return
                self.plot_canvas.plot_scatter(self.current_data, x_col, y_col)
            elif plot_type == "Столбчатый":
                if not y_col:
                    self.show_error("Выберите колонку для оси Y")
                    return
                # Для столбчатого графика группируем данные
                grouped_data = (
                    self.current_data.groupby(x_col)[y_col].mean().reset_index()
                )
                self.plot_canvas.plot_bar(grouped_data, x_col, y_col)
                self.show_status_message(f"График построен: {plot_type}")
        except Exception as e:
            self.show_error(f"Ошибка построения графика: {str(e)}")
            self.show_status_message("Ошибка построения графика")

    def show_message(self, message):
        """Показ сообщения"""
        QMessageBox.information(self, "Информация", message)

    def show_error(self, error):
        """Показ ошибки"""
        QMessageBox.critical(self, "Ошибка", error)

    def show_status_message(self, message):
        """Показ сообщения в статус баре"""
        if hasattr(self, "status_bar"):
            self.status_bar.showMessage(message, 5000)  # Показываем на 5 секунд

    def clear_sql_result(self):
        """Очистка результата SQL запроса"""
        self.sql_result_table.setRowCount(0)
        self.sql_result_table.setColumnCount(0)
        self.last_sql_result = None
        self.show_status_message("Результат SQL запроса очищен")

    def clear_plot(self):
        """Очистка канвы графика"""
        self.plot_canvas.fig.clear()
        self.plot_canvas.draw()
        self.show_status_message("График очищен")


def main():
    """Главная функция"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")  # Современный стиль

    window = DatasetAnalyzer()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
