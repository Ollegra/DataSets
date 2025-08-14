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
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont
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

            # Сохраняем данные в таблицу
            data.to_sql(
                table_name, self.internal_engine, if_exists="replace", index=False
            )
            self.current_table_name = table_name
            return True, f"Данные загружены в таблицу '{table_name}'"
        except Exception as e:
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
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                padding: 0 3px;
                color: blue;
                font-weight: bold;
                font-size: 14px;
            }
            QTabWidget::pane { /* The tab widget frame */
                border-top: 2px solid #C2C7CB;
            }
            QTabWidget::tab-bar {
                left: 5px; /* move to the right by 5px */
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
        btn_csv = QPushButton("Загрузить CSV")
        btn_csv.clicked.connect(self.load_csv)
        file_layout.addWidget(btn_csv)

        btn_excel = QPushButton("Загрузить Excel")
        btn_excel.clicked.connect(self.load_excel)
        file_layout.addWidget(btn_excel)

        if DBF_AVAILABLE:
            btn_dbf = QPushButton("Загрузить DBF")
            btn_dbf.clicked.connect(self.load_dbf)
            file_layout.addWidget(btn_dbf)

        # Кнопка очистки данных в группе файлов
        btn_clear = QPushButton("Очистить внутренние данные")
        btn_clear.clicked.connect(self.clear_internal_data)
        btn_clear.setStyleSheet("color: blue;")
        file_layout.addWidget(btn_clear)

        top_container.addWidget(file_group)

        # Группа подключения к БД
        db_group = QGroupBox("Подключение к базе данных")
        db_layout = QGridLayout(db_group)

        # Управление данными
        btn_disconnect = QPushButton("Отключить внешнюю БД")
        btn_disconnect.clicked.connect(self.disconnect_external_db)
        btn_disconnect.setStyleSheet("color: blue;")
        db_layout.addWidget(btn_disconnect, 0, 0, 1, 2)

        # SQLite
        btn_sqlite = QPushButton("Подключить SQLite")
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

        btn_mysql = QPushButton("Подключить MySQL")
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

        btn_sqlserver = QPushButton("Подключить SQL Server")
        btn_sqlserver.clicked.connect(self.connect_sqlserver)
        db_layout.addWidget(btn_sqlserver, 8, 0, 1, 2)

        # Информация о подключении
        self.db_status = QLabel("Статус: Внутренняя БД готова")
        self.db_status.setStyleSheet("color: green; font-weight: bold;")
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

        btn_refresh_tables = QPushButton("Обновить список")
        btn_refresh_tables.clicked.connect(self.refresh_table_list)
        table_layout.addWidget(btn_refresh_tables, 0, 2)

        btn_view_table = QPushButton("Просмотреть таблицу")
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

        btn_export_current = QPushButton("Экспорт текущих данных")
        btn_export_current.clicked.connect(self.export_current_data)
        export_layout.addWidget(btn_export_current, 1, 0, 1, 2)

        btn_export_sql_result = QPushButton("Экспорт результата SQL")
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

        btn_execute = QPushButton("Выполнить запрос")
        btn_execute.clicked.connect(self.execute_sql)
        btn_layout.addWidget(btn_execute)

        btn_export_csv = QPushButton("Экспорт в CSV")
        btn_export_csv.clicked.connect(self.export_sql_result_to_csv)
        btn_layout.addWidget(btn_export_csv)

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
        btn_plot = QPushButton("Построить график")
        btn_plot.clicked.connect(self.create_plot)
        control_layout.addWidget(btn_plot)

        control_layout.addStretch()

        # Canvas для графика
        self.plot_canvas = PlotCanvas(self, width=8, height=6)

        layout.addWidget(control_panel)
        layout.addWidget(self.plot_canvas)

        self.tabs.addTab(plot_widget, "Графики")

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


def main():
    """Главная функция"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")  # Современный стиль

    window = DatasetAnalyzer()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
