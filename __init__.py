# -*- coding: utf-8 -*-

# =============================================================================
# Mod Organizer 2 - Nexus Mods 依赖分析器
# 版本: 8.0.0
# 作者: Renil <renil@foxmail.com> & Gemini AI
#
# 描述:
#   一个功能强大的Mod Organizer 2插件，旨在帮助用户管理复杂的模组依赖关系。
#   它能够分析单个模组的依赖树，为整个模组列表生成建议的加载顺序，
#   以及查找已安装模组所缺失的翻译。
#
# --- v8.0.0 更新日志 ---
#   - [新功能] 新增HTML报告导出功能，可将完整分析结果保存为独立的HTML文件。
#   - [核心重构] 彻底重写了排序修正逻辑，引入“最小破坏”原则：
#     - 在修正排序问题时，插件会计算每个可能移动方案对整个加载顺序的影响。
#     - 优先选择能够解决问题且对其他模组依赖关系破坏最小的移动方案。
#   - [核心重构] 排序修正逻辑现在能够感知“分隔符”，会优先在模组所在的分隔符内部进行移动，
#     尽可能地保留用户原有的模组组织结构。
#   - [改进] 循环依赖的处理更加稳健，新的排序算法能更好地找到一个全局的次优解，
#     避免在修正后再次出现问题。
#   - [UI] 诊断报告中现在会显示问题模组所在的分隔符，方便定位。
# =============================================================================

from __future__ import annotations
import os
import sys
import json
import time
import re
import logging
import heapq
import threading
import queue
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set, Any, Optional, Tuple, Callable

# 尝试导入必要的库
try:
    import mobase
    from PyQt6.QtWidgets import (
        QApplication, QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
        QMessageBox, QGroupBox, QPlainTextEdit, QProgressBar, QTreeWidget,
        QTreeWidgetItem, QTabWidget, QWidget, QStyle, QHeaderView,
        QSplitter, QAbstractItemView, QTableWidget, QTableWidgetItem, QListWidget,
        QListWidgetItem, QInputDialog, QDialogButtonBox, QFileDialog, QScrollArea, QMenu,
        QCheckBox, QTreeWidgetItemIterator, QComboBox, QSpinBox, QFormLayout
    )
    from PyQt6.QtCore import QObject, QThread, pyqtSignal, Qt, QUrl, pyqtSlot, QTimer, QSize, QEvent
    from PyQt6.QtGui import QIcon, QTextCursor, QColor, QBrush, QDesktopServices, QPixmap, QPainter, QCursor, QAction, QKeySequence, QFont
except ImportError:
    # 在MO2环境外运行时提供桩代码
    print("Mobase or PyQt6 not found. This script must be run within Mod Organizer 2.")
    class QObject: pass
    class QDialog: pass
    class QTimer: pass
    class mobase:
        class IPluginTool: pass
    class QApplication:
        @staticmethod
        def translate(context: str, text: str) -> str: return text
        @staticmethod
        def processEvents(): pass
        @staticmethod
        def clipboard():
            class Clipboard:
                def setText(self, text): pass
            return Clipboard()

# 将插件自带的库目录添加到Python路径中
libs_path = os.path.join(os.path.dirname(__file__), "libs")
dlls_path = os.path.join(os.path.dirname(__file__), "dlls")
if libs_path not in sys.path:
    sys.path.insert(0, libs_path)
if os.path.exists(dlls_path):
    os.add_dll_directory(dlls_path)

# --- 核心依赖导入 ---
try:
    import sqlite3
    from lxml import html as lxml_html
    import patchright.sync_api as playwright
    from patchright.sync_api import TimeoutError as PlaywrightTimeoutError
    import pytomlpp
    import orjson
    import graphviz
    DEPENDENCIES_MET = True
except ImportError:
    DEPENDENCIES_MET = False

# 设置日志记录器
log = logging.getLogger(__name__)

# 用于拦截非必要网络资源的正则表达式
RESOURCE_EXCLUSION_REGEX = re.compile(r"\.(css|jpg|jpeg|png|gif|svg|woff|woff2|ttf|ico)(\?.*)?$")
def block_unnecessary_requests(route):
    """Patchright路由处理函数，用于阻止加载图片、CSS等资源以提高速度"""
    if RESOURCE_EXCLUSION_REGEX.search(route.request.url):
        route.abort()
    else:
        route.continue_()

# =============================================================================
# 1. 配置管理
# =============================================================================
class PluginSettings:
    """集中管理所有配置项，并处理从MO2动态获取的路径。"""
    def __init__(self, organizer: mobase.IOrganizer, plugin_name: str):
        self._organizer = organizer
        self._plugin_name = plugin_name

        base_data_path = Path(organizer.pluginDataPath())
        self.BASE_DIR = base_data_path / 'dep_analysis'
        os.makedirs(self.BASE_DIR, exist_ok=True)

        self.RULES_PATH = self.BASE_DIR / 'rules.toml'
        self.CACHE_DB_PATH = self.BASE_DIR / 'nexus_cache.sqlite'
        self.SETTINGS_PATH = self.BASE_DIR / 'settings.json'
        self.BROWSER_DATA_DIR = self.BASE_DIR / 'browser_data' 

        self._load_settings()

        self.GAME_NAME = organizer.managedGame().gameNexusName() or 'skyrimspecialedition'
        self.SANITIZED_GAME_NAME = re.sub(r'[^a-zA-Z0-9_]', '_', self.GAME_NAME)
        self.CACHE_TABLE_NAME = f"mod_cache_{self.SANITIZED_GAME_NAME}"
        self.NEXUS_BASE_URL = 'https://www.nexusmods.com'
        self.NEXUS_LOGIN_URL = 'https://users.nexusmods.com/auth/sign_in'
        self.NEXUS_SECURITY_URL = 'https://users.nexusmods.com/account/security'
        self.NEXUS_CONTENT_SETTINGS_URL = 'https://next.nexusmods.com/settings/content-blocking'
        
        self.CATEGORY_PRIORITIES = {
            "VR": 10, "Modders Resources": 10, "Utilities": 10, "Bug Fixes": 11, "User Interface": 15,
            "Gameplay": 20, "Immersion": 21, "Combat": 25, "Stealth": 26, "Skills and Leveling": 30,
            "Magic - Gameplay": 35, "Races, Classes, and Birthsigns": 36, "Guilds/Factions": 40,
            "Quests and Adventures": 50, "Locations - New": 51, "Dungeons": 52, "Creatures and Mounts": 55,
            "NPC": 58, "Followers & Companions": 59, "Weapons": 60, "Armour": 61, "Clothing and Accessories": 62,
            "Items and Objects - Player": 65, "Models and Textures": 70, "Visuals and Graphics": 71,
            "Environmental": 72, "Animation": 75, "Body, Face, and Hair": 78, "Audio": 80,
            "Presets - ENB and ReShade": 85, "Overhauls": 90, "Miscellaneous": 95, "Patches": 99, "Default": 50
        }

    def _get_default_settings(self) -> dict:
        """返回所有设置的默认值。"""
        return {
            "cache_expiration_days": 180,
            "request_timeout": 45000,
            "max_recursion_depth": 10,
            "request_delay_ms": 3000,
            "uninstalled_mod_fetch_depth": 2,
            "max_retries": 3,
            "retry_delay_ms": 3000,
            "max_workers": 4,
            "browser_type": "chrome",
            "headless": False,
            "log_level": "INFO",
            "block_resources": True,
            "blocked_extensions": "css,jpg,jpeg,png,gif,svg,woff,woff2,ttf,ico"
        }

    def _load_settings(self):
        """从 settings.json 加载配置，如果文件不存在则创建。"""
        defaults = self._get_default_settings()
        if not self.SETTINGS_PATH.exists():
            self.settings_data = defaults
            self._save_settings()
        else:
            try:
                with open(self.SETTINGS_PATH, 'r', encoding='utf-8') as f:
                    self.settings_data = json.load(f)
                # 确保所有默认键都存在
                for key, value in defaults.items():
                    self.settings_data.setdefault(key, value)
            except (json.JSONDecodeError, IOError):
                self.settings_data = defaults
        
        for key, value in self.settings_data.items():
            setattr(self, key.upper(), value)

    def _save_settings(self):
        """将当前的设置数据保存到文件。"""
        try:
            with open(self.SETTINGS_PATH, 'w', encoding='utf-8') as f:
                json.dump(self.settings_data, f, indent=4)
        except IOError as e:
            log.error(f"保存设置文件失败: {e}")

    def update_settings(self, new_settings: dict):
        """更新设置并保存。"""
        self.settings_data.update(new_settings)
        self._save_settings()
        self._load_settings()

# =============================================================================
# 2. Playwright 浏览器管理
# =============================================================================
class PlaywrightManager:
    """
    统一管理Playwright实例、浏览器上下文和页面。
    将用于数据抓取的主浏览器实例与用于用户登录的临时浏览器实例完全分离，以防止死锁。
    """
    def __init__(self, settings: PluginSettings, result_queue: queue.Queue):
        self.settings = settings
        self.result_queue = result_queue
        self.playwright_instance: Optional[playwright.Playwright] = None
        self.main_context: Optional[playwright.BrowserContext] = None
        self.main_page: Optional[playwright.Page] = None
        self._lock = threading.Lock()
        self.__tr = lambda text: QApplication.translate("PlaywrightManager", text)

    def _log(self, message: str, level: str = "info"):
        """向主线程发送日志消息。"""
        log_entry = f"[{level.upper()}] {message}"
        self.result_queue.put({'type': 'log', 'data': log_entry})
        if level == "error": log.error(message)
        elif level == "warning": log.warning(message)
        else: log.info(message)

    def start(self) -> bool:
        """启动Playwright服务。这是整个生命周期的第一步。"""
        with self._lock:
            if self.playwright_instance:
                return True
            try:
                self._log(self.__tr("正在启动Playwright服务..."))
                self.playwright_instance = playwright.sync_playwright().start()
                self._log(self.__tr("Playwright服务已启动。"))
                return True
            except Exception as e:
                self._log(self.__tr("启动Playwright服务失败: {error}").format(error=e), "critical")
                return False

    def stop(self):
        """停止Playwright服务并清理所有资源。这是插件关闭时的最后一步。"""
        with self._lock:
            self._internal_close_main_context()
            if self.playwright_instance:
                try:
                    self.playwright_instance.stop()
                    self._log(self.__tr("Playwright服务已停止。"))
                except Exception as e:
                    self._log(self.__tr("停止Playwright服务时出错: {error}").format(error=e), "warning")
                self.playwright_instance = None

    def _internal_close_main_context(self):
        """仅关闭用于数据抓取的主浏览器上下文。"""
        # 移除了外层的 "with self._lock:" 来避免死锁
        if self.main_context:
            try:
                # 在关闭前尝试导航到空白页，以中断可能导致挂起的脚本
                if self.main_page and not self.main_page.is_closed():
                    try:
                        self.main_page.goto("about:blank", timeout=1000)
                    except Exception:
                        pass # 忽略此处的超时等错误
                self.main_context.close()
                self._log(self.__tr("主浏览器上下文已关闭。"))
            except Exception as e:
                self._log(self.__tr("关闭主浏览器上下文时出错: {error}").format(error=e), "warning")
        self.main_context = None
        self.main_page = None
    def close_main_context(self):
        """仅关闭用于数据抓取的主浏览器上下文。这是一个线程安全的公共方法。"""
        with self._lock:
            self._internal_close_main_context()

    def get_page(self) -> Optional[playwright.Page]:
        """
        获取一个用于数据抓取的主页面。
        如果主浏览器实例不存在或已关闭，则会根据当前设置自动创建一个新的。
        """
        with self._lock:
            if self.main_page and not self.main_page.is_closed():
                return self.main_page

            if not self.playwright_instance:
                self._log(self.__tr("Playwright服务未运行，无法获取页面。"), "error")
                return None
            
            # 如果之前的上下文存在但已关闭，先清理
            if self.main_context:
                self._internal_close_main_context()

            try:
                mode_str = self.__tr("有头") if not self.settings.HEADLESS else self.__tr("无头")
                self._log(self.__tr("正在初始化主浏览器上下文 ({mode})...").format(mode=mode_str))
                self.main_context = self.playwright_instance.chromium.launch_persistent_context(
                    self.settings.BROWSER_DATA_DIR,
                    channel=self.settings.BROWSER_TYPE,
                    headless=self.settings.HEADLESS,
                    no_viewport=True,
                )
                
                self.main_page = self.main_context.pages[0] if self.main_context.pages else self.main_context.new_page()
                
                if self.settings.BLOCK_RESOURCES:
                    self._log(self.__tr("正在设置网络请求规则以优化速度..."))
                    extensions = self.settings.BLOCKED_EXTENSIONS.replace(" ", "").split(',')
                    regex_pattern = r"\.(" + "|".join(extensions) + r")(\?.*)?$"
                    self.main_page.route(re.compile(regex_pattern), block_unnecessary_requests)

                return self.main_page
            except Exception as e:
                self._log(self.__tr("初始化主浏览器上下文时发生严重错误: {error}").format(error=e), "critical")
                self.result_queue.put({'type': 'error', 'data': self.__tr("Playwright初始化失败，可能是相关文件损坏或被杀毒软件阻止。")})
                self.close_main_context()
                return None

    def perform_login(self):
        """
        在一个独立的临时浏览器实例中执行登录流程。
        此操作现在是可中断的。
        """
        if not self.playwright_instance:
            self._log(self.__tr("Playwright服务未运行，无法登录。"), "error")
            self.result_queue.put({'type': 'login_complete', 'data': False})
            return
        self.close_main_context()
        self._log(self.__tr("正在启动一个独立的浏览器用于登录..."))

        # 获取 stop_event
        stop_event = threading.Event()
        current_thread = threading.current_thread()
        if isinstance(current_thread, WorkerThread):
            stop_event = current_thread.stop_event

        login_context = None
        try:
            login_context = self.playwright_instance.chromium.launch_persistent_context(
                self.settings.BROWSER_DATA_DIR,
                channel=self.settings.BROWSER_TYPE,
                headless=False,
                no_viewport=True,
            )

            page = login_context.pages[0] if login_context.pages else login_context.new_page()

            self.result_queue.put({'type': 'log', 'data': "--------------------------------------------------"})
            self.result_queue.put({'type': 'log', 'data': self.__tr("请在弹出的浏览器窗口中登录Nexus Mods。")})
            self.result_queue.put({'type': 'log', 'data': self.__tr("登录成功后，请手动关闭该浏览器窗口以继续。")})
            self.result_queue.put({'type': 'log', 'data': self.__tr("您可以点击“停止”按钮来随时中止登录。")}) # 新增提示
            self.result_queue.put({'type': 'log', 'data': "--------------------------------------------------"})

            login_page_url = f"{self.settings.NEXUS_LOGIN_URL}?redirect_url={self.settings.NEXUS_BASE_URL}"
            page.goto(login_page_url)

            user_closed_window = False
            while not stop_event.is_set():
                try:
                    # 等待500毫秒，如果窗口关闭事件发生，则会立即返回
                    login_context.wait_for_event('close', timeout=500)
                    # 如果没有超时，说明是用户关闭了窗口
                    user_closed_window = True
                    self._log(self.__tr("登录浏览器已由用户关闭。"))
                    break # 退出轮询
                except PlaywrightTimeoutError:
                    # 超时是正常现象，继续下一次循环检查 stop_event
                    continue
                
            if stop_event.is_set():
                self._log(self.__tr("登录操作已被用户中止。"), "warning")
                # 主动关闭浏览器
                if login_context:
                    login_context.close()
                self.result_queue.put({'type': 'login_complete', 'data': True})
            elif user_closed_window:
                self._log(self.__tr("登录流程结束。"))
                self.result_queue.put({'type': 'login_complete', 'data': True})
            else: # 理论上不会发生，作为保险
                self.result_queue.put({'type': 'login_complete', 'data': False})

        except Exception as e:
            self._log(self.__tr("登录流程中发生错误: {error}").format(error=e), "critical")
            self.result_queue.put({'type': 'error', 'data': self.__tr("登录失败，请检查日志。")})
            self.result_queue.put({'type': 'login_complete', 'data': False})
        finally:
            # 确保临时的登录浏览器上下文总是被关闭
            if login_context:
                try:
                    login_context.close()
                except Exception:
                    pass # 可能已经关闭

    def restart_browser_for_settings_change(self):
        """为应用新设置而重启主浏览器。"""
        self._log(self.__tr("检测到设置变更，正在重启主浏览器..."))
        self.close_main_context()
        # 下一次 get_page() 调用时将自动使用新设置重新创建浏览器
        self.result_queue.put({'type': 'browser_restarted', 'data': None})

# =============================================================================
# 3. 核心分析逻辑
# =============================================================================
class ModAnalyzer:
    """封装了所有与数据抓取、解析和依赖关系计算相关的功能。"""
    def __init__(self, organizer: mobase.IOrganizer, plugin_name: str, result_queue: queue.Queue, playwright_manager: PlaywrightManager, stop_event: threading.Event):
        self.organizer = organizer
        self.plugin_name = plugin_name
        self.settings = PluginSettings(organizer, plugin_name)
        self.result_queue = result_queue
        self.playwright_manager = playwright_manager
        self._stop_requested = stop_event
        self.__tr = lambda text: QApplication.translate("ModAnalyzer", text)

        self.ignore_ids: Set[str] = set()
        self.replacement_map: Dict[str, str] = {}
        self.ignore_requirements_of_ids: Set[str] = set()
        self.folder_to_id: Dict[str, str] = {}
        self.id_to_folders: Dict[str, List[str]] = defaultdict(list)
        self.installed_ids: Set[str] = set()
        
        self.conn: Optional[sqlite3.Connection] = None
        
        self.initialize()

    def _put_result(self, type: str, data: Any):
        self.result_queue.put({'type': type, 'data': data})

    def log(self, message: str, level: str = "info"):
        level_map = {"INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}
        current_log_level = level_map.get(self.settings.LOG_LEVEL, logging.INFO)
        message_level = level_map.get(level.upper(), logging.INFO)

        if message_level >= current_log_level:
            log_entry = f"[{level.upper()}] {message}"
            self._put_result('log', log_entry)
        
        if message_level == logging.ERROR: log.error(message)
        elif message_level == logging.WARNING: log.warning(message)
        else: log.info(message)

    def request_stop(self):
        self._stop_requested.set()
        self.log(self.__tr("已收到停止信号，正在中止当前操作..."))

    def initialize(self):
        """初始化分析器，加载配置和数据。"""
        self.log(self.__tr("正在初始化分析器..."))
        self._init_db()
        self._load_rules()
        self._parse_installed_mods()
        self.log(self.__tr("分析器初始化完成。"))

    def _init_db(self):
        """初始化SQLite数据库连接和表结构。"""
        try:
            self.conn = sqlite3.connect(self.settings.CACHE_DB_PATH, check_same_thread=False)
            self.conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.settings.CACHE_TABLE_NAME} (
                    mod_id TEXT PRIMARY KEY, name TEXT, category TEXT, 
                    update_timestamp INTEGER, cache_timestamp TEXT, data TEXT
                )
            ''')
            self.conn.commit()
            self.log(self.__tr("缓存数据库 '{table}' 初始化成功。").format(table=self.settings.CACHE_TABLE_NAME))
        except Exception as e:
            self.log(self.__tr("初始化缓存数据库时出错: {error}").format(error=e), "error")
            self.conn = None

    def is_browser_ready(self) -> bool:
        return self.playwright_manager.main_page is not None and not self.playwright_manager.main_page.is_closed()

    def initialize_browser_and_check_login(self):
        """初始化或获取一个浏览器页面并检查登录状态。"""
        self.log(self.__tr("正在初始化浏览器并检查登录状态..."))
        page = self.playwright_manager.get_page()
        if not page:
            self._put_result('browser_ready', False)
            return

        try:
            self.log(self.__tr("正在访问Nexus Mods账户页面以验证登录..."))
            page.goto(self.settings.NEXUS_SECURITY_URL, wait_until='domcontentloaded', timeout=self.settings.REQUEST_TIMEOUT)
            
            is_logged_in = self.settings.NEXUS_LOGIN_URL not in page.url
            
            if is_logged_in:
                page.goto(self.settings.NEXUS_BASE_URL)
                self.log(self.__tr("验证成功，已登录Nexus Mods。"))
                self._put_result('login_status', {'success': True})
            else:
                page.goto(self.settings.NEXUS_BASE_URL)
                self.log(self.__tr("未登录Nexus Mods。部分模组（如成人内容）可能无法抓取。"), "warning")
                self._put_result('login_status', {'success': False})

            self._put_result('browser_ready', True)

        except Exception as e:
            self.log(self.__tr("检查登录状态时出错: {error}").format(error=e), "error")
            self.playwright_manager.close_main_context()
            self._put_result('browser_ready', False)
    
    def _check_before_analysis(self, analysis_type_for_error: str) -> bool:
        """在执行任何分析任务前检查浏览器状态。"""
        if not self.is_browser_ready():
            self.log("浏览器未就绪，无法开始分析。请尝试重启插件或在设置中重新登录。", "error")
            self._put_result('error', "浏览器未就绪，请先在设置中登录或检查日志。")
            self._put_result('analysis_complete', {"type": analysis_type_for_error, "data": None})
            return False
        return True

    def analyze_single_mod_dependencies(self, initial_mod_id: str, hide_vr: bool, hide_optional: bool, hide_recommended: bool):
        if not self._check_before_analysis("single_mod"): return
        try:
            self._parse_installed_mods()
            self._put_result('progress', (0, 0, self.__tr("开始分析单个模组...")))
            root_data = self._build_dependency_tree(initial_mod_id, set(), 0, hide_vr, hide_optional, hide_recommended)
            if not self._stop_requested.is_set():
                self._put_result('progress', (1, 1, self.__tr("分析完成。")))
            else:
                self._put_result('progress', (1, 1, self.__tr("分析已中止。")))
            self._put_result('analysis_complete', {"type": "single_mod", "data": root_data})
        except Exception as e:
            self.log(f"分析单个模组时出错: {e}", "error")
            self._put_result('error', str(e))

    def generate_dependency_graph(self, initial_mod_id: str, hide_vr: bool, hide_optional: bool, hide_recommended: bool):
        if not self._check_before_analysis("graph"): return
        try:
            self._parse_installed_mods()
            self._put_result('progress', (0, 0, self.__tr("开始生成依赖关系图...")))

            tree_data = self._build_dependency_tree(initial_mod_id, set(), 0, hide_vr, hide_optional, hide_recommended)
            if self._stop_requested.is_set() or not tree_data:
                self._put_result('analysis_complete', {"type": "graph", "data": None})
                return

            font_name = self._find_chinese_font()
            
            dot = graphviz.Digraph(
                comment=f'Mod ID {initial_mod_id} 的依赖关系图',
                graph_attr={'fontname': font_name},
                node_attr={'fontname': font_name, 'shape': 'box', 'style': 'rounded'},
                edge_attr={'fontname': font_name}
            )
            
            self._add_nodes_to_graph(dot, tree_data)

            svg_data = dot.pipe(format='svg')
            dot_source = dot.source
            
            if not self._stop_requested.is_set():
                self._put_result('progress', (1, 1, self.__tr("关系图生成完成。")))
            self._put_result('analysis_complete', {"type": "graph", "data": {"svg_data": svg_data, "dot_source": dot_source}})

        except ImportError:
            self.log("Graphviz库未安装。", "error")
            self._put_result('error', self.__tr("Graphviz库未安装，无法生成关系图。"))
        except graphviz.backend.ExecutableNotFound:
            self.log("未找到Graphviz可执行程序。", "error")
            self._put_result('error', self.__tr("未找到Graphviz可执行程序。请确保已安装Graphviz并将其添加至系统PATH。"))
        except Exception as e:
            self.log(f"生成关系图时出错: {e}", "error")
            self._put_result('error', str(e))

    def _add_nodes_to_graph(self, dot: graphviz.Digraph, node_data: Dict):
        """递归地将节点和边添加到Graphviz图中"""
        if not node_data: return
        
        node_id = node_data['id']
        node_label = f"{node_data.get('name', '未知')}\nID: {node_id}"
        
        fillcolor = "lightgrey"
        status = node_data.get('status')
        if status == 'satisfied': fillcolor = "lightgreen"
        elif status == 'missing': fillcolor = "lightcoral"
        elif status == 'ignored': fillcolor = "lightblue"
        elif status == 'cycle': fillcolor = "orange"
        elif status == 'truncated': fillcolor = "#8e44ad"

        dot.node(node_id, node_label, fillcolor=fillcolor, style='filled')

        for child in node_data.get('children', []):
            child_id = child['id']
            self._add_nodes_to_graph(dot, child)
            dot.edge(node_id, child_id)

    def generate_sorted_load_order(self, run_diagnosis: bool):
        if not self._check_before_analysis("full_analysis"): return
        try:
            self._parse_installed_mods()
            if not self.folder_to_id:
                self.log(self.__tr("未找到任何带有效ID的已启用模组，无法生成分析报告。"), "error")
                self._put_result('analysis_complete', {"type": "full_analysis", "data": {"error": "no_mods"}})
                return
            
            known_mod_data, result, mod_tags = self._build_full_dependency_network()
            if self._stop_requested.is_set(): 
                self._put_result('analysis_complete', {"type": "full_analysis", "data": {}})
                return
            
            full_graph, reverse_graph, all_nodes = result
            missing_report = self._identify_missing_dependencies(all_nodes, reverse_graph, known_mod_data)
            if self._stop_requested.is_set(): 
                self._put_result('analysis_complete', {"type": "full_analysis", "data": {}})
                return

            sorted_order, cyclic_nodes = self._perform_topological_sort(full_graph, known_mod_data)
            
            load_order_problems = []
            if run_diagnosis:
                load_order_problems = self._analyze_current_load_order(full_graph)

            if not self._stop_requested.is_set():
                self._put_result('analysis_complete', {"type": "full_analysis", "data": {
                    "sorted_order": sorted_order, "broken_cycle_nodes": cyclic_nodes,
                    "missing_report": missing_report, "full_graph": full_graph,
                    "mod_tags": mod_tags, "load_order_problems": load_order_problems
                }})
            else:
                 self._put_result('analysis_complete', {"type": "full_analysis", "data": {}})
        except Exception as e:
            self.log(f"生成分析报告时出错: {e}", "error")
            self._put_result('error', str(e))

    def find_missing_translations(self, language_query: str, show_original_update_time: bool):
        """扫描已安装的模组以查找缺失的翻译。"""
        if not self._check_before_analysis("translations"): return
        try:
            self._parse_installed_mods()
            total_mods = len(self.installed_ids)
            self._put_result('progress', (0, total_mods, self.__tr("开始扫描翻译...")))
            
            results = defaultdict(lambda: {'name': '', 'update_timestamp': 0, 'translations': []})
            lang_lower = language_query.lower()

            for i, mod_id in enumerate(list(self.installed_ids)):
                if self._stop_requested.is_set():
                    self._put_result('analysis_complete', {"type": "translations", "data": {}})
                    return

                mod_data = self.get_mod_data(mod_id)
                if not mod_data: continue
                mod_name = mod_data.get('name', f"ID {mod_id}")
                self._put_result('progress', (i + 1, total_mods, f"{self.__tr('正在检查')}: {mod_name}"))

                translations_on_page = mod_data.get("translations", [])
                if not translations_on_page: continue

                all_trans_ids = {t['id'] for t in translations_on_page if t.get('id')}
                if not all_trans_ids.isdisjoint(self.installed_ids):
                    self.log(self.__tr("检测到 '{mod_name}' 的一个翻译版本已安装，跳过。").format(mod_name=mod_name))
                    continue

                found_translations_for_this_mod = []
                for trans_info in translations_on_page:
                    if self._stop_requested.is_set(): break
                    trans_id = trans_info.get('id')
                    if trans_id and lang_lower in trans_info.get('language', '').lower():
                        trans_mod_data = self.get_mod_data(trans_id)
                        if not trans_mod_data: continue
                        
                        update_timestamp = trans_mod_data.get('update_timestamp', 0)
                        found_translations_for_this_mod.append({
                            'id': trans_id,
                            'name': trans_mod_data.get('name', trans_info.get('name', '')),
                            'language': trans_info.get('language', ''), 
                            'update_timestamp': update_timestamp
                        })
                
                if found_translations_for_this_mod:
                    results[mod_id]['name'] = mod_name
                    if show_original_update_time:
                        results[mod_id]['update_timestamp'] = mod_data.get('update_timestamp', 0)
                    results[mod_id]['translations'].extend(found_translations_for_this_mod)

                if self._stop_requested.is_set(): break
            
            if not self._stop_requested.is_set():
                self._put_result('progress', (total_mods, total_mods, self.__tr("扫描完成。")))

            self._put_result('analysis_complete', {"type": "translations", "data": results})
        except Exception as e:
            self.log(f"查找翻译时出错: {e}", "error")
            self._put_result('error', str(e))

    def delete_cache_entries(self, items_to_delete: List[Dict]):
        if not self.conn: return
        self.log(self.__tr("收到删除 {count} 个缓存条目的请求...").format(count=len(items_to_delete)))
        ids_to_delete = [item['id'] for item in items_to_delete if item['id'] != 'N/A']
        
        try:
            cursor = self.conn.cursor()
            placeholders = ','.join('?' for _ in ids_to_delete)
            cursor.execute(f"DELETE FROM {self.settings.CACHE_TABLE_NAME} WHERE mod_id IN ({placeholders})", ids_to_delete)
            self.conn.commit()
            self.log(self.__tr("已从缓存中删除 {count} 个条目。").format(count=cursor.rowcount))
        except Exception as e:
            self.log(f"删除缓存时出错: {e}", "error")

        self._put_result('analysis_complete', {"type": "cache_deleted", "data": self.get_all_cache_data()})

    def clear_cache(self):
        if not self.conn: return
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DELETE FROM {self.settings.CACHE_TABLE_NAME}")
            self.conn.commit()
            self.log(self.__tr("已清空当前游戏的所有缓存。"))
        except Exception as e:
            self.log(f"清空缓存时出错: {e}", "error")
        self._put_result('analysis_complete', {"type": "cache_cleared", "data": None})

    def get_and_send_cache_data(self):
        all_data = self.get_all_cache_data()
        self._put_result('cache_data_ready', all_data)
        
    def update_and_save_rules(self, new_rules_data: dict):
        try:
            with open(self.settings.RULES_PATH, 'w', encoding='utf-8') as f:
                pytomlpp.dump(new_rules_data, f)
            self.log(self.__tr("规则已成功保存到 rules.toml。"))
            self._load_rules()
        except Exception as e:
            self.log(self.__tr("保存规则文件时出错: {error}").format(error=e), "error")
            self._put_result('error', self.__tr("保存规则失败，请检查日志。"))
    
    def add_ids_to_rule_list(self, list_name: str, section_name: str, ids_to_add: List[str]):
        """将给定的ID列表添加到rules.toml的指定列表中。"""
        try:
            if self.settings.RULES_PATH.exists():
                with open(self.settings.RULES_PATH, 'r', encoding='utf-8') as f:
                    rules_data = pytomlpp.load(f)
            else:
                rules_data = {}

            if section_name not in rules_data:
                rules_data[section_name] = {}
            if list_name not in rules_data[section_name]:
                rules_data[section_name][list_name] = []

            existing_ids = set(map(str, rules_data[section_name][list_name]))
            new_ids = [id_str for id_str in ids_to_add if id_str not in existing_ids]
            
            if not new_ids:
                self.log(self.__tr("所有待添加的ID已存在于规则 '{section}.{list}' 中，无需操作。").format(section=section_name, list=list_name))
                return

            rules_data[section_name][list_name].extend(new_ids)
            
            try:
                sorted_ids_as_int = sorted([int(i) for i in rules_data[section_name][list_name]])
                rules_data[section_name][list_name] = sorted_ids_as_int
            except ValueError:
                rules_data[section_name][list_name].sort()

            with open(self.settings.RULES_PATH, 'w', encoding='utf-8') as f:
                pytomlpp.dump(rules_data, f)
            
            self.log(self.__tr("已成功将 {count} 个新ID添加到规则 '{section}.{list}'。").format(count=len(new_ids), section=section_name, list=list_name))
            self._load_rules()

        except Exception as e:
            self.log(self.__tr("更新规则文件时出错: {error}").format(error=e), "error")
            self._put_result('error', self.__tr("更新规则失败，请检查日志。"))

    def update_and_save_settings(self, new_settings_data: dict):
        """接收新设置，更新并重新加载。"""
        try:
            self.settings.update_settings(new_settings_data)
            self.log(self.__tr("插件设置已成功保存并重新加载。"))
            self._put_result('settings_updated', True)
        except Exception as e:
            self.log(self.__tr("保存设置时出错: {error}").format(error=e), "error")
            self._put_result('error', self.__tr("保存设置失败，请检查日志。"))

    # --- 内部辅助方法 ---
    def _find_chinese_font(self) -> str:
        """返回一个常见的中文字体名称供Graphviz使用。"""
        if sys.platform == "win32":
            return "SimHei" # 黑体
        else:
            return "WenQuanYi Zen Hei" # 文泉驿正黑

    def _load_rules(self):
        if not self.settings.RULES_PATH.exists():
            self._create_default_rules_file()
        
        try:
            with open(self.settings.RULES_PATH, 'r', encoding='utf-8') as f:
                rules_data = pytomlpp.load(f)
            
            self.ignore_ids = set(str(i) for i in rules_data.get('Ignore', {}).get('ids', []))
            self.replacement_map = {str(k): str(v) for k, v in rules_data.get('Replace', {}).items()}
            self.ignore_requirements_of_ids = set(str(i) for i in rules_data.get('IgnoreRequirementsOf', {}).get('ids', []))
            self.log(self.__tr("成功加载 {ign} 条忽略, {rep} 条替换, {ign_req} 条前置忽略规则。").format(
                ign=len(self.ignore_ids), rep=len(self.replacement_map), ign_req=len(self.ignore_requirements_of_ids)))
        except Exception as e:
            self.log(self.__tr("解析规则文件时出错: {error}").format(error=e), "error")
            self.ignore_ids, self.replacement_map, self.ignore_requirements_of_ids = set(), {}, set()

    def _create_default_rules_file(self):
        default_rules = {
            'Ignore': {'#comment': self.__tr("在此列表中的模组ID将被分析器视为“已安装”或“已满足”。"), 'ids': [3863, 12604]},
            'Replace': {'#comment': self.__tr("格式为: '被替代的ID' = '替换为的ID' (注意：键值都应为字符串)"), '658': '97145'},
            'IgnoreRequirementsOf': {'#comment': self.__tr("在此列表中的已安装模组，其所有的前置要求都将被忽略。"), 'ids': []}
        }
        try:
            with open(self.settings.RULES_PATH, 'w', encoding='utf-8') as f:
                pytomlpp.dump(default_rules, f)
            self.log(self.__tr("已创建默认规则文件: rules.toml"))
        except IOError:
            self.log(self.__tr("创建规则文件模板失败！"), "error")

    def _parse_installed_mods(self):
        mod_list = self.organizer.modList()
        self.folder_to_id.clear(); self.id_to_folders.clear()
        for mod_name in mod_list.allModsByProfilePriority():
            mod = mod_list.getMod(mod_name)
            if mod and not mod.isSeparator() and mod.nexusId() > 0:
                mod_id = str(mod.nexusId())
                self.folder_to_id[mod_name] = mod_id
                self.id_to_folders[mod_id].append(mod_name)
        self.installed_ids = set(self.folder_to_id.values())
        self.log(self.__tr("已解析 {count} 个已安装的带ID的模组。").format(count=len(self.installed_ids)))

    def _is_cache_entry_valid(self, cache_timestamp: str) -> bool:
        if self.settings.CACHE_EXPIRATION_DAYS == 0: return True
        try:
            return datetime.now() - datetime.fromisoformat(cache_timestamp) < timedelta(days=self.settings.CACHE_EXPIRATION_DAYS)
        except (ValueError, TypeError): return False

    @staticmethod
    def _extract_mod_id_from_url(url: str) -> Optional[str]:
        if match := re.search(r'/mods/(\d+)', url): return match.group(1)
        return None

    def get_mod_data(self, mod_id: str) -> Optional[Dict[str, Any]]:
        if self._stop_requested.is_set(): return None
        if not self.conn: return None

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT cache_timestamp, data FROM {self.settings.CACHE_TABLE_NAME} WHERE mod_id = ?", (mod_id,))
        row = cursor.fetchone()
        if row:
            cache_timestamp, data_json = row
            if self._is_cache_entry_valid(cache_timestamp):
                self.log(self.__tr("缓存命中: ID {mod_id}").format(mod_id=mod_id))
                return orjson.loads(data_json)

        last_error = None
        for attempt in range(self.settings.MAX_RETRIES):
            if self._stop_requested.is_set(): return None
            try:
                if attempt > 0: 
                    self.log(self.__tr("抓取 {mod_id} 失败，在 {delay} 秒后重试 ({attempt}/{max_retries})...").format(
                        mod_id=mod_id, delay=self.settings.RETRY_DELAY_MS / 1000.0, attempt=attempt + 1, max_retries=self.settings.MAX_RETRIES))
                    time.sleep(self.settings.RETRY_DELAY_MS / 1000.0)
                else: 
                    time.sleep(self.settings.REQUEST_DELAY_MS / 1000.0)
                
                page = self.playwright_manager.get_page()
                if not page or page.is_closed():
                    raise ConnectionError("Playwright页面未初始化或已关闭。")

                url = f"{self.settings.NEXUS_BASE_URL}/{self.settings.GAME_NAME}/mods/{mod_id}"
                self.log(self.__tr("正在抓取: ID {mod_id}").format(mod_id=mod_id))
                page.goto(url, wait_until='domcontentloaded', timeout=self.settings.REQUEST_TIMEOUT)
                
                content = page.content()
                tree = lxml_html.fromstring(content)
                
                if notice_header := tree.xpath('//div[@class="info-content"]/h3[starts-with(@id, "Notice")]'):
                    notice_text = notice_header[0].text_content().strip()
                    self.log(self.__tr("模组 {mod_id} 不可用 ({reason})，跳过。").format(mod_id=mod_id, reason=notice_text), "warning")
                    
                    if notice_text == "Adult content":
                        self._put_result('adult_content_blocked', {'mod_id': mod_id})

                    mod_data = {"id": mod_id, "name": self.__tr("模组不可用 ({reason}) (ID: {mod_id})").format(reason=notice_text, mod_id=mod_id), "error": "unavailable", "category": "Default", "dependencies": {}, "translations": [], "update_timestamp": 0}
                    self._cache_mod_data(mod_data)
                    return mod_data

                page.wait_for_selector('#pagetitle > h1', timeout=15000)
                title_node = tree.xpath('//*[@id="pagetitle"]/h1')
                mod_name = title_node[0].text_content().strip() if title_node else self.__tr("未知模组名称")
                
                breadcrumb_nodes = tree.xpath('//ul[@id="breadcrumb"]/li/a')
                category = breadcrumb_nodes[-1].text_content().strip() if len(breadcrumb_nodes) > 1 else "Default"
                
                update_timestamp = 0
                if time_node := tree.xpath('//div[@id="fileinfo"]//div[h3[text()="Last updated"]]/time'):
                    if timestamp_str := time_node[0].get('data-date'):
                        update_timestamp = int(timestamp_str)

                def scrape_section(header_text: str) -> List[Dict[str, str]]:
                    deps = []
                    tables = tree.xpath(f'//h3[contains(text(), "{header_text}")]/following-sibling::table[1]')
                    if tables:
                        for row in tables[0].xpath('.//tbody/tr'):
                            if name_cell := row.xpath('.//td[@class="table-require-name"]/a'):
                                dep_url = name_cell[0].get('href', '')
                                full_dep_url = dep_url if dep_url.startswith('http') else self.settings.NEXUS_BASE_URL + dep_url
                                notes = (row.xpath('.//td[@class="table-require-notes"]')[0].text_content().strip() if row.xpath('.//td[@class="table-require-notes"]') else "")
                                deps.append({'name': name_cell[0].text_content().strip(), 'url': full_dep_url, 'notes': notes})
                    return deps
                
                def scrape_translations_section() -> List[Dict[str, str]]:
                    translations = []
                    if translation_dt := tree.xpath('//dt[contains(normalize-space(.), "Translations")]'):
                        if table := translation_dt[0].xpath('./following-sibling::dd[1]//table[contains(@class, "translation-table")]'):
                            for row in table[0].xpath('.//tbody/tr'):
                                if (lang_cell_link := row.xpath('.//td[@class="table-translation-name"]/a')) and (name_cell := row.xpath('.//td[@class="table-translation-notes"]/span')):
                                    trans_id = self._extract_mod_id_from_url(lang_cell_link[0].get('href', ''))
                                    trans_name = name_cell[0].text_content().strip()
                                    lang_name = lang_cell_link[0].text_content().strip()
                                    if trans_id and trans_name and lang_name:
                                        translations.append({'id': trans_id, 'name': trans_name, 'language': lang_name})
                    return translations

                mod_data = {"id": mod_id, "name": mod_name, "category": category, "update_timestamp": update_timestamp, "dependencies": {'requires': scrape_section('Nexus requirements'), 'required_by': scrape_section('Mods requiring this file')}, "translations": scrape_translations_section()}
                self.log(self.__tr("抓取成功: {mod_name} ({mod_id})").format(mod_name=mod_data['name'], mod_id=mod_id))
                self._cache_mod_data(mod_data)
                return mod_data
            
            except PlaywrightTimeoutError as e:
                last_error = e
                self.log(f"抓取 {mod_id} 时超时 (尝试 {attempt + 1}/{self.settings.MAX_RETRIES}): {e}", "warning")
                if attempt == self.settings.MAX_RETRIES - 1:
                    self._put_result('cloudflare_block_suspected', {'mod_id': mod_id})
            except Exception as e:
                last_error = e
                self.log(f"抓取 {mod_id} 失败 (尝试 {attempt + 1}/{self.settings.MAX_RETRIES}): {e}", "error")
        
        self.log(f"所有重试均失败，无法获取模组 {mod_id} 的数据。", "error")
        mod_data = {"id": mod_id, "name": f"抓取失败: ID {mod_id}", "error": str(last_error), "category": "Default", "dependencies": {}, "translations": [], "update_timestamp": 0}
        self._cache_mod_data(mod_data)
        return mod_data

    def _cache_mod_data(self, mod_data: Dict[str, Any]):
        """将单个模组数据写入SQLite数据库。"""
        if not self.conn: return
        try:
            cursor = self.conn.cursor()
            data_to_insert = (
                mod_data.get('id'),
                mod_data.get('name'),
                mod_data.get('category'),
                mod_data.get('update_timestamp'),
                datetime.now().isoformat(),
                orjson.dumps(mod_data)
            )
            cursor.execute(f'''
                INSERT OR REPLACE INTO {self.settings.CACHE_TABLE_NAME} (mod_id, name, category, update_timestamp, cache_timestamp, data)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            self.conn.commit()
        except Exception as e:
            self.log(f"缓存模组 {mod_data.get('id')} 到数据库时出错: {e}", "error")

    def _get_effective_id(self, required_id: str) -> str:
        return self.replacement_map.get(required_id, required_id)

    def _is_dependency_satisfied(self, required_id: str) -> bool:
        return self._get_effective_id(required_id) in self.installed_ids

    def _build_dependency_tree(self, mod_id: str, visited_ids: Set[str], depth: int, hide_vr: bool, hide_optional: bool, hide_recommended: bool) -> Optional[Dict]:
        if self._stop_requested.is_set(): return None
        if mod_id in self.ignore_ids: return None

        node_data = {"id": mod_id, "name": self.__tr("加载中..."), "children": []}
        node_data['is_installed'] = self._get_effective_id(mod_id) in self.installed_ids

        if mod_id in visited_ids:
            mod_data = self.get_mod_data(mod_id)
            if mod_data: node_data.update(mod_data)
            node_data["status"] = "cycle"
            return node_data
        
        visited_ids.add(mod_id)
        
        mod_data = self.get_mod_data(mod_id)
        if not mod_data or self._stop_requested.is_set(): return None
        
        node_data.update(mod_data)
        
        if mod_id != self._get_effective_id(mod_id):
            replacer_data = self.get_mod_data(self._get_effective_id(mod_id))
            if replacer_data: node_data['replacement_info'] = {"name": replacer_data.get('name', f'ID {self._get_effective_id(mod_id)}'), "id": self._get_effective_id(mod_id)}
        
        is_fully_satisfied = node_data['is_installed']
        
        if mod_data.get("error"):
            node_data['status'] = "missing"
            is_fully_satisfied = False

        if depth >= self.settings.MAX_RECURSION_DEPTH:
            node_data['status'] = "truncated"
            is_fully_satisfied = False
        elif not mod_data.get("error") and "dependencies" in mod_data:
            node_data['status'] = "satisfied" if node_data['is_installed'] else "missing"
            reqs = mod_data.get("dependencies", {}).get("requires", [])
            self._put_result('progress', (0, len(reqs), self.__tr("正在分析 {name} 的依赖...").format(name=node_data['name'])))
            node_data["children"] = []
            for i, req in enumerate(reqs):
                if self._stop_requested.is_set(): break
                
                notes_lower = req.get('notes', '').lower()
                is_vr_dep = 'vr' in notes_lower
                is_optional_dep = any(k in notes_lower for k in ['optional', 'addon', 'not mandatory', 'not needed', 'not required'])
                is_recommended_dep = 'recommend' in notes_lower

                if (hide_vr and is_vr_dep) or (hide_optional and is_optional_dep) or (hide_recommended and is_recommended_dep):
                    continue

                if req_id := self._extract_mod_id_from_url(req['url']):
                    child_data = self._build_dependency_tree(req_id, visited_ids.copy(), depth + 1, hide_vr, hide_optional, hide_recommended)
                    if child_data:
                        child_data['notes'] = req.get('notes', '')
                        node_data["children"].append(child_data)
                        if not child_data.get('is_fully_satisfied', False):
                            is_fully_satisfied = False
                self._put_result('progress', (i + 1, len(reqs), self.__tr("正在分析 {name} 的依赖...").format(name=node_data['name'])))
        
        node_data['is_fully_satisfied'] = is_fully_satisfied
        return node_data

    def _build_full_dependency_network(self) -> Tuple[Dict, Tuple, Dict]:
        self._put_result('progress', (0, 100, self.__tr("阶段 1: 抓取已安装模组...")))
        known_mod_data, mod_tags = {}, defaultdict(set)
        known_ids, next_layer = set(self.installed_ids), set()
        
        total_installed = len(self.installed_ids)
        for i, mod_id in enumerate(list(self.installed_ids)):
            if self._stop_requested.is_set(): return {}, (defaultdict(list), defaultdict(list), set()), {}
            mod_data = self.get_mod_data(mod_id)
            if mod_data:
                known_mod_data[mod_id] = mod_data
                self._put_result('full_analysis_mod_fetched', mod_data)
                if "dependencies" in mod_data and mod_id not in self.ignore_requirements_of_ids:
                    for req in mod_data.get("dependencies", {}).get("requires", []):
                        if req_id := self._extract_mod_id_from_url(req['url']):
                            if req_id not in known_ids: next_layer.add(req_id)
                            notes_lower = req.get('notes', '').lower()
                            if 'vr' in notes_lower: mod_tags[mod_id].add('vr')
                            if any(k in notes_lower for k in ['optional', 'addon', 'not mandatory', 'not needed', 'not required']): mod_tags[mod_id].add('optional')
                            if 'recommend' in notes_lower: mod_tags[mod_id].add('recommended')

            self._put_result('progress', (i + 1, total_installed, f"{self.__tr('抓取已安装模组')} {i+1}/{total_installed}"))
        
        for depth in range(self.settings.UNINSTALLED_MOD_FETCH_DEPTH):
            if self._stop_requested.is_set() or not next_layer: break
            current_layer, next_layer = next_layer, set()
            known_ids.update(current_layer)
            total_current = len(current_layer)
            self._put_result('progress', (0, total_current, f"{self.__tr('抓取第 {d} 层依赖...').format(d=depth + 1)}"))
            for i, mod_id in enumerate(list(current_layer)):
                if self._stop_requested.is_set(): break
                mod_data = self.get_mod_data(mod_id)
                if mod_data:
                    known_mod_data[mod_id] = mod_data
                    self._put_result('full_analysis_mod_fetched', mod_data)
                    if depth < self.settings.UNINSTALLED_MOD_FETCH_DEPTH - 1 and "dependencies" in mod_data:
                        for req in mod_data.get("dependencies", {}).get("requires", []):
                            if req_id := self._extract_mod_id_from_url(req['url']):
                                if req_id not in known_ids: next_layer.add(req_id)
                self._put_result('progress', (i + 1, total_current, f"{self.__tr('抓取第 {d} 层依赖').format(d=depth + 1)} {i+1}/{total_current}"))
        
        graph, reverse_graph = defaultdict(list), defaultdict(list)
        for mod_id, mod_data in known_mod_data.items():
            if "dependencies" in mod_data and mod_id not in self.ignore_requirements_of_ids:
                for req in mod_data.get("dependencies", {}).get("requires", []):
                    if req_id := self._extract_mod_id_from_url(req['url']):
                        notes = req.get('notes', '')
                        graph[mod_id].append({'id': req_id, 'notes': notes})
                        reverse_graph[req_id].append({'id': mod_id, 'notes': notes})
        self._put_result('progress', (100, 100, self.__tr("依赖网络构建完成。")))
        return known_mod_data, (graph, reverse_graph, known_ids), mod_tags

    def _identify_missing_dependencies(self, all_nodes: Set[str], reverse_graph: defaultdict, known_mod_data: Dict) -> Dict[str, Dict]:
        unmet = {nid for nid in all_nodes if not self._is_dependency_satisfied(nid) and nid not in self.ignore_ids}
        if not unmet: return {}
        report = {}
        for unmet_id in sorted(list(unmet)):
            req_by_installed, req_by_missing = [], set()
            for req_info in reverse_graph.get(unmet_id, []):
                req_by_id, notes = req_info['id'], req_info['notes']
                if req_by_id in self.ignore_requirements_of_ids: continue
                
                tags = set()
                notes_lower = notes.lower()
                if 'vr' in notes_lower: tags.add('vr')
                if any(k in notes_lower for k in ['optional', 'addon', 'not mandatory', 'not needed', 'not required']): tags.add('optional')
                if 'recommend' in notes_lower: tags.add('recommended')

                if req_by_id in self.installed_ids:
                    for folder in self.id_to_folders.get(req_by_id, []):
                        req_by_installed.append((folder, notes, tags))
                elif self._get_effective_id(req_by_id) in unmet:
                    req_by_missing.add((known_mod_data.get(req_by_id, {}).get("name", f"ID {req_by_id}"), req_by_id))
            
            if not req_by_installed and not req_by_missing: continue

            effective_id = self._get_effective_id(unmet_id)
            entry = {"name": known_mod_data.get(unmet_id, {}).get("name", f"ID {unmet_id}"), "required_by_installed": sorted(req_by_installed), "required_by_missing": sorted(list(req_by_missing)), "effective_id": effective_id}
            if unmet_id != effective_id: entry['effective_name'] = known_mod_data.get(effective_id, {}).get('name', f'ID {effective_id}')
            report[unmet_id] = entry
        return report

    def _perform_topological_sort(self, full_graph: defaultdict, known_mod_data: Dict) -> Tuple[List[str], List[str]]:
        self._put_result('progress', (0, 100, self.__tr("正在执行拓扑排序...")))
        graph, in_degree = defaultdict(list), {f: 0 for f in self.folder_to_id}
        
        for dep_folder, dep_id in self.folder_to_id.items():
            if dep_id in self.ignore_requirements_of_ids: continue
            for req_info in full_graph.get(dep_id, []):
                req_id = req_info['id']
                if not self._is_dependency_satisfied(req_id): continue
                actual_provider_id = self._get_effective_id(req_id)
                for provider_folder in self.id_to_folders.get(actual_provider_id, []):
                    if provider_folder in in_degree:
                        graph[provider_folder].append(dep_folder)
                        in_degree[dep_folder] += 1
        
        current_priority_map = {name: i for i, name in enumerate(self.organizer.modList().allModsByProfilePriority())}
        
        ready_queue = []
        for folder, degree in in_degree.items():
            if degree == 0:
                mod_id = self.folder_to_id[folder]
                category_id = known_mod_data.get(mod_id, {}).get("category", "Default")
                main_priority = self.settings.CATEGORY_PRIORITIES.get(category_id, 50)
                current_pos = current_priority_map.get(folder, 9999)
                heapq.heappush(ready_queue, (main_priority, current_pos, folder))
        
        sorted_order, broken_cycle_nodes = [], []
        while len(sorted_order) < len(self.folder_to_id):
            if self._stop_requested.is_set(): return [], []
            if ready_queue:
                main_priority, current_pos, u_folder = heapq.heappop(ready_queue)
                if u_folder in sorted_order: continue
                sorted_order.append(u_folder)
                for v_folder in graph.get(u_folder, []):
                    if in_degree.get(v_folder, 0) > 0:
                        in_degree[v_folder] -= 1
                        if in_degree[v_folder] == 0:
                            v_mod_id = self.folder_to_id[v_folder]
                            v_category_id = known_mod_data.get(v_mod_id, {}).get("category", "Default")
                            v_main_priority = self.settings.CATEGORY_PRIORITIES.get(v_category_id, 50)
                            v_current_pos = current_priority_map.get(v_folder, 9999)
                            heapq.heappush(ready_queue, (v_main_priority, v_current_pos, v_folder))
            else:
                remaining_nodes = {f for f, d in in_degree.items() if d > 0 and f not in sorted_order}
                if not remaining_nodes: break
                
                breaker_node = min(remaining_nodes, key=lambda f: (
                    self.settings.CATEGORY_PRIORITIES.get(known_mod_data.get(self.folder_to_id[f], {}).get("category", "Default"), 50),
                    current_priority_map.get(f, 9999),
                    f
                ))
                
                broken_cycle_nodes.append(breaker_node)
                in_degree[breaker_node] = 0
                
                b_mod_id = self.folder_to_id[breaker_node]
                b_category_id = known_mod_data.get(b_mod_id, {}).get("category", "Default")
                b_main_priority = self.settings.CATEGORY_PRIORITIES.get(b_category_id, 50)
                b_current_pos = current_priority_map.get(breaker_node, 9999)
                heapq.heappush(ready_queue, (b_main_priority, b_current_pos, breaker_node))
                
        self._put_result('progress', (100, 100, self.__tr("排序完成。")))
        return sorted_order, broken_cycle_nodes

    def _get_mod_separators(self) -> Dict[str, str]:
        """为每个模组查找其所属的分隔符。"""
        mod_list = self.organizer.modList()
        all_mods = mod_list.allModsByProfilePriority()
        separator_map = {}
        current_separator = self.__tr("无分隔符")
        for mod_name in all_mods:
            mod = mod_list.getMod(mod_name)
            if mod.isSeparator():
                current_separator = mod_name
            elif mod.nexusId() > 0:
                separator_map[mod_name] = current_separator
        return separator_map

    def _analyze_current_load_order(self, full_graph: defaultdict) -> List[Dict]:
        """分析当前MO2排序，找出明显问题，并记录其分隔符。"""
        problems = []
        current_priority_list = self.organizer.modList().allModsByProfilePriority()
        priority_map = {mod_name: i for i, mod_name in enumerate(current_priority_list)}
        separator_map = self._get_mod_separators()

        for dependent_folder, dependent_id in self.folder_to_id.items():
            if dependent_id in self.ignore_requirements_of_ids:
                continue

            dependent_priority = priority_map.get(dependent_folder)
            if dependent_priority is None:
                continue

            requirements = full_graph.get(dependent_id, [])
            for req_info in requirements:
                provider_id_original = req_info.get('id')
                if not provider_id_original:
                    continue
                
                provider_id_effective = self._get_effective_id(provider_id_original)

                if provider_id_effective in self.installed_ids:
                    for provider_folder in self.id_to_folders.get(provider_id_effective, []):
                        provider_priority = priority_map.get(provider_folder)

                        if provider_priority is not None and provider_priority > dependent_priority:
                            problems.append({
                                "mod_folder": dependent_folder,
                                "mod_id": dependent_id,
                                "provider_folder": provider_folder,
                                "provider_id": provider_id_effective,
                                "notes": req_info.get('notes', ''),
                                "separator": separator_map.get(dependent_folder, self.__tr("未知"))
                            })
        return problems

    def _calculate_disruption_score(self, order: List[str], full_graph: defaultdict, folder_to_id_map: Dict[str, str]) -> int:
        """计算给定顺序的总“破坏”分数（即有多少依赖关系被违反）。"""
        score = 0
        priority_map = {mod_name: i for i, mod_name in enumerate(order)}
        
        for dependent_folder, dependent_priority in priority_map.items():
            dependent_id = folder_to_id_map.get(dependent_folder)
            if not dependent_id or dependent_id in self.ignore_requirements_of_ids:
                continue
                
            for req_info in full_graph.get(dependent_id, []):
                provider_id_original = req_info.get('id')
                if not provider_id_original: continue
                
                provider_id_effective = self._get_effective_id(provider_id_original)
                
                if provider_id_effective in self.installed_ids:
                    for provider_folder in self.id_to_folders.get(provider_id_effective, []):
                        provider_priority = priority_map.get(provider_folder)
                        if provider_priority is not None and provider_priority > dependent_priority:
                            score += 1 # 发现一个被破坏的依赖
        return score

    def get_all_cache_data(self) -> List[Dict]:
        if not self.conn: return []
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT mod_id, name, category, cache_timestamp FROM {self.settings.CACHE_TABLE_NAME}")
            return [{'id': row[0], 'name': row[1], 'category': row[2], 'timestamp': row[3]} for row in cursor.fetchall()]
        except Exception as e:
            self.log(f"获取所有缓存数据时出错: {e}", "error")
            return []

# =============================================================================
# 4. 后台工作线程
# =============================================================================
class WorkerThread(threading.Thread):
    def __init__(self, organizer, plugin_name, task_queue, result_queue):
        super().__init__(daemon=True)
        self.organizer = organizer
        self.plugin_name = plugin_name
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.analyzer: Optional[ModAnalyzer] = None
        self.playwright_manager: Optional[PlaywrightManager] = None
        self.stop_event = threading.Event()

    def request_stop(self):
        self.stop_event.set()

    def run(self):
        try:
            self.playwright_manager = PlaywrightManager(PluginSettings(self.organizer, self.plugin_name), self.result_queue)
            if not self.playwright_manager.start():
                return

            self.analyzer = ModAnalyzer(self.organizer, self.plugin_name, self.result_queue, self.playwright_manager, self.stop_event)
            
            while True:
                task = self.task_queue.get()
                if task is None:
                    break

                self.stop_event.clear() 
                self.handle_task(task)

        except Exception as e:
            log.critical(f"工作线程发生未捕获的异常: {e}", exc_info=True)
            if self.result_queue:
                self.result_queue.put({'type': 'error', 'data': f"工作线程崩溃: {e}"})
        finally:
            log.info("工作线程正在关闭，开始清理资源...")
            if self.playwright_manager:
                self.playwright_manager.stop()

            if self.analyzer and self.analyzer.conn:
                self.analyzer.conn.close()
                log.info("数据库连接已在线程退出时关闭。")

    def handle_task(self, task):
        task_type = task.get('type')
        
        if task_type == 'update_rules':
            self.analyzer.update_and_save_rules(task.get('rules'))
        elif task_type == 'add_to_rules':
            self.analyzer.add_ids_to_rule_list(task.get('list_name'), task.get('section_name'), task.get('ids'))
        elif task_type == 'update_settings':
            new_settings = task.get('settings')
            self.analyzer.update_and_save_settings(new_settings)
            if self.playwright_manager:
                self.playwright_manager.settings = self.analyzer.settings
                self.playwright_manager.restart_browser_for_settings_change()
        elif task_type == 'initialize_browser':
            self.analyzer.initialize_browser_and_check_login()
        elif task_type == 'perform_login':
            self.playwright_manager.perform_login()
        elif task_type == 'analyze_single':
            self.analyzer.analyze_single_mod_dependencies(
                task.get('mod_id'), task.get('hide_vr'), task.get('hide_optional'), task.get('hide_recommended')
            )
        elif task_type == 'generate_graph':
            self.analyzer.generate_dependency_graph(
                task.get('mod_id'), task.get('hide_vr'), task.get('hide_optional'), task.get('hide_recommended')
            )
        elif task_type == 'analyze_full':
            self.analyzer.generate_sorted_load_order(task.get('run_diagnosis'))
        elif task_type == 'find_translations':
            self.analyzer.find_missing_translations(task.get('language'), task.get('show_original_update_time'))
        elif task_type == 'delete_cache':
            self.analyzer.delete_cache_entries(task.get('items'))
        elif task_type == 'clear_cache':
            self.analyzer.clear_cache()
        elif task_type == 'get_cache':
            self.analyzer.get_and_send_cache_data()

# =============================================================================
# 5. 自定义UI组件
# =============================================================================
class SearchBar(QWidget):
    """一个可重用的、带高亮功能的搜索栏控件。"""
    search_triggered = pyqtSignal(str)
    next_result = pyqtSignal()
    closed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.__tr = lambda text: QApplication.translate("SearchBar", text)
        self.init_ui()
        self.hide()

    def init_ui(self):
        layout = QHBoxLayout()
        layout.setContentsMargins(2, 2, 2, 2)
        
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(self.__tr("输入ID、名称、备注等..."))
        
        self.next_button = QPushButton(self.__tr("下一个"))
        self.close_button = QPushButton("X")
        self.close_button.setFixedSize(24, 24)
        self.close_button.setToolTip(self.__tr("关闭 (Esc)"))

        layout.addWidget(QLabel(self.__tr("搜索:")))
        layout.addWidget(self.search_input)
        layout.addWidget(self.next_button)
        layout.addWidget(self.close_button)
        self.setLayout(layout)

        self.search_input.returnPressed.connect(self.next_result.emit)
        self.search_input.textChanged.connect(self.search_triggered.emit)
        self.next_button.clicked.connect(self.next_result.emit)
        self.close_button.clicked.connect(self.closed.emit)

class ContextMenuTreeWidget(QTreeWidget):
    """自定义树控件，增加了右键菜单和中键点击功能。"""
    def __init__(self, settings: PluginSettings, parent=None):
        super().__init__(parent)
        self.settings = settings
        self.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

    def mousePressEvent(self, event):
        """处理鼠标中键点击事件，用于在浏览器中打开Nexus Mods页面。"""
        if event.button() == Qt.MouseButton.MiddleButton:
            item = self.itemAt(event.pos())
            if item:
                mod_id = item.data(0, Qt.ItemDataRole.UserRole)
                if not mod_id or not mod_id.isdigit(): mod_id = item.text(1)
                if not mod_id or not mod_id.isdigit(): mod_id = item.text(2)
                
                if mod_id and mod_id.isdigit():
                    url = QUrl(f"{self.settings.NEXUS_BASE_URL}/{self.settings.GAME_NAME}/mods/{mod_id}")
                    QDesktopServices.openUrl(url)
        super().mousePressEvent(event)

class CacheTreeItem(QTreeWidgetItem):
    """自定义树形控件项，用于实现Mod ID列的数字排序。"""
    def __lt__(self, other: QTreeWidgetItem) -> bool:
        column = self.treeWidget().sortColumn()
        if column == 0: 
            try:
                self_text = self.text(0)
                other_text = other.text(0)
                if self_text.isdigit() and other_text.isdigit():
                    return int(self_text) < int(other_text)
            except (ValueError, TypeError):
                pass 
        return super().__lt__(other)

class ImageViewer(QScrollArea):
    """一个支持缩放和拖动平移的图片查看器。"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.image_label = QLabel(self)
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setWidget(self.image_label)

        self._original_pixmap = QPixmap()
        self.scale_factor = 1.0
        self.panning = False
        self.last_mouse_pos = None

    def set_pixmap(self, pixmap: QPixmap):
        self._original_pixmap = pixmap
        self.scale_factor = 1.0
        self.image_label.setPixmap(self._original_pixmap)
        self.image_label.adjustSize()

    def wheelEvent(self, event):
        if self._original_pixmap.isNull(): return
        if event.angleDelta().y() > 0: self.scale_factor *= 1.25
        else: self.scale_factor *= 0.8
        self.scale_factor = max(0.1, min(self.scale_factor, 10.0))
        new_size = self._original_pixmap.size() * self.scale_factor
        scaled_pixmap = self._original_pixmap.scaled(new_size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        self.image_label.setPixmap(scaled_pixmap)
        self.image_label.adjustSize()

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.panning = True
            self.last_mouse_pos = event.pos()
            self.setCursor(Qt.CursorShape.ClosedHandCursor)

    def mouseMoveEvent(self, event):
        if self.panning and self.last_mouse_pos:
            delta = event.pos() - self.last_mouse_pos
            self.last_mouse_pos = event.pos()
            h_bar, v_bar = self.horizontalScrollBar(), self.verticalScrollBar()
            h_bar.setValue(h_bar.value() - delta.x())
            v_bar.setValue(v_bar.value() - delta.y())

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.panning = False
            self.last_mouse_pos = None
            self.setCursor(Qt.CursorShape.ArrowCursor)

# =============================================================================
# 6. 排序修正和规则管理器UI
# =============================================================================
class CorrectionDialog(QDialog):
    """用于显示和确认模组顺序修正的对话框（Diff视图）。"""
    def __init__(self, original_data: List[Dict], proposed_data: List[Dict], moved_mods: Set[str], selected_mods: Set[str], parent=None):
        super().__init__(parent)
        self.__tr = lambda text: QApplication.translate("CorrectionDialog", text)
        self.original_data = original_data
        self.proposed_data = proposed_data
        self.moved_mods = moved_mods
        self.selected_mods = selected_mods
        self.init_ui()
        self.populate_trees()

    def init_ui(self):
        self.setWindowTitle(self.__tr("确认模组顺序修正"))
        self.setMinimumSize(1200, 700)
        layout = QVBoxLayout(self)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        original_group = QGroupBox(self.__tr("原始顺序 (黄色:选中 / 蓝色:受影响)"))
        original_layout = QVBoxLayout(original_group)
        self.original_tree = QTreeWidget()
        self.original_tree.setHeaderLabels([self.__tr("顺序"), self.__tr("模组/分隔符名称")])
        self.original_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.original_tree.header().setStretchLastSection(True)
        original_layout.addWidget(self.original_tree)
        splitter.addWidget(original_group)

        proposed_group = QGroupBox(self.__tr("建议顺序 (绿色:选中 / 淡绿:受影响)"))
        proposed_layout = QVBoxLayout(proposed_group)
        self.proposed_tree = QTreeWidget()
        self.proposed_tree.setHeaderLabels([self.__tr("顺序"), self.__tr("模组/分隔符名称")])
        self.proposed_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.proposed_tree.header().setStretchLastSection(True)
        proposed_layout.addWidget(self.proposed_tree)
        splitter.addWidget(proposed_group)
        
        splitter.setSizes([600, 600])
        layout.addWidget(splitter)
        
        button_box = QDialogButtonBox(self)
        apply_btn = button_box.addButton(self.__tr("应用更改"), QDialogButtonBox.ButtonRole.ApplyRole)
        cancel_btn = button_box.addButton(self.__tr("取消"), QDialogButtonBox.ButtonRole.RejectRole)
        
        apply_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        
        layout.addWidget(button_box)

    def populate_trees(self):
        selected_orig_brush = QBrush(QColor("#fff3cd"))
        affected_orig_brush = QBrush(QColor("#cfe2f3"))
        selected_new_brush = QBrush(QColor("#d4edda"))
        affected_new_brush = QBrush(QColor("#b6d7a8"))

        separator_font = self.original_tree.font()
        separator_font.setBold(True)
        separator_brush = QBrush(QColor("#6c757d"))

        consequentially_moved_mods = self.moved_mods - self.selected_mods

        for mod in self.original_data:
            item = QTreeWidgetItem([str(mod['priority']) if mod['priority'] != -1 else "", mod['name']])
            brush = None
            if mod['is_separator']:
                item.setFont(1, separator_font)
                item.setForeground(1, separator_brush)
            elif mod['name'] in self.selected_mods:
                brush = selected_orig_brush
            elif mod['name'] in consequentially_moved_mods:
                brush = affected_orig_brush
            
            if brush:
                for i in range(item.columnCount()):
                    item.setBackground(i, brush)
            self.original_tree.addTopLevelItem(item)

        for mod in self.proposed_data:
            item = QTreeWidgetItem([str(mod['priority']) if mod['priority'] != -1 else "", mod['name']])
            brush = None
            if mod['is_separator']:
                item.setFont(1, separator_font)
                item.setForeground(1, separator_brush)
            elif mod['name'] in self.selected_mods:
                brush = selected_new_brush
            elif mod['name'] in consequentially_moved_mods:
                brush = affected_new_brush
            
            if brush:
                for i in range(item.columnCount()):
                    item.setBackground(i, brush)
            self.proposed_tree.addTopLevelItem(item)

class RulesManagerDialog(QDialog):
    def __init__(self, current_rules: dict, parent=None):
        super().__init__(parent)
        self.__tr = lambda text: QApplication.translate("RulesManagerDialog", text)
        self.new_rules = current_rules
        self._init_ui()
        self._populate_data()

    def _init_ui(self):
        self.setWindowTitle(self.__tr("规则管理器"))
        self.setMinimumSize(600, 700)
        layout = QVBoxLayout(self)
        
        ignore_group = QGroupBox(self.__tr("忽略列表 (将这些ID视为已安装)"))
        ignore_layout = QVBoxLayout(ignore_group)
        self.ignore_list = QListWidget()
        ignore_layout.addWidget(self.ignore_list)
        ignore_btn_layout = QHBoxLayout()
        add_ignore_btn, remove_ignore_btn = QPushButton(self.__tr("添加")), QPushButton(self.__tr("删除"))
        ignore_btn_layout.addStretch()
        ignore_btn_layout.addWidget(add_ignore_btn)
        ignore_btn_layout.addWidget(remove_ignore_btn)
        ignore_layout.addLayout(ignore_btn_layout)
        layout.addWidget(ignore_group)

        ignore_req_group = QGroupBox(self.__tr("忽略其前置列表 (将忽略这些ID的所有前置)"))
        ignore_req_layout = QVBoxLayout(ignore_req_group)
        self.ignore_req_list = QListWidget()
        ignore_req_layout.addWidget(self.ignore_req_list)
        ignore_req_btn_layout = QHBoxLayout()
        add_ignore_req_btn, remove_ignore_req_btn = QPushButton(self.__tr("添加")), QPushButton(self.__tr("删除"))
        ignore_req_btn_layout.addStretch()
        ignore_req_btn_layout.addWidget(add_ignore_req_btn)
        ignore_req_btn_layout.addWidget(remove_ignore_req_btn)
        ignore_req_layout.addLayout(ignore_req_btn_layout)
        layout.addWidget(ignore_req_group)

        replace_group = QGroupBox(self.__tr("替换列表 ('被替换的ID' -> '替换为的ID')"))
        replace_layout = QVBoxLayout(replace_group)
        self.replace_table = QTableWidget()
        self.replace_table.setColumnCount(2)
        self.replace_table.setHorizontalHeaderLabels([self.__tr("被替换的ID"), self.__tr("替换为的ID")])
        self.replace_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        replace_layout.addWidget(self.replace_table)
        replace_btn_layout = QHBoxLayout()
        add_replace_btn, remove_replace_btn = QPushButton(self.__tr("添加")), QPushButton(self.__tr("删除"))
        replace_btn_layout.addStretch()
        replace_btn_layout.addWidget(add_replace_btn)
        replace_btn_layout.addWidget(remove_replace_btn)
        replace_layout.addLayout(replace_btn_layout)
        layout.addWidget(replace_group)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        layout.addWidget(button_box)

        add_ignore_btn.clicked.connect(lambda: self._add_item_to_list(self.ignore_list, self.__tr("添加忽略ID")))
        remove_ignore_btn.clicked.connect(lambda: self._remove_item_from_list(self.ignore_list))
        add_ignore_req_btn.clicked.connect(lambda: self._add_item_to_list(self.ignore_req_list, self.__tr("添加前置忽略ID")))
        remove_ignore_req_btn.clicked.connect(lambda: self._remove_item_from_list(self.ignore_req_list))
        add_replace_btn.clicked.connect(self._add_replace_item)
        remove_replace_btn.clicked.connect(self._remove_replace_item)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)

    def _populate_data(self):
        self.ignore_list.addItems(sorted([str(i) for i in self.new_rules.get('Ignore', {}).get('ids', [])]))
        self.ignore_req_list.addItems(sorted([str(i) for i in self.new_rules.get('IgnoreRequirementsOf', {}).get('ids', [])]))
        replace_map = self.new_rules.get('Replace', {})
        self.replace_table.setRowCount(0) 
        for key, value in sorted(replace_map.items()):
            if key == '#comment': continue
            row_position = self.replace_table.rowCount()
            self.replace_table.insertRow(row_position)
            self.replace_table.setItem(row_position, 0, QTableWidgetItem(str(key)))
            self.replace_table.setItem(row_position, 1, QTableWidgetItem(str(value)))

    def _add_item_to_list(self, list_widget: QListWidget, title: str):
        text, ok = QInputDialog.getText(self, title, self.__tr("请输入要添加的Nexus Mod ID:"))
        if ok and text.isdigit():
            if not list_widget.findItems(text, Qt.MatchFlag.MatchExactly):
                list_widget.addItem(QListWidgetItem(text))
        elif ok:
            QMessageBox.warning(self, self.__tr("输入无效"), self.__tr("ID必须为纯数字。"))
            
    def _remove_item_from_list(self, list_widget: QListWidget):
        for item in list_widget.selectedItems():
            list_widget.takeItem(list_widget.row(item))

    def _add_replace_item(self):
        original_id, ok1 = QInputDialog.getText(self, self.__tr("添加替换规则"), self.__tr("第一步: 输入被替换的模组ID:"))
        if not (ok1 and original_id.isdigit()):
            if ok1: QMessageBox.warning(self, self.__tr("输入无效"), self.__tr("ID必须为纯数字。"))
            return
        
        replacement_id, ok2 = QInputDialog.getText(self, self.__tr("添加替换规则"), self.__tr("第二步: 输入用于替换的模组ID:"))
        if not (ok2 and replacement_id.isdigit()):
            if ok2: QMessageBox.warning(self, self.__tr("输入无效"), self.__tr("ID必须为纯数字。"))
            return

        row_count = self.replace_table.rowCount()
        self.replace_table.insertRow(row_count)
        self.replace_table.setItem(row_count, 0, QTableWidgetItem(original_id))
        self.replace_table.setItem(row_count, 1, QTableWidgetItem(replacement_id))

    def _remove_replace_item(self):
        selected_rows = sorted(list(set(index.row() for index in self.replace_table.selectedIndexes())), reverse=True)
        for row in selected_rows:
            self.replace_table.removeRow(row)

    def get_new_rules(self) -> dict:
        ignore_ids = [int(self.ignore_list.item(i).text()) for i in range(self.ignore_list.count())]
        ignore_req_ids = [int(self.ignore_req_list.item(i).text()) for i in range(self.ignore_req_list.count())]
        replace_map = {}
        for row in range(self.replace_table.rowCount()):
            if (key_item := self.replace_table.item(row, 0)) and (value_item := self.replace_table.item(row, 1)) and key_item.text() and value_item.text():
                replace_map[key_item.text()] = value_item.text()
        
        return {'Ignore': {'ids': sorted(ignore_ids)}, 'IgnoreRequirementsOf': {'ids': sorted(ignore_req_ids)}, 'Replace': replace_map}

    def accept(self):
        self.new_rules = self.get_new_rules()
        super().accept()

# =============================================================================
# 7. 插件主UI
# =============================================================================
class AnalyzerDialog(QDialog):
    def __init__(self, organizer: mobase.IOrganizer, plugin_name: str, parent=None):
        super().__init__(parent)
        self.organizer = organizer
        self.plugin_name = plugin_name
        self.settings = PluginSettings(organizer, plugin_name)
        self.__tr = lambda text: QApplication.translate("AnalyzerDialog", text)
        self.is_running_analysis = False
        self.browser_ready = False
        self.is_logged_in = False
        self.cloudflare_warning_shown = False
        self.analysis_tree_items = {}
        self.current_graph_result = None
        self.last_full_analysis_data = None
        
        self.search_bars = {}
        self.search_states = {}
        self.settings_widgets = {}

        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()

        self._init_ui()
        self._setup_worker()
        self.installEventFilter(self)

    def showEvent(self, event):
        """窗口显示时自动开始初始化"""
        super().showEvent(event)
        if not self.browser_ready and not self.is_running_analysis:
            self.task_queue.put({'type': 'initialize_browser'})

    def _init_ui(self):
        self.setWindowTitle(self.__tr("Nexus Mods 依赖分析器"))
        self.setMinimumSize(1000, 800)
        layout = QVBoxLayout(self)
        main_splitter = QSplitter(Qt.Orientation.Vertical)
        self.tabs = QTabWidget()
        self.create_single_mod_tab()
        self.create_graph_tab()
        self.create_full_analysis_tab()
        self.create_translations_tab()
        self.create_cache_tab()
        self.create_settings_tab()
        main_splitter.addWidget(self.tabs)
        log_group = QGroupBox(self.__tr("日志"))
        log_layout = QVBoxLayout(log_group)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QPlainTextEdit.LineWrapMode.WidgetWidth)
        log_layout.addWidget(self.log_view)
        main_splitter.addWidget(log_group)
        main_splitter.setSizes([600, 200])
        layout.addWidget(main_splitter)
        status_layout = QHBoxLayout()
        self.login_status_label = QLabel(self.__tr("登录状态: 正在检查..."))
        status_layout.addWidget(self.login_status_label)
        status_layout.addStretch()
        self.stage_label = QLabel(self.__tr("正在初始化..."))
        status_layout.addWidget(self.stage_label)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        status_layout.addWidget(self.progress_bar, 1)
        layout.addLayout(status_layout)
        
    def create_single_mod_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        search_bar = SearchBar()
        self.search_bars[tab] = search_bar
        layout.addWidget(search_bar)

        top_layout = QVBoxLayout()
        input_layout = QHBoxLayout()
        input_layout.addWidget(QLabel(self.__tr("Nexus Mod ID:")))
        self.mod_id_input = QLineEdit()
        self.mod_id_input.setPlaceholderText(self.__tr("例如: 3863"))
        input_layout.addWidget(self.mod_id_input)
        self.analyze_single_btn = QPushButton(self.__tr("分析依赖树"))
        self.analyze_single_btn.setEnabled(False)
        input_layout.addWidget(self.analyze_single_btn)
        self.generate_graph_btn = QPushButton(self.__tr("生成关系图"))
        self.generate_graph_btn.setEnabled(False)
        input_layout.addWidget(self.generate_graph_btn)
        top_layout.addLayout(input_layout)

        filter_layout = QHBoxLayout()
        filter_layout.addStretch()
        filter_layout.addWidget(QLabel(self.__tr("筛选:")))
        self.hide_vr_checkbox_single = QCheckBox(self.__tr("隐藏VR"))
        self.hide_optional_checkbox_single = QCheckBox(self.__tr("隐藏可选"))
        self.hide_recommended_checkbox_single = QCheckBox(self.__tr("隐藏推荐"))
        filter_layout.addWidget(self.hide_vr_checkbox_single)
        filter_layout.addWidget(self.hide_optional_checkbox_single)
        filter_layout.addWidget(self.hide_recommended_checkbox_single)
        top_layout.addLayout(filter_layout)

        layout.addLayout(top_layout)

        self.single_mod_tree = ContextMenuTreeWidget(self.settings)
        self.single_mod_tree.setHeaderLabels([self.__tr("模组名称"), self.__tr("状态"), self.__tr("备注")])
        self.single_mod_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.single_mod_tree.header().setStretchLastSection(False)
        self.single_mod_tree.header().resizeSection(0, 400)
        self.single_mod_tree.header().resizeSection(1, 120)
        layout.addWidget(self.single_mod_tree)
        self.tabs.addTab(tab, self.__tr("依赖树分析"))

        self.analyze_single_btn.clicked.connect(self.trigger_single_mod_analysis)
        self.generate_graph_btn.clicked.connect(self.trigger_generate_graph)
        self.single_mod_tree.customContextMenuRequested.connect(self.show_tree_context_menu)
        
        self.hide_vr_checkbox_single.stateChanged.connect(self.trigger_single_mod_analysis)
        self.hide_optional_checkbox_single.stateChanged.connect(self.trigger_single_mod_analysis)
        self.hide_recommended_checkbox_single.stateChanged.connect(self.trigger_single_mod_analysis)

    def create_graph_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        toolbar = QHBoxLayout()
        self.save_graph_btn = QPushButton(self.__tr("保存 SVG 图像..."))
        self.save_graph_btn.setEnabled(False)
        self.save_dot_btn = QPushButton(self.__tr("导出 .dot 文件..."))
        self.save_dot_btn.setEnabled(False)
        toolbar.addWidget(self.save_graph_btn)
        toolbar.addWidget(self.save_dot_btn)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        self.graph_viewer = ImageViewer()
        layout.addWidget(self.graph_viewer)
        
        self.tabs.addTab(tab, self.__tr("依赖关系图"))
        self.save_graph_btn.clicked.connect(self.save_graph)
        self.save_dot_btn.clicked.connect(self.save_dot_file)

    def create_full_analysis_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        search_bar = SearchBar()
        self.search_bars[tab] = search_bar
        layout.addWidget(search_bar)
        
        top_layout = QHBoxLayout()
        top_layout.addWidget(QLabel(self.__tr("分析所有已启用模组，生成建议的加载顺序报告。")))
        top_layout.addStretch()
        self.analyze_full_btn = QPushButton(self.__tr("生成完整分析报告"))
        self.analyze_full_btn.setEnabled(False)
        self.export_html_btn = QPushButton(self.__tr("导出为HTML报告"))
        self.export_html_btn.setEnabled(False)
        top_layout.addWidget(self.analyze_full_btn)
        top_layout.addWidget(self.export_html_btn)
        layout.addLayout(top_layout)

        filter_layout = QHBoxLayout()
        self.diagnosis_checkbox = QCheckBox(self.__tr("启用加载顺序诊断"))
        self.diagnosis_checkbox.setChecked(True)
        filter_layout.addWidget(self.diagnosis_checkbox)
        filter_layout.addStretch()
        filter_layout.addWidget(QLabel(self.__tr("筛选报告:")))
        self.hide_vr_checkbox = QCheckBox(self.__tr("隐藏VR"))
        self.hide_optional_checkbox = QCheckBox(self.__tr("隐藏可选"))
        self.hide_recommended_checkbox = QCheckBox(self.__tr("隐藏推荐"))
        filter_layout.addWidget(self.hide_vr_checkbox)
        filter_layout.addWidget(self.hide_optional_checkbox)
        filter_layout.addWidget(self.hide_recommended_checkbox)
        layout.addLayout(filter_layout)

        self.full_analysis_tree = ContextMenuTreeWidget(self.settings)
        self.full_analysis_tree.setHeaderLabels([self.__tr("#"), self.__tr("模组文件夹"), self.__tr("Nexus ID"), self.__tr("备注 / 所在分隔符")])
        self.full_analysis_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.full_analysis_tree.header().resizeSection(0, 80)
        self.full_analysis_tree.header().resizeSection(1, 350)
        self.full_analysis_tree.header().resizeSection(2, 100)
        layout.addWidget(self.full_analysis_tree)
        self.tabs.addTab(tab, self.__tr("完整分析"))
        
        self.analyze_full_btn.clicked.connect(self.trigger_full_profile_analysis)
        self.export_html_btn.clicked.connect(self.trigger_export_html)
        self.full_analysis_tree.customContextMenuRequested.connect(self.show_tree_context_menu)
        self.hide_vr_checkbox.stateChanged.connect(self.filter_full_analysis_view)
        self.hide_optional_checkbox.stateChanged.connect(self.filter_full_analysis_view)
        self.hide_recommended_checkbox.stateChanged.connect(self.filter_full_analysis_view)

    def create_translations_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        search_bar = SearchBar()
        self.search_bars[tab] = search_bar
        layout.addWidget(search_bar)

        top_layout = QHBoxLayout()
        top_layout.addWidget(QLabel(self.__tr("目标语言:")))
        self.language_input = QLineEdit(self.__tr("Mandarin"))
        self.language_input.setPlaceholderText(self.__tr("例如: Mandarin, German, Russian"))
        top_layout.addWidget(self.language_input)
        self.find_missing_trans_btn = QPushButton(self.__tr("开始查找"))
        self.find_missing_trans_btn.setEnabled(False)
        top_layout.addWidget(self.find_missing_trans_btn)
        
        self.show_original_mod_update_time_checkbox = QCheckBox(self.__tr("显示原版模组更新时间"))
        top_layout.addStretch()
        top_layout.addWidget(self.show_original_mod_update_time_checkbox)
        layout.addLayout(top_layout)

        self.translations_tree = ContextMenuTreeWidget(self.settings)
        self.translations_tree.setSortingEnabled(True)
        self.translations_tree.setHeaderLabels([self.__tr("原版模组 / 翻译名称"), self.__tr("Nexus ID"), self.__tr("语言"), self.__tr("更新时间")])
        self.translations_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.translations_tree.header().setStretchLastSection(False)
        self.translations_tree.header().resizeSection(0, 350)
        self.translations_tree.header().resizeSection(1, 100)
        self.translations_tree.header().resizeSection(2, 150)
        self.translations_tree.sortByColumn(3, Qt.SortOrder.DescendingOrder)
        layout.addWidget(self.translations_tree)
        self.tabs.addTab(tab, self.__tr("查找缺失的翻译"))

        self.find_missing_trans_btn.clicked.connect(self.trigger_find_missing_translations)
        self.translations_tree.customContextMenuRequested.connect(self.show_tree_context_menu)

    def create_cache_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        search_bar = SearchBar()
        self.search_bars[tab] = search_bar
        layout.addWidget(search_bar)

        button_layout = QHBoxLayout()
        self.refresh_cache_btn = QPushButton(self.__tr("刷新列表"))
        self.delete_selected_cache_btn = QPushButton(self.__tr("删除选中项"))
        self.clear_all_cache_btn = QPushButton(self.__tr("清空所有缓存"))
        button_layout.addWidget(self.refresh_cache_btn)
        button_layout.addWidget(self.delete_selected_cache_btn)
        button_layout.addWidget(self.clear_all_cache_btn)
        button_layout.addStretch()
        layout.addLayout(button_layout)
        self.cache_tree = ContextMenuTreeWidget(self.settings)
        self.cache_tree.setHeaderLabels([self.__tr("Mod ID"), self.__tr("模组名称"), self.__tr("分类"), self.__tr("缓存时间")])
        self.cache_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.cache_tree.header().resizeSection(1, 400)
        layout.addWidget(self.cache_tree)
        self.tabs.addTab(tab, self.__tr("缓存管理"))
        self.refresh_cache_btn.clicked.connect(self.trigger_refresh_cache)
        self.delete_selected_cache_btn.clicked.connect(self.trigger_delete_selected_cache)
        self.clear_all_cache_btn.clicked.connect(self.trigger_clear_all_cache)
        self.cache_tree.customContextMenuRequested.connect(self.show_tree_context_menu)

    def create_settings_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        scroll_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        # 浏览器和登录
        browser_group = QGroupBox(self.__tr("浏览器与登录"))
        browser_form_layout = QFormLayout(browser_group)
        self.settings_widgets['browser_type'] = QComboBox()
        self.settings_widgets['browser_type'].addItems(["chrome", "msedge"])
        browser_form_layout.addRow(self.__tr("浏览器类型:"), self.settings_widgets['browser_type'])
        
        self.settings_widgets['headless'] = QCheckBox(self.__tr("使用无头模式 (后台运行浏览器)"))
        headless_layout = QVBoxLayout()
        headless_layout.addWidget(self.settings_widgets['headless'])
        self.headless_warning_label = QLabel(self.__tr("<b>警告:</b> 无头模式可能无法绕过Cloudflare防护，导致抓取失败。"))
        self.headless_warning_label.setStyleSheet("color: orange;")
        self.headless_warning_label.setWordWrap(True)
        self.headed_info_label = QLabel(self.__tr("<b>提示:</b> 有头模式更稳定，但会显示浏览器窗口。分析时请勿关闭此窗口。"))
        self.headed_info_label.setStyleSheet("color: #2980b9;")
        self.headed_info_label.setWordWrap(True)
        headless_layout.addWidget(self.headless_warning_label)
        headless_layout.addWidget(self.headed_info_label)
        browser_form_layout.addRow(headless_layout)

        self.login_btn = QPushButton(self.__tr("登录 / 刷新 Nexus Cookies"))
        browser_form_layout.addRow(self.login_btn)
        scroll_layout.addWidget(browser_group)

        # 高级模式开关
        advanced_mode_checkbox = QCheckBox(self.__tr("显示高级网络设置"))
        scroll_layout.addWidget(advanced_mode_checkbox)

        # 网络设置 (高级)
        self.network_group = QGroupBox(self.__tr("高级网络设置"))
        network_form_layout = QFormLayout(self.network_group)
        self.settings_widgets['request_timeout'] = QSpinBox()
        self.settings_widgets['request_timeout'].setRange(1000, 120000)
        self.settings_widgets['request_timeout'].setSuffix(" ms")
        network_form_layout.addRow(self.__tr("请求超时:"), self.settings_widgets['request_timeout'])
        self.settings_widgets['request_delay_ms'] = QSpinBox()
        self.settings_widgets['request_delay_ms'].setRange(0, 10000)
        self.settings_widgets['request_delay_ms'].setSuffix(" ms")
        network_form_layout.addRow(self.__tr("请求延迟:"), self.settings_widgets['request_delay_ms'])
        self.settings_widgets['max_retries'] = QSpinBox()
        self.settings_widgets['max_retries'].setRange(0, 10)
        network_form_layout.addRow(self.__tr("最大重试次数:"), self.settings_widgets['max_retries'])
        self.settings_widgets['retry_delay_ms'] = QSpinBox()
        self.settings_widgets['retry_delay_ms'].setRange(0, 30000)
        self.settings_widgets['retry_delay_ms'].setSuffix(" ms")
        network_form_layout.addRow(self.__tr("重试延迟:"), self.settings_widgets['retry_delay_ms'])
        self.settings_widgets['block_resources'] = QCheckBox(self.__tr("拦截图片/CSS等资源以加速"))
        network_form_layout.addRow(self.settings_widgets['block_resources'])
        self.settings_widgets['blocked_extensions'] = QLineEdit()
        network_form_layout.addRow(self.__tr("拦截文件后缀 (逗号分隔):"), self.settings_widgets['blocked_extensions'])
        self.settings_widgets['log_level'] = QComboBox()
        self.settings_widgets['log_level'].addItems(["INFO", "WARNING", "ERROR", "CRITICAL"])
        network_form_layout.addRow(self.__tr("日志等级:"), self.settings_widgets['log_level'])
        scroll_layout.addWidget(self.network_group)
        self.network_group.hide()

        # 分析设置
        analysis_group = QGroupBox(self.__tr("分析设置"))
        analysis_form_layout = QFormLayout(analysis_group)
        self.settings_widgets['cache_expiration_days'] = QSpinBox()
        self.settings_widgets['cache_expiration_days'].setRange(0, 3650)
        self.settings_widgets['cache_expiration_days'].setSpecialValueText(self.__tr("永不"))
        analysis_form_layout.addRow(self.__tr("缓存有效期 (天):"), self.settings_widgets['cache_expiration_days'])
        self.settings_widgets['max_recursion_depth'] = QSpinBox()
        self.settings_widgets['max_recursion_depth'].setRange(1, 50)
        analysis_form_layout.addRow(self.__tr("最大递归深度:"), self.settings_widgets['max_recursion_depth'])
        self.settings_widgets['uninstalled_mod_fetch_depth'] = QSpinBox()
        self.settings_widgets['uninstalled_mod_fetch_depth'].setRange(0, 10)
        analysis_form_layout.addRow(self.__tr("未安装模组抓取层数:"), self.settings_widgets['uninstalled_mod_fetch_depth'])
        scroll_layout.addWidget(analysis_group)

        # 规则管理
        rules_group = QGroupBox(self.__tr("规则管理"))
        rules_layout = QVBoxLayout(rules_group)
        self.manage_rules_btn = QPushButton(self.__tr("编辑规则文件 (rules.toml)..."))
        rules_layout.addWidget(self.manage_rules_btn)
        scroll_layout.addWidget(rules_group)

        scroll_area.setWidget(scroll_widget)
        layout.addWidget(scroll_area)

        # 保存按钮
        save_layout = QHBoxLayout()
        save_layout.addStretch()
        self.save_settings_btn = QPushButton(self.__tr("保存设置"))
        save_layout.addWidget(self.save_settings_btn)
        layout.addLayout(save_layout)

        self.tabs.addTab(tab, self.__tr("设置"))

        self._populate_settings_tab()

        advanced_mode_checkbox.stateChanged.connect(lambda state: self.network_group.setVisible(state == Qt.CheckState.Checked.value))
        self.settings_widgets['headless'].stateChanged.connect(self._update_headless_labels)
        self.login_btn.clicked.connect(self.trigger_login)
        self.manage_rules_btn.clicked.connect(self.open_rules_manager)
        self.save_settings_btn.clicked.connect(self.trigger_save_settings)

    def _update_headless_labels(self, state):
        is_headless = (state == Qt.CheckState.Checked.value)
        self.headless_warning_label.setVisible(is_headless)
        self.headed_info_label.setVisible(not is_headless)

    def _populate_settings_tab(self):
        """用当前设置填充设置UI"""
        for key, widget in self.settings_widgets.items():
            value = self.settings.settings_data.get(key)
            if isinstance(widget, QComboBox):
                widget.setCurrentText(str(value))
            elif isinstance(widget, QSpinBox):
                widget.setValue(int(value))
            elif isinstance(widget, QCheckBox):
                widget.setChecked(bool(value))
            elif isinstance(widget, QLineEdit):
                widget.setText(str(value))
        self._update_headless_labels(self.settings_widgets['headless'].checkState().value)

    def trigger_save_settings(self):
        """收集UI中的设置并发送更新任务"""
        new_settings = {}
        for key, widget in self.settings_widgets.items():
            if isinstance(widget, QComboBox):
                new_settings[key] = widget.currentText()
            elif isinstance(widget, QSpinBox):
                new_settings[key] = widget.value()
            elif isinstance(widget, QCheckBox):
                new_settings[key] = widget.isChecked()
            elif isinstance(widget, QLineEdit):
                new_settings[key] = widget.text()
        
        self.task_queue.put({'type': 'update_settings', 'settings': new_settings})

    def _setup_worker(self):
        self.worker = WorkerThread(self.organizer, self.plugin_name, self.task_queue, self.result_queue)
        self.worker.start()
        self.result_timer = QTimer(self)
        self.result_timer.timeout.connect(self.process_results)
        self.result_timer.start(100)
        self.tabs.currentChanged.connect(self.on_tab_changed)

        for tab, search_bar in self.search_bars.items():
            search_bar.search_triggered.connect(lambda text, t=tab: self.reset_search(t))
            search_bar.next_result.connect(lambda t=tab: self.execute_search(t))
            search_bar.closed.connect(lambda t=tab: self.close_search_bar(t))

    def eventFilter(self, source, event):
        if event.type() == QEvent.Type.KeyPress:
            if event.matches(QKeySequence.StandardKey.Find):
                self.toggle_search_bar()
                return True
            if event.key() == Qt.Key.Key_Escape:
                current_tab = self.tabs.currentWidget()
                if current_tab in self.search_bars and self.search_bars[current_tab].isVisible():
                    self.close_search_bar(current_tab)
                    return True
        return super().eventFilter(source, event)

    def toggle_search_bar(self):
        current_tab = self.tabs.currentWidget()
        if current_tab in self.search_bars:
            search_bar = self.search_bars[current_tab]
            search_bar.setVisible(not search_bar.isVisible())
            if search_bar.isVisible():
                search_bar.search_input.setFocus()
                search_bar.search_input.selectAll()
            else:
                self.close_search_bar(current_tab)

    def close_search_bar(self, tab):
        if tab in self.search_bars:
            self.search_bars[tab].hide()
            self.clear_search_highlight(tab)
            self.search_states.pop(tab, None)

    def reset_search(self, tab):
        if tab in self.search_states:
            self.search_states[tab]['current_index'] = -1
            self.search_states[tab]['results'] = []
    
    def execute_search(self, tab):
        if tab not in self.search_bars: return
        search_bar = self.search_bars[tab]
        search_text = search_bar.search_input.text().lower()
        
        if not search_text:
            self.clear_search_highlight(tab)
            return

        tree = tab.findChild(QTreeWidget)
        if not tree: return

        if tab not in self.search_states:
            self.search_states[tab] = {'text': '', 'results': [], 'current_index': -1}
        
        state = self.search_states[tab]

        if state['text'] != search_text:
            self.clear_search_highlight(tab)
            state['text'] = search_text
            state['results'] = []
            state['current_index'] = -1
            
            iterator = QTreeWidgetItemIterator(tree)
            while iterator.value():
                item = iterator.value()
                for i in range(item.columnCount()):
                    if search_text in item.text(i).lower():
                        state['results'].append(item)
                        break
                iterator += 1

        if not state['results']:
            self.append_to_log_view(self.__tr("未找到 '{text}' 的匹配项。").format(text=search_text))
            return

        self.clear_search_highlight(tab)

        state['current_index'] = (state['current_index'] + 1) % len(state['results'])
        item_to_highlight = state['results'][state['current_index']]
        
        tree.scrollToItem(item_to_highlight, QAbstractItemView.ScrollHint.PositionAtCenter)
        tree.setCurrentItem(item_to_highlight)
        
        highlight_brush = QBrush(QColor("#a2d2ff"))
        for i in range(item_to_highlight.columnCount()):
            item_to_highlight.setBackground(i, highlight_brush)
        
        state['highlighted_item'] = item_to_highlight

    def clear_search_highlight(self, tab):
        state = self.search_states.get(tab)
        if state and 'highlighted_item' in state and state['highlighted_item']:
            try:
                for i in range(state['highlighted_item'].columnCount()):
                    state['highlighted_item'].setBackground(i, QBrush(QColor(Qt.GlobalColor.transparent)))
            except RuntimeError:
                pass
            state['highlighted_item'] = None

    def process_results(self):
        try:
            while not self.result_queue.empty():
                result = self.result_queue.get_nowait()
                result_type, data = result.get('type'), result.get('data')

                if result_type == 'log': self.append_to_log_view(data)
                elif result_type == 'error': self.on_error(data)
                elif result_type == 'browser_ready': self.on_browser_ready(data)
                elif result_type == 'progress': self.update_progress(*data)
                elif result_type == 'analysis_complete': self.on_analysis_complete(data['type'], data['data'])
                elif result_type == 'full_analysis_mod_fetched': self.add_full_analysis_tree_item(data)
                elif result_type == 'cache_data_ready': self.populate_cache_tree(data)
                elif result_type == 'login_status': self.on_login_status_update(data)
                elif result_type == 'login_complete': self.on_login_complete(data)
                elif result_type == 'settings_updated': self.on_settings_updated(data)
                elif result_type == 'browser_restarted': self.on_browser_restarted()
                elif result_type == 'adult_content_blocked': self.on_adult_content_blocked(data)
                elif result_type == 'cloudflare_block_suspected': self.on_cloudflare_block_suspected(data)
        except queue.Empty: pass
        except Exception as e: log.error(f"处理结果队列时出错: {e}")

    def _start_task(self, task_type: str, button_to_toggle: QPushButton, **kwargs):
        if task_type not in ['perform_login', 'update_settings', 'initialize_browser'] and not self.browser_ready:
            QMessageBox.warning(self, self.__tr("浏览器未就绪"), self.__tr("浏览器正在初始化或初始化失败。请稍候或在“设置”中尝试重新登录。"))
            return
        if self.is_running_analysis:
            if not kwargs.get('is_auto_trigger', False):
                 QMessageBox.warning(self, self.__tr("操作正在进行"), self.__tr("请等待当前分析任务完成。"))
            return

        if task_type in ['analyze_single', 'analyze_full', 'find_translations']:
            self.cloudflare_warning_shown = False

        self.clear_active_tree()
        self._toggle_ui_state(True, button_to_toggle)
        self.task_queue.put({'type': task_type, **kwargs})

    def _toggle_ui_state(self, is_starting: bool, button: QPushButton):
        self.is_running_analysis = is_starting
        if not button: return

        if not hasattr(button, 'original_text'):
            button.original_text = button.text()
        
        try:
            button.clicked.disconnect()
        except TypeError:
            pass
        
        if is_starting:
            button.setText(self.__tr("停止"))
            button.clicked.connect(self.trigger_stop)
        else:
            button.setText(button.original_text)
            if button is self.analyze_single_btn: button.clicked.connect(self.trigger_single_mod_analysis)
            elif button is self.generate_graph_btn: button.clicked.connect(self.trigger_generate_graph)
            elif button is self.analyze_full_btn: button.clicked.connect(self.trigger_full_profile_analysis)
            elif button is self.find_missing_trans_btn: button.clicked.connect(self.trigger_find_missing_translations)
            elif button is self.delete_selected_cache_btn: button.clicked.connect(self.trigger_delete_selected_cache)
            elif button is self.clear_all_cache_btn: button.clicked.connect(self.trigger_clear_all_cache)
            elif button is self.refresh_cache_btn: button.clicked.connect(self.trigger_refresh_cache)
            elif button is self.login_btn: button.clicked.connect(self.trigger_login)
            elif button is self.save_settings_btn: button.clicked.connect(self.trigger_save_settings)
            elif button is self.export_html_btn: button.clicked.connect(self.trigger_export_html)

        all_buttons = [
            self.analyze_single_btn, self.generate_graph_btn, self.analyze_full_btn,
            self.find_missing_trans_btn, self.delete_selected_cache_btn, self.clear_all_cache_btn,
            self.refresh_cache_btn, self.login_btn, self.save_settings_btn, self.manage_rules_btn,
            self.export_html_btn
        ]
        
        for b in all_buttons:
            if b is not button:
                if not is_starting:
                    is_always_enabled = b in [self.save_settings_btn, self.login_btn, self.manage_rules_btn]
                    is_analysis_dependent = b in [self.export_html_btn]
                    
                    b.setEnabled((self.browser_ready or is_always_enabled) and not is_analysis_dependent)
                    if b is self.export_html_btn:
                        b.setEnabled(bool(self.last_full_analysis_data))
                else:
                    b.setEnabled(False)
            else:
                b.setEnabled(True)

    def update_progress(self, current: int, total: int, text: str):
        self.stage_label.setText(text)
        if total == 0: self.progress_bar.setRange(0, 0)
        else:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(current)

    def append_to_log_view(self, text: str):
        self.log_view.appendPlainText(text)
        self.log_view.moveCursor(QTextCursor.MoveOperation.End)

    def clear_active_tree(self):
        active_tab = self.tabs.currentWidget()
        if active_tab:
            self.close_search_bar(active_tab)
            if tree := active_tab.findChild(QTreeWidget):
                tree.clear()
                if tree is self.full_analysis_tree: self.analysis_tree_items.clear()
            if graph_viewer := active_tab.findChild(ImageViewer):
                graph_viewer.set_pixmap(QPixmap())
                self.current_graph_result = None
                self.save_graph_btn.setEnabled(False)
                self.save_dot_btn.setEnabled(False)

    def add_single_mod_tree_item_recursive(self, parent_item, node_data):
        if not node_data: return
        
        status = node_data.get("status")
        is_installed = node_data.get("is_installed", False)
        
        status_text, status_color = self.__tr("未知"), QColor("white")

        if status == "satisfied":
            status_text, status_color = self.__tr("✔ 已安装"), QColor("#27ae60")
        elif status == "missing":
            status_text, status_color = self.__tr("❌ 缺失"), QColor("#c0392b")
        elif status == "ignored":
            status_text, status_color = self.__tr("➖ 已忽略"), QColor("#7f8c8d")
        elif status == "cycle":
            status_text, status_color = self.__tr("🔁 循环"), QColor("#f39c12")
        elif status == "truncated":
            status_text, status_color = self.__tr("✂️ 已截断"), QColor("#8e44ad")

        if status in ["cycle", "truncated"]:
            status_text += self.__tr(" (已安装)") if is_installed else self.__tr(" (未安装)")

        item = QTreeWidgetItem(parent_item)
        mod_id = node_data.get('id')
        item.setData(0, Qt.ItemDataRole.UserRole, mod_id)
        
        if "replacement_info" in node_data:
            replacer_info = node_data["replacement_info"]
            original_name = node_data.get("name", f"ID {mod_id}")
            item.setText(0, replacer_info.get("name", ""))
            item.setToolTip(0, self.__tr("Nexus ID: {id}").format(id=replacer_info.get("id")))
            replacement_note = self.__tr("（替代了 {name}）").format(name=original_name)
            original_notes = node_data.get("notes", "")
            item.setText(2, f"{replacement_note} {original_notes}".strip())
            item.setData(0, Qt.ItemDataRole.UserRole, replacer_info.get("id"))
        else:
            item.setText(0, node_data.get("name", ""))
            item.setToolTip(0, self.__tr("Nexus ID: {id}").format(id=mod_id))
            item.setText(2, node_data.get("notes", ""))

        item.setText(1, status_text)
        item.setForeground(1, QBrush(status_color))
        
        item.setExpanded(not node_data.get('is_fully_satisfied', False))
        
        for child_data in node_data.get("children", []):
            self.add_single_mod_tree_item_recursive(item, child_data)

    def add_full_analysis_tree_item(self, mod_data):
        tree, mod_id = self.full_analysis_tree, mod_data.get("id")
        if not mod_id or not self.worker or not self.worker.analyzer: return
        if mod_id in self.analysis_tree_items: return
        
        status = self.__tr("已安装") if mod_id in self.worker.analyzer.installed_ids else self.__tr("未安装 (依赖)")
        folder_name = self.worker.analyzer.id_to_folders.get(mod_id, [mod_data.get("name")])[0]
        item = QTreeWidgetItem(["", folder_name, mod_id, status])
        item.setData(0, Qt.ItemDataRole.UserRole, mod_id)
        self.analysis_tree_items[mod_id] = item
        tree.addTopLevelItem(item)

    def on_analysis_complete(self, analysis_type, data):
        button_map = {
            "single_mod": self.analyze_single_btn, "graph": self.generate_graph_btn,
            "full_analysis": self.analyze_full_btn, "translations": self.find_missing_trans_btn, 
            "cache_deleted": self.delete_selected_cache_btn, "cache_cleared": self.clear_all_cache_btn
        }
        
        if active_button := button_map.get(analysis_type):
            self._toggle_ui_state(False, active_button)

        if analysis_type == "single_mod":
            self.clear_active_tree()
            if data and not data.get("error"):
                self.add_single_mod_tree_item_recursive(self.single_mod_tree.invisibleRootItem(), data)
            self.update_progress(1, 1, self.__tr("依赖树分析完成！"))
        elif analysis_type == "graph":
            if data and data.get("svg_data"):
                svg_data = data["svg_data"]
                pixmap = QPixmap()
                pixmap.loadFromData(svg_data)
                self.graph_viewer.set_pixmap(pixmap)
                self.current_graph_result = data
                self.save_graph_btn.setEnabled(True)
                self.save_dot_btn.setEnabled(True)
                for i in range(self.tabs.count()):
                    if self.tabs.tabText(i) == self.__tr("依赖关系图"):
                        self.tabs.setCurrentIndex(i)
                        break
            self.update_progress(1, 1, self.__tr("依赖关系图生成完成！"))
        elif analysis_type == "full_analysis":
            self.last_full_analysis_data = data
            self.export_html_btn.setEnabled(bool(data and "error" not in data))
            if data and "error" in data: self.on_error(self.__tr("无法生成分析报告。"))
            elif data:
                self.populate_full_analysis_results(data)
                self.update_progress(1, 1, self.__tr("完整分析报告生成完毕！"))
        elif analysis_type == "translations":
            self.populate_translations_tree(data)
            self.update_progress(1, 1, self.__tr("缺失翻译扫描完成！"))
        elif analysis_type == "cache_deleted": self.populate_cache_tree(data)
        elif analysis_type == "cache_cleared": self.populate_cache_tree([])
        
        self.stage_label.setText(self.__tr("准备就绪"))

    def populate_full_analysis_results(self, data):
        if not self.worker or not self.worker.analyzer: return
        self.clear_active_tree()
        tree = self.full_analysis_tree
        
        if data.get("load_order_problems"):
            problem_group = QTreeWidgetItem(tree, [self.__tr("诊断报告 (排序问题)")])
            problem_group.setForeground(0, QBrush(QColor("red")))
            for problem in data["load_order_problems"]:
                remark = self.__tr("应排在 '{provider}' 之后 (在: {separator})").format(
                    provider=problem['provider_folder'],
                    separator=problem['separator']
                )
                problem_item = QTreeWidgetItem(problem_group, 
                    ["", 
                     problem['mod_folder'],
                     problem['mod_id'],
                     remark
                    ])
                problem_item.setData(0, Qt.ItemDataRole.UserRole, problem['mod_id'])
                problem_item.setData(1, Qt.ItemDataRole.UserRole, problem['mod_folder'])
                problem_item.setData(2, Qt.ItemDataRole.UserRole, problem['provider_folder'])

        if data.get("missing_report"):
            missing_group = QTreeWidgetItem(tree, [self.__tr("诊断报告 (依赖缺失)")])
            missing_group.setForeground(0, QBrush(QColor("orange")))
            for mid, report in data["missing_report"].items():
                missing_mod_item = QTreeWidgetItem(missing_group, ["", f"{report['name']}", mid])
                missing_mod_item.setData(0, Qt.ItemDataRole.UserRole, mid)
                missing_mod_item.setForeground(1, QBrush(QColor("#c0392b")))
                for folder, notes, tags in report["required_by_installed"]:
                    requiring_mod_id = self.worker.analyzer.folder_to_id.get(folder, 'N/A')
                    child_item = QTreeWidgetItem(missing_mod_item, ["", folder, requiring_mod_id, notes])
                    child_item.setData(0, Qt.ItemDataRole.UserRole, requiring_mod_id)
                    child_item.setData(1, Qt.ItemDataRole.UserRole, tags)
        
        sorted_group = QTreeWidgetItem(tree, [self.__tr("建议的加载顺序")])
        for i, folder_name in enumerate(data.get("sorted_order", [])):
            mod_id = self.worker.analyzer.folder_to_id.get(folder_name)
            remark = self.__tr("循环依赖打破点") if folder_name in data.get("broken_cycle_nodes", []) else ""
            item = QTreeWidgetItem(sorted_group, [str(i + 1), folder_name, mod_id, remark])
            item.setData(0, Qt.ItemDataRole.UserRole, mod_id)
            if remark: item.setForeground(3, QBrush(QColor("#f39c12")))
        
        tree.expandAll()
        self.filter_full_analysis_view()

    def filter_full_analysis_view(self):
        hide_vr = self.hide_vr_checkbox.isChecked()
        hide_optional = self.hide_optional_checkbox.isChecked()
        hide_recommended = self.hide_recommended_checkbox.isChecked()
        root = self.full_analysis_tree.invisibleRootItem()
        
        for i in range(root.childCount()):
            top_item = root.child(i)
            if top_item.text(0) == self.__tr("诊断报告 (依赖缺失)"):
                for j in range(top_item.childCount()):
                    missing_mod_item = top_item.child(j)
                    visible_requirers = 0
                    for k in range(missing_mod_item.childCount()):
                        requirer_item = missing_mod_item.child(k)
                        tags = requirer_item.data(1, Qt.ItemDataRole.UserRole)
                        if tags:
                            is_vr = 'vr' in tags
                            is_optional = 'optional' in tags
                            is_recommended = 'recommended' in tags
                            should_hide = (hide_vr and is_vr) or (hide_optional and is_optional) or (hide_recommended and is_recommended)
                            requirer_item.setHidden(should_hide)
                            if not should_hide:
                                visible_requirers += 1
                        else:
                            visible_requirers += 1
                    missing_mod_item.setHidden(visible_requirers == 0)

    def populate_translations_tree(self, data: Dict[str, Dict]):
        tree = self.translations_tree
        tree.clear()
        if not data:
            QTreeWidgetItem(tree, [self.__tr("未发现任何符合条件的缺失翻译。")])
            return

        for original_mod_id, original_mod_info in data.items():
            original_update_timestamp = original_mod_info.get('update_timestamp', 0)
            original_update_time_str = datetime.fromtimestamp(original_update_timestamp).strftime("%Y-%m-%d") if original_update_timestamp else ""
            
            parent_item = QTreeWidgetItem(tree, [original_mod_info['name'], original_mod_id, "", original_update_time_str])
            parent_item.setData(0, Qt.ItemDataRole.UserRole, original_mod_id)
            parent_item.setForeground(0, QBrush(QColor(Qt.GlobalColor.darkBlue)))
            
            sorted_translations = sorted(original_mod_info['translations'], key=lambda x: x.get('update_timestamp', 0), reverse=True)

            for trans_data in sorted_translations:
                timestamp = trans_data.get('update_timestamp', 0)
                update_time_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d") if timestamp else self.__tr("未知")
                
                child_item = QTreeWidgetItem(parent_item, [trans_data['name'], trans_data['id'], trans_data['language'], update_time_str])
                child_item.setData(0, Qt.ItemDataRole.UserRole, trans_data['id'])

    def populate_cache_tree(self, data: List[Dict]):
        tree = self.cache_tree
        tree.clear(); tree.setSortingEnabled(False)
        items = []
        for item_data in data:
            mod_id = item_data.get('id', 'N/A')
            name = item_data.get('name', 'N/A')
            category = item_data.get('category', 'N/A')
            ts = item_data.get('timestamp', 'N/A')
            try: 
                display_time = datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError): 
                display_time = ts
            item = CacheTreeItem([mod_id, name, category, display_time])
            item.setData(0, Qt.ItemDataRole.UserRole, mod_id)
            items.append(item)
        tree.addTopLevelItems(items)
        tree.setSortingEnabled(True)
        tree.sortByColumn(0, Qt.SortOrder.AscendingOrder)

    def trigger_single_mod_analysis(self):
        if self.is_running_analysis:
            return

        mod_id = self.mod_id_input.text().strip()
        if not mod_id.isdigit():
            if self.sender() in [self.analyze_single_btn, self.generate_graph_btn]:
                 QMessageBox.warning(self, self.__tr("输入无效"), self.__tr("请输入一个纯数字的 Nexus Mod ID。"))
            return

        self._start_task('analyze_single', 
                         self.analyze_single_btn,
                         mod_id=mod_id,
                         hide_vr=self.hide_vr_checkbox_single.isChecked(),
                         hide_optional=self.hide_optional_checkbox_single.isChecked(),
                         hide_recommended=self.hide_recommended_checkbox_single.isChecked(),
                         is_auto_trigger=self.sender() not in [self.analyze_single_btn, self.generate_graph_btn])

    def trigger_generate_graph(self):
        if self.is_running_analysis:
            return

        mod_id = self.mod_id_input.text().strip()
        if not mod_id.isdigit():
            QMessageBox.warning(self, self.__tr("输入无效"), self.__tr("请输入一个纯数字的 Nexus Mod ID。"))
            return
        
        self._start_task('generate_graph', 
                         self.generate_graph_btn,
                         mod_id=mod_id,
                         hide_vr=self.hide_vr_checkbox_single.isChecked(),
                         hide_optional=self.hide_optional_checkbox_single.isChecked(),
                         hide_recommended=self.hide_recommended_checkbox_single.isChecked())

    def trigger_full_profile_analysis(self):
        run_diagnosis = self.diagnosis_checkbox.isChecked()
        self._start_task('analyze_full', self.analyze_full_btn, run_diagnosis=run_diagnosis)

    def trigger_find_missing_translations(self):
        language = self.language_input.text().strip()
        if language: 
            self._start_task('find_translations', self.find_missing_trans_btn, 
                             language=language, 
                             show_original_update_time=self.show_original_mod_update_time_checkbox.isChecked())
        else: 
            QMessageBox.warning(self, self.__tr("输入为空"), self.__tr("请输入您想要查找的翻译语言。"))

    def trigger_delete_selected_cache(self):
        selected_items = self.cache_tree.selectedItems()
        if not selected_items:
            QMessageBox.information(self, self.__tr("未选择"), self.__tr("请先在列表中选择要删除的缓存条目。"))
            return
        items_to_delete = [{'id': item.text(0)} for item in selected_items]
        self._start_task('delete_cache', self.delete_selected_cache_btn, items=items_to_delete)

    def trigger_clear_all_cache(self):
        if QMessageBox.question(self, self.__tr("确认清理"), self.__tr("您确定要删除当前游戏的所有已缓存数据吗？此操作不可逆！"),
                                      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            self._start_task('clear_cache', self.clear_all_cache_btn)

    def trigger_refresh_cache(self):
        if self.is_running_analysis:
            self.append_to_log_view(self.__tr("[INFO] 分析进行中，刷新请求已忽略。"))
            return
        self.cache_tree.clear()
        self.task_queue.put({'type': 'get_cache'})

    def trigger_stop(self):
        if self.is_running_analysis and self.worker and self.worker.analyzer:
            self.append_to_log_view(self.__tr("[UI] 用户请求停止操作。"))
            self.worker.request_stop()
            self.stage_label.setText(self.__tr("正在发送停止信号... 请稍候。"))
            if sender := self.sender(): sender.setEnabled(False)

    def trigger_login(self):
        self._start_task('perform_login', self.login_btn)

    def show_tree_context_menu(self, position):
        tree = self.sender()
        if not isinstance(tree, QTreeWidget) or not (selected_items := tree.selectedItems()): return

        menu = QMenu()
        
        is_diagnosis_item = False
        if tree is self.full_analysis_tree:
            parent = selected_items[0].parent()
            if parent and parent.text(0).startswith(self.__tr("诊断报告 (排序问题)")):
                is_diagnosis_item = True
        
        if is_diagnosis_item:
            correct_order_action = QAction(self.__tr("修正选中模组的顺序..."), self)
            correct_order_action.triggered.connect(self.correct_load_order)
            menu.addAction(correct_order_action)
        else:
            ids = set()
            for item in selected_items:
                mod_id = item.data(0, Qt.ItemDataRole.UserRole)
                if not mod_id or not mod_id.isdigit(): mod_id = item.text(1)
                if not mod_id or not mod_id.isdigit(): mod_id = item.text(2)
                if mod_id and mod_id.isdigit(): ids.add(mod_id)

            sorted_ids = sorted(list(ids))
            if not sorted_ids: return

            copy_action = QAction(self.__tr("复制 Nexus ID ({count})").format(count=len(sorted_ids)), self)
            copy_action.triggered.connect(lambda: QApplication.clipboard().setText("\n".join(sorted_ids)))
            menu.addAction(copy_action)
            menu.addSeparator()
            add_ignore_action = QAction(self.__tr("添加到忽略列表"), self)
            add_ignore_action.triggered.connect(lambda: self.task_queue.put({'type': 'add_to_rules', 'section_name': 'Ignore', 'list_name': 'ids', 'ids': sorted_ids}))
            menu.addAction(add_ignore_action)
            add_ignore_req_action = QAction(self.__tr("添加到忽略前置列表"), self)
            add_ignore_req_action.triggered.connect(lambda: self.task_queue.put({'type': 'add_to_rules', 'section_name': 'IgnoreRequirementsOf', 'list_name': 'ids', 'ids': sorted_ids}))
            menu.addAction(add_ignore_req_action)
        
        menu.exec(tree.viewport().mapToGlobal(position))

    def correct_load_order(self):
        if not self.last_full_analysis_data or not self.worker or not self.worker.analyzer:
            QMessageBox.warning(self, self.__tr("无数据"), self.__tr("无法执行修正，请先生成一份完整的分析报告。"))
            return

        selected_items = self.full_analysis_tree.selectedItems()
        parent = selected_items[0].parent() if selected_items else None
        if not selected_items or not parent or not parent.text(0).startswith(self.__tr("诊断报告 (排序问题)")):
            QMessageBox.information(self, self.__tr("选择无效"), self.__tr("请从“诊断报告 (排序问题)”中选择一个或多个具体的问题项进行修正。"))
            return

        problems_to_fix = set()
        user_selected_folders = set()
        for item in selected_items:
            dependent_folder = item.data(1, Qt.ItemDataRole.UserRole)
            provider_folder = item.data(2, Qt.ItemDataRole.UserRole)
            if dependent_folder and provider_folder:
                problems_to_fix.add((dependent_folder, provider_folder))
                user_selected_folders.add(dependent_folder)
        
        if not problems_to_fix: return

        analyzer = self.worker.analyzer
        full_graph = self.last_full_analysis_data.get("full_graph", defaultdict(list))
        
        mod_list = self.organizer.modList()
        original_order = mod_list.allModsByProfilePriority()
        
        # --- 获取分隔符信息 ---
        separator_map = {}
        separator_boundaries = defaultdict(lambda: {'start': len(original_order), 'end': -1})
        current_separator = self.__tr("无分隔符")
        separator_boundaries[current_separator] = {'start': 0, 'end': -1}
        
        for i, mod_name in enumerate(original_order):
            if mod_list.getMod(mod_name).isSeparator():
                if current_separator != self.__tr("无分隔符"):
                    separator_boundaries[current_separator]['end'] = i
                current_separator = mod_name
                separator_boundaries[current_separator]['start'] = i + 1
            else:
                separator_map[mod_name] = current_separator
        separator_boundaries[current_separator]['end'] = len(original_order)
        # --- 结束获取分隔符信息 ---
        
        proposed_order = list(original_order)
        mods_to_move = {p[0] for p in problems_to_fix}

        # 迭代几次以求稳定解
        for _ in range(len(mods_to_move) + 2):
            made_change_in_pass = False
            for dependent_mod in mods_to_move:
                best_pos = -1
                min_disruption = float('inf')
                
                try:
                    original_pos = proposed_order.index(dependent_mod)
                except ValueError: continue

                # 必须排在这些模组之后
                must_be_after_mods = {p[1] for p in problems_to_fix if p[0] == dependent_mod}
                last_provider_pos = -1
                for provider in must_be_after_mods:
                    try: last_provider_pos = max(last_provider_pos, proposed_order.index(provider))
                    except ValueError: continue

                dependent_separator = separator_map.get(dependent_mod, self.__tr("无分隔符"))
                sep_start = separator_boundaries[dependent_separator]['start']
                sep_end = separator_boundaries[dependent_separator]['end']
                
                search_start = max(sep_start, last_provider_pos + 1)
                
                temp_order = list(proposed_order)
                temp_order.pop(original_pos)
                
                current_best_order = None

                # 在分隔符内搜索
                for i in range(search_start, sep_end + 1):
                    candidate_order = temp_order[:i] + [dependent_mod] + temp_order[i:]
                    disruption = analyzer._calculate_disruption_score(candidate_order, full_graph, analyzer.folder_to_id)
                    
                    if disruption < min_disruption:
                        min_disruption = disruption
                        best_pos = i
                        current_best_order = candidate_order
                    elif disruption == min_disruption:
                        if abs(i - original_pos) < abs(best_pos - original_pos):
                            best_pos = i
                            current_best_order = candidate_order

                if current_best_order and proposed_order != current_best_order:
                    proposed_order = current_best_order
                    made_change_in_pass = True

            if not made_change_in_pass:
                break
        
        moved_mods = {mod for i, mod in enumerate(original_order) if proposed_order[i] != mod}
        moved_mods.update({mod for i, mod in enumerate(proposed_order) if original_order[i] != mod})

        if not moved_mods:
            QMessageBox.information(self, self.__tr("无需调整"), self.__tr("根据您的选择和最小破坏原则，当前顺序已是最佳。"))
            return

        dialog = CorrectionDialog(
            [{"name": n, "priority": i, "is_separator": mod_list.getMod(n).isSeparator()} for i, n in enumerate(original_order)],
            [{"name": n, "priority": i, "is_separator": mod_list.getMod(n).isSeparator()} for i, n in enumerate(proposed_order)],
            moved_mods, user_selected_folders, self
        )
        if dialog.exec():
            self.append_to_log_view(self.__tr("开始修正模组加载顺序..."))
            self.setEnabled(False)
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
            
            try:
                mod_list.setPriorities(proposed_order)
                QApplication.processEvents()
                self.organizer.refresh(False)
                QMessageBox.information(self, self.__tr("操作完成"), self.__tr("模组顺序已修正。建议重新运行分析以验证结果。"))
            except Exception as e:
                self.append_to_log_view(f"[CRITICAL] {self.__tr('修正排序时出错')}: {e}")
                QMessageBox.critical(self, self.__tr("错误"), self.__tr("修正排序时发生错误，请查看日志。"))
            finally:
                QApplication.restoreOverrideCursor()
                self.setEnabled(True)
                self.trigger_full_profile_analysis()

    def on_error(self, message: str):
        self.update_progress(1, 1, self.__tr("发生错误！"))
        QMessageBox.critical(self, self.__tr("发生错误"), message)
        if self.is_running_analysis:
            for btn in [self.analyze_single_btn, self.generate_graph_btn, self.analyze_full_btn, self.find_missing_trans_btn, self.delete_selected_cache_btn, self.clear_all_cache_btn]:
                if btn.text() == self.__tr("停止"):
                    self._toggle_ui_state(False, btn)
                    break

    def open_rules_manager(self):
        if not self.worker or not self.worker.analyzer: return
        analyzer = self.worker.analyzer
        current_rules = {"Ignore": {"ids": list(analyzer.ignore_ids)}, "IgnoreRequirementsOf": {"ids": list(analyzer.ignore_requirements_of_ids)}, "Replace": analyzer.replacement_map}
        dialog = RulesManagerDialog(current_rules, self)
        if dialog.exec():
            self.task_queue.put({'type': 'update_rules', 'rules': dialog.get_new_rules()})

    def on_tab_changed(self, index):
        if self.tabs.tabText(index) == self.__tr("缓存管理"):
            self.trigger_refresh_cache()

    def on_browser_ready(self, success: bool):
        if success:
            self.browser_ready = True
            self.analyze_single_btn.setEnabled(True)
            self.generate_graph_btn.setEnabled(True)
            self.analyze_full_btn.setEnabled(True)
            self.find_missing_trans_btn.setEnabled(True)
            self.stage_label.setText(self.__tr("准备就绪"))
            self.append_to_log_view(self.__tr("浏览器准备就绪，可以开始分析。"))
        else:
            self.stage_label.setText(self.__tr("浏览器初始化失败"))

    def on_browser_restarted(self):
        self.append_to_log_view(self.__tr("浏览器已重启，正在重新验证状态..."))
        self.browser_ready = False
        self.task_queue.put({'type': 'initialize_browser'})

    def on_login_status_update(self, data: dict):
        self.is_logged_in = data.get('success', False)
        if self.is_logged_in:
            self.login_status_label.setText(self.__tr("登录状态: <b style='color:green;'>已登录</b>"))
        else:
            self.login_status_label.setText(self.__tr("登录状态: <b style='color:orange;'>未登录</b> (部分功能受限)"))

    def on_login_complete(self, success: bool):
        self._toggle_ui_state(False, self.login_btn)
        if success:
            QMessageBox.information(self, self.__tr("登录流程结束"), self.__tr("登录浏览器已关闭。现在将重新初始化分析浏览器并检查最终登录状态..."))
            self.browser_ready = False
            self.append_to_log_view(self.__tr("正在使用新Cookies重启浏览器..."))
            self.task_queue.put({'type': 'initialize_browser'})
        else:
            QMessageBox.critical(self, self.__tr("登录失败"), self.__tr("登录流程中发生错误，请查看日志获取详细信息。"))

    def on_settings_updated(self, success: bool):
        self._toggle_ui_state(False, self.save_settings_btn)
        if success:
            QMessageBox.information(self, self.__tr("设置已保存"), self.__tr("设置已成功保存并应用。浏览器实例将自动重启以应用新设置。"))
            self.settings = PluginSettings(self.organizer, self.plugin_name)
            self._populate_settings_tab()
        else:
            QMessageBox.critical(self, self.__tr("保存失败"), self.__tr("保存设置失败，请查看日志。"))

    def on_adult_content_blocked(self, data: dict):
        mod_id = data.get('mod_id')
        msg_box = QMessageBox(self)
        msg_box.setIcon(QMessageBox.Icon.Warning)
        msg_box.setWindowTitle(self.__tr("内容屏蔽提示"))
        msg_box.setText(self.__tr("模组 {mod_id} 因<b>成人内容</b>被屏蔽。").format(mod_id=mod_id))
        
        info_text = self.__tr("如果您已登录，这通常意味着您需要在Nexus Mods网站上手动开启成人内容显示选项。")
        info_text += f"<br><br><b><a href='{self.settings.NEXUS_CONTENT_SETTINGS_URL}'>{self.__tr('点击此处打开N网内容设置页面')}</a></b>"
        msg_box.setInformativeText(info_text)
        
        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg_box.exec()

    def on_cloudflare_block_suspected(self, data: dict):
        if self.cloudflare_warning_shown:
            return
        self.cloudflare_warning_shown = True
        
        mod_id = data.get('mod_id')
        QMessageBox.warning(self, 
            self.__tr("抓取超时警告"),
            self.__tr("抓取模组 {mod_id} 信息时发生超时，这很可能是因为Cloudflare人机验证。\n\n"
                      "如果问题持续出现，强烈建议您在“设置”标签页中<b>关闭“无头模式”</b>（即切换为有头模式）并重试。").format(mod_id=mod_id)
        )

    def save_graph(self):
        if not self.current_graph_result or not self.current_graph_result.get("svg_data"):
            QMessageBox.warning(self, self.__tr("无图像"), self.__tr("没有可保存的依赖关系图。"))
            return
        file_path, _ = QFileDialog.getSaveFileName(self, self.__tr("保存 SVG 图像"), "", "SVG Images (*.svg)")
        if file_path:
            try:
                with open(file_path, 'wb') as f: f.write(self.current_graph_result["svg_data"])
                self.append_to_log_view(self.__tr("关系图已保存到: {path}").format(path=file_path))
            except Exception as e: self.on_error(self.__tr("保存文件时出错: {error}").format(error=e))

    def save_dot_file(self):
        if not self.current_graph_result or not self.current_graph_result.get("dot_source"):
            QMessageBox.warning(self, self.__tr("无数据"), self.__tr("没有可保存的 .dot 源数据。"))
            return
        file_path, _ = QFileDialog.getSaveFileName(self, self.__tr("导出 .dot 文件"), "", "DOT Source (*.dot)")
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f: f.write(self.current_graph_result["dot_source"])
                self.append_to_log_view(self.__tr(".dot 源文件已保存到: {path}").format(path=file_path))
            except Exception as e: self.on_error(self.__tr("保存文件时出错: {error}").format(error=e))

    def trigger_export_html(self):
        if not self.last_full_analysis_data:
            QMessageBox.warning(self, self.__tr("无数据"), self.__tr("请先生成一份完整的分析报告。"))
            return

        default_filename = f"MO2_Analysis_{self.settings.SANITIZED_GAME_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        file_path, _ = QFileDialog.getSaveFileName(self, self.__tr("保存HTML报告"), default_filename, "HTML Files (*.html)")

        if file_path:
            try:
                html_content = self.generate_html_report(self.last_full_analysis_data)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                self.append_to_log_view(self.__tr("报告已成功导出到: {path}").format(path=file_path))
                QDesktopServices.openUrl(QUrl.fromLocalFile(file_path))
            except Exception as e:
                self.on_error(self.__tr("导出HTML时出错: {error}").format(error=e))

    def generate_html_report(self, data: dict) -> str:
        """根据分析数据生成一个独立的HTML报告文件。"""
        game_name = self.settings.GAME_NAME
        report_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # --- CSS样式 ---
        css = """
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif; margin: 0; padding: 0; background-color: #f4f7f6; color: #333; }
            .container { max-width: 1200px; margin: 20px auto; padding: 20px; background-color: #fff; box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-radius: 8px; }
            h1, h2, h3 { color: #2c3e50; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; }
            h1 { text-align: center; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th, td { padding: 12px; border: 1px solid #ddd; text-align: left; }
            th { background-color: #3498db; color: white; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            tr:hover { background-color: #eaf5ff; }
            .tag { display: inline-block; padding: 2px 6px; font-size: 0.8em; border-radius: 4px; color: white; margin-left: 5px; }
            .tag-vr { background-color: #9b59b6; }
            .tag-optional { background-color: #f39c12; }
            .tag-recommended { background-color: #2ecc71; }
            .problem { color: #c0392b; font-weight: bold; }
            .missing { background-color: #ffebee; }
            .order-problem { background-color: #fff9c4; }
            .cycle-breaker { color: #e67e22; font-weight: bold; }
            .mod-link { color: #2980b9; text-decoration: none; }
            .mod-link:hover { text-decoration: underline; }
            footer { text-align: center; margin-top: 20px; font-size: 0.9em; color: #7f8c8d; }
        </style>
        """

        def create_mod_link(mod_id, text):
            return f'<a class="mod-link" href="{self.settings.NEXUS_BASE_URL}/{game_name}/mods/{mod_id}" target="_blank">{text}</a>'

        # --- HTML主体 ---
        html = f"<!DOCTYPE html><html lang='zh-CN'><head><meta charset='UTF-8'><title>MO2 依赖分析报告</title>{css}</head><body>"
        html += f"<div class='container'><h1>Mod Organizer 2 依赖分析报告</h1>"
        html += f"<p><strong>游戏:</strong> {game_name}<br><strong>报告生成时间:</strong> {report_time}</p>"

        # 诊断报告 - 依赖缺失
        if missing_report := data.get("missing_report"):
            html += "<h2>诊断报告: 依赖缺失</h2><table><tr><th>缺失的模组</th><th>ID</th><th>被以下已安装模组需要</th></tr>"
            for mid, report in missing_report.items():
                req_by_html = "<ul>"
                for folder, notes, tags in report["required_by_installed"]:
                    tags_html = "".join([f'<span class="tag tag-{t}">{t}</span>' for t in tags])
                    req_by_html += f"<li>{folder} ({notes or '无备注'}) {tags_html}</li>"
                req_by_html += "</ul>"
                html += f"<tr class='missing'><td>{create_mod_link(mid, report['name'])}</td><td>{mid}</td><td>{req_by_html}</td></tr>"
            html += "</table>"
        
        # 诊断报告 - 排序问题
        if problems := data.get("load_order_problems"):
            html += "<h2>诊断报告: 加载顺序问题</h2><table><tr><th>模组</th><th>问题描述</th><th>所在分隔符</th></tr>"
            for p in problems:
                desc = f"应排在 <strong>{p['provider_folder']}</strong> 之后"
                html += f"<tr class='order-problem'><td>{create_mod_link(p['mod_id'], p['mod_folder'])}</td><td>{desc}</td><td>{p['separator']}</td></tr>"
            html += "</table>"

        # 建议的加载顺序
        if sorted_order := data.get("sorted_order"):
            html += "<h2>建议的加载顺序</h2><table><tr><th>#</th><th>模组文件夹</th><th>Nexus ID</th><th>备注</th></tr>"
            for i, folder in enumerate(sorted_order):
                mod_id = self.worker.analyzer.folder_to_id.get(folder, "N/A")
                remark = ""
                if folder in data.get("broken_cycle_nodes", []):
                    remark = "<span class='cycle-breaker'>循环依赖打破点</span>"
                html += f"<tr><td>{i+1}</td><td>{folder}</td><td>{create_mod_link(mod_id, mod_id) if mod_id != 'N/A' else 'N/A'}</td><td>{remark}</td></tr>"
            html += "</table>"

        html += "<footer>由 Nexus Mods 依赖分析器生成</footer></div></body></html>"
        return html

    def closeEvent(self, event):
        self.append_to_log_view(self.__tr("正在关闭插件窗口..."))
        self.setEnabled(False)
        self.result_timer.stop()

        if self.worker and self.worker.is_alive():
            self.task_queue.put(None)
            self.append_to_log_view(self.__tr("正在等待后台线程关闭... (最多5秒)"))
            start_time = time.time()
            while self.worker.is_alive() and (time.time() - start_time) < 5:
                QApplication.processEvents()
                time.sleep(0.1)

            if self.worker.is_alive():
                self.append_to_log_view(self.__tr("警告: 后台线程未能正常关闭。浏览器进程可能需要手动结束。"))
            else:
                self.append_to_log_view(self.__tr("后台线程已成功关闭。"))
        
        super().closeEvent(event)

# =============================================================================
# 8. MO2插件主类
# =============================================================================
class ModDepAnalyzerPlugin(mobase.IPluginTool):
    def __init__(self):
        super().__init__()
        self._organizer: Optional[mobase.IOrganizer] = None
        self._window: Optional[AnalyzerDialog] = None
        self.__tr = lambda text: QApplication.translate("ModDepAnalyzerPlugin", text)

    def init(self, organizer: mobase.IOrganizer) -> bool:
        self._organizer = organizer
        return True

    def name(self) -> str: return "dep_analysis"
    def author(self) -> str: return "Renil & Gemini AI"
    def description(self) -> str: return self.__tr("分析Nexus Mods依赖关系并为MO2生成排序建议的工具。")
    def version(self) -> mobase.VersionInfo: return mobase.VersionInfo(8, 0, 0, mobase.ReleaseType.FINAL)
    def isActive(self) -> bool: return DEPENDENCIES_MET
    def displayName(self) -> str: return self.__tr("Nexus Mods 依赖分析器")
    def tooltip(self) -> str: return self.__tr("启动依赖分析工具")
    def icon(self) -> QIcon: return QIcon(":/MO/gui/icons/search-list")

    def settings(self) -> List[mobase.PluginSetting]:
        return []

    def display(self):
        if not DEPENDENCIES_MET:
            QMessageBox.critical(None, self.__tr("缺少依赖项"), 
                self.__tr("此插件必需的库 (lxml, Patchright, pytomlpp, orjson, graphviz) 未能加载。\n"
                          "请仔细阅读插件说明（README），并按照指示手动安装所有必需的依赖项。"))
            return
        if self._window and self._window.isVisible():
            self._window.raise_()
            self._window.activateWindow()
            return
        self._window = AnalyzerDialog(self._organizer, self.name())
        self._window.show()

def createPlugin() -> mobase.IPlugin:
    return ModDepAnalyzerPlugin()
