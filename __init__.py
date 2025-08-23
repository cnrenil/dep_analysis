# -*- coding: utf-8 -*-

# =============================================================================
# Mod Organizer 2 - Nexus Mods 依赖分析器 + Wabbajack 安装器
# 版本: 8.3.0
# 作者: Renil <renil@foxmail.com> & Gemini AI
#
# 描述:
#   一个功能强大的Mod Organizer 2插件，旨在帮助用户管理复杂的模组依赖关系。
#   它能够分析单个模组的依赖树，为整个模组列表生成建议的加载顺序，
#   查找已安装模组所缺失的翻译，以及安装Wabbajack整合包。
# =============================================================================

import os
import sys
from . import logger as plugin_logging

log = plugin_logging.get_logger(__name__)

try:
    import mobase
    from PyQt6.QtWidgets import QMessageBox, QApplication
    from PyQt6.QtGui import QIcon
except ImportError:
    # 桩代码，用于在 MO2 环境外进行静态分析
    log("Mobase or PyQt6 not found. This script must be run within Mod Organizer 2.")
    class mobase:
        class IPluginTool: pass
    class QMessageBox:
        @staticmethod
        def critical(*args, **kwargs): pass
    class QApplication:
        @staticmethod
        def translate(context: str, text: str) -> str: return text
    class QIcon: pass

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
    import requests
    import bsdiff4
    import xxhash
    DEPENDENCIES_MET = True
except ImportError as e:
    print(f"一个必需的依赖项缺失: {e}")
    DEPENDENCIES_MET = False

# --- 插件模块导入 ---
from .ui.main_window import AnalyzerDialog

class ModDepAnalyzerPlugin(mobase.IPluginTool):
    """MO2 插件主类"""
    def __init__(self):
        super().__init__()
        self._organizer = None
        self._window = None
        # 使用 lambda 函数简化翻译调用
        self.__tr = lambda text: QApplication.translate("ModDepAnalyzerPlugin", text)

    def init(self, organizer: mobase.IOrganizer) -> bool:
        self._organizer = organizer
        # 在这里初始化翻译，可以帮助PyQt的lupdate工具找到需要翻译的字符串
        try:
            from .core import settings, wabbajack_installer
            from .ui import main_window, dialogs, widgets
            from .utils import playwright_manager
            
            QApplication.translate("PluginSettings", "dummy")
            QApplication.translate("PlaywrightManager", "dummy")
            QApplication.translate("ModAnalyzer", "dummy")
            QApplication.translate("WorkerThread", "dummy")
            QApplication.translate("SearchBar", "dummy")
            QApplication.translate("CorrectionDialog", "dummy")
            QApplication.translate("RulesManagerDialog", "dummy")
            QApplication.translate("AnalyzerDialog", "dummy")
            QApplication.translate("WabbajackInstaller", "dummy")

        except Exception as e:
            print(f"翻译设置期间出错: {e}")

        return True

    def name(self) -> str: return "dep_analysis"
    def author(self) -> str: return "Renil & Gemini AI"
    def description(self) -> str: return self.__tr("分析Nexus Mods依赖关系并为MO2生成排序建议的工具。")
    def version(self) -> mobase.VersionInfo: return mobase.VersionInfo(8, 3, 0, mobase.ReleaseType.FINAL)
    def isActive(self) -> bool: return DEPENDENCIES_MET
    def displayName(self) -> str: return self.__tr("Nexus Mods 依赖分析器")
    def tooltip(self) -> str: return self.__tr("启动依赖分析工具")
    def icon(self) -> QIcon: return QIcon(":/MO/gui/icons/search-list")

    def settings(self) -> list:
        return []

    def display(self):
        """显示插件主窗口"""
        if not DEPENDENCIES_MET:
            QMessageBox.critical(None, self.__tr("缺少依赖项"), 
                self.__tr("此插件必需的库 (lxml, Patchright, pytomlpp, orjson, graphviz, requests, bsdiff4, xxhash) 未能加载。\n"
                          "请仔细阅读插件说明（README），并按照指示手动安装所有必需的依赖项。"))
            return
        # 如果窗口已存在且可见，则将其置于顶层
        if self._window and self._window.isVisible():
            self._window.raise_()
            self._window.activateWindow()
            return
        
        # 创建并显示主窗口
        self._window = AnalyzerDialog(self._organizer, self.name())
        self._window.show()

def createPlugin() -> mobase.IPlugin:
    """MO2用于创建插件实例的函数"""
    return ModDepAnalyzerPlugin()
