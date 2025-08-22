# -*- coding: utf-8 -*-

import queue
import time
import logging
from datetime import datetime
from collections import defaultdict

try:
    import mobase
    from PyQt6.QtWidgets import (
        QApplication, QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
        QMessageBox, QGroupBox, QPlainTextEdit, QProgressBar, QTreeWidget,
        QTreeWidgetItem, QTabWidget, QWidget, QHeaderView,
        QSplitter, QAbstractItemView, QFileDialog, QScrollArea, QMenu,
        QCheckBox, QTreeWidgetItemIterator, QComboBox, QSpinBox, QFormLayout
    )
    from PyQt6.QtCore import QObject, QThread, pyqtSignal, Qt, QUrl, pyqtSlot, QTimer, QSize, QEvent
    from PyQt6.QtGui import QIcon, QTextCursor, QColor, QBrush, QDesktopServices, QPixmap, QPainter, QCursor, QAction, QKeySequence, QFont
except ImportError:
    # 桩代码，用于在 MO2 环境外进行静态分析
    class QDialog: pass
    class QTimer:
        def __init__(self, parent=None): pass
        def timeout(self): return self
        def connect(self, slot): pass
        def start(self, interval): pass
        def stop(self): pass

from ..core.settings import PluginSettings
from ..core.worker import WorkerThread
from .widgets import SearchBar, ImageViewer, ContextMenuTreeWidget, CacheTreeItem
from .dialogs import CorrectionDialog, RulesManagerDialog

log = logging.getLogger(__name__)

class AnalyzerDialog(QDialog):
    """插件主UI窗口"""
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
        self.show_original_mod_update_time_checkbox.setChecked(True)
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

    def populate_translations_tree(self, data: dict):
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

    def populate_cache_tree(self, data: list):
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
        
        proposed_order = list(original_order)
        mods_to_move = {p[0] for p in problems_to_fix}

        for _ in range(len(mods_to_move) + 2):
            made_change_in_pass = False
            for dependent_mod in mods_to_move:
                best_pos = -1
                min_disruption = float('inf')
                
                try:
                    original_pos = proposed_order.index(dependent_mod)
                except ValueError: continue

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

        html = f"<!DOCTYPE html><html lang='zh-CN'><head><meta charset='UTF-8'><title>MO2 依赖分析报告</title>{css}</head><body>"
        html += f"<div class='container'><h1>Mod Organizer 2 依赖分析报告</h1>"
        html += f"<p><strong>游戏:</strong> {game_name}<br><strong>报告生成时间:</strong> {report_time}</p>"

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
        
        if problems := data.get("load_order_problems"):
            html += "<h2>诊断报告: 加载顺序问题</h2><table><tr><th>模组</th><th>问题描述</th><th>所在分隔符</th></tr>"
            for p in problems:
                desc = f"应排在 <strong>{p['provider_folder']}</strong> 之后"
                html += f"<tr class='order-problem'><td>{create_mod_link(p['mod_id'], p['mod_folder'])}</td><td>{desc}</td><td>{p['separator']}</td></tr>"
            html += "</table>"

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
