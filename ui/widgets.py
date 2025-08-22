# -*- coding: utf-8 -*-

from PyQt6.QtWidgets import (
    QWidget, QHBoxLayout, QLineEdit, QPushButton, QLabel, QTreeWidget, 
    QAbstractItemView, QTreeWidgetItem, QScrollArea, QApplication
)
from PyQt6.QtCore import pyqtSignal, Qt, QUrl
from PyQt6.QtGui import QPixmap, QDesktopServices

from ..core.settings import PluginSettings

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
