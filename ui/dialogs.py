# -*- coding: utf-8 -*-

from typing import List, Dict, Set

from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QSplitter, QGroupBox, QTreeWidget, QHeaderView, 
    QDialogButtonBox, QTreeWidgetItem, QListWidget, QTableWidget, QHBoxLayout, 
    QPushButton, QInputDialog, QMessageBox, QTableWidgetItem, QListWidgetItem, QApplication
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QBrush, QColor

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
