# -*- coding: utf-8 -*-

import time
import re
import logging
import heapq
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Set, Any, Optional, Tuple

try:
    import mobase
    from PyQt6.QtWidgets import QApplication
except ImportError:
    class mobase: pass
    class QApplication:
        @staticmethod
        def translate(context: str, text: str) -> str: return text

# --- 核心依赖导入 ---
try:
    from lxml import html as lxml_html
    from patchright.sync_api import TimeoutError as PlaywrightTimeoutError
    import pytomlpp
    import orjson
    import graphviz
except ImportError:
    # 这些库应该由主 __init__.py 保证存在
    pass

from .settings import PluginSettings
from ..utils.playwright_manager import PlaywrightManager
from ..utils.helpers import _find_chinese_font, _extract_mod_id_from_url

log = logging.getLogger(__name__)

class ModAnalyzer:
    """封装了所有与数据抓取、解析和依赖关系计算相关的功能。"""
    def __init__(self, organizer: mobase.IOrganizer, plugin_name: str, result_queue, playwright_manager: PlaywrightManager, stop_event):
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

            font_name = _find_chinese_font()
            
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
                                    trans_id = _extract_mod_id_from_url(lang_cell_link[0].get('href', ''))
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

                if req_id := _extract_mod_id_from_url(req['url']):
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
                        if req_id := _extract_mod_id_from_url(req['url']):
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
                            if req_id := _extract_mod_id_from_url(req['url']):
                                if req_id not in known_ids: next_layer.add(req_id)
                self._put_result('progress', (i + 1, total_current, f"{self.__tr('抓取第 {d} 层依赖').format(d=depth + 1)} {i+1}/{total_current}"))
        
        graph, reverse_graph = defaultdict(list), defaultdict(list)
        for mod_id, mod_data in known_mod_data.items():
            if "dependencies" in mod_data and mod_id not in self.ignore_requirements_of_ids:
                for req in mod_data.get("dependencies", {}).get("requires", []):
                    if req_id := _extract_mod_id_from_url(req['url']):
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
