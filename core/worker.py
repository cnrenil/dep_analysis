# -*- coding: utf-8 -*-

import threading
import queue

from .analyzer import ModAnalyzer
from .settings import PluginSettings
from .wabbajack_installer import WabbajackInstaller
from ..utils.playwright_manager import PlaywrightManager
from .. import logger as plugin_logging # 导入新的日志模块

log = plugin_logging.get_logger(__name__)

class WorkerThread(threading.Thread):
    """后台工作线程，用于执行所有耗时操作"""
    def __init__(self, organizer, plugin_name, task_queue, result_queue):
        super().__init__(daemon=True)
        self.organizer = organizer
        self.plugin_name = plugin_name
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.analyzer: ModAnalyzer = None
        self.wabbajack_installer: WabbajackInstaller = None
        self.playwright_manager: PlaywrightManager = None
        self.stop_event = threading.Event()
        self._log_listener_thread = None

    def _log_listener(self):
        """监听日志队列并将记录放入结果队列。"""
        while True:
            try:
                record = plugin_logging.log_queue.get()
                if record is None:  # 收到停止信号
                    break
                
                # 格式化日志消息
                log_entry = {
                    'level': record.levelname,
                    'message': record.getMessage(),
                    'name': record.name
                }
                self.result_queue.put({'type': 'log', 'data': log_entry})
            except Exception:
                # 在日志系统本身出现问题时，打印到控制台
                import traceback
                traceback.print_exc()


    def request_stop(self):
        """请求停止当前任务"""
        self.stop_event.set()
        if self.analyzer:
            self.analyzer.request_stop()

    def run(self):
        """线程主循环"""
        try:
            # 启动日志监听器
            self._log_listener_thread = threading.Thread(target=self._log_listener, daemon=True)
            self._log_listener_thread.start()

            settings = PluginSettings(self.organizer, self.plugin_name)
            plugin_logging.setup_logging(settings.LOG_LEVEL)

            self.playwright_manager = PlaywrightManager(settings, self.result_queue)
            if not self.playwright_manager.start():
                return

            self.analyzer = ModAnalyzer(self.organizer, self.plugin_name, self.result_queue, self.playwright_manager, self.stop_event)
            self.wabbajack_installer = WabbajackInstaller(self.organizer, self.plugin_name, self.result_queue, self.stop_event, self.playwright_manager, self.analyzer.settings)
            
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
            
            # 停止日志监听器
            plugin_logging.log_queue.put(None)
            if self._log_listener_thread:
                self._log_listener_thread.join()

            if self.playwright_manager:
                self.playwright_manager.stop()

            if self.analyzer and self.analyzer.conn:
                self.analyzer.conn.close()
                log.info("数据库连接已在线程退出时关闭。")

    def handle_task(self, task):
        """根据任务类型分发任务"""
        task_type = task.get('type')
        
        # 更新日志级别以匹配最新设置
        plugin_logging.setup_logging(self.analyzer.settings.LOG_LEVEL)
        
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
        elif task_type == 'install_wabbajack':
            self.wabbajack_installer.run_installation(
                task.get('file_path'), task.get('install_path'), 
                task.get('download_path'), task.get('parse_only'),
                task.get('download_only')
            )
