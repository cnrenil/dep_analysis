# -*- coding: utf-8 -*-

import threading
import queue
import re
from typing import Optional, Callable

try:
    from PyQt6.QtWidgets import QApplication
except ImportError:
    # 桩代码，用于在 MO2 环境外进行静态分析
    class QApplication:
        @staticmethod
        def translate(context: str, text: str) -> str: return text

try:
    import patchright.sync_api as playwright
    from patchright.sync_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    # 这些库应该由主 __init__.py 保证存在
    pass

from ..core.settings import PluginSettings
from .helpers import block_unnecessary_requests

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
        # 接收一个外部的日志记录函数，以实现日志的集中管理
        self.log: Callable[[str, str], None] = lambda msg, level: print(f"[{level.upper()}] {msg}")

    def start(self) -> bool:
        """启动Playwright服务。这是整个生命周期的第一步。"""
        with self._lock:
            if self.playwright_instance:
                return True
            try:
                self.log(self.__tr("正在启动Playwright服务..."), "info")
                self.playwright_instance = playwright.sync_playwright().start()
                self.log(self.__tr("Playwright服务已启动。"), "info")
                return True
            except Exception as e:
                self.log(self.__tr("启动Playwright服务失败: {error}").format(error=e), "critical")
                return False

    def stop(self):
        """停止Playwright服务并清理所有资源。这是插件关闭时的最后一步。"""
        with self._lock:
            self._internal_close_main_context()
            if self.playwright_instance:
                try:
                    self.playwright_instance.stop()
                    self.log(self.__tr("Playwright服务已停止。"), "info")
                except Exception as e:
                    self.log(self.__tr("停止Playwright服务时出错: {error}").format(error=e), "warning")
                self.playwright_instance = None

    def _internal_close_main_context(self):
        """（内部方法）仅关闭用于数据抓取的主浏览器上下文。"""
        if self.main_context:
            try:
                # 在关闭上下文之前，尝试导航到一个空白页，可以减少一些关闭时的挂起问题
                if self.main_page and not self.main_page.is_closed():
                    try:
                        self.main_page.goto("about:blank", timeout=1000)
                    except Exception:
                        pass 
                self.main_context.close()
                self.log(self.__tr("主浏览器上下文已关闭。"), "info")
            except Exception as e:
                self.log(self.__tr("关闭主浏览器上下文时出错: {error}").format(error=e), "warning")
        self.main_context = None
        self.main_page = None

    def close_main_context(self):
        """（公共方法）线程安全地关闭用于数据抓取的主浏览器上下文。"""
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
                self.log(self.__tr("Playwright服务未运行，无法获取页面。"), "error")
                return None
            
            # 如果上下文存在但页面已关闭，先清理旧的上下文
            if self.main_context:
                self._internal_close_main_context()

            try:
                mode_str = self.__tr("有头") if not self.settings.HEADLESS else self.__tr("无头")
                self.log(self.__tr("正在初始化主浏览器上下文 ({mode})...").format(mode=mode_str), "info")
                self.main_context = self.playwright_instance.chromium.launch_persistent_context(
                    str(self.settings.BROWSER_DATA_DIR), # 路径需要是字符串
                    channel=self.settings.BROWSER_TYPE,
                    headless=self.settings.HEADLESS,
                    no_viewport=True,
                )
                
                self.main_page = self.main_context.pages[0] if self.main_context.pages else self.main_context.new_page()
                
                # 根据设置决定是否拦截不必要的网络资源
                if self.settings.BLOCK_RESOURCES:
                    self.log(self.__tr("正在设置网络请求规则以优化速度..."), "info")
                    extensions = self.settings.BLOCKED_EXTENSIONS.replace(" ", "").split(',')
                    regex_pattern = r"\.(" + "|".join(extensions) + r")(\?.*)?$"
                    self.main_page.route(re.compile(regex_pattern), block_unnecessary_requests)

                return self.main_page
            except Exception as e:
                self.log(self.__tr("初始化主浏览器上下文时发生严重错误: {error}").format(error=e), "critical")
                self.result_queue.put({'type': 'error', 'data': self.__tr("Playwright初始化失败，可能是相关文件损坏或被杀毒软件阻止。")})
                self.close_main_context()
                return None

    def perform_login(self):
        """
        在一个独立的、临时的、有头浏览器实例中执行登录流程。
        此操作现在是可中断的。
        """
        if not self.playwright_instance:
            self.log(self.__tr("Playwright服务未运行，无法登录。"), "error")
            self.result_queue.put({'type': 'login_complete', 'data': False})
            return
        
        # 登录前先关闭主数据抓取浏览器，防止cookies冲突
        self.close_main_context()
        self.log(self.__tr("正在启动一个独立的浏览器用于登录..."), "info")

        # 从当前工作线程获取停止事件，以允许用户中断登录
        stop_event = threading.Event()
        current_thread = threading.current_thread()
        if hasattr(current_thread, 'stop_event'):
            stop_event = current_thread.stop_event

        login_context = None
        try:
            # 始终以有头模式启动登录浏览器
            login_context = self.playwright_instance.chromium.launch_persistent_context(
                str(self.settings.BROWSER_DATA_DIR),
                channel=self.settings.BROWSER_TYPE,
                headless=False,
                no_viewport=True,
            )

            page = login_context.pages[0] if login_context.pages else login_context.new_page()

            # 向UI发送清晰的指示
            self.log("--------------------------------------------------", "info")
            self.log(self.__tr("请在弹出的浏览器窗口中登录Nexus Mods。"), "info")
            self.log(self.__tr("登录成功后，请手动关闭该浏览器窗口以继续。"), "info")
            self.log(self.__tr("您可以点击“停止”按钮来随时中止登录。"), "info")
            self.log("--------------------------------------------------", "info")

            login_page_url = f"{self.settings.NEXUS_LOGIN_URL}?redirect_url={self.settings.NEXUS_BASE_URL}"
            page.goto(login_page_url)

            # 轮询检查浏览器窗口是否被用户关闭或操作是否被中止
            user_closed_window = False
            while not stop_event.is_set():
                try:
                    # 等待关闭事件，设置一个短超时以保持循环的响应性
                    login_context.wait_for_event('close', timeout=500)
                    user_closed_window = True
                    self.log(self.__tr("登录浏览器已由用户关闭。"), "info")
                    break
                except PlaywrightTimeoutError:
                    continue
                
            if stop_event.is_set():
                self.log(self.__tr("登录操作已被用户中止。"), "warning")
                if login_context:
                    login_context.close()
                self.result_queue.put({'type': 'login_complete', 'data': True})
            elif user_closed_window:
                self.log(self.__tr("登录流程结束。"), "info")
                self.result_queue.put({'type': 'login_complete', 'data': True})
            else:
                self.result_queue.put({'type': 'login_complete', 'data': False})

        except Exception as e:
            self.log(self.__tr("登录流程中发生错误: {error}").format(error=e), "critical")
            self.result_queue.put({'type': 'error', 'data': self.__tr("登录失败，请检查日志。")})
            self.result_queue.put({'type': 'login_complete', 'data': False})
        finally:
            if login_context:
                try:
                    login_context.close()
                except Exception:
                    pass

    def restart_browser_for_settings_change(self):
        """为应用新设置而重启主浏览器。"""
        self.log(self.__tr("检测到设置变更，正在重启主浏览器..."), "info")
        self.close_main_context()
        self.result_queue.put({'type': 'browser_restarted', 'data': None})
