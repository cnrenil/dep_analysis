# -*- coding: utf-8 -*-

import zipfile
import lzma
import json
import os
import hashlib
import base64
import logging
import concurrent.futures
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional

try:
    from PyQt6.QtWidgets import QApplication
    import requests
    import bsdiff4
except ImportError:
    class QApplication:
        @staticmethod
        def translate(context: str, text: str) -> str: return text

from ..utils.playwright_manager import PlaywrightManager

log = logging.getLogger(__name__)

class WabbajackInstaller:
    """处理Wabbajack整合包的解析、下载和安装。"""
    def __init__(self, organizer, plugin_name, result_queue, stop_event, playwright_manager: PlaywrightManager):
        self.organizer = organizer
        self.plugin_name = plugin_name
        self.result_queue = result_queue
        self._stop_requested = stop_event
        self.playwright_manager = playwright_manager
        self.wabbajack_file_path: Optional[Path] = None
        self.install_path: Optional[Path] = None
        self.download_path: Optional[Path] = None
        self.modlist_data: Dict[str, Any] = {}
        self.archive_hashes: Dict[str, str] = {} # Map Hash -> Name
        self.__tr = lambda text: QApplication.translate("WabbajackInstaller", text)

    def _put_result(self, type: str, data: Any):
        if not self._stop_requested.is_set():
            self.result_queue.put({'type': type, 'data': data})

    def log(self, message: str, level: str = "info"):
        self._put_result('log', f"[{level.upper()}] {message}")
        if level == "error": log.error(message)
        elif level == "warning": log.warning(message)
        else: log.info(message)

    def run_installation(self, wabbajack_file: str, install_path: str, download_path: str):
        """开始完整的安装流程。"""
        self.wabbajack_file_path = Path(wabbajack_file)
        self.install_path = Path(install_path)
        self.download_path = Path(download_path)

        try:
            if not self.wabbajack_file_path.exists():
                raise FileNotFoundError("Wabbajack file not found.")

            self.install_path.mkdir(parents=True, exist_ok=True)
            self.download_path.mkdir(parents=True, exist_ok=True)

            # 1. 解析 Wabbajack 文件
            self.log(self.__tr("正在解析Wabbajack文件..."))
            self.parse_wabbajack_file()
            if self._stop_requested.is_set(): return

            # 2. 下载所有必需的压缩包
            self.log(self.__tr("开始下载所有必需的模组文件..."))
            self.download_archives()
            if self._stop_requested.is_set(): return

            # 3. 处理安装指令
            self.log(self.__tr("所有文件下载完成，开始安装..."))
            self.process_directives()
            if self._stop_requested.is_set(): return

            self.log(self.__tr("Wabbajack整合包安装成功！"))
            self._put_result('wabbajack_complete', {'success': True})

        except Exception as e:
            self.log(f"{self.__tr('安装过程中发生错误')}: {e}", "error")
            self._put_result('wabbajack_complete', {'success': False, 'error': str(e)})

    def parse_wabbajack_file(self):
        """解压并解析modlist.json和图片。"""
        with zipfile.ZipFile(self.wabbajack_file_path, 'r') as zf:
            modlist_compressed = zf.read('modlist')
            self.modlist_data = json.loads(lzma.decompress(modlist_compressed))
            
            image_name = self.modlist_data.get("Image")
            if image_name and image_name in zf.namelist():
                image_data = zf.read(image_name)
                self._put_result('wabbajack_info_ready', {
                    'info': self.modlist_data,
                    'image': image_data
                })
            else:
                self._put_result('wabbajack_info_ready', {
                    'info': self.modlist_data,
                    'image': None
                })
        
        for archive in self.modlist_data.get("Archives", []):
            self.archive_hashes[archive["Hash"]] = archive["Name"]

    def download_archives(self):
        archives = self.modlist_data.get("Archives", [])
        self._put_result('wabbajack_archives_total', len(archives))

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_archive = {executor.submit(self.download_single_archive, archive): archive for archive in archives}
            for i, future in enumerate(concurrent.futures.as_completed(future_to_archive)):
                if self._stop_requested.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    self.log(self.__tr("下载已中止。"), "warning")
                    return
                
                archive = future_to_archive[future]
                try:
                    result = future.result()
                    self._put_result('wabbajack_archive_progress', {'name': archive['Name'], 'status': result})
                except Exception as exc:
                    self.log(f"{archive['Name']} {self.__tr('生成下载任务时出错')}: {exc}", "error")
                    self._put_result('wabbajack_archive_progress', {'name': archive['Name'], 'status': 'Error'})

    def download_single_archive(self, archive: Dict[str, Any]) -> str:
        """下载单个压缩包并校验哈希。"""
        archive_name = archive["Name"]
        target_path = self.download_path / archive_name
        expected_hash = archive["Hash"]
        state = archive["State"]
        downloader_type = state["$type"].split(',')[0]

        if target_path.exists() and self.verify_hash(target_path, expected_hash):
            self.log(f"{archive_name} {self.__tr('已存在且校验通过，跳过。')}")
            return "Skipped"

        downloader_map = {
            "HttpDownloader": self._download_http,
            "NexusDownloader": self._download_nexus,
            "GameFileSourceDownloader": self._copy_game_file,
            "WabbajackCDNDownloader+State": self._download_http,
        }

        if downloader_type in downloader_map:
            try:
                downloader_map[downloader_type](state, target_path)
                if self.verify_hash(target_path, expected_hash):
                    self.log(f"{archive_name} {self.__tr('下载并校验成功。')}")
                    return "Downloaded"
                else:
                    self.log(f"{archive_name} {self.__tr('下载后哈希校验失败！')}", "error")
                    target_path.unlink(missing_ok=True)
                    return "Hash Mismatch"
            except Exception as e:
                self.log(f"{self.__tr('下载')} {archive_name} {self.__tr('时出错')}: {e}", "error")
                target_path.unlink(missing_ok=True)
                return "Error"
        else:
            self.log(f"{self.__tr('未知的下载器类型')}: {downloader_type} for {archive_name}", "warning")
            return "Unknown Downloader"

    def _download_http(self, state: Dict[str, Any], target_path: Path):
        url = state["Url"]
        self.log(f"{self.__tr('正在从URL下载')}: {target_path.name}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(target_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if self._stop_requested.is_set(): raise InterruptedError("Download cancelled")
                    f.write(chunk)

    def _download_nexus(self, state: Dict[str, Any], target_path: Path):
        mod_id = state["ModID"]
        file_id = state["FileID"]
        game_name = state["GameName"]
        # Nexus下载通常需要API key或登录会话。这里我们尝试使用Playwright。
        # 这是一个简化的实现，实际可能更复杂。
        url = f"https://www.nexusmods.com/{game_name}/mods/{mod_id}?tab=files&file_id={file_id}"
        self.log(f"{self.__tr('正在准备从Nexus下载')}: {target_path.name} (ModID: {mod_id})")
        
        page = self.playwright_manager.get_page()
        if not page:
            raise ConnectionError("Playwright page not available for Nexus download.")

        page.goto(url)
        
        # 尝试找到并点击手动下载按钮，然后处理可能的慢速下载确认
        try:
            # 等待主要下载按钮出现
            primary_download_button = page.locator(f'a[href*="/mods/{mod_id}/files/{file_id}?key="]').first
            primary_download_button.wait_for(state="visible", timeout=15000)
            
            with page.expect_download() as download_info:
                primary_download_button.click()
                
                # 检查是否有慢速下载按钮
                slow_download_button = page.locator('a:has-text("Slow Download")')
                if slow_download_button.is_visible(timeout=5000):
                    slow_download_button.click()

            download = download_info.value
            download.save_as(target_path)
        except Exception as e:
            raise RuntimeError(f"Failed to automate Nexus download for {target_path.name}. Please download it manually. Error: {e}")

    def _copy_game_file(self, state: Dict[str, Any], target_path: Path):
        game_path = Path(self.organizer.managedGame().gameDirectory().absolutePath())
        source_file = game_path / state["GameFile"]
        self.log(f"{self.__tr('正在从游戏目录复制')}: {source_file}")
        if source_file.exists():
            shutil.copy(source_file, target_path)
        else:
            raise FileNotFoundError(f"Game file not found: {source_file}")

    def verify_hash(self, file_path: Path, b64_hash: str) -> bool:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        
        computed_hash = base64.b64encode(hasher.digest()).decode('utf-8')
        return computed_hash == b64_hash

    def process_directives(self):
        directives = self.modlist_data.get("Directives", [])
        self._put_result('wabbajack_directives_total', len(directives))

        with zipfile.ZipFile(self.wabbajack_file_path, 'r') as wj_zip:
            for i, directive in enumerate(directives):
                if self._stop_requested.is_set():
                    self.log(self.__tr("安装已中止。"), "warning")
                    return

                directive_type = directive["$type"]
                target_rel_path = directive["To"]
                target_abs_path = self.install_path / target_rel_path
                target_abs_path.parent.mkdir(parents=True, exist_ok=True)
                
                self._put_result('wabbajack_directive_progress', {'path': target_rel_path, 'status': 'Processing'})
                
                try:
                    if directive_type == "FromArchive":
                        archive_hash, path_in_archive = directive["ArchiveHashPath"]
                        archive_name = self.archive_hashes[archive_hash]
                        archive_path = self.download_path / archive_name
                        with zipfile.ZipFile(archive_path, 'r') as zf:
                            zf.extract(path_in_archive, target_abs_path.parent)
                            # 提取后可能需要重命名
                            extracted_path = target_abs_path.parent / path_in_archive
                            if extracted_path != target_abs_path:
                                extracted_path.rename(target_abs_path)

                    elif directive_type in ["InlineFile", "RemappedInlineFile"]:
                        source_id = directive["SourceDataID"]
                        inline_data = wj_zip.read(source_id)
                        with open(target_abs_path, 'wb') as f:
                            f.write(inline_data)

                    elif directive_type == "PatchedFromArchive":
                        archive_hash, path_in_archive = directive["ArchiveHashPath"]
                        archive_name = self.archive_hashes[archive_hash]
                        archive_path = self.download_path / archive_name
                        patch_id = directive["PatchID"]
                        
                        with zipfile.ZipFile(archive_path, 'r') as zf:
                            original_data = zf.read(path_in_archive)
                        
                        patch_data = wj_zip.read(patch_id)
                        
                        patched_data = bsdiff4.patch(original_data, patch_data)
                        
                        with open(target_abs_path, 'wb') as f:
                            f.write(patched_data)
                    
                    self._put_result('wabbajack_directive_progress', {'path': target_rel_path, 'status': 'Done'})

                except Exception as e:
                    self.log(f"{self.__tr('处理指令失败')} {target_rel_path}: {e}", "error")
                    self._put_result('wabbajack_directive_progress', {'path': target_rel_path, 'status': 'Error'})
