# -*- coding: utf-8 -*-

import zipfile
import json
import os
import base64
import concurrent.futures
import shutil
import threading
import time
import gzip
import queue
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Iterator
from urllib.parse import urlparse, urlunparse, parse_qs

try:
    from PyQt6.QtWidgets import QApplication
    import requests
    import bsdiff4
    import xxhash
except ImportError:
    class QApplication:
        @staticmethod
        def translate(context: str, text: str) -> str: return text

from ..utils.playwright_manager import PlaywrightManager
from ..core.settings import PluginSettings
from .. import logging as plugin_logging

log = plugin_logging.get_logger(__name__)

DOMAIN_REMAPS = {
    "wabbajack.b-cdn.net": "authored-files.wabbajack.org",
    "wabbajack-mirror.b-cdn.net": "mirror.wabbajack.org",
    "wabbajack-patches.b-cdn.net": "patches.wabbajack.org",
    "wabbajacktest.b-cdn.net": "test-files.wabbajack.org"
}

class InstallCancelledError(Exception):
    """自定义异常，用于表示安装被用户取消。"""
    pass

class WabbajackInstaller:
    """处理Wabbajack整合包的解析、下载和安装，支持断点续传和低内存占用。"""
    def __init__(self, organizer, plugin_name, result_queue, stop_event, playwright_manager: PlaywrightManager, settings: PluginSettings):
        self.organizer = organizer
        self.plugin_name = plugin_name
        self.result_queue = result_queue
        self._stop_requested = stop_event
        self.playwright_manager = playwright_manager
        self.settings = settings
        self.wabbajack_file_path: Optional[Path] = None
        self.install_path: Optional[Path] = None
        self.download_path: Optional[Path] = None
        self.progress_file_path: Optional[Path] = None
        self.modlist_data: Dict[str, Any] = {}
        self.archive_hashes: Dict[str, str] = {} # 映射：哈希 -> 文件名
        self.parse_only_mode = False
        self.__tr = lambda text: QApplication.translate("WabbajackInstaller", text)
        
        self._progress_lock = threading.Lock()
        self._progress_state = {}
        self._progress_reporter_thread = None
        self._downloading_active = False
        
        self._directives_processed_count = 0
        self._eta_reporter_thread = None
        self._directive_processing_active = False
        self._progress_save_lock = threading.Lock()
        self._progress_data: Dict[str, Any] = {}


    def _check_stop_signal(self):
        """检查是否收到了停止信号，如果收到则抛出异常。"""
        if self._stop_requested.is_set():
            raise InstallCancelledError("Installation was cancelled by the user.")

    def _put_result(self, type: str, data: Any):
        """向UI线程安全地发送结果。"""
        if type in ['wabbajack_complete', 'wabbajack_download_update'] or not self._stop_requested.is_set():
            self.result_queue.put({'type': type, 'data': data})

    def _reset_progress_state(self):
        """重置进度跟踪状态。"""
        with self._progress_lock:
            self._progress_state = {
                'total_size': 0,
                'total_downloaded': 0,
                'workers': {} 
            }

    def _report_progress(self):
        """定期向UI发送聚合的下载进度。"""
        while self._downloading_active:
            if self._stop_requested.is_set(): break
            with self._progress_lock:
                state_copy = {
                    'total_size': self._progress_state.get('total_size', 0),
                    'total_downloaded': self._progress_state.get('total_downloaded', 0),
                    'workers': dict(self._progress_state.get('workers', {}))
                }
            
            self._put_result('wabbajack_download_update', state_copy)
            time.sleep(1)

    def run_installation(self, wabbajack_file: str, install_path: str, download_path: str, parse_only: bool):
        """启动完整的安装流程。"""
        try:
            self.wabbajack_file_path = Path(wabbajack_file)
            self.install_path = Path(install_path)
            self.download_path = Path(download_path)
            self.progress_file_path = self.install_path / "wabbajack_progress.json"
            self.parse_only_mode = parse_only

            self._progress_data = self._load_progress()

            if not self.wabbajack_file_path.exists():
                raise FileNotFoundError(self.__tr("Wabbajack文件未找到。"))

            log.info(self.__tr("正在解析Wabbajack文件..."))
            self.parse_wabbajack_file()
            self._check_stop_signal()
            
            if not self.parse_only_mode:
                os.makedirs(self.install_path, exist_ok=True)
                os.makedirs(self.download_path, exist_ok=True)

            self.download_archives()
            self._check_stop_signal()

            self.process_directives()
            self._check_stop_signal()
            
            if self.parse_only_mode:
                log.info(self.__tr("“仅测试解析”模式已完成模拟流程。"))
            else:
                log.info(self.__tr("Wabbajack整合包已成功安装！"))
                if self.progress_file_path and self.progress_file_path.exists():
                    os.remove(self.progress_file_path)
                    log.info(self.__tr("安装成功，已删除临时进度文件。"))


            self._put_result('wabbajack_complete', {'success': True, 'parse_only': self.parse_only_mode})

        except InstallCancelledError:
            log.warning(self.__tr("安装已中止。"))
        except Exception as e:
            log.error(f"{self.__tr('安装过程中发生错误')}: {e}", exc_info=True)
            self._put_result('wabbajack_complete', {'success': False, 'error': str(e)})
        finally:
            self._downloading_active = False
            if self._progress_reporter_thread and self._progress_reporter_thread.is_alive():
                self._progress_reporter_thread.join()
            self._put_result('wabbajack_download_update', None)
            if self._stop_requested.is_set():
                self._put_result('wabbajack_complete', {'success': False, 'stopped': True})

    def parse_wabbajack_file(self):
        """解压并解析modlist和图片。"""
        with zipfile.ZipFile(self.wabbajack_file_path, 'r') as zf:
            self.modlist_data = json.loads(zf.read('modlist'))
            
            image_name = self.modlist_data.get("Image")
            image_data = None
            if image_name and image_name in zf.namelist():
                image_data = zf.read(image_name)

            self._put_result('wabbajack_info_ready', {
                'info': self.modlist_data,
                'image': image_data
            })
        
        for archive in self.modlist_data.get("Archives", []):
            self.archive_hashes[archive["Hash"]] = archive["Name"]

    def _task_generator(self, archives_to_download: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """
        一个生成器，作为生产者。它会立即产出非Nexus下载任务，
        然后按照规则（检查缓存、10秒延迟）逐个获取Nexus链接并产出HTTP下载任务。
        """
        nexus_archives = [a for a in archives_to_download if a["State"]["$type"].split(',')[0] == "NexusDownloader"]
        other_archives = [a for a in archives_to_download if a["State"]["$type"].split(',')[0] != "NexusDownloader"]

        log.info(self.__tr("阶段 1: 正在提交 {count} 个非Nexus直接下载任务...").format(count=len(other_archives)))
        for archive in other_archives:
            yield archive
        
        if nexus_archives:
            log.info(self.__tr("阶段 2: 开始获取 {count} 个Nexus下载链接并提交... (链接获取将串行并有延迟)").format(count=len(nexus_archives)))
        
        for i, archive in enumerate(nexus_archives):
            self._check_stop_signal()

            if i > 0:
                log.debug(self.__tr("为减轻Nexus服务器负担，强制等待10秒..."))
                for _ in range(10):
                    time.sleep(1)
                    self._check_stop_signal()
            
            log.debug(self.__tr("正在处理 {name} ({num}/{total})...").format(name=archive['Name'], num=i+1, total=len(nexus_archives)))
            
            download_url = None
            archive_hash = archive["Hash"]
            cached_url_info = self._progress_data.get('resolved_urls', {}).get(archive_hash)

            if cached_url_info and time.time() < cached_url_info.get('expires', 0):
                download_url = cached_url_info['url']
                log.info(self.__tr("发现有效的缓存链接，将直接使用。"))
            else:
                try:
                    download_url = self._get_nexus_download_url(archive['State'])
                    parsed_url = urlparse(download_url)
                    query_params = parse_qs(parsed_url.query)
                    if 'expires' in query_params:
                        expires_timestamp = int(query_params['expires'][0])
                        safe_expires = expires_timestamp - 3600 # 减去一小时作为安全缓冲
                        self._progress_data.setdefault('resolved_urls', {})[archive_hash] = {
                            'url': download_url,
                            'expires': safe_expires
                        }
                        self._save_progress(self._progress_data)
                except Exception as e:
                    log.error(self.__tr("获取 {name} 的下载链接失败: {error}, 该文件将被跳过。").format(name=archive['Name'], error=e))
                    yield {"$type": "ErrorState", **archive}
                    continue

            http_task = archive.copy()
            http_task['State'] = {
                "$type": "HttpDownloader, Wabbajack.Lib",
                "Url": download_url
            }
            yield http_task


    def download_archives(self):
        """使用生产者-消费者模式并行下载所有文件。"""
        all_archives = self.modlist_data.get("Archives", [])
        archives_to_download = []
        completed_tasks = 0
        
        log.info(self.__tr("阶段 0: 正在检查文件..."))
        self._put_result('wabbajack_phase_start', {'phase': 'checking', 'total': len(all_archives)})
        
        verified_files = self._progress_data.get('verified_archives', {})
        needs_save = False

        for archive in all_archives:
            self._check_stop_signal()
            archive_hash = archive["Hash"]
            target_path = self.download_path / archive["Name"]
            
            if archive_hash in verified_files and Path(verified_files[archive_hash]).exists():
                 log.debug(f"{self.__tr('文件已在进度文件中标记为已验证，跳过')}: {archive['Name']}")
            elif target_path.exists() and self.verify_hash(target_path, archive_hash):
                log.debug(f"{self.__tr('文件已存在且校验通过，跳过')}: {archive['Name']}")
                self._progress_data.setdefault('verified_archives', {})[archive_hash] = str(target_path)
                needs_save = True
            else:
                archives_to_download.append(archive)
                continue
            
            self._put_result('wabbajack_archive_progress', {'name': archive['Name'], 'status': 'Skipped'})
            completed_tasks += 1
            self._put_result('wabbajack_task_progress', {'current': completed_tasks, 'total': len(all_archives)})
        
        if needs_save:
            self._save_progress(self._progress_data)

        if not archives_to_download:
            log.info(self.__tr("所有文件均已存在或无需下载。"))
            return

        self._reset_progress_state()
        total_size = sum(a.get('Size', 0) for a in archives_to_download)
        with self._progress_lock:
            self._progress_state['total_size'] = total_size

        self._downloading_active = True
        self._progress_reporter_thread = threading.Thread(target=self._report_progress, daemon=True)
        self._progress_reporter_thread.start()
        
        total_tasks = len(all_archives)
        self._put_result('wabbajack_phase_start', {'phase': 'downloading', 'total': total_tasks})
        
        log.info(self.__tr("阶段 3: 正在并行下载所有文件... (链接获取与文件下载将同时进行)"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.settings.MAX_WORKERS, thread_name_prefix="WJ-Worker") as executor:
            task_generator = self._task_generator(archives_to_download)
            
            futures = {
                executor.submit(self.download_single_archive, task, completed_tasks + i + 1, total_tasks): task
                for i, task in enumerate(task_generator)
            }
            
            for future in concurrent.futures.as_completed(futures):
                self._check_stop_signal()
                try:
                    future.result()
                except InstallCancelledError:
                    for f in futures.keys(): f.cancel()
                    raise
                except Exception as e:
                    archive_name = futures[future]["Name"]
                    log.error(f"下载 {archive_name} 时任务失败: {e}")

    def _simulate_download(self, worker_id: str, archive_name: str, total_size: int, speed_mbps: float):
        """用真实的进度更新来模拟下载过程。"""
        speed_bps = speed_mbps * 1024 * 1024
        if total_size == 0 or speed_bps == 0:
            self._update_download_progress(worker_id, archive_name, total_size, total_size, total_size)
            return

        update_interval = 0.05
        downloaded = 0
        self._update_download_progress(worker_id, archive_name, 0, total_size, 0)
        
        while downloaded < total_size:
            self._check_stop_signal()
            
            time.sleep(update_interval)
            chunk_size = int(speed_bps * update_interval)
            downloaded += chunk_size
            
            if downloaded >= total_size:
                chunk_size -= (downloaded - total_size)
                downloaded = total_size
            
            self._update_download_progress(worker_id, archive_name, downloaded, total_size, chunk_size)

    def download_single_archive(self, archive: Dict[str, Any], current_task_num: int, total_tasks: int) -> str:
        """下载单个文件并校验哈希。Worker ID由线程决定。"""
        self._check_stop_signal()
        worker_id = threading.current_thread().name
        archive_name = archive["Name"]
        archive_hash = archive["Hash"]
        state = archive["State"]
        downloader_type = state["$type"].split(',')[0]
        
        self._put_result('wabbajack_task_progress', {'current': current_task_num, 'total': total_tasks})

        if downloader_type == "ErrorState":
            self._put_result('wabbajack_archive_progress', {'name': archive_name, 'status': 'Error'})
            return "Error"

        if self.parse_only_mode:
            log.debug(f"[{self.__tr('仅解析')}] {self.__tr('模拟下载')} {archive_name} ({downloader_type})")
            total_size = archive.get("Size", 1024 * 1024)
            
            sim_speed = 30.0
            if downloader_type == "GameFileSourceDownloader": sim_speed = 300.0
            elif downloader_type == "NexusDownloader" or state.get("Url", "").startswith("https://www.nexusmods.com"): sim_speed = 5.0
            
            try:
                self._simulate_download(worker_id, archive_name, total_size, sim_speed)
            finally:
                self._clear_worker_progress(worker_id)

            log.debug(f"[{self.__tr('仅解析')}] {self.__tr('模拟下载完成')}: {archive_name}")
            self._put_result('wabbajack_archive_progress', {'name': archive_name, 'status': "Parse Only"})
            return "Parse Only"

        target_path = self.download_path / archive_name
        
        downloader_map = {
            "HttpDownloader": self._download_http,
            "GameFileSourceDownloader": self._copy_game_file,
            "WabbajackCDNDownloader+State": self._download_wabbajack_cdn,
        }

        result = "Error"
        try:
            if downloader_type in downloader_map:
                downloader_map[downloader_type](state, target_path, worker_id)
                if self.verify_hash(target_path, archive_hash):
                    result = "Downloaded"
                    self._progress_data.setdefault('verified_archives', {})[archive_hash] = str(target_path)
                    self._save_progress(self._progress_data)
                    log.debug(f"{self.__tr('下载完成')}: {archive_name}")
                else:
                    log.error(f"{archive_name} {self.__tr('下载后哈希校验失败！')}")
                    target_path.unlink(missing_ok=True)
                    result = self.__tr("哈希不匹配")
            else:
                log.warning(f"{self.__tr('未知的下载器类型')}: {downloader_type} for {archive_name}")
                result = self.__tr("未知下载器")
        except Exception as e:
            if not isinstance(e, InstallCancelledError):
                log.error(f"{self.__tr('下载')} {archive_name} {self.__tr('时出错')}: {e}", exc_info=True)
            target_path.unlink(missing_ok=True)
            result = self.__tr("错误")
            if isinstance(e, InstallCancelledError):
                raise
        
        self._put_result('wabbajack_archive_progress', {'name': archive_name, 'status': result})
        return result

    def _update_download_progress(self, worker_id, file_name, downloaded, total, chunk_size):
        """更新单个线程和总体的下载进度。"""
        with self._progress_lock:
            if self.parse_only_mode:
                self._progress_state['total_downloaded'] = self._progress_state.get('total_downloaded', 0) + chunk_size
            else:
                 self._progress_state['total_downloaded'] += chunk_size

            worker_data = self._progress_state['workers'].get(worker_id, {})
            
            if 'start_time' not in worker_data or worker_data.get('file') != file_name:
                worker_data['start_time'] = time.time()
                worker_data['downloaded_since_start'] = 0

            worker_data['file'] = file_name
            worker_data['downloaded'] = downloaded
            worker_data['total'] = total
            worker_data['downloaded_since_start'] += chunk_size
            
            elapsed = time.time() - worker_data['start_time']
            worker_data['speed'] = worker_data['downloaded_since_start'] / elapsed if elapsed > 0 else 0
            self._progress_state['workers'][worker_id] = worker_data

    def _clear_worker_progress(self, worker_id):
        """从UI中清理一个已完成的工作线程。"""
        with self._progress_lock:
            self._progress_state['workers'].pop(worker_id, None)

    def _get_remapped_url_and_headers(self, url_str: str) -> tuple[str, dict]:
        """应用WabbajackCDN域名重映射并返回新URL和请求头。"""
        parsed_url = urlparse(url_str)
        new_host = DOMAIN_REMAPS.get(parsed_url.netloc)
        
        if new_host:
            new_url_parts = parsed_url._replace(netloc=new_host)
            new_url = urlunparse(new_url_parts)
            headers = {'Host': new_host}
            return new_url, headers
        return url_str, {}

    def _download_http(self, state: Dict[str, Any], target_path: Path, worker_id: str):
        """处理标准的HTTP下载。"""
        url, headers = self._get_remapped_url_and_headers(state["Url"])
        log.debug(f"({worker_id}) {self.__tr('正在从URL下载')}: {target_path.name}")
        
        last_exception = None
        try:
            for attempt in range(self.settings.MAX_RETRIES):
                self._check_stop_signal()
                try:
                    with requests.get(url, headers=headers, stream=True, timeout=30) as r:
                        r.raise_for_status()
                        total_size = int(r.headers.get('content-length', 0))
                        downloaded = 0
                        with open(target_path, 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                self._check_stop_signal()
                                if chunk:
                                    f.write(chunk)
                                    chunk_len = len(chunk)
                                    downloaded += chunk_len
                                    self._update_download_progress(worker_id, target_path.name, downloaded, total_size, chunk_len)
                    return
                except requests.RequestException as e:
                    last_exception = e
                    log.warning(f"({worker_id}) {self.__tr('下载 {file} 时出错 (尝试 {n}/{max})').format(file=target_path.name, n=attempt + 1, max=self.settings.MAX_RETRIES)}: {e}")
                    time.sleep(self.settings.RETRY_DELAY_MS / 1000)
            
            if last_exception:
                raise last_exception
        finally:
            self._clear_worker_progress(worker_id)

    def _download_wabbajack_cdn(self, state: Dict[str, Any], target_path: Path, worker_id: str):
        """处理WabbajackCDN文件的多分块下载逻辑。"""
        base_url = state["Url"]
        log.debug(f"({worker_id}) {self.__tr('正在处理WabbajackCDN文件')}: {target_path.name}")
        
        try:
            def_url, def_headers = self._get_remapped_url_and_headers(f"{base_url}/definition.json.gz")
            with requests.get(def_url, headers=def_headers, timeout=30) as r:
                r.raise_for_status()
                decompressed_data = gzip.decompress(r.content)
                definition = json.loads(decompressed_data)

            log.debug(f"({worker_id}) {self.__tr('文件定义获取成功，准备下载 {count} 个分块...').format(count=len(definition['Parts']))}")
            
            downloaded_parts = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.settings.MAX_WORKERS) as part_executor:
                future_to_part = {part_executor.submit(self._download_cdn_part, base_url, part): part for part in definition['Parts']}
                for future in concurrent.futures.as_completed(future_to_part):
                    self._check_stop_signal()
                    part_index, part_data = future.result()
                    downloaded_parts[part_index] = part_data

            with open(target_path, "wb") as f:
                for part in sorted(definition['Parts'], key=lambda p: p['Offset']):
                    self._check_stop_signal()
                    f.seek(part['Offset'])
                    part_data = downloaded_parts.get(part['Index'])
                    if not part_data: raise IOError(f"Part {part['Index']} was not downloaded.")
                    f.write(part_data)
                    self._update_download_progress(worker_id, target_path.name, f.tell(), definition['Size'], len(part_data))
        finally:
            self._clear_worker_progress(worker_id)

    def _download_cdn_part(self, base_url: str, part_info: dict) -> tuple[int, bytes]:
        """下载WabbajackCDN文件的单个分块。"""
        worker_id = threading.current_thread().name
        part_url, part_headers = self._get_remapped_url_and_headers(f"{base_url}/parts/{part_info['Index']}")
        
        for attempt in range(self.settings.MAX_RETRIES):
            self._check_stop_signal()
            try:
                with requests.get(part_url, headers=part_headers, timeout=60) as r:
                    r.raise_for_status()
                    data = r.content
                h = xxhash.xxh64(data).intdigest().to_bytes(8, 'little', signed=False)
                computed_hash = base64.b64encode(h).decode('utf-8')
                if computed_hash == part_info['Hash']:
                    return part_info['Index'], data
                else:
                    log.warning(f"({worker_id}) {self.__tr('分块 {idx} 哈希校验失败 (尝试 {n})').format(idx=part_info['Index'], n=attempt + 1)}")
            except requests.RequestException as e:
                log.warning(f"({worker_id}) {self.__tr('下载分块 {idx} 时出错 (尝试 {n})').format(idx=part_info['Index'], n=attempt + 1)}: {e}")
            time.sleep(self.settings.RETRY_DELAY_MS / 1000)
        raise IOError(f"Failed to download part {part_info['Index']} for {base_url} after multiple retries.")

    def _get_nexus_download_url(self, state: Dict[str, Any]) -> str:
        """使用Playwright和API调用来获取真实的Nexus下载链接，跳过等待时间。"""
        mod_id, file_id, game_name = state["ModID"], state["FileID"], state["GameName"].lower()
        page_url = f"https://www.nexusmods.com/{game_name}/mods/{mod_id}?tab=files&file_id={file_id}"
        log.debug(f"正在为 {mod_id}/{file_id} 获取下载链接...")

        page = self.playwright_manager.get_page()
        if not page:
            raise ConnectionError(self.__tr("Playwright页面不可用。"))
        
        page.goto(page_url, wait_until='domcontentloaded')
        game_id = page.locator('#section').get_attribute('data-game-id')

        if not game_id:
            raise ValueError("Could not determine game_id from page.")

        api_url = f"{self.settings.NEXUS_BASE_URL}/Core/Libs/Common/Managers/Downloads?GenerateDownloadUrl"
        
        script = """
        async (args) => {
            const response = await fetch(args.apiUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': args.pageUrl
                },
                body: `fid=${args.fileId}&game_id=${args.gameId}`
            });
            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`API request failed with status ${response.status}: ${errorText}`);
            }
            return await response.json();
        }
        """
        
        api_response = page.evaluate(script, {
            "apiUrl": api_url,
            "pageUrl": page_url,
            "fileId": file_id,
            "gameId": game_id
        })

        if api_response and api_response.get('url'):
            log.debug(api_response['url'])
            return api_response['url']
        
        raise ValueError("API response did not contain a valid URL.")

    def _download_nexus(self, state: Dict[str, Any], target_path: Path, worker_id: str):
        """处理Nexus下载，先获取链接再用requests下载。"""
        log.debug(f"({worker_id}) {self.__tr('正在准备从Nexus下载')}: {target_path.name}")
        try:
            download_url = self._get_nexus_download_url(state)
            log.debug(f"({worker_id}) 获取到下载链接: {download_url}")
            
            http_state = {"Url": download_url}
            self._download_http(http_state, target_path, worker_id)
        except Exception as e:
            log.error(f"({worker_id}) Nexus下载失败: {e}", exc_info=True)
            raise

    def _copy_game_file(self, state: Dict[str, Any], target_path: Path, worker_id: str):
        """从游戏目录复制文件。"""
        total_size = state.get("Size", 0)
        self._update_download_progress(worker_id, target_path.name, 0, total_size, 0)
        try:
            game_path = Path(self.organizer.managedGame().gameDirectory().absolutePath())
            source_file = game_path / state["GameFile"]
            log.debug(f"({worker_id}) {self.__tr('正在从游戏目录复制')}: {source_file}")
            if source_file.exists():
                shutil.copy(source_file, target_path)
                self._update_download_progress(worker_id, target_path.name, total_size, total_size, total_size)
            else:
                raise FileNotFoundError(f"{self.__tr('游戏文件未找到')}: {source_file}")
        finally:
            self._clear_worker_progress(worker_id)

    def verify_hash(self, file_path: Path, b64_hash: str) -> bool:
        """使用xxHash64校验文件哈希。"""
        h = xxhash.xxh64()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                h.update(chunk)
        hash_int = h.intdigest()
        hash_bytes = hash_int.to_bytes(8, 'little', signed=False)
        computed_hash = base64.b64encode(hash_bytes).decode('utf-8')
        return computed_hash == b64_hash

    def _load_progress(self) -> dict:
        """加载安装进度，如果文件不存在则返回一个空的默认结构。"""
        default_progress = {
            'last_completed_directive': -1,
            'verified_archives': {},
            'resolved_urls': {}
        }
        if self.parse_only_mode:
            return default_progress
        if self.progress_file_path and self.progress_file_path.exists():
            try:
                with open(self.progress_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for key, value in default_progress.items():
                        data.setdefault(key, value)
                    return data
            except (json.JSONDecodeError, IOError):
                log.warning(self.__tr("进度文件损坏，将重新开始。"))
        return default_progress


    def _save_progress(self, progress_data: dict):
        """原子地保存安装进度。"""
        if self.parse_only_mode or not self.progress_file_path:
            return
        
        temp_path = self.progress_file_path.with_suffix('.tmp')
        with self._progress_save_lock:
            try:
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(progress_data, f, indent=4)
                os.replace(temp_path, self.progress_file_path)
            except IOError as e:
                log.error(f"{self.__tr('无法保存进度')}: {e}")
            finally:
                if temp_path.exists():
                    os.remove(temp_path)

    def _report_directive_eta(self, start_time: float, total_directives: int, start_index: int):
        """定期报告指令处理的预计剩余时间。"""
        while self._directive_processing_active:
            try:
                self._check_stop_signal()
            except InstallCancelledError:
                break

            elapsed_time = time.time() - start_time
            
            with self._progress_lock:
                processed_since_start = self._directives_processed_count - start_index

            if elapsed_time > 2 and processed_since_start > 0:
                speed = processed_since_start / elapsed_time
                remaining_directives = total_directives - self._directives_processed_count
                if speed > 0:
                    eta_seconds = int(remaining_directives / speed)
                    eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
                    log.info(self.__tr("已处理 {done}/{total} 条指令，预计剩余时间: {eta}").format(
                        done=self._directives_processed_count, total=total_directives, eta=eta_str))
            
            time.sleep(5)

    def _process_single_directive(self, directive: Dict, wj_zip_handle: zipfile.ZipFile):
        """处理单个安装指令，此方法在线程池中执行。"""
        self._check_stop_signal()
        worker_id = threading.current_thread().name
        self._put_result('wabbajack_directive_update', {'worker_id': worker_id, 'directive': directive, 'active': True})
        
        target_rel_path = directive["To"]
        if self.parse_only_mode:
            log.debug(f"[{self.__tr('仅解析')}] {self.__tr('模拟应用指令')} '{directive['$type']}' {self.__tr('到')} '{target_rel_path}'")
            time.sleep(0.01)
        else:
            target_abs_path = self.install_path / target_rel_path
            os.makedirs(target_abs_path.parent, exist_ok=True)
            
            if directive["$type"] == "FromArchive":
                archive_hash, path_in_archive = directive["ArchiveHashPath"]
                archive_path = self.download_path / self.archive_hashes[archive_hash]
                with zipfile.ZipFile(archive_path, 'r') as zf, zf.open(path_in_archive) as source, open(target_abs_path, 'wb') as dest:
                    shutil.copyfileobj(source, dest)
            elif directive["$type"] in ["InlineFile", "RemappedInlineFile"]:
                with wj_zip_handle.open(directive["SourceDataID"]) as source, open(target_abs_path, 'wb') as dest:
                    shutil.copyfileobj(source, dest)
            elif directive["$type"] == "PatchedFromArchive":
                archive_hash, path_in_archive = directive["ArchiveHashPath"]
                archive_path = self.download_path / self.archive_hashes[archive_hash]
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    original_data = zf.read(path_in_archive)
                patch_data = wj_zip_handle.read(directive["PatchID"])
                patched_data = bsdiff4.patch(original_data, patch_data)
                with open(target_abs_path, 'wb') as f: f.write(patched_data)

        self._put_result('wabbajack_directive_update', {'worker_id': worker_id, 'directive': directive, 'active': False})

    def process_directives(self):
        """使用线程池并行处理所有安装指令。"""
        directives = self.modlist_data.get("Directives", [])
        if not directives:
            log.info(self.__tr("没有需要处理的安装指令。"))
            return
            
        start_index = self._progress_data.get('last_completed_directive', -1) + 1
        
        log.info(self.__tr("最终阶段: 正在安装文件..."))
        self._put_result('wabbajack_phase_start', {'phase': 'installing', 'total': len(directives)})
        if start_index > 0:
            log.info(self.__tr("检测到安装进度，从第 {index} 条指令恢复。").format(index=start_index + 1))
            self._put_result('wabbajack_task_progress', {'current': start_index, 'total': len(directives)})

        self._directives_processed_count = start_index
        start_time = time.time()
        self._directive_processing_active = True
        completed_indices = set(range(start_index))
        
        self._eta_reporter_thread = threading.Thread(
            target=self._report_directive_eta, 
            args=(start_time, len(directives), start_index), 
            daemon=True
        )
        self._eta_reporter_thread.start()

        try:
            with zipfile.ZipFile(self.wabbajack_file_path, 'r') as wj_zip:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.settings.MAX_WORKERS, thread_name_prefix="WJ-Installer") as executor:
                    
                    futures = {
                        executor.submit(self._process_single_directive, directive, wj_zip): i
                        for i, directive in enumerate(directives) if i >= start_index
                    }

                    for future in concurrent.futures.as_completed(futures):
                        self._check_stop_signal()
                        index = futures[future]
                        try:
                            future.result()
                            
                            with self._progress_save_lock:
                                completed_indices.add(index)
                                last_consecutive = self._progress_data.get('last_completed_directive', -1)
                                while (last_consecutive + 1) in completed_indices:
                                    last_consecutive += 1
                                self._progress_data['last_completed_directive'] = last_consecutive
                                self._save_progress(self._progress_data)

                            with self._progress_lock:
                                self._directives_processed_count = len(completed_indices)

                            self._put_result('wabbajack_task_progress', {'current': self._directives_processed_count, 'total': len(directives)})
                        
                        except Exception as e:
                            self._check_stop_signal()
                            directive = directives[index]
                            log.error(f"{self.__tr('处理指令失败')} {directive['To']}: {e}", exc_info=True)
                            self._stop_requested.set()
                            raise InstallCancelledError(f"Failed on directive {index}") from e
        finally:
            self._directive_processing_active = False
            if self._eta_reporter_thread:
                self._eta_reporter_thread.join(timeout=1)