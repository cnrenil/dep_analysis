# -*- coding: utf-8 -*-

import os
import re
import json
import logging
from pathlib import Path

try:
    import mobase
except ImportError:
    class mobase:
        class IOrganizer: pass
        def managedGame(self): pass
        def pluginDataPath(self): return "."

log = logging.getLogger(__name__)

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
