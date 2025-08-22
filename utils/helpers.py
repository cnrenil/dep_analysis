# -*- coding: utf-8 -*-

import re
import sys
from typing import Optional

# 用于拦截非必要网络资源的正则表达式
RESOURCE_EXCLUSION_REGEX = re.compile(r"\.(css|jpg|jpeg|png|gif|svg|woff|woff2|ttf|ico)(\?.*)?$")

def block_unnecessary_requests(route):
    """Patchright路由处理函数，用于阻止加载图片、CSS等资源以提高速度"""
    if RESOURCE_EXCLUSION_REGEX.search(route.request.url):
        route.abort()
    else:
        route.continue_()

def _find_chinese_font() -> str:
    """返回一个常见的中文字体名称供Graphviz使用。"""
    if sys.platform == "win32":
        return "SimHei" # 黑体
    # 可以为其他操作系统添加更多字体
    # elif sys.platform == "darwin":
    #     return "PingFang SC"
    else:
        # 一个在Linux上常见的选择
        return "WenQuanYi Zen Hei" 

def _extract_mod_id_from_url(url: str) -> Optional[str]:
    """从Nexus Mods URL中提取模组ID"""
    if match := re.search(r'/mods/(\d+)', url): 
        return match.group(1)
    return None
