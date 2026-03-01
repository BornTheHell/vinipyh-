import sys, os, re, json, socket, sqlite3, queue, struct
import subprocess, time, traceback, ctypes, base64, threading
from urllib.parse import urlparse, parse_qs, unquote
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor, as_completed

if sys.platform == "win32":
    import winreg

from PyQt6.QtCore    import Qt, QThread, pyqtSignal, QTimer, QMutex
from PyQt6.QtGui     import QColor, QTextCursor, QAction, QPalette, QFont
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTextEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QLabel, QMessageBox, QMenu, QFrame, QStatusBar,
    QAbstractItemView, QComboBox, QProgressBar, QDialog,
)


def resource_path(name: str) -> str:
    """Путь к ресурсам, включённым в exe через --add-binary (sing-box.exe)."""
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, name)

def data_path(name: str) -> str:
    """Путь к записываемым файлам (БД, временные конфиги) — рядом с exe."""
    if hasattr(sys, "_MEIPASS"):
        base = os.path.dirname(sys.executable)
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, name)

# =============================================================================
# КОНСТАНТЫ
# =============================================================================
SOCKS_PORT       = 20808   # основной SOCKS5 (активное подключение)
HTTP_PORT        = 20809   # основной HTTP прокси
CHECK_SOCKS_BASE = 21000   # порты для массовой проверки (21000, 21002, 21004…)
CHECK_CONCURRENCY = 4      # параллельных проверок одновременно
CHECK_TIMEOUT    = 10      # секунд на проверку одного сервера

TUN_ADDR4 = "172.19.0.1/30"
TUN_ADDR6 = "fdfe:dcba:9876::1/126"

SPEED_TEST_URL_HOST = "speed.cloudflare.com"
SPEED_TEST_URL_PATH = "/__down?bytes=2000000"   # 2 MB
SPEED_TEST_DURATION = 5                          # секунд замера

# =============================================================================
# ТЕМА
# =============================================================================
DARK = """
QMainWindow,QWidget{background:#0d1117;color:#e6edf3;
  font-family:'Consolas','Courier New',monospace}
QSplitter::handle{background:#21262d}
QTextEdit{background:#161b22;color:#e6edf3;border:1px solid #30363d;
  border-radius:6px;padding:8px;
  font-family:'Consolas','Courier New',monospace;font-size:12px;
  selection-background-color:#264f78}
QTableWidget{background:#161b22;color:#e6edf3;border:1px solid #30363d;
  border-radius:6px;gridline-color:#21262d;outline:none;font-size:12px}
QTableWidget::item{padding:5px 8px;border:none}
QTableWidget::item:selected{background:#1f6feb;color:#fff}
QTableWidget::item:hover{background:#1c2128}
QHeaderView::section{background:#1c2128;color:#7d8590;border:none;
  border-bottom:1px solid #30363d;border-right:1px solid #30363d;
  padding:7px 8px;font-size:11px;font-weight:bold}
QPushButton{background:#21262d;color:#e6edf3;border:1px solid #30363d;
  border-radius:6px;padding:7px 14px;font-size:12px;
  font-family:'Consolas','Courier New',monospace;min-width:80px}
QPushButton:hover{background:#30363d;border-color:#58a6ff;color:#58a6ff}
QPushButton:pressed{background:#161b22}
QPushButton:disabled{color:#484f58;border-color:#21262d}
QPushButton#con{background:#238636;border-color:#2ea043;color:#fff;font-weight:bold}
QPushButton#con:hover{background:#2ea043}
QPushButton#dis{background:#da3633;border-color:#f85149;color:#fff;font-weight:bold}
QPushButton#dis:hover{background:#f85149}
QPushButton#imp{background:#1f6feb;border-color:#388bfd;color:#fff;font-weight:bold}
QPushButton#imp:hover{background:#388bfd}
QPushButton#check_btn{background:#0d419d;border-color:#1f6feb;color:#fff;font-weight:bold}
QPushButton#check_btn:hover{background:#1158c7}
QPushButton#stop_btn{background:#6e3f0b;border-color:#d29922;color:#fff;font-weight:bold}
QPushButton#stop_btn:hover{background:#9e6a03}
QLabel{color:#7d8590;font-size:11px;
  font-family:'Consolas','Courier New',monospace}
QLabel#T{color:#58a6ff;font-size:16px;font-weight:bold;letter-spacing:2px}
QLabel#S{color:#58a6ff;font-size:11px;font-weight:bold;
  letter-spacing:1px;padding:4px 0}
QComboBox{background:#161b22;color:#e6edf3;border:1px solid #30363d;
  border-radius:6px;padding:5px 10px;font-size:12px;
  font-family:'Consolas','Courier New',monospace;min-width:100px}
QComboBox:hover{border-color:#58a6ff}
QComboBox QAbstractItemView{background:#161b22;color:#e6edf3;
  border:1px solid #30363d;selection-background-color:#1f6feb}
QProgressBar{background:#161b22;border:1px solid #30363d;border-radius:4px;
  text-align:center;color:#e6edf3;font-size:11px}
QProgressBar::chunk{background:#1f6feb;border-radius:3px}
QStatusBar{background:#161b22;color:#7d8590;
  border-top:1px solid #21262d;font-size:11px}
QMenu{background:#161b22;color:#e6edf3;border:1px solid #30363d;
  border-radius:6px;padding:4px}
QMenu::item{padding:6px 20px;border-radius:4px}
QMenu::item:selected{background:#21262d;color:#f85149}
QScrollBar:vertical{background:#0d1117;width:8px;border:none}
QScrollBar:horizontal{background:#0d1117;height:8px;border:none}
QScrollBar::handle:vertical,QScrollBar::handle:horizontal{
  background:#30363d;border-radius:4px;min-height:16px;min-width:16px}
QScrollBar::handle:vertical:hover,QScrollBar::handle:horizontal:hover{background:#484f58}
QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical,
QScrollBar::add-line:horizontal,QScrollBar::sub-line:horizontal{height:0;width:0}
QFrame#D{background:#21262d;max-height:1px}
"""

# =============================================================================
# ADMIN
# =============================================================================
def is_admin() -> bool:
    try:    return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except: return False

def relaunch_as_admin():
    script = os.path.abspath(sys.argv[0])
    args   = " ".join(f'"{a}"' for a in sys.argv[1:])
    ctypes.windll.shell32.ShellExecuteW(
        None, "runas", sys.executable, f'"{script}" {args}', None, 1)
    sys.exit(0)

# =============================================================================
# БАЗА ДАННЫХ  (с миграцией для старых схем)
# =============================================================================
class DB:
    def __init__(self, path: str):
        self.path = path
        with self._c() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS folders(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                url TEXT DEFAULT '',
                auto_update INTEGER DEFAULT 0
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS servers(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folder_id INTEGER DEFAULT NULL,
                protocol TEXT DEFAULT 'vless',
                name TEXT DEFAULT '', uuid TEXT DEFAULT '',
                address TEXT NOT NULL, port INTEGER NOT NULL,
                security TEXT DEFAULT 'none',
                transport TEXT DEFAULT 'tcp',
                flow TEXT DEFAULT '', sni TEXT DEFAULT '',
                pbk TEXT DEFAULT '', sid TEXT DEFAULT '',
                fp TEXT DEFAULT '',
                method TEXT DEFAULT '',
                password TEXT DEFAULT ''
            )""")
            # Миграция: добавляем новые колонки если их нет
            cols = {r[1] for r in c.execute("PRAGMA table_info(servers)")}
            for col, defn in [
                ("protocol",  "TEXT DEFAULT 'vless'"),
                ("method",    "TEXT DEFAULT ''"),
                ("password",  "TEXT DEFAULT ''"),
                ("folder_id", "INTEGER DEFAULT NULL"),
            ]:
                if col not in cols:
                    c.execute(f"ALTER TABLE servers ADD COLUMN {col} {defn}")

    def _c(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    # ── Folders ────────────────────────────────────────────────────────────────
    def all_folders(self) -> list[dict]:
        with self._c() as c:
            return [dict(r) for r in c.execute("SELECT * FROM folders ORDER BY id")]

    def add_folder(self, name: str, url: str = "", auto_update: int = 0) -> int:
        with self._c() as c:
            cur = c.execute(
                "INSERT OR IGNORE INTO folders(name,url,auto_update) VALUES(?,?,?)",
                (name, url, auto_update))
            if cur.lastrowid:
                return cur.lastrowid
            return c.execute("SELECT id FROM folders WHERE name=?", (name,)).fetchone()[0]

    def delete_folder(self, fid: int):
        with self._c() as c:
            c.execute("DELETE FROM servers WHERE folder_id=?", (fid,))
            c.execute("DELETE FROM folders WHERE id=?", (fid,))

    def rename_folder(self, fid: int, new_name: str):
        with self._c() as c:
            c.execute("UPDATE folders SET name=? WHERE id=?", (new_name, fid))

    def folder_server_count(self, fid) -> int:
        with self._c() as c:
            if fid is None:
                return c.execute(
                    "SELECT COUNT(*) FROM servers WHERE folder_id IS NULL").fetchone()[0]
            return c.execute(
                "SELECT COUNT(*) FROM servers WHERE folder_id=?", (fid,)).fetchone()[0]

    def clear_folder(self, fid: int):
        with self._c() as c:
            c.execute("DELETE FROM servers WHERE folder_id=?", (fid,))

    # ── Servers ────────────────────────────────────────────────────────────────
    def all(self, folder_id=None, all_folders=False) -> list[dict]:
        with self._c() as c:
            if all_folders:
                return [dict(r) for r in c.execute("SELECT * FROM servers ORDER BY id")]
            if folder_id is None:
                return [dict(r) for r in c.execute(
                    "SELECT * FROM servers WHERE folder_id IS NULL ORDER BY id")]
            return [dict(r) for r in c.execute(
                "SELECT * FROM servers WHERE folder_id=? ORDER BY id", (folder_id,))]

    def add(self, d: dict) -> int:
        d2 = dict(d)
        d2.setdefault("folder_id", None)
        with self._c() as c:
            return c.execute(
                "INSERT INTO servers(folder_id,protocol,name,uuid,address,port,security,"
                "transport,flow,sni,pbk,sid,fp,method,password) VALUES"
                "(:folder_id,:protocol,:name,:uuid,:address,:port,:security,:transport,"
                ":flow,:sni,:pbk,:sid,:fp,:method,:password)", d2
            ).lastrowid

    def delete(self, sid: int):
        with self._c() as c: c.execute("DELETE FROM servers WHERE id=?", (sid,))

    def delete_many(self, ids: list[int]):
        with self._c() as c:
            c.executemany("DELETE FROM servers WHERE id=?", [(i,) for i in ids])

    def exists(self, address: str, port: int, uuid: str = "", password: str = "",
               folder_id=None) -> bool:
        with self._c() as c:
            if folder_id is None:
                return bool(c.execute(
                    "SELECT 1 FROM servers WHERE address=? AND port=?"
                    " AND (uuid=? OR password=?) AND folder_id IS NULL",
                    (address, port, uuid, password)
                ).fetchone())
            return bool(c.execute(
                "SELECT 1 FROM servers WHERE address=? AND port=?"
                " AND (uuid=? OR password=?) AND folder_id=?",
                (address, port, uuid, password, folder_id)
            ).fetchone())

# =============================================================================
# ПАРСЕРЫ
# =============================================================================
def _empty_server() -> dict:
    return dict(protocol="vless", name="", uuid="", address="", port=443,
                security="none", transport="tcp", flow="", sni="", pbk="",
                sid="", fp="", method="", password="")

def parse_vless(raw: str) -> dict | None:
    raw = raw.strip()
    if not raw.startswith("vless://"): return None
    try:
        p = urlparse(raw); q = parse_qs(p.query)
        def g(k, d=""): return unquote(q.get(k, [d])[0])
        if not p.username or not p.hostname: return None
        s = _empty_server()
        s.update(protocol="vless",
                 name=unquote(p.fragment) if p.fragment else p.hostname,
                 uuid=p.username, address=p.hostname, port=p.port or 443,
                 security=g("security","none"), transport=g("type","tcp"),
                 flow=g("flow"), sni=g("sni"), pbk=g("pbk"),
                 sid=g("sid"), fp=g("fp"))
        return s
    except: return None

def parse_ss(raw: str) -> dict | None:
    """
    Shadowsocks ссылка. Поддерживаемые форматы:
      ss://BASE64(method:password)@host:port#name
      ss://BASE64(method:password@host:port)#name
      ss://method:password@host:port#name   (без base64)
    """
    raw = raw.strip()
    if not raw.startswith("ss://"): return None
    try:
        # Отрезаем name (fragment)
        name = ""
        if "#" in raw:
            raw, frag = raw.rsplit("#", 1)
            name = unquote(frag)

        body = raw[5:]   # убираем 'ss://'

        # Пробуем формат: BASE64@host:port
        if "@" in body:
            b64_part, hostport = body.rsplit("@", 1)
            try:
                decoded = base64.b64decode(
                    b64_part + "==", altchars=b"-_").decode()
                method, password = decoded.split(":", 1)
            except Exception:
                # Не base64 — значит уже method:password
                method, password = b64_part.split(":", 1)
            # Парсим host:port
            if hostport.startswith("["):   # IPv6
                host = hostport[1:hostport.index("]")]
                port = int(hostport.split("]:")[-1])
            else:
                host, port_s = hostport.rsplit(":", 1)
                port = int(port_s)
        else:
            # Формат: BASE64(method:password@host:port)
            decoded = base64.b64decode(
                body + "==", altchars=b"-_").decode()
            userinfo, hostport = decoded.rsplit("@", 1)
            method, password = userinfo.split(":", 1)
            host, port_s = hostport.rsplit(":", 1)
            port = int(port_s)

        s = _empty_server()
        s.update(protocol="ss",
                 name=name or host,
                 address=host, port=port,
                 method=method.lower(), password=password)
        return s
    except: return None

def parse_trojan(raw: str) -> dict | None:
    """trojan://password@host:port?sni=...&security=tls#name"""
    raw = raw.strip()
    if not raw.startswith("trojan://"): return None
    try:
        p = urlparse(raw); q = parse_qs(p.query)
        def g(k, d=""): return unquote(q.get(k, [d])[0])
        if not p.username or not p.hostname: return None
        s = _empty_server()
        s.update(protocol="trojan",
                 name=unquote(p.fragment) if p.fragment else p.hostname,
                 password=p.username,
                 address=p.hostname, port=p.port or 443,
                 security=g("security", "tls"),
                 transport=g("type", "tcp"),
                 sni=g("sni"), fp=g("fp"))
        return s
    except: return None


def parse_vmess(raw: str) -> dict | None:
    """vmess://BASE64(json)"""
    raw = raw.strip()
    if not raw.startswith("vmess://"): return None
    try:
        b64 = raw[8:]
        # Добиваем до кратного 4
        decoded = base64.b64decode(b64 + "==").decode("utf-8", errors="replace")
        j = json.loads(decoded)
        if not j.get("add"): return None
        s = _empty_server()
        net = j.get("net", "tcp")
        tls_val = str(j.get("tls", "")).strip().lower()
        s.update(protocol="vmess",
                 name=j.get("ps") or j.get("add",""),
                 uuid=j.get("id",""),
                 address=j.get("add",""),
                 port=int(j.get("port", 443)),
                 security="tls" if tls_val in ("tls","1","true") else "none",
                 transport=net if net else "tcp",
                 sni=j.get("sni", j.get("host", "")),
                 fp=j.get("fp",""))
        return s
    except: return None


def parse_hysteria2(raw: str) -> dict | None:
    """hysteria2://password@host:port?...#name  или  hy2://..."""
    raw = raw.strip()
    if not (raw.startswith("hysteria2://") or raw.startswith("hy2://")):
        return None
    try:
        if raw.startswith("hy2://"):
            raw = "hysteria2://" + raw[6:]
        p = urlparse(raw); q = parse_qs(p.query)
        def g(k, d=""): return unquote(q.get(k, [d])[0])
        if not p.hostname: return None
        password = p.username or p.password or g("auth") or ""
        s = _empty_server()
        s.update(protocol="hysteria2",
                 name=unquote(p.fragment) if p.fragment else p.hostname,
                 password=password,
                 address=p.hostname, port=p.port or 443,
                 sni=g("sni"), fp=g("pinSHA256"))
        return s
    except: return None


def parse_any(raw: str) -> dict | None:
    raw = raw.strip()
    if raw.startswith("vless://"):     return parse_vless(raw)
    if raw.startswith("ss://"):        return parse_ss(raw)
    if raw.startswith("trojan://"):    return parse_trojan(raw)
    if raw.startswith("vmess://"):     return parse_vmess(raw)
    if raw.startswith("hysteria2://"): return parse_hysteria2(raw)
    if raw.startswith("hy2://"):       return parse_hysteria2(raw)
    return None

# =============================================================================
# КОНФИГ SING-BOX  (sing-box 1.10 – 1.14+)
# =============================================================================
def get_singbox_version(exe: str) -> tuple[int, int]:
    try:
        r = subprocess.run([exe, "version"], capture_output=True, text=True, timeout=5)
        m = re.search(r"sing-box version (\d+)\.(\d+)", r.stdout + r.stderr)
        if m: return int(m.group(1)), int(m.group(2))
    except: pass
    return (0, 0)

def _vless_outbound(srv: dict, tag: str = "proxy") -> dict:
    security  = srv.get("security", "none")
    transport = srv.get("transport", "tcp")
    ob: dict = {
        "type": "vless", "tag": tag,
        "server": srv["address"], "server_port": int(srv["port"]),
        "uuid":   srv["uuid"],
        "flow":   srv.get("flow", "") or "",
    }
    if security == "reality":
        ob["tls"] = {
            "enabled": True, "server_name": srv.get("sni",""),
            "reality": {
                "enabled":    True,
                "public_key": srv.get("pbk",""),
                "short_id":   srv.get("sid",""),
            },
            "utls": {"enabled": True,
                     "fingerprint": srv.get("fp","chrome") or "chrome"},
        }
    elif security == "tls":
        ob["tls"] = {
            "enabled": True, "server_name": srv.get("sni",""),
            "utls": {"enabled": True,
                     "fingerprint": srv.get("fp","chrome") or "chrome"},
        }
    t = transport.lower()
    if   t == "ws":          ob["transport"] = {"type":"ws",          "path":"/"}
    elif t == "grpc":        ob["transport"] = {"type":"grpc",        "service_name":""}
    elif t in ("h2","http"): ob["transport"] = {"type":"http",        "host":[srv.get("sni","")], "path":"/"}
    elif t in ("httpupgrade","xhttp","splithttp","raw"):
        ob["transport"] = {"type":"httpupgrade", "path":"/"}
    return ob

def _ss_outbound(srv: dict, tag: str = "proxy") -> dict:
    return {
        "type": "shadowsocks", "tag": tag,
        "server": srv["address"], "server_port": int(srv["port"]),
        "method": srv.get("method","aes-256-gcm"),
        "password": srv.get("password",""),
    }

def _trojan_outbound(srv: dict, tag: str = "proxy") -> dict:
    ob: dict = {
        "type": "trojan", "tag": tag,
        "server": srv["address"], "server_port": int(srv["port"]),
        "password": srv.get("password", ""),
    }
    security = srv.get("security", "tls")
    if security in ("tls", "reality"):
        ob["tls"] = {
            "enabled": True,
            "server_name": srv.get("sni", srv["address"]),
            "utls": {"enabled": True,
                     "fingerprint": srv.get("fp", "chrome") or "chrome"},
        }
    transport = srv.get("transport", "tcp").lower()
    if transport == "ws":
        ob["transport"] = {"type": "ws", "path": "/"}
    elif transport == "grpc":
        ob["transport"] = {"type": "grpc", "service_name": ""}
    return ob


def _vmess_outbound(srv: dict, tag: str = "proxy") -> dict:
    ob: dict = {
        "type": "vmess", "tag": tag,
        "server": srv["address"], "server_port": int(srv["port"]),
        "uuid": srv.get("uuid", ""),
        "security": "auto",
        "alter_id": 0,
    }
    security = srv.get("security", "none")
    if security == "tls":
        ob["tls"] = {
            "enabled": True,
            "server_name": srv.get("sni", srv["address"]),
            "utls": {"enabled": True,
                     "fingerprint": srv.get("fp", "chrome") or "chrome"},
        }
    transport = srv.get("transport", "tcp").lower()
    if transport == "ws":
        ob["transport"] = {"type": "ws", "path": "/",
                           "headers": {"Host": srv.get("sni","")}}
    elif transport == "grpc":
        ob["transport"] = {"type": "grpc", "service_name": ""}
    elif transport in ("h2","http"):
        ob["transport"] = {"type": "http",
                           "host": [srv.get("sni","")], "path": "/"}
    return ob


def _hysteria2_outbound(srv: dict, tag: str = "proxy") -> dict:
    ob: dict = {
        "type": "hysteria2", "tag": tag,
        "server": srv["address"], "server_port": int(srv["port"]),
        "password": srv.get("password", ""),
        "tls": {
            "enabled": True,
            "server_name": srv.get("sni", srv["address"]),
            "insecure": True,   # большинство публичных серверов используют self-signed
        },
    }
    if srv.get("fp"):
        ob["tls"]["certificate_path"] = ""  # pin hash не поддерживается напрямую, skip
    return ob


def _make_outbound(srv: dict, tag: str = "proxy") -> dict:
    proto = srv.get("protocol", "vless")
    if proto == "ss":        return _ss_outbound(srv, tag)
    if proto == "trojan":    return _trojan_outbound(srv, tag)
    if proto == "vmess":     return _vmess_outbound(srv, tag)
    if proto == "hysteria2": return _hysteria2_outbound(srv, tag)
    return _vless_outbound(srv, tag)

def _inbounds(mode: str, socks: int = SOCKS_PORT, http: int = HTTP_PORT) -> list[dict]:
    ins: list[dict] = [
        {"type":"socks","tag":"socks-in","listen":"127.0.0.1",
         "listen_port":socks,"users":[]},
        {"type":"http", "tag":"http-in", "listen":"127.0.0.1",
         "listen_port":http},
    ]
    if mode == "tun":
        ins.append({
            "type":         "tun", "tag": "tun-in",
            # sing-box 1.10+: inet4/inet6_address → address[]
            "address":      [TUN_ADDR4, TUN_ADDR6],
            "auto_route":   True,
            "strict_route": True,
            "stack":        "system",
            "sniff":        True,
            "sniff_override_destination": True,
        })
    return ins

def build_config(srv: dict, mode: str,
                 socks: int = SOCKS_PORT,
                 http:  int = HTTP_PORT) -> dict:
    """
    Генерирует конфиг sing-box 1.10+.
    mode: 'tun' | 'proxy'
    Формат DNS соответствует sing-box ≥1.12 (без legacy-полей).
    """
    return {
        "log": {"level": "warn", "timestamp": True},

        # ── DNS ────────────────────────────────────────────────────────────
        # Новый API 1.12+:
        #  • address_resolver вместо bootstrap-правила outbound=any
        #  • Нет detour в server-объектах (маршрут определяется через route)
        #  • Нет dns-outbound; DNS перехватывается action hijack-dns в route
        "dns": {
            "servers": [
                {
                    "tag":              "dns-remote",
                    "address":          "tls://8.8.8.8",
                    
                    "address_resolver": "dns-direct",
                    "detour":           "proxy",
                },
                {
                    "tag":     "dns-direct",
                    "address": "udp://223.5.5.5",
                    "detour":  "direct",
                },
            ],
            "rules": [
                
                {"domain": [srv["address"]], "server": "dns-direct"},
            ],
            "final":             "dns-remote",
            "strategy":          "prefer_ipv4",
            "independent_cache": True,
        },

        "inbounds": _inbounds(mode, socks, http),

        "outbounds": [
            _make_outbound(srv, "proxy"),
            {"type":"direct","tag":"direct"},
            {"type":"block", "tag":"block"},
        ],

        # ── Routing ────────────────────────────────────────────────────────
        "route": {
            "rules": [
                
                {"protocol":"dns",         "action":"hijack-dns"},
                {"ip_is_private":True,     "outbound":"direct"},
            ],
            "final":                 "proxy",
            "auto_detect_interface": True,
        },
    }

# =============================================================================
# СЕТЕВЫЕ УТИЛИТЫ
# =============================================================================
def kill_proc(*names: str):
    """Принудительно убивает процессы по имени и ждёт их завершения."""
    for name in names:
        try:
            subprocess.run(["taskkill","/F","/IM",name],
                           capture_output=True, timeout=5)
        except: pass
    # Даём ОС время освободить ресурсы (TUN-адаптер)
    time.sleep(0.6)

def is_port_free(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(1)
        try:   s.bind(("127.0.0.1", port)); return True
        except: return False

def free_port(port: int):
    try:
        r = subprocess.run(["netstat","-ano"],
                           capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if f":{port}" in line and ("LISTEN" in line or "ESTABLISHED" in line):
                p = line.strip().split()
                if p: subprocess.run(["taskkill","/F","/PID",p[-1]],
                                     capture_output=True, timeout=3)
    except: pass

def wait_port_open(host: str, port: int, timeout: float = 6.0) -> bool:
    """Ждёт пока порт начнёт слушать (sing-box стартует ~0.5–2 сек)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except: time.sleep(0.2)
    return False

# ── SOCKS5 raw HTTP ──────────────────────────────────────────────────────────
def _socks5_connect(proxy_port: int, host: str, port: int,
                    timeout: float = 8.0) -> socket.socket | None:
    """
    Устанавливает TCP-туннель через SOCKS5-прокси (RFC 1928).
    Возвращает готовый socket или None при ошибке.
    """
    try:
        s = socket.create_connection(("127.0.0.1", proxy_port), timeout=5)
        # Handshake: VER=5, NMETHODS=1, METHOD=0(no auth)
        s.sendall(b"\x05\x01\x00")
        if s.recv(2) != b"\x05\x00": s.close(); return None
        # CONNECT: VER=5, CMD=1(connect), RSV=0, ATYP=3(domain)
        host_b = host.encode()
        s.sendall(b"\x05\x01\x00\x03" + bytes([len(host_b)]) + host_b +
                  struct.pack(">H", port))
        resp = s.recv(10)
        if len(resp) < 2 or resp[1] != 0: s.close(); return None
        s.settimeout(timeout)
        return s
    except: return None

def fetch_via_socks5(proxy_port: int, host: str, path: str,
                     timeout: float = 10.0, max_bytes: int = 4096) -> bytes:
    """HTTP GET через SOCKS5. Возвращает тело ответа или b''."""
    s = _socks5_connect(proxy_port, host, 80, timeout)
    if not s: return b""
    try:
        req = (f"GET {path} HTTP/1.0\r\n"
               f"Host: {host}\r\n"
               f"User-Agent: Mozilla/5.0\r\n\r\n").encode()
        s.sendall(req)
        buf = b""
        while len(buf) < max_bytes + 8192:
            chunk = s.recv(8192)
            if not chunk: break
            buf += chunk
        # Извлекаем тело (после \r\n\r\n)
        if b"\r\n\r\n" in buf:
            return buf.split(b"\r\n\r\n", 1)[1]
        return buf
    except: return b""
    finally: s.close()

def get_real_ip() -> str:
    """Возвращает реальный внешний IP без прокси."""
    try:
        s = socket.create_connection(("api.ipify.org", 80), timeout=8)
        s.sendall(b"GET /?format=text HTTP/1.0\r\nHost: api.ipify.org\r\n\r\n")
        data = b""
        while True:
            c = s.recv(4096)
            if not c: break
            data += c
        s.close()
        body = data.split(b"\r\n\r\n", 1)[-1].strip().decode()
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", body): return body
    except: pass
    return ""

def check_ip_via_socks5(proxy_port: int) -> str:
    """
    Возвращает внешний IP через SOCKS5 (api.ipify.org:80).
    Возвращает '' при ошибке или таймауте.
    """
    body = fetch_via_socks5(proxy_port, "api.ipify.org", "/?format=text",
                            timeout=CHECK_TIMEOUT, max_bytes=64)
    body_str = body.strip().decode(errors="ignore")
    if re.match(r"^\d+\.\d+\.\d+\.\d+$", body_str):
        return body_str
    return ""

def measure_speed_via_socks5(proxy_port: int) -> float:
    """
    Скорость загрузки через SOCKS5, Мбит/с.
    Загружает до SPEED_TEST_DURATION секунд с speed.cloudflare.com.
    """
    s = _socks5_connect(proxy_port, SPEED_TEST_URL_HOST, 80, timeout=12.0)
    if not s: return 0.0
    try:
        req = (f"GET {SPEED_TEST_URL_PATH} HTTP/1.0\r\n"
               f"Host: {SPEED_TEST_URL_HOST}\r\n"
               f"User-Agent: Mozilla/5.0\r\n\r\n").encode()
        s.sendall(req)
        # Пропускаем заголовки
        hdr = b""
        while b"\r\n\r\n" not in hdr:
            c = s.recv(4096)
            if not c: return 0.0
            hdr += c
        t0    = time.perf_counter()
        total = len(hdr.split(b"\r\n\r\n", 1)[1])
        while True:
            c = s.recv(65536)
            if not c: break
            total += len(c)
            if time.perf_counter() - t0 >= SPEED_TEST_DURATION: break
        elapsed = time.perf_counter() - t0
        if elapsed < 0.1: return 0.0
        return round(total * 8 / elapsed / 1_000_000, 2)   # Мбит/с
    except: return 0.0
    finally: s.close()

# =============================================================================
# СИСТЕМНЫЙ ПРОКСИ WINDOWS
# =============================================================================
_REG = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"

def _preg():
    return winreg.OpenKey(winreg.HKEY_CURRENT_USER, _REG,
                          0, winreg.KEY_READ | winreg.KEY_WRITE)

def proxy_save() -> dict:
    s = {"ProxyEnable":0, "ProxyServer":"", "ProxyOverride":""}
    if sys.platform != "win32": return s
    try:
        k = _preg()
        for n,d in s.items():
            try:   s[n] = winreg.QueryValueEx(k,n)[0]
            except FileNotFoundError: s[n] = d
        winreg.CloseKey(k)
    except: pass
    return s

def proxy_set(host: str, port: int):
    if sys.platform != "win32": return
    k = _preg()
    winreg.SetValueEx(k,"ProxyEnable",  0,winreg.REG_DWORD,1)
    winreg.SetValueEx(k,"ProxyServer",  0,winreg.REG_SZ,f"{host}:{port}")
    winreg.SetValueEx(k,"ProxyOverride",0,winreg.REG_SZ,
                      "localhost;127.*;10.*;172.*;192.168.*;<local>")
    winreg.CloseKey(k); _pbcast()

def proxy_restore(s: dict):
    if sys.platform != "win32": return
    try:
        k = _preg()
        winreg.SetValueEx(k,"ProxyEnable",  0,winreg.REG_DWORD,int(s.get("ProxyEnable",0)))
        winreg.SetValueEx(k,"ProxyServer",  0,winreg.REG_SZ,   s.get("ProxyServer",""))
        winreg.SetValueEx(k,"ProxyOverride",0,winreg.REG_SZ,   s.get("ProxyOverride",""))
        winreg.CloseKey(k); _pbcast()
    except: pass

def _pbcast():
    try:
        res = ctypes.c_long()
        ctypes.windll.user32.SendMessageTimeoutW(
            0xFFFF,0x001A,0,"Internet Settings",0x0002,200,ctypes.byref(res))
    except: pass

# =============================================================================
# ФОНОВЫЕ ПОТОКИ
# =============================================================================
class PingWorker(QThread):
    result = pyqtSignal(int, int)   # row, ms

    def __init__(self, row, host, port):
        super().__init__()
        self.row, self.host, self.port = row, host, port

    def run(self):
        try:
            t = time.perf_counter()
            with socket.create_connection((self.host, self.port), timeout=5): pass
            self.result.emit(self.row, int((time.perf_counter()-t)*1000))
        except: self.result.emit(self.row, -1)


class LogReader(QThread):
    line     = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, proc: subprocess.Popen):
        super().__init__()
        self.proc = proc; self._stop = False

    def run(self):
        try:
            for raw in iter(self.proc.stdout.readline, b""):
                if self._stop: break
                t = re.sub(r"\x1b\[[0-9;]*m","",
                           raw.decode("utf-8",errors="replace").rstrip())
                if t: self.line.emit(t)
        except: pass
        self.finished.emit()

    def stop(self): self._stop = True


class MassChecker(QThread):
    """
    Массовая проверка серверов через SOCKS5 (без TUN).
    Запускает sing-box в прокси-режиме для каждого сервера,
    проверяет IP через api.ipify.org, опционально замеряет скорость.
    """
    # (index, total, server_name, status_text, color)
    progress  = pyqtSignal(int, int, str, str, str)
    # (working_ids, failed_ids, details)
    # details: {id: {"ip":str, "speed":float, "ping":int}}
    done      = pyqtSignal(list, list, dict)

    def __init__(self, servers: list[dict], exe: str,
                 cfg_dir: str, real_ip: str, do_speed: bool = True):
        super().__init__()
        self.servers  = servers
        self.exe      = exe
        self.cfg_dir  = cfg_dir
        self.real_ip  = real_ip
        self.do_speed = do_speed
        self._stop    = False
        # Пул доступных портов (SOCKS, HTTP пара для каждого воркера)
        self._port_q: queue.Queue[int] = queue.Queue()
        for i in range(CHECK_CONCURRENCY):
            self._port_q.put(CHECK_SOCKS_BASE + i * 2)

    def request_stop(self): self._stop = True

    def run(self):
        working: list[int] = []
        failed:  list[int] = []
        details: dict[int, dict] = {}
        total = len(self.servers)

        with ThreadPoolExecutor(max_workers=CHECK_CONCURRENCY) as pool:
            futures = {
                pool.submit(self._check_one, srv): srv
                for srv in self.servers
            }
            done_count = 0
            for future in as_completed(futures):
                if self._stop: break
                srv    = futures[future]
                result = future.result()   # {"ok":bool,"ip":str,"speed":float,"ping":int}
                done_count += 1
                sid = srv["id"]
                if result["ok"]:
                    working.append(sid)
                    details[sid] = result
                    self.progress.emit(
                        done_count, total, srv.get("name",""),
                        f"✓  {result['ip']}  {result['speed']} Мбит/с  {result['ping']} ms",
                        "#3fb950")
                else:
                    failed.append(sid)
                    self.progress.emit(
                        done_count, total, srv.get("name",""),
                        "✗  недоступен", "#f85149")

        self.done.emit(working, failed, details)

    def _check_one(self, srv: dict) -> dict:
        port = self._port_q.get()
        result = {"ok": False, "ip": "", "speed": 0.0, "ping": -1}
        proc   = None
        try:
            if self._stop: return result

            socks = port
            http  = port + 1

            # Конфиг без TUN
            cfg = build_config(srv, "proxy", socks=socks, http=http)
            cfg_path = os.path.join(
                self.cfg_dir, f"_check_{port}.json")
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f)

            proc = subprocess.Popen(
                [self.exe, "run", "-c", cfg_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )

            # Ждём готовности SOCKS5
            if not wait_port_open("127.0.0.1", socks, timeout=5.0):
                return result

            # Проверяем IP
            got_ip = check_ip_via_socks5(socks)
            if not got_ip or got_ip == self.real_ip:
                return result

            result["ok"] = True
            result["ip"] = got_ip

            # TCP пинг
            try:
                t0 = time.perf_counter()
                with socket.create_connection(
                        (srv["address"], int(srv["port"])), timeout=5): pass
                result["ping"] = int((time.perf_counter()-t0)*1000)
            except: pass

            # Замер скорости
            if self.do_speed and not self._stop:
                result["speed"] = measure_speed_via_socks5(socks)

        except Exception:
            pass
        finally:
            if proc:
                try: proc.terminate(); proc.wait(timeout=3)
                except: proc.kill()
            # Удаляем временный конфиг
            try:
                cfg_p = os.path.join(self.cfg_dir, f"_check_{port}.json")
                if os.path.exists(cfg_p): os.remove(cfg_p)
            except: pass
            self._port_q.put(port)   # возвращаем порт в пул

        return result


# =============================================================================
# АВТО-ОБНОВЛЕНИЕ ПАПОК ИЗ URL
# =============================================================================
AUTO_UPDATE_URLS = {
    "SS+All RUS": "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_SS+All_RUS.txt",
    "VLESS RUS":  "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/refs/heads/main/BLACK_VLESS_RUS.txt",
}


class FolderUpdater(QThread):
    """Скачивает серверы из URL и обновляет папку в БД.
    Если direct=False, пробует через SOCKS5 (активный VPN).
    """
    log     = pyqtSignal(str, str)     # text, color
    done    = pyqtSignal(str, int)     # folder_name, added_count

    def __init__(self, folder_name: str, url: str, db_path: str,
                 proxy_port: int = 0):
        super().__init__()
        self.folder_name = folder_name
        self.url         = url
        self.db_path     = db_path
        self.proxy_port  = proxy_port   # 0 = прямое соединение

    def _fetch(self) -> str:
        """Load URL text, trying direct first then SOCKS5."""
        from urllib.error import URLError
        headers = {"User-Agent": "Mozilla/5.0 (VPNClient/3.1)"}

        # 1) Direct connection
        try:
            req = Request(self.url, headers=headers)
            with urlopen(req, timeout=20) as r:
                return r.read().decode("utf-8", errors="replace")
        except URLError:
            pass

        # 2) Via active SOCKS5 proxy if available
        if self.proxy_port:
            try:
                parsed = urlparse(self.url)
                host   = parsed.hostname or ""
                path   = parsed.path + ("?" + parsed.query if parsed.query else "")
                s = _socks5_connect(self.proxy_port, host, 80, timeout=25.0)
                if s:
                    crlf = "\r\n"
                    req_str = (
                        "GET " + path + " HTTP/1.0" + crlf +
                        "Host: " + host + crlf +
                        "User-Agent: " + headers["User-Agent"] + crlf + crlf
                    ).encode()
                    s.sendall(req_str)
                    buf = b""
                    while True:
                        chunk = s.recv(65536)
                        if not chunk:
                            break
                        buf += chunk
                    s.close()
                    sep = b"\r\n\r\n"
                    if sep in buf:
                        body = buf.split(sep, 1)[1]
                        return body.decode("utf-8", errors="replace")
            except Exception:
                pass
        raise ConnectionError(
            "Cannot download URL (no network or VPN not connected)")

    def run(self):
        try:
            self.log.emit(
                f"[UPDATE] Загрузка «{self.folder_name}» из {self.url[:60]}…","#d29922")
            raw   = self._fetch()
            lines = raw.splitlines()
            db    = DB(self.db_path)
            fid   = db.add_folder(self.folder_name, url=self.url, auto_update=1)
            # Очищаем старые серверы папки и загружаем заново
            db.clear_folder(fid)
            added = 0
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"): continue
                s = parse_any(line)
                if not s: continue
                s["folder_id"] = fid
                db.add(s)
                added += 1

            self.log.emit(
                f"[UPDATE] «{self.folder_name}»: загружено {added} серверов","#3fb950")
            self.done.emit(self.folder_name, added)
        except Exception as e:
            self.log.emit(
                f"[UPDATE] Ошибка «{self.folder_name}»: {e} "
                f"(попробуйте нажать 🔄 Обновить когда VPN подключён)","#f85149")
            self.done.emit(self.folder_name, 0)


# =============================================================================
# ГЛАВНОЕ ОКНО
# =============================================================================
class VpnApp(QMainWindow):

    def __init__(self):
        super().__init__()
        self._exe     = resource_path("sing-box.exe")
        self._cfgdir  = data_path("")
        self._cfg     = data_path("_singbox_main.json")
        self._db      = DB(data_path("vpn_servers.db"))
        self._sb_ver  = (0, 0)

        self._proc:        subprocess.Popen | None = None
        self._reader:      LogReader | None        = None
        self._checker:     MassChecker | None      = None
        self._pings:       list[PingWorker]        = []
        self._ids:         list[int]               = []
        self._conn_row:    int | None              = None
        self._proxy_bak:   dict | None             = None
        self._pending:     int = 0
        self._updaters:    list[FolderUpdater]     = []

        self._build_ui()
        self._load()
        self._detect_singbox()
        # Создаём папки если ещё не существуют
        self._ensure_url_folders()
        # Авто-обновление через 1 секунду (даём UI прогрузиться)
        QTimer.singleShot(1000, self._auto_update_url_folders)

    # ─── DETECT ───────────────────────────────────────────────────────────────

    def _detect_singbox(self):
        if not os.path.exists(self._exe):
            self._log("[SYS] sing-box.exe не найден! "
                      "Скачайте: https://github.com/SagerNet/sing-box/releases",
                      "#f85149")
            return
        self._sb_ver = get_singbox_version(self._exe)
        maj, mn = self._sb_ver
        ver_s = f"{maj}.{mn}" if maj else "?"
        self._log(f"[SYS] sing-box {ver_s} найден  ✓", "#3fb950")
        adm = is_admin()
        self._log(
            f"[SYS] Права Admin: {'✓ Да (TUN доступен)' if adm else '✗ Нет  (только Прокси-режим)'}",
            "#3fb950" if adm else "#d29922")
        self._sb_status(f"sing-box {ver_s}  ·  "
                        f"{'Admin ✓' if adm else 'Нет Admin'}  ·  "
                        f"SOCKS5 127.0.0.1:{SOCKS_PORT}")

    # ─── BUILD UI ─────────────────────────────────────────────────────────────

    def _build_ui(self):
        self.setWindowTitle("VINIPYX  ·  sing-box  v3")
        self.setMinimumSize(1120, 820); self.resize(1300, 920)

        root = QWidget(); self.setCentralWidget(root)
        rl = QVBoxLayout(root); rl.setContentsMargins(12,10,12,8); rl.setSpacing(7)

        # Header
        hdr = QHBoxLayout()
        tl = QLabel("⬡  VINIPYH   ·  sing-box"); tl.setObjectName("T")
        hdr.addWidget(tl); hdr.addStretch()
        self._ind = QLabel("● ОТКЛЮЧЕНО")
        self._ind.setStyleSheet("color:#f85149;font-size:13px;font-weight:bold;")
        hdr.addWidget(self._ind)
        rl.addLayout(hdr)
        div = QFrame(); div.setObjectName("D"); div.setFrameShape(QFrame.Shape.HLine)
        rl.addWidget(div)

        sp = QSplitter(Qt.Orientation.Vertical); sp.setHandleWidth(4)
        rl.addWidget(sp, 1)

        # ── Import
        iw = QWidget(); il = QVBoxLayout(iw); il.setContentsMargins(0,0,0,0); il.setSpacing(4)
        lb = QLabel("▸ ИМПОРТ  (VLESS / Shadowsocks)"); lb.setObjectName("S"); il.addWidget(lb)
        ir = QHBoxLayout()
        self._imp = QTextEdit()
        self._imp.setPlaceholderText(
            "Вставьте ссылки (vless:// или ss://) — по одной на строку…")
        self._imp.setFixedHeight(80); ir.addWidget(self._imp)
        bi = QPushButton("⬆  ИМПОРТ"); bi.setObjectName("imp")
        bi.setFixedSize(130,80); bi.clicked.connect(self._import)
        ir.addWidget(bi); il.addLayout(ir); sp.addWidget(iw)

        # ── Table area (splitter: folder panel | server table)
        tarea = QWidget(); tarea_l = QHBoxLayout(tarea)
        tarea_l.setContentsMargins(0,0,0,0); tarea_l.setSpacing(0)
        htsp = QSplitter(Qt.Orientation.Horizontal); htsp.setHandleWidth(4)

        # Folder panel
        fpw = QWidget(); fpl = QVBoxLayout(fpw); fpl.setContentsMargins(0,0,4,0); fpl.setSpacing(3)
        from PyQt6.QtWidgets import QListWidget, QInputDialog
        flbl = QLabel("▸ ПАПКИ"); flbl.setObjectName("S"); fpl.addWidget(flbl)
        self._folder_list = QListWidget()
        self._folder_list.setMinimumWidth(160); self._folder_list.setMaximumWidth(240)
        self._folder_list.setStyleSheet(
            "QListWidget{background:#161b22;color:#e6edf3;border:1px solid #30363d;"
            "border-radius:6px;font-size:12px;}"
            "QListWidget::item{padding:5px 8px;}"
            "QListWidget::item:selected{background:#1f6feb;color:#fff;}"
            "QListWidget::item:hover{background:#1c2128;}")
        self._folder_list.currentRowChanged.connect(lambda _: self._load(reload_folders=False))
        fpl.addWidget(self._folder_list)
        # Folder buttons
        fb1 = QPushButton("➕ Папка"); fb1.setFixedHeight(26)
        fb1.setStyleSheet("font-size:11px;padding:2px 6px;min-width:0;")
        fb1.clicked.connect(self._new_folder)
        fb2 = QPushButton("✏ Переим."); fb2.setFixedHeight(26)
        fb2.setStyleSheet("font-size:11px;padding:2px 6px;min-width:0;")
        fb2.clicked.connect(self._rename_folder)
        fb3 = QPushButton("🔄 Обновить"); fb3.setFixedHeight(26)
        fb3.setStyleSheet("font-size:11px;padding:2px 6px;min-width:0;")
        fb3.clicked.connect(self._update_url_folder)
        fb4 = QPushButton("🗑 Удалить"); fb4.setFixedHeight(26)
        fb4.setStyleSheet("font-size:11px;padding:2px 6px;min-width:0;color:#f85149;")
        fb4.clicked.connect(self._delete_folder)
        for b in [fb1, fb2, fb3, fb4]: fpl.addWidget(b)
        htsp.addWidget(fpw)

        # Table widget
        tw = QWidget(); tl2 = QVBoxLayout(tw); tl2.setContentsMargins(0,0,0,0); tl2.setSpacing(4)
        lb2 = QLabel("▸ СЕРВЕРЫ"); lb2.setObjectName("S"); tl2.addWidget(lb2)
        self._tbl = QTableWidget()
        self._tbl.setColumnCount(6)
        self._tbl.setHorizontalHeaderLabels(
            ["  Ст.", "Флаг / Название", "Адрес", "Протокол", "Пинг", "Скорость"])
        self._tbl.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._tbl.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._tbl.verticalHeader().setVisible(False)
        self._tbl.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self._tbl.customContextMenuRequested.connect(self._ctx)
        h = self._tbl.horizontalHeader()
        for c, m in [(0,QHeaderView.ResizeMode.Fixed),
                     (1,QHeaderView.ResizeMode.Stretch),
                     (2,QHeaderView.ResizeMode.Stretch),
                     (3,QHeaderView.ResizeMode.Fixed),
                     (4,QHeaderView.ResizeMode.Fixed),
                     (5,QHeaderView.ResizeMode.Fixed)]:
            h.setSectionResizeMode(c,m)
        self._tbl.setColumnWidth(0,55); self._tbl.setColumnWidth(3,120)
        self._tbl.setColumnWidth(4,75); self._tbl.setColumnWidth(5,100)
        self._tbl.verticalHeader().setDefaultSectionSize(32)
        tl2.addWidget(self._tbl)
        htsp.addWidget(tw)
        htsp.setSizes([180, 900])
        tarea_l.addWidget(htsp)
        sp.addWidget(tarea)

        # ── Log
        lw = QWidget(); ll = QVBoxLayout(lw); ll.setContentsMargins(0,0,0,0); ll.setSpacing(4)
        lhdr = QHBoxLayout()
        lb3 = QLabel("▸ ЛОГИ"); lb3.setObjectName("S"); lhdr.addWidget(lb3)
        lhdr.addStretch()
        bc = QPushButton("✕ ЛОГ"); bc.setFixedSize(65,18)
        bc.setStyleSheet("font-size:10px;padding:0 5px;color:#7d8590;min-width:0;")
        bc.clicked.connect(lambda: self._logw.clear()); lhdr.addWidget(bc)
        ll.addLayout(lhdr)
        self._logw = QTextEdit(); self._logw.setReadOnly(True); self._logw.setFixedHeight(190)
        self._logw.setStyleSheet(
            "QTextEdit{background:#010409;color:#3fb950;"
            "border:1px solid #238636;font-size:11px;}")
        ll.addWidget(self._logw); sp.addWidget(lw)
        sp.setSizes([100,440,220])

        # ── Progress bar (скрыта по умолчанию)
        self._prog = QProgressBar(); self._prog.setFixedHeight(18)
        self._prog.setVisible(False); rl.addWidget(self._prog)

        # ── Controls
        ctrl = QHBoxLayout(); ctrl.setSpacing(6)
        ctrl.addWidget(QLabel("Режим:"))
        self._mode = QComboBox()
        self._mode.addItems([
            "🔒 TUN  — весь трафик  [Admin]",
            "🌐 Прокси  — HTTP/SOCKS5",
        ])
        self._mode.setFixedHeight(32); ctrl.addWidget(self._mode)
        ctrl.addStretch()

        self._bp  = QPushButton("◎ ПИНГ")
        self._bp.setObjectName("ping_btn")
        self._bp.setStyleSheet(
            "QPushButton{background:#9e6a03;border-color:#d29922;color:#fff;font-weight:bold}"
            "QPushButton:hover{background:#d29922}")
        self._bc  = QPushButton("▶ ПОДКЛЮЧИТЬ"); self._bc.setObjectName("con")
        self._bd  = QPushButton("■ ОТКЛЮЧИТЬ");  self._bd.setObjectName("dis")
        self._bck = QPushButton("🔍 ПРОВЕРИТЬ СЕРВЕРЫ"); self._bck.setObjectName("check_btn")
        self._bst = QPushButton("⏹ СТОП ПРОВЕРКИ");     self._bst.setObjectName("stop_btn")
        self._bd.setEnabled(False); self._bst.setVisible(False)

        self._bp.clicked.connect(self._ping_all)
        self._bc.clicked.connect(self._connect)
        self._bd.clicked.connect(self._disconnect)
        self._bck.clicked.connect(self._start_mass_check)
        self._bst.clicked.connect(self._stop_mass_check)

        for b in [self._bp, self._bck, self._bst, self._bc, self._bd]:
            ctrl.addWidget(b)
        rl.addLayout(ctrl)

        hint = QLabel(
            "ℹ  TUN требует Admin → run_as_admin.bat   ·   "
            "«Проверить серверы» запускает sing-box для каждого сервера и проверяет реальный IP.")
        hint.setStyleSheet("color:#484f58;font-size:10px;")
        rl.addWidget(hint)

        self._sbar = QStatusBar(); self.setStatusBar(self._sbar)

    # ─── LOAD ─────────────────────────────────────────────────────────────────

    def _current_folder_id(self):
        """
        Возвращает folder_id или None:
          idx=0 → "Все серверы" (all_folders=True)
          idx=1 → "Без папки" (folder_id=None, all_folders=False)
          idx>=2 → конкретная папка
        """
        idx = self._folder_list.currentRow()
        if idx <= 0: return None   # all / не выбрано
        if idx == 1: return None   # "Без папки" — тоже None, но all_folders=False
        folders = self._db.all_folders()
        fi = idx - 2
        if fi < len(folders): return folders[fi]["id"]
        return None

    def _load(self, reload_folders=True):
        if reload_folders:
            self._refresh_folder_list()
        self._tbl.setRowCount(0); self._ids.clear()
        fid = self._current_folder_id()
        idx = self._folder_list.currentRow()
        if idx == 0:
            servers = self._db.all(all_folders=True)
        elif idx == 1:
            servers = self._db.all(folder_id=None, all_folders=False)
        else:
            servers = self._db.all(folder_id=fid)
        for s in servers: self._row_add(s)

    def _refresh_folder_list(self):
        self._folder_list.blockSignals(True)
        cur = self._folder_list.currentRow()
        if cur < 0: cur = 0
        self._folder_list.clear()
        total = self._db.all(all_folders=True)
        self._folder_list.addItem(f"📂 Все серверы  [{len(total)}]")
        no_folder_cnt = self._db.folder_server_count(None)
        self._folder_list.addItem(f"📁 Без папки  [{no_folder_cnt}]")
        for f in self._db.all_folders():
            cnt = self._db.folder_server_count(f["id"])
            icon = "🔄" if f.get("auto_update") else "📁"
            self._folder_list.addItem(f"{icon} {f['name']}  [{cnt}]")
        # Restore selection
        if cur < self._folder_list.count():
            self._folder_list.setCurrentRow(cur)
        else:
            self._folder_list.setCurrentRow(0)
        self._folder_list.blockSignals(False)

    def _row_add(self, srv: dict, ping: int = -2, speed: float = -1.0):
        r = self._tbl.rowCount(); self._tbl.insertRow(r)
        self._ids.append(srv["id"])
        proto = srv.get("protocol","vless").upper()
        if proto == "VLESS":
            sec   = srv.get("security","none")
            trans = srv.get("transport","tcp").upper()
            proto_s = f"VLESS/{trans}" + (f"/{sec.upper()}" if sec not in ("","none") else "")
        elif proto == "SS":
            proto_s = f"SS/{srv.get('method','?').upper()}"
        elif proto == "TROJAN":
            sec = srv.get("security","tls")
            trans = srv.get("transport","tcp").upper()
            proto_s = f"TROJAN/{trans}/{sec.upper()}"
        elif proto == "VMESS":
            trans = srv.get("transport","tcp").upper()
            sec = srv.get("security","none")
            proto_s = f"VMESS/{trans}" + (f"/{sec.upper()}" if sec not in ("","none") else "")
        elif proto == "HYSTERIA2":
            proto_s = "HYSTERIA2"
        else:
            proto_s = proto

        def it(txt, col="#e6edf3", align=None):
            i = QTableWidgetItem(txt); i.setForeground(QColor(col))
            if align: i.setTextAlignment(align)
            return i

        C = Qt.AlignmentFlag.AlignCenter
        self._tbl.setItem(r,0, it("○","#484f58",C))
        self._tbl.setItem(r,1, it(srv.get("name") or srv["address"]))
        self._tbl.setItem(r,2, it(f"{srv['address']}:{srv['port']}","#8b949e"))
        self._tbl.setItem(r,3, it(proto_s,"#58a6ff",C))
        # Пинг
        if ping == -2:
            self._tbl.setItem(r,4, it("—","#484f58",C))
        else:
            self._set_ping_cell(r, ping)
        # Скорость
        if speed < 0:
            self._tbl.setItem(r,5, it("—","#484f58",C))
        else:
            self._tbl.setItem(r,5, it(f"{speed} Мбит/с","#58a6ff",C))

    def _set_ping_cell(self, row: int, ms: int):
        C = Qt.AlignmentFlag.AlignCenter
        it = self._tbl.item(row,4)
        if not it: it = QTableWidgetItem(); self._tbl.setItem(row,4,it)
        it.setTextAlignment(C)
        if ms < 0:   it.setText("Timeout"); it.setForeground(QColor("#f85149"))
        elif ms<100: it.setText(f"{ms} ms"); it.setForeground(QColor("#3fb950"))
        elif ms<300: it.setText(f"{ms} ms"); it.setForeground(QColor("#d29922"))
        else:        it.setText(f"{ms} ms"); it.setForeground(QColor("#f85149"))

    # ─── IMPORT ───────────────────────────────────────────────────────────────

    def _import(self):
        txt = self._imp.toPlainText().strip()
        if not txt: QMessageBox.warning(self,"Импорт","Вставьте ссылки."); return
        folder_id = self._current_folder_id()
        added, skip, err = self._import_lines(txt.splitlines(), folder_id, show_log=True)
        self._imp.clear()
        self._sb_status(f"Импорт: +{added}  дубл:{skip}  ошибок:{err}")

    def _import_lines(self, lines, folder_id, show_log=False) -> tuple[int,int,int]:
        added = skip = err = 0
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"): continue
            s = parse_any(line)
            if not s:
                err += 1
                if show_log:
                    self._log(f"[PARSE] Неизвестный формат: {line[:70]}","#f85149")
                continue
            if self._db.exists(s["address"],s["port"],s.get("uuid",""),
                               s.get("password",""), folder_id):
                skip += 1; continue
            s["folder_id"] = folder_id
            s["id"] = self._db.add(s)
            if self._current_folder_id() == folder_id:
                self._row_add(s)
            added += 1
            if show_log:
                self._log(f"[PARSE] ✓ [{s['protocol'].upper()}] "
                          f"{s['name']}  {s['address']}:{s['port']}","#3fb950")
        return added, skip, err

    # ─── CONTEXT MENU ─────────────────────────────────────────────────────────

    def _ctx(self, pos):
        row = self._tbl.rowAt(pos.y())
        if row < 0 or row >= len(self._ids): return
        m = QMenu(self)
        a1 = QAction("🗑  Удалить сервер",  self); a1.triggered.connect(lambda: self._del(row))
        a2 = QAction("▶  Подключиться",     self); a2.triggered.connect(lambda: self._connect_row(row))
        m.addAction(a2); m.addSeparator(); m.addAction(a1)
        m.exec(self._tbl.viewport().mapToGlobal(pos))

    def _del(self, row: int):
        if row < 0 or row >= len(self._ids): return
        name = (self._tbl.item(row,1).text() if self._tbl.item(row,1) else f"#{row+1}")
        if QMessageBox.question(self,"Удалить?",f"Удалить «{name}»?",
            QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No
        ) != QMessageBox.StandardButton.Yes: return
        self._db.delete(self._ids[row])
        self._tbl.removeRow(row); self._ids.pop(row)

    # ─── PING ─────────────────────────────────────────────────────────────────

    def _ping_all(self):
        n = self._tbl.rowCount()
        if not n: return
        self._bp.setEnabled(False); self._pings.clear(); self._pending = n
        for row in range(n):
            ai = self._tbl.item(row,2)
            if not ai: self._pending-=1; continue
            parts = ai.text().rsplit(":",1)
            addr  = parts[0]; port = int(parts[1]) if len(parts)>1 else 443
            it = self._tbl.item(row,4)
            if it: it.setText("…"); it.setForeground(QColor("#7d8590"))
            w = PingWorker(row,addr,port)
            w.result.connect(self._on_ping); w.finished.connect(self._on_ping_done)
            self._pings.append(w); w.start()
        self._log(f"[PING] Проверяем {n} серверов…","#d29922")

    def _on_ping(self, row: int, ms: int): self._set_ping_cell(row, ms)
    def _on_ping_done(self):
        self._pending -= 1
        if self._pending <= 0: self._bp.setEnabled(True); self._log("[PING] Готово.","#3fb950")

    # ─── MASS CHECK ───────────────────────────────────────────────────────────

    def _start_mass_check(self):
        if not os.path.exists(self._exe):
            QMessageBox.critical(self,"sing-box не найден",
                                 f"sing-box.exe не найден:\n{self._exe}"); return
        fid = self._current_folder_id()
        idx = self._folder_list.currentRow()
        servers = self._db.all(folder_id=fid, all_folders=(idx==0))
        if not servers: QMessageBox.information(self,"Проверка","Список серверов пуст."); return

        self._log("[CHECK] Определяем реальный IP…","#58a6ff")
        real_ip = get_real_ip()
        if not real_ip:
            self._log("[CHECK] Не удалось получить реальный IP — продолжаем без проверки на совпадение","#d29922")
            real_ip = "0.0.0.0"
        else:
            self._log(f"[CHECK] Реальный IP: {real_ip}","#58a6ff")

        self._log(f"[CHECK] Запуск проверки {len(servers)} серверов "
                  f"({CHECK_CONCURRENCY} параллельно, таймаут {CHECK_TIMEOUT}с)…","#58a6ff")
        self._log("[CHECK] Нерабочие серверы будут удалены из БД. "
                  "Рабочие — отсортированы по пингу.","#d29922")

        self._prog.setMaximum(len(servers))
        self._prog.setValue(0); self._prog.setVisible(True)
        self._bck.setEnabled(False); self._bst.setVisible(True)

        # Убиваем текущие sing-box (могут конфликтовать по портам)
        kill_proc("sing-box.exe")
        time.sleep(0.3)

        self._checker = MassChecker(
            servers, self._exe, self._cfgdir, real_ip, do_speed=True)
        self._checker.progress.connect(self._on_check_progress)
        self._checker.done.connect(self._on_check_done)
        self._checker.start()

    def _stop_mass_check(self):
        if self._checker:
            self._checker.request_stop()
            self._log("[CHECK] Остановка…","#d29922")

    def _on_check_progress(self, cur: int, total: int,
                            name: str, status: str, color: str):
        self._prog.setValue(cur)
        self._log(f"[CHECK {cur}/{total}] {name[:40]}  →  {status}", color)

    def _on_check_done(self, working: list[int], failed: list[int],
                       details: dict):
        self._prog.setVisible(False)
        self._bck.setEnabled(True); self._bst.setVisible(False)

        self._log(f"[CHECK] Готово: рабочих {len(working)}, "
                  f"нерабочих {len(failed)}","#58a6ff")

        # Удаляем нерабочие из БД
        if failed:
            self._db.delete_many(failed)
            self._log(f"[CHECK] Удалено из БД: {len(failed)} серверов","#f85149")

        # Перезагружаем таблицу — только рабочие, сортировка по пингу
        fid = self._current_folder_id()
        idx = self._folder_list.currentRow()
        all_servers = self._db.all(folder_id=fid, all_folders=(idx == 0))
        # Сортируем: сначала рабочие по пингу, потом остальные
        def sort_key(s):
            d = details.get(s["id"], {})
            if d:  return (0, d.get("ping", 9999))
            return (1, 9999)
        all_servers.sort(key=sort_key)

        self._tbl.setRowCount(0); self._ids.clear()
        for s in all_servers:
            d    = details.get(s["id"], {})
            ping = d.get("ping", -2) if d else -2
            spd  = d.get("speed", -1.0) if d else -1.0
            self._row_add(s, ping=ping, speed=spd)

        self._log(f"[CHECK] Таблица обновлена: {len(all_servers)} рабочих серверов "
                  f"отсортированы по пингу","#3fb950")

    # ─── FOLDER MANAGEMENT ───────────────────────────────────────────────────

    def _new_folder(self):
        from PyQt6.QtWidgets import QInputDialog
        name, ok = QInputDialog.getText(self, "Новая папка", "Название папки:")
        if not ok or not name.strip(): return
        self._db.add_folder(name.strip())
        self._load(reload_folders=True)

    def _rename_folder(self):
        from PyQt6.QtWidgets import QInputDialog
        idx = self._folder_list.currentRow()
        if idx < 2: QMessageBox.information(self,"Переименование","Выберите папку."); return
        folders = self._db.all_folders()
        fi = idx - 2
        if fi >= len(folders): return
        f = folders[fi]
        name, ok = QInputDialog.getText(self, "Переименовать", "Новое имя:", text=f["name"])
        if not ok or not name.strip(): return
        self._db.rename_folder(f["id"], name.strip())
        self._load(reload_folders=True)

    def _delete_folder(self):
        idx = self._folder_list.currentRow()
        if idx < 2:
            QMessageBox.information(self,"Удалить папку","Выберите конкретную папку."); return
        folders = self._db.all_folders()
        fi = idx - 2
        if fi >= len(folders): return
        f = folders[fi]
        cnt = self._db.folder_server_count(f["id"])
        if QMessageBox.question(
            self, "Удалить папку?",
            f"Удалить папку «{f['name']}» и {cnt} серверов в ней?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) != QMessageBox.StandardButton.Yes: return
        self._db.delete_folder(f["id"])
        self._folder_list.setCurrentRow(0)
        self._load(reload_folders=True)

    def _update_url_folder(self):
        """Обновление выбранной папки из её URL."""
        idx = self._folder_list.currentRow()
        if idx < 2:
            # Обновить все авто-папки
            self._auto_update_url_folders(); return
        folders = self._db.all_folders()
        fi = idx - 2
        if fi >= len(folders): return
        f = folders[fi]
        if not f.get("url"):
            QMessageBox.information(self, "Обновление",
                "У этой папки нет URL. Папки с URL создаются автоматически при запуске."); return
        self._run_folder_updater(f["name"], f["url"])

    def _ensure_url_folders(self):
        """Создаёт пустые авто-папки при первом запуске (без загрузки серверов)."""
        for name, url in AUTO_UPDATE_URLS.items():
            self._db.add_folder(name, url=url, auto_update=1)
        self._load(reload_folders=True)

    def _auto_update_url_folders(self):
        """Обновляет авто-папки из URL. При ошибке сети — тихо пишет в лог."""
        # Предотвращаем повторный запуск пока идёт загрузка
        if getattr(self, "_auto_updating", False): return
        self._auto_updating = True
        self._log("[UPDATE] Авто-обновление папок из URL…","#58a6ff")
        self._log("[UPDATE] Если нет прямого доступа к GitHub — подключитесь к VPN "
                  "и нажмите 🔄 Обновить в панели папок","#7d8590")
        self._pending_updates = len(AUTO_UPDATE_URLS)
        for name, url in AUTO_UPDATE_URLS.items():
            self._run_folder_updater(name, url)

    def _run_folder_updater(self, name: str, url: str):
        # Передаём активный SOCKS5 порт для загрузки через VPN если нет прямого доступа
        proxy_port = SOCKS_PORT if self._proc is not None else 0
        upd = FolderUpdater(name, url, self._db.path, proxy_port=proxy_port)
        upd.log.connect(self._log)
        upd.done.connect(self._on_folder_updated)
        self._updaters.append(upd)
        upd.start()

    def _on_folder_updated(self, folder_name: str, added: int):
        # Снимаем флаг когда все авто-папки загружены
        if hasattr(self, '_pending_updates'):
            self._pending_updates -= 1
            if self._pending_updates <= 0:
                self._auto_updating = False
        self._load(reload_folders=True)
        self._sb_status(f"«{folder_name}»: обновлено {added} серверов")

    # ─── CONNECT ──────────────────────────────────────────────────────────────

    def _connect_row(self, row: int):
        self._tbl.selectRow(row); self._connect()

    def _selected(self) -> dict | None:
        if not self._tbl.selectedItems():
            QMessageBox.information(self,"Подключение","Выберите сервер."); return None
        row = self._tbl.currentRow()
        if row < 0 or row >= len(self._ids): return None
        sid = self._ids[row]
        # Ищем среди ВСЕХ серверов (all_folders=True), иначе серверы в папках не найдутся
        for s in self._db.all(all_folders=True):
            if s["id"] == sid: return s
        return None

    def _connect(self):
        if not os.path.exists(self._exe):
            QMessageBox.critical(self,"sing-box не найден",
                f"sing-box.exe не найден:\n{self._exe}\n\n"
                "Скачайте: https://github.com/SagerNet/sing-box/releases"); return

        srv = self._selected()
        if not srv: return
        tun = self._mode.currentIndex() == 0

        if tun and not is_admin():
            r = QMessageBox.question(self,"Нужен Admin",
                "TUN-режим требует Администратора.\n\nПерезапустить с Admin?",
                QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No)
            if r == QMessageBox.StandardButton.Yes: relaunch_as_admin()
            self._mode.setCurrentIndex(1); tun = False
            self._log("[SYS] Переключено в Прокси-режим.","#d29922")

        if self._sb_ver == (0,0):
            self._sb_ver = get_singbox_version(self._exe)

        # Принудительно убиваем ВСЕ sing-box и ждём освобождения TUN-адаптера
        self._log("[SYS] Завершение sing-box.exe (принудительно)…","#7d8590")
        kill_proc("sing-box.exe")   # уже включает sleep(0.6)

        for p in [SOCKS_PORT, HTTP_PORT]:
            free_port(p)
        time.sleep(0.3)

        if not is_port_free(SOCKS_PORT):
            QMessageBox.warning(self,"Порт занят",
                f"Порт {SOCKS_PORT} не освобождён — завершите мешающий процесс."); return

        # Конфиг
        cfg = build_config(srv, "tun" if tun else "proxy",
                           socks=SOCKS_PORT, http=HTTP_PORT)
        with open(self._cfg,"w",encoding="utf-8") as f:
            json.dump(cfg,f,indent=2,ensure_ascii=False)

        label = "TUN-туннель" if tun else "Прокси"
        proto = srv.get("protocol","vless").upper()
        if proto == "VLESS":
            details = (f"VLESS/{srv.get('transport','tcp').upper()}"
                       f"/{srv.get('security','none').upper()}")
        elif proto == "SS":
            details = f"SS/{srv.get('method','?').upper()}"
        elif proto == "TROJAN":
            details = f"TROJAN/{srv.get('transport','tcp').upper()}"
        elif proto == "VMESS":
            details = f"VMESS/{srv.get('transport','tcp').upper()}"
        elif proto == "HYSTERIA2":
            details = "HYSTERIA2"
        else:
            details = proto

        self._log(f"[SING] {label}: {srv['name']} ({srv['address']}:{srv['port']})","#58a6ff")
        self._log(f"[SING] Протокол: {details}","#58a6ff")

        try:
            self._proc = subprocess.Popen(
                [self._exe,"run","-c",self._cfg],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        except Exception as e:
            QMessageBox.critical(self,"Ошибка запуска",str(e)); return

        self._reader = LogReader(self._proc)
        self._reader.line.connect(self._on_log_line)
        self._reader.finished.connect(self._on_exit)
        self._reader.start()

        if not tun:
            time.sleep(1.0)
            try:
                self._proxy_bak = proxy_save()
                proxy_set("127.0.0.1", HTTP_PORT)
                self._log(f"[SYS] Системный прокси → 127.0.0.1:{HTTP_PORT}  ✓","#3fb950")
            except Exception as e:
                self._log(f"[SYS] Прокси не выставлен: {e}","#d29922")

        row = self._tbl.currentRow()
        it  = self._tbl.item(row,0)
        if it: it.setText("●"); it.setForeground(QColor("#3fb950"))
        self._conn_row = row
        self._ind.setText("● ПОДКЛЮЧЕНО")
        self._ind.setStyleSheet("color:#3fb950;font-size:13px;font-weight:bold;")
        self._bc.setEnabled(False); self._bd.setEnabled(True)
        self._sb_status(f"✓ {srv['name']}  ·  {label}  ·  SOCKS5 127.0.0.1:{SOCKS_PORT}")
        self._log("━"*55,"#30363d")
        self._log(f"✓  {'TUN — весь трафик через VPN' if tun else 'Прокси-режим'}","#3fb950")
        self._log(f"   SOCKS5: 127.0.0.1:{SOCKS_PORT}   HTTP: 127.0.0.1:{HTTP_PORT}","#58a6ff")
        self._log("   Проверка: https://2ip.ru","#58a6ff")
        self._log("━"*55,"#30363d")

    # ─── DISCONNECT ────────────────────────────────────────────────────────────

    def _disconnect(self):
        self._log("[SYS] Отключение…","#d29922")
        if self._reader: self._reader.stop()
        if self._proc:
            try: self._proc.terminate(); self._proc.wait(timeout=4)
            except: self._proc.kill()
            self._proc = None
        kill_proc("sing-box.exe")
        if self._proxy_bak: proxy_restore(self._proxy_bak); self._proxy_bak = None
        if self._conn_row is not None:
            it = self._tbl.item(self._conn_row,0)
            if it: it.setText("○"); it.setForeground(QColor("#484f58"))
            self._conn_row = None
        self._ind.setText("● ОТКЛЮЧЕНО")
        self._ind.setStyleSheet("color:#f85149;font-size:13px;font-weight:bold;")
        self._bc.setEnabled(True); self._bd.setEnabled(False)
        self._sb_status("Отключено"); self._log("[SYS] Отключено.","#f85149")

    def _on_exit(self):
        if self._proc is not None:
            self._log("[SING] Процесс завершился — см. ошибки выше.","#f85149")
            self._disconnect()

    # ─── LOG ──────────────────────────────────────────────────────────────────

    def _on_log_line(self, line: str):
        if "FATAL" in line or "ERROR" in line: col="#f85149"
        elif "WARN" in line: col="#d29922"
        elif "started" in line.lower() or "listening" in line.lower(): col="#3fb950"
        else: col="#8b949e"
        self._log(f"[sing] {line}", col)

    def _log(self, txt: str, col: str="#e6edf3"):
        ts  = time.strftime("%H:%M:%S")
        cur = self._logw.textCursor()
        cur.movePosition(QTextCursor.MoveOperation.End)
        self._logw.setTextCursor(cur)
        self._logw.setTextColor(QColor("#484f58"))
        self._logw.insertPlainText(f"[{ts}] ")
        self._logw.setTextColor(QColor(col))
        self._logw.insertPlainText(txt+"\n")
        self._logw.ensureCursorVisible()

    def _sb_status(self, msg: str): self._sbar.showMessage(f"  {msg}")

    def closeEvent(self, e):
        if self._checker: self._checker.request_stop()
        self._disconnect()
        for f in [self._cfg]:
            try:
                if os.path.exists(f): os.remove(f)
            except: pass
        super().closeEvent(e)


# =============================================================================
# EXCEPTHOOK
# =============================================================================
def _hook(t, v, tb):
    s = "".join(traceback.format_exception(t,v,tb))
    app = QApplication.instance()
    if app: QMessageBox.critical(None,"Ошибка",f"Исключение:\n\n{s}")
    else:   print(s,file=sys.stderr)

# =============================================================================
# MAIN
# =============================================================================
def main():
    sys.excepthook = _hook

    if sys.platform == "win32" and not is_admin():
        _tmp = QApplication(sys.argv)
        ans  = QMessageBox.question(None,"Admin требуется",
            "TUN-режим требует прав Администратора.\n\n"
            "Перезапустить с Admin (рекомендуется)?\n"
            "«Нет» — только Прокси-режим.",
            QMessageBox.StandardButton.Yes|QMessageBox.StandardButton.No)
        if ans == QMessageBox.StandardButton.Yes: relaunch_as_admin()
        del _tmp

    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("VPN Client — sing-box")
    app.setStyleSheet(DARK)

    pal = QPalette()
    for role, color in [
        (QPalette.ColorRole.Window,         "#0d1117"),
        (QPalette.ColorRole.WindowText,     "#e6edf3"),
        (QPalette.ColorRole.Base,           "#161b22"),
        (QPalette.ColorRole.AlternateBase,  "#1c2128"),
        (QPalette.ColorRole.Text,           "#e6edf3"),
        (QPalette.ColorRole.Button,         "#21262d"),
        (QPalette.ColorRole.ButtonText,     "#e6edf3"),
        (QPalette.ColorRole.Highlight,      "#1f6feb"),
        (QPalette.ColorRole.HighlightedText,"#ffffff"),
    ]:
        pal.setColor(role, QColor(color))
    app.setPalette(pal)

    w = VpnApp(); w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
