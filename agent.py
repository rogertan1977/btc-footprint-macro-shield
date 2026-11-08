"""
Zone-Based Footprint & Macro Validation Agent (伏击圈足迹与宏观护盾狙击手)
-------------------------------------------------------------------------
硬件目标: Windows 老旧工控机/PC (Python 3.15, i7-4770, 8GB RAM).
环境特性: 纯 Python 标准库配置解析 (零 pyyaml/C++ 依赖), 仅需基础 aiohttp.
设计原则: 内存绝对 O(1)、指数退避重连、防抖迟滞带、WAL 模式持久化、宏观状态机无锁并发.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import math
import os
import random
import re
import sqlite3
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# ------------------------------------------------------------------------------
# 0. 日志系统配置 (Windows 控制台友好)
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ZoneSniper")


# ------------------------------------------------------------------------------
# 1. 纯 Python 标准库轻量级 YAML 递归解析器 (零三方依赖，防弹级防崩)
# ------------------------------------------------------------------------------
def _strip_yaml_comments(line: str) -> str:
    """去除行内注释，严格保护字符串内双引号或单引号包含的 '#' 字符"""
    in_single = False
    in_double = False
    result: List[str] = []
    for ch in line:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            break
        result.append(ch)
    return "".join(result)


def _cast_yaml_scalar(val_str: str) -> Any:
    """将 YAML 标量文本自动转换为 Python 标准类型 (bool, int, float, str, None)"""
    v = val_str.strip()
    if not v:
        return None

    # 引号显式字符串解包
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        return v[1:-1]

    lower_v = v.lower()
    if lower_v in ("true", "yes", "on"):
        return True
    if lower_v in ("false", "no", "off"):
        return False
    if lower_v in ("null", "~", "none"):
        return None

    # 尝试解析整数
    try:
        return int(v)
    except ValueError:
        pass

    # 尝试解析浮点数
    try:
        return float(v)
    except ValueError:
        pass

    # 保留为纯文本
    return v


def parse_yaml_subset(raw_text: str) -> Dict[str, Any]:
    """
    纯标准库实现的通用缩进栈 YAML 子集解析器。
    支持任意多级嵌套字典、空行、行内注释以及类型自动映射。
    """
    root: Dict[str, Any] = {}
    # 栈记录格式: (缩进空格数, 当前字典对象引用)
    stack: List[Tuple[int, Dict[str, Any]]] = [(-1, root)]

    lines = raw_text.splitlines()
    for line_idx, raw_line in enumerate(lines, start=1):
        clean = _strip_yaml_comments(raw_line)
        stripped = clean.strip()
        if not stripped:
            continue  # 空行或整行注释

        indent = len(clean) - len(clean.lstrip(" "))
        if ":" not in stripped:
            continue

        key_part, _, val_part = stripped.partition(":")
        key = key_part.strip().strip('"').strip("'")
        val_str = val_part.strip()

        # 退栈机制：若当前缩进小于或等于栈顶缩进，退栈寻找其父级字典
        while len(stack) > 1 and stack[-1][0] >= indent:
            stack.pop()

        parent_dict = stack[-1][1]

        if not val_str:
            # 这是一个嵌套字典的开始节点 (如 `target:`)
            new_section: Dict[str, Any] = {}
            parent_dict[key] = new_section
            stack.append((indent, new_section))
        else:
            # 这是一个具体的键值对 (如 `symbol: "BTCUSDT"`)
            parent_dict[key] = _cast_yaml_scalar(val_str)

    return root


# ------------------------------------------------------------------------------
# 2. 紧凑型微观内存数据结构 (Slots 优化，内存绝对 O(1))
# ------------------------------------------------------------------------------
@dataclass(slots=True)
class TradeTick:
    timestamp: float
    price: float
    quantity: float
    is_buyer_maker: bool  # True 表示市价卖单吃买盘 (Taker Sell), False 表示市价买单吃卖盘


@dataclass(slots=True)
class OrderBookSnapshot:
    timestamp: float
    bids: List[Tuple[float, float]]  # [(price, qty), ...]
    asks: List[Tuple[float, float]]


@dataclass(slots=True)
class MacroStatus:
    timestamp: float
    on_rrp_billions: float
    t10y3m_spread: float
    hy_spread: float
    sahm_rule: float
    is_liquidity_dry: bool
    is_recession_alert: bool
    is_credit_stress: bool
    risk_level: str  # "SAFE", "CAUTION", "CRITICAL_RISK"
    summary_text: str


@dataclass(slots=True)
class SniperSignal:
    timestamp: float
    symbol: str
    signal_type: str  # "SELLING_EXHAUSTION_ABSORPTION", "BUYING_EXHAUSTION_DISTRIBUTION"
    trigger_price: float
    zone_range: Tuple[float, float]
    window_sec: float
    delta_cvd: float
    total_volume: float
    price_change_pct: float
    wall_volume: float
    macro_context: MacroStatus


# ------------------------------------------------------------------------------
# 3. 数据库引擎: SQLite WAL 模式 (防死锁与离线复盘)
# ------------------------------------------------------------------------------
class SQLiteWALRecorder:
    """具备 WAL 模式的高效离线记录器，确保工控机在多任务下不产生数据库文件锁死"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sniper_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    trigger_price REAL NOT NULL,
                    delta_cvd REAL NOT NULL,
                    total_volume REAL NOT NULL,
                    price_change_pct REAL NOT NULL,
                    wall_volume REAL NOT NULL,
                    macro_risk_level TEXT NOT NULL,
                    macro_summary TEXT NOT NULL,
                    raw_payload TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_ts ON sniper_events(created_at);"
            )
            conn.commit()

    def record_signal(self, sig: SniperSignal) -> None:
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO sniper_events (
                        created_at, symbol, signal_type, trigger_price, delta_cvd,
                        total_volume, price_change_pct, wall_volume, macro_risk_level,
                        macro_summary, raw_payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sig.timestamp,
                        sig.symbol,
                        sig.signal_type,
                        sig.trigger_price,
                        sig.delta_cvd,
                        sig.total_volume,
                        sig.price_change_pct,
                        sig.wall_volume,
                        sig.macro_context.risk_level,
                        sig.macro_context.summary_text,
                        json.dumps(
                            {
                                "zone": sig.zone_range,
                                "window_sec": sig.window_sec,
                                "rrp": sig.macro_context.on_rrp_billions,
                                "t10y3m": sig.macro_context.t10y3m_spread,
                                "hy_spread": sig.macro_context.hy_spread,
                                "sahm": sig.macro_context.sahm_rule,
                            }
                        ),
                    ),
                )
                conn.commit()
            logger.info("🎯 [持久化] 信号已安全写入 SQLite (WAL) 存储库")
        except Exception as e:
            logger.critical(f"❌ 数据库写入崩溃: {e}", exc_info=True)


# ------------------------------------------------------------------------------
# 4. 宏观护盾: FRED 宏观流控与指标分析器 (Macro Shield)
# ------------------------------------------------------------------------------
class FredMacroShield:
    """
    负责在后台低频跟踪美国流动性与衰退指标。
    前台微观引擎直接微秒级读取只读内存实例，绝不在信号触发的关键路径发生网络等待。
    """

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: str, enabled: bool = True, refresh_hours: float = 12.0) -> None:
        self.api_key = api_key
        self.enabled = enabled
        self.refresh_interval_sec = refresh_hours * 3600.0
        self._cached_status: MacroStatus = self._create_fallback_status("初始化中")
        self._lock = asyncio.Lock()

    def _create_fallback_status(self, reason: str) -> MacroStatus:
        return MacroStatus(
            timestamp=time.time(),
            on_rrp_billions=999.0,
            t10y3m_spread=0.5,
            hy_spread=3.0,
            sahm_rule=0.1,
            is_liquidity_dry=False,
            is_recession_alert=False,
            is_credit_stress=False,
            risk_level="SAFE",
            summary_text=f"宏观护盾备用基准 ({reason})",
        )

    def get_current_macro_status(self) -> MacroStatus:
        return self._cached_status

    async def start_background_updater(self) -> None:
        if not self.enabled or not self.api_key or "YOUR_FRED" in self.api_key:
            logger.warning("⚠️ FRED 宏观护盾未配置 API Key，将使用被动安全基线模式运行。")
            return

        while True:
            try:
                await self.refresh_macro_data()
            except Exception as e:
                logger.error(f"❌ FRED 宏观护盾刷新遇到未知异常: {e}")
            await asyncio.sleep(self.refresh_interval_sec)

    async def refresh_macro_data(self) -> None:
        timeout = aiohttp.ClientTimeout(total=20.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            rrp = await self._fetch_series_latest(session, "RRPONTSYD")  # 隔夜逆回购 (Billions)
            t10y3m = await self._fetch_series_latest(session, "T10Y3M")  # 10Y-3M 美债利差 (%)
            hy_spread = await self._fetch_series_latest(session, "BAMLH0A0HYM2")  # 高收益债信用利差 (%)
            sahm = await self._fetch_series_latest(session, "SAHMCURRENT")  # 萨姆规则衰退指标 (%)

            is_dry = rrp < 200.0 if rrp is not None else False
            is_recession = (t10y3m is not None and t10y3m >= 0.0) or (sahm is not None and sahm >= 0.50)
            is_credit = hy_spread is not None and hy_spread >= 4.50

            warnings = []
            if is_dry:
                warnings.append(f"ON RRP 流动性枯竭 (${rrp:.1f}B < $200B)")
            if t10y3m is not None and t10y3m >= 0.0:
                warnings.append(f"10Y-3M 解除倒挂 ({t10y3m:.2f}%) 警惕系统性风险释放")
            if sahm is not None and sahm >= 0.50:
                warnings.append(f"萨姆规则触发衰退警戒 ({sahm:.2f}% >= 0.5%)")
            if is_credit:
                warnings.append(f"信用利差走阔至高危区 ({hy_spread:.2f}% >= 4.5%)")

            if len(warnings) >= 2 or is_credit:
                risk_lvl = "CRITICAL_RISK"
            elif len(warnings) == 1:
                risk_lvl = "CAUTION"
            else:
                risk_lvl = "SAFE"

            summary = " | ".join(warnings) if warnings else "宏观水温适宜，系统性违约概率偏低"

            async with self._lock:
                self._cached_status = MacroStatus(
                    timestamp=time.time(),
                    on_rrp_billions=rrp if rrp is not None else 0.0,
                    t10y3m_spread=t10y3m if t10y3m is not None else 0.0,
                    hy_spread=hy_spread if hy_spread is not None else 0.0,
                    sahm_rule=sahm if sahm is not None else 0.0,
                    is_liquidity_dry=is_dry,
                    is_recession_alert=is_recession,
                    is_credit_stress=is_credit,
                    risk_level=risk_lvl,
                    summary_text=summary,
                )
            logger.info(f"🛡️ [FRED 宏观护盾更新] 风险等级: {risk_lvl} | {summary}")

    async def _fetch_series_latest(self, session: aiohttp.ClientSession, series_id: str) -> Optional[float]:
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,
        }
        try:
            async with session.get(self.BASE_URL, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"FRED API 返回错误 HTTP {resp.status} for series {series_id}")
                    return None
                data = await resp.json()
                observations = data.get("observations", [])
                for obs in observations:
                    val_str = obs.get("value", ".")
                    if val_str != ".":
                        return float(val_str)
                return None
        except Exception as e:
            logger.warning(f"无法抓取 FRED 指标 {series_id}: {e}")
            return None


# ------------------------------------------------------------------------------
# 5. 微观足迹引擎: O(1) CVD 环形缓冲与冰山吸收计算器
# ------------------------------------------------------------------------------
class CircularFootprintEngine:
    """
    内存复杂度严格为 O(1) 的逐笔微观足迹引擎。
    采用增量差值维护，杜绝任何 pandas / numpy 开销，适配老旧工控机 CPU。
    """

    def __init__(
        self,
        window_seconds: float = 15.0,
        min_vol_threshold: float = 50.0,
        imbalance_ratio: float = 2.5,
        price_impact_tolerance: float = 0.0008,
        depth_wall_ratio: float = 2.0,
    ) -> None:
        self.window_seconds = window_seconds
        self.min_vol_threshold = min_vol_threshold
        self.imbalance_ratio = imbalance_ratio
        self.price_impact_tolerance = price_impact_tolerance
        self.depth_wall_ratio = depth_wall_ratio

        self.trades: deque[TradeTick] = deque(maxlen=4096)
        self.latest_book: Optional[OrderBookSnapshot] = None

        self.running_buy_vol: float = 0.0
        self.running_sell_vol: float = 0.0
        self.running_cvd: float = 0.0

    def update_orderbook(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> None:
        self.latest_book = OrderBookSnapshot(
            timestamp=time.time(),
            bids=bids[:10],
            asks=asks[:10],
        )

    def append_trade(self, price: float, qty: float, is_buyer_maker: bool) -> None:
        now = time.time()
        tick = TradeTick(timestamp=now, price=price, quantity=qty, is_buyer_maker=is_buyer_maker)
        self.trades.append(tick)

        if is_buyer_maker:
            self.running_sell_vol += qty
            self.running_cvd -= qty
        else:
            self.running_buy_vol += qty
            self.running_cvd += qty

        cutoff = now - self.window_seconds
        while self.trades and self.trades[0].timestamp < cutoff:
            evicted = self.trades.popleft()
            if evicted.is_buyer_maker:
                self.running_sell_vol -= evicted.quantity
                self.running_cvd += evicted.quantity
            else:
                self.running_buy_vol -= evicted.quantity
                self.running_cvd -= evicted.quantity

        if not self.trades:
            self.running_buy_vol = 0.0
            self.running_sell_vol = 0.0
            self.running_cvd = 0.0

    def evaluate_absorption(self, current_price: float) -> Optional[Tuple[str, float, float, float, float]]:
        """
        核心冰山吸收与派发数学检测
        返回: (signal_type, delta_cvd, total_vol, price_change_pct, wall_volume)
        """
        if len(self.trades) < 20 or self.latest_book is None:
            return None

        total_vol = self.running_buy_vol + self.running_sell_vol
        if total_vol < self.min_vol_threshold:
            return None

        first_tick = self.trades[0]
        price_change_pct = (current_price - first_tick.price) / first_tick.price

        top5_bid_vol = sum(q for _, q in self.latest_book.bids[:5])
        top5_ask_vol = sum(q for _, q in self.latest_book.asks[:5])

        # 1. 卖盘竭尽 / 冰山买单吸收
        if (
            self.running_sell_vol > self.running_buy_vol * self.imbalance_ratio
            and price_change_pct >= -self.price_impact_tolerance
            and top5_bid_vol >= self.running_sell_vol * self.depth_wall_ratio
        ):
            return (
                "SELLING_EXHAUSTION_ABSORPTION",
                self.running_cvd,
                total_vol,
                price_change_pct,
                top5_bid_vol,
            )

        # 2. 买盘竭尽 / 派发压制
        if (
            self.running_buy_vol > self.running_sell_vol * self.imbalance_ratio
            and price_change_pct <= self.price_impact_tolerance
            and top5_ask_vol >= self.running_buy_vol * self.depth_wall_ratio
        ):
            return (
                "BUYING_EXHAUSTION_DISTRIBUTION",
                self.running_cvd,
                total_vol,
                price_change_pct,
                top5_ask_vol,
            )

        return None


# ------------------------------------------------------------------------------
# 6. Telegram 工业级防崩溃报警管道 (MarkdownV2 强转义)
# ------------------------------------------------------------------------------
class TelegramReporter:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = bool(bot_token and chat_id and "YOUR_TELEGRAM" not in bot_token)

    @staticmethod
    def escape_md_v2(text: Any) -> str:
        s = str(text)
        return re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", s)

    async def send_alert(self, sig: SniperSignal) -> None:
        if not self.enabled:
            logger.info(f"📢 [本地模拟告警 - 未配 TG] {sig.signal_type} @ {sig.trigger_price}")
            return

        if sig.signal_type == "SELLING_EXHAUSTION_ABSORPTION":
            title = "🟢 【伏击圈信号：卖盘竭尽 \\+ 冰山吸收】"
            action_desc = "市价猛烈砸盘遭多头主力冰山限价单全部拦截，价格锁死不跌，多头防线极度坚固。"
        elif sig.signal_type == "BUYING_EXHAUSTION_DISTRIBUTION":
            title = "🔴 【伏击圈信号：买盘竭尽 \\+ 庄家派发】"
            action_desc = "市价疯狂抢筹但价格滞涨，上方遭遇机构级巨量限价压盘，追高动能衰竭。"
        else:
            title = "⚡ 【伏击圈信号：空头踩踏 / 轧空爆发】"
            action_desc = "CVD 脉冲飙升伴随持仓量极速断崖下跌，空头爆仓螺旋形成。"

        macro_shield_emoji = "✅"
        if sig.macro_context.risk_level == "CAUTION":
            macro_shield_emoji = "⚠️"
        elif sig.macro_context.risk_level == "CRITICAL_RISK":
            macro_shield_emoji = "🚨"

        msg = (
            f"{title}\n\n"
            f"🎯 *标的代号*: `{self.escape_md_v2(sig.symbol)}`\n"
            f"📍 *当前现价*: `{self.escape_md_v2(f'{sig.trigger_price:.2f}')}`\n"
            f"📐 *伏击区间*: `[{self.escape_md_v2(sig.zone_range[0])} ~ {self.escape_md_v2(sig.zone_range[1])}]`\n"
            f"⏱️ *监控窗口*: `{self.escape_md_v2(f'{sig.window_sec:.0f}s')}`\n"
            f"📊 *窗口总成交*: `{self.escape_md_v2(f'{sig.total_volume:.2f}')}`\n"
            f"📉 *净 CVD 变动*: `{self.escape_md_v2(f'{sig.delta_cvd:+.2f}')}`\n"
            f"📌 *微观价格偏移*: `{self.escape_md_v2(f'{sig.price_change_pct * 100:+.3f}%')}`\n"
            f"🧱 *盘口防御厚度*: `{self.escape_md_v2(f'{sig.wall_volume:.2f}')}`\n\n"
            f"💡 *微观结构判断*:\n_{self.escape_md_v2(action_desc)}_\n\n"
            f"🛡️ *FRED 宏观护盾状态*: {macro_shield_emoji} *{self.escape_md_v2(sig.macro_context.risk_level)}*\n"
            f"• 隔夜逆回购: `{self.escape_md_v2(f'${sig.macro_context.on_rrp_billions:.1f}B')}`\n"
            f"• 10Y\\-3M 利差: `{self.escape_md_v2(f'{sig.macro_context.t10y3m_spread:.2f}%')}`\n"
            f"• 高收益债利差: `{self.escape_md_v2(f'{sig.macro_context.hy_spread:.2f}%')}`\n"
            f"• 衰退风险提示: _{self.escape_md_v2(sig.macro_context.summary_text)}_\n\n"
            f"⏰ *时间戳*: `{self.escape_md_v2(datetime.datetime.fromtimestamp(sig.timestamp).strftime('%Y-%m-%d %H:%M:%S'))}`"
        )

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": msg,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }

        try:
            timeout = aiohttp.ClientTimeout(total=10.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        logger.error(f"❌ Telegram 报警发送失败: HTTP {resp.status} - {err_text}")
                    else:
                        logger.info("🚀 [Telegram] MarkdownV2 精确警报已成功投递！")
        except Exception as e:
            logger.error(f"❌ Telegram 推送异常: {e}")


# ------------------------------------------------------------------------------
# 7. 防弹 Binance 数据客户端 (指数退避与看门狗心跳)
# ------------------------------------------------------------------------------
class BinanceBulletproofStream:
    def __init__(self, symbol: str, is_futures: bool, footprint_engine: CircularFootprintEngine) -> None:
        self.symbol = symbol.lower()
        self.is_futures = is_futures
        self.engine = footprint_engine
        self._running = False
        self._last_data_time = 0.0

    @property
    def ws_url(self) -> str:
        streams = f"{self.symbol}@aggTrade/{self.symbol}@depth20@100ms"
        if self.is_futures:
            return f"wss://fstream.binance.com/stream?streams={streams}"
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    async def listen_loop(self) -> None:
        self._running = True
        attempt = 0
        base_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info(f"🔌 正在建立 Binance WebSocket 连接: {self.ws_url}")
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=10.0, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(
                        self.ws_url,
                        heartbeat=15.0,
                        max_msg_size=10 * 1024 * 1024,
                    ) as ws:
                        logger.info("🟢 WebSocket 连接成功建立，足迹微观流已挂载！")
                        attempt = 0
                        self._last_data_time = time.time()
                        watchdog_task = asyncio.create_task(self._stall_watchdog(ws))

                        try:
                            async for msg in ws:
                                if not self._running:
                                    break
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    self._last_data_time = time.time()
                                    self._handle_message(msg.data)
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    logger.warning(f"WebSocket 收到断开事件: {msg.type}")
                                    break
                        finally:
                            watchdog_task.cancel()
                            await asyncio.gather(watchdog_task, return_exceptions=True)

            except (aiohttp.ClientError, asyncio.TimeoutError) as net_err:
                logger.warning(f"⚠️ 网络通信中断: {net_err}")
            except Exception as e:
                logger.error(f"❌ WebSocket 未预期异常: {e}", exc_info=True)

            if not self._running:
                break

            attempt += 1
            delay = min(max_delay, base_delay * (2 ** (attempt - 1))) * random.uniform(0.8, 1.2)
            logger.info(f"⏳ 将在 {delay:.2f} 秒后执行指数退避重连 (尝试次数: {attempt})...")
            await asyncio.sleep(delay)

    async def _stall_watchdog(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while self._running:
            await asyncio.sleep(5.0)
            if time.time() - self._last_data_time > 15.0:
                logger.warning("🚨 [看门狗] 超过 15 秒未收到交易所推送，强制重置底层连接...")
                await ws.close()
                break

    def _handle_message(self, raw_payload: str) -> None:
        try:
            packet = json.loads(raw_payload)
            stream_name = packet.get("stream", "")
            data = packet.get("data", {})

            if "aggTrade" in stream_name:
                p = float(data["p"])
                q = float(data["q"])
                m = bool(data["m"])
                self.engine.append_trade(p, q, m)
            elif "depth20" in stream_name:
                bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
                asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
                self.engine.update_orderbook(bids, asks)
        except Exception:
            pass

    async def stop(self) -> None:
        self._running = False


# ------------------------------------------------------------------------------
# 8. 伏击圈主控状态机引擎 (Agent Orchestrator)
# ------------------------------------------------------------------------------
class ZoneBasedFootprintAgent:
    """
    总控协调器：
    - SLEEPING 状态：REST 低频探测，Windows 老工控机 CPU 0% 挂机。
    - ACTIVE 状态：触及伏击圈，唤醒 WebSocket 与微观足迹引擎。
    - COOLDOWN 状态：迟滞带与时间防抖退出，杜绝临界振荡。
    """

    STATE_SLEEPING = "SLEEPING"
    STATE_ACTIVE = "ACTIVE"
    STATE_COOLDOWN = "COOLDOWN"

    def __init__(self, config_path: str = "config.yaml") -> None:
        self.config = self._load_config(config_path)
        self.target = self.config.get("target", {})
        self.symbol = str(self.target.get("symbol", "BTCUSDT")).upper()
        self.is_futures = bool(self.target.get("is_futures", True))
        self.lower_bound = float(self.target.get("lower_bound", 75000.0))
        self.upper_bound = float(self.target.get("upper_bound", 76300.0))
        self.hysteresis_pct = float(self.target.get("hysteresis_pct", 0.0025))
        self.cooldown_seconds = float(self.target.get("cooldown_seconds", 30))
        self.sleep_poll_interval = float(self.target.get("sleep_poll_interval_sec", 5.0))

        micro_cfg = self.config.get("micro_engine", {})
        self.footprint_engine = CircularFootprintEngine(
            window_seconds=float(micro_cfg.get("window_seconds", 15)),
            min_vol_threshold=float(micro_cfg.get("min_volume_threshold", 50.0)),
            imbalance_ratio=float(micro_cfg.get("cvd_imbalance_ratio", 2.5)),
            price_impact_tolerance=float(micro_cfg.get("price_impact_tolerance_pct", 0.0008)),
            depth_wall_ratio=float(micro_cfg.get("depth_absorption_ratio", 2.0)),
        )

        macro_cfg = self.config.get("macro_shield", {})
        self.macro_shield = FredMacroShield(
            api_key=str(macro_cfg.get("api_key", "")),
            enabled=bool(macro_cfg.get("enabled", True)),
            refresh_hours=float(macro_cfg.get("refresh_interval_hours", 12.0)),
        )

        storage_cfg = self.config.get("storage", {})
        self.db = SQLiteWALRecorder(db_path=str(storage_cfg.get("db_path", "data/sniper_records.db")))

        notif_cfg = self.config.get("notification", {})
        self.reporter = TelegramReporter(
            bot_token=str(notif_cfg.get("telegram_bot_token", "")),
            chat_id=str(notif_cfg.get("telegram_chat_id", "")),
        )

        self.current_state = self.STATE_SLEEPING
        self.ws_stream: Optional[BinanceBulletproofStream] = None
        self.ws_task: Optional[asyncio.Task[None]] = None
        self.last_in_zone_time = 0.0
        self.last_signal_time = 0.0
        self.signal_cooldown_sec = 10.0

    def _load_config(self, path: str) -> Dict[str, Any]:
        """使用内置的纯标准库解析器读取配置文件，无需 pyyaml 库"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"配置文件不存在: {path}")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        return parse_yaml_subset(content)

    def is_in_zone(self, price: float) -> bool:
        return self.lower_bound <= price <= self.upper_bound

    def is_completely_out_of_zone(self, price: float) -> bool:
        lower_threshold = self.lower_bound * (1.0 - self.hysteresis_pct)
        upper_threshold = self.upper_bound * (1.0 + self.hysteresis_pct)
        return price < lower_threshold or price > upper_threshold

    async def fetch_rest_price(self, session: aiohttp.ClientSession) -> Optional[float]:
        url = (
            f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={self.symbol}"
            if self.is_futures
            else f"https://api.binance.com/api/v3/ticker/price?symbol={self.symbol}"
        )
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4.0)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["price"])
        except Exception:
            pass
        return None

    async def _activate_micro_engine(self) -> None:
        if self.ws_task is not None and not self.ws_task.done():
            return
        logger.info(f"🔥 【全面唤醒】现价已突破伏击圈 [{self.lower_bound} ~ {self.upper_bound}]！启动 Footprint CVD 微观引擎...")
        self.ws_stream = BinanceBulletproofStream(
            symbol=self.symbol,
            is_futures=self.is_futures,
            footprint_engine=self.footprint_engine,
        )
        self.ws_task = asyncio.create_task(self.ws_stream.listen_loop())

    async def _deactivate_micro_engine(self) -> None:
        logger.info("❄️ 【进入深度休眠】价格完全脱离伏击圈迟滞带，关闭 WebSocket，工控机 CPU 归零。")
        if self.ws_stream:
            await self.ws_stream.stop()
        if self.ws_task:
            self.ws_task.cancel()
            await asyncio.gather(self.ws_task, return_exceptions=True)
            self.ws_task = None
            self.ws_stream = None

    async def run(self) -> None:
        logger.info(f"🎯 Zone-Based Footprint Sniper 启动成功！标的: {self.symbol}")
        logger.info(f"📍 伏击目标区间: {self.lower_bound} <---> {self.upper_bound}")

        asyncio.create_task(self.macro_shield.start_background_updater())

        timeout = aiohttp.ClientTimeout(total=5.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    if self.current_state == self.STATE_SLEEPING:
                        current_price = await self.fetch_rest_price(session)
                        if current_price is None:
                            await asyncio.sleep(self.sleep_poll_interval)
                            continue

                        if self.is_in_zone(current_price):
                            self.current_state = self.STATE_ACTIVE
                            self.last_in_zone_time = time.time()
                            await self._activate_micro_engine()
                        else:
                            await asyncio.sleep(self.sleep_poll_interval)
                            continue

                    elif self.current_state in (self.STATE_ACTIVE, self.STATE_COOLDOWN):
                        if self.footprint_engine.trades:
                            current_price = self.footprint_engine.trades[-1].price
                        else:
                            current_price = await self.fetch_rest_price(session) or self.lower_bound

                        if self.is_in_zone(current_price):
                            self.current_state = self.STATE_ACTIVE
                            self.last_in_zone_time = time.time()
                        elif self.is_completely_out_of_zone(current_price):
                            if self.current_state == self.STATE_ACTIVE:
                                logger.info("⚠️ 价格超出伏击圈边缘，启动防抖冷却计时 (Cooldown)...")
                                self.current_state = self.STATE_COOLDOWN
                            elif self.current_state == self.STATE_COOLDOWN:
                                if time.time() - self.last_in_zone_time > self.cooldown_seconds:
                                    self.current_state = self.STATE_SLEEPING
                                    await self._deactivate_micro_engine()
                                    continue

                        eval_res = self.footprint_engine.evaluate_absorption(current_price)
                        now = time.time()
                        if eval_res is not None and (now - self.last_signal_time > self.signal_cooldown_sec):
                            sig_type, delta_cvd, tot_vol, p_diff, wall_vol = eval_res
                            self.last_signal_time = now

                            macro_ctx = self.macro_shield.get_current_macro_status()

                            signal = SniperSignal(
                                timestamp=now,
                                symbol=self.symbol,
                                signal_type=sig_type,
                                trigger_price=current_price,
                                zone_range=(self.lower_bound, self.upper_bound),
                                window_sec=self.footprint_engine.window_seconds,
                                delta_cvd=delta_cvd,
                                total_volume=tot_vol,
                                price_change_pct=p_diff,
                                wall_volume=wall_vol,
                                macro_context=macro_ctx,
                            )

                            logger.info(f"⚡ [微观信号爆发] {sig_type} 现价: {current_price:.2f} CVD: {delta_cvd:+.2f}")
                            self.db.record_signal(signal)
                            asyncio.create_task(self.reporter.send_alert(signal))

                        await asyncio.sleep(0.1)

                except asyncio.CancelledError:
                    break
                except Exception as loop_err:
                    logger.error(f"主调度状态机循环异常: {loop_err}", exc_info=True)
                    await asyncio.sleep(1.0)

        await self._deactivate_micro_engine()
        logger.info("🛑 Agent 安全关闭完成。")


# ------------------------------------------------------------------------------
# 9. 启动引导入口 (Windows Proactor/Selector 兼容适配与自包含配置生成)
# ------------------------------------------------------------------------------
CONFIG_TEMPLATE = """# ==============================================================================
# Zone-Based Footprint & Macro Validation Agent 生产配置文件
# ==============================================================================

target:
  symbol: "BTCUSDT"
  is_futures: true
  lower_bound: 75000.0
  upper_bound: 76300.0
  hysteresis_pct: 0.0025
  cooldown_seconds: 30
  sleep_poll_interval_sec: 5.0

micro_engine:
  window_seconds: 15
  min_volume_threshold: 50.0
  cvd_imbalance_ratio: 2.5
  price_impact_tolerance_pct: 0.0008
  depth_absorption_ratio: 2.0

macro_shield:
  enabled: true
  api_key: "YOUR_FRED_API_KEY_HERE"
  refresh_interval_hours: 12
  thresholds:
    on_rrp_critical_billions: 200.0
    hy_spread_critical_pct: 4.50
    sahm_rule_critical: 0.50

notification:
  telegram_bot_token: "YOUR_TELEGRAM_BOT_TOKEN"
  telegram_chat_id: "YOUR_CHAT_ID"

storage:
  db_path: "data/sniper_records.db"
"""


def main() -> None:
    # 针对 Windows 老工控机 Python 异步信号处理优化
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if not os.path.exists("config.yaml"):
        print("未检测到 config.yaml，正在使用内建模板直接生成标准配置...")
        with open("config.yaml", "w", encoding="utf-8") as f:
            f.write(CONFIG_TEMPLATE)
        print("已创建 config.yaml，请填入你的 FRED API Key 和 Telegram 配置后再次启动。")
        return

    agent = ZoneBasedFootprintAgent("config.yaml")
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("用户按下 Ctrl+C，正在紧急中断并关闭所有资源...")


if __name__ == "__main__":
    main()