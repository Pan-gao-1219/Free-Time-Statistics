"""
班级课表空余时间统计分析工具 - 增强专业版 v2
新增功能:
  · 异步并行解析 (asyncio + ThreadPoolExecutor)
  · 磁盘缓存 (pickle, 基于文件哈希)
  · 逐行读取 Excel (openpyxl 流模式)，大幅降低内存占用
  · scipy 稀疏矩阵存储 occ_sum
  · 图表懒加载 (切换到对应标签才生成)
  · 支持 ZIP 压缩包 / 直接多文件上传
  · 导出: Markdown 报告 / Excel (多 Sheet) / CSV / 图表 PNG
  · matplotlib 中文字体自动检测 (Noto Sans CJK SC)
  · 增强可视化: 气泡图、箱线分布、分组柱状图、日历热力图
"""

# ── stdlib ────────────────────────────────────────────────────────────────────
import os, re, zipfile, tempfile, asyncio, hashlib, pickle, io, shutil, time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

# ── third-party ───────────────────────────────────────────────────────────────
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib
matplotlib.use("Agg")          # 无 GUI 后端
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from openpyxl import load_workbook
from scipy import sparse

# xlrd 是可选依赖，仅读取 .xls 时需要
try:
    import xlrd
    _XLRD_OK = True
except ImportError:
    _XLRD_OK = False

# ── 页面配置 ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="📅 课表空余分析",
    page_icon="📅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# 中文字体配置（matplotlib）
# ─────────────────────────────────────────────────────────────────────────────
_CN_FONT = None

def _setup_cn_font():
    """自动检测并配置 matplotlib 中文字体"""
    global _CN_FONT
    if _CN_FONT is not None:
        return _CN_FONT
    candidates = [
        "Noto Sans CJK SC", "Noto Sans CJK TC", "Noto Sans CJK JP",
        "Noto Serif CJK SC", "Noto Serif CJK JP",
        "SimHei", "WenQuanYi Micro Hei", "Microsoft YaHei",
        "PingFang SC", "Heiti SC",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    for c in candidates:
        if c in available:
            _CN_FONT = c
            break
    if _CN_FONT is None:
        _CN_FONT = "DejaVu Sans"  # 降级方案（中文会显示方块，但不崩溃）
    matplotlib.rcParams["font.family"] = [_CN_FONT, "DejaVu Sans"]
    matplotlib.rcParams["axes.unicode_minus"] = False
    return _CN_FONT

_setup_cn_font()

# ─────────────────────────────────────────────────────────────────────────────
# 自定义 CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.main-header {
    font-size: 2.4rem; font-weight: 700;
    background: linear-gradient(135deg, #1E3A8A, #3B82F6);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin-bottom: .4rem;
}
.sub-header { font-size: 1rem; color: #6B7280; margin-bottom: 1.5rem; }

.kpi-card {
    background: linear-gradient(135deg, #EFF6FF, #DBEAFE);
    border-left: 4px solid #3B82F6;
    border-radius: .6rem; padding: 1rem 1.2rem;
    box-shadow: 0 2px 8px rgba(59,130,246,.15);
}
.kpi-value { font-size: 1.9rem; font-weight: 700; color: #1E3A8A; line-height:1.1; }
.kpi-label { font-size: .8rem; color: #6B7280; margin-top:.2rem; }
.kpi-delta { font-size: .75rem; color: #059669; font-weight: 600; }

.section-title {
    font-size: 1.15rem; font-weight: 700; color: #1E3A8A;
    border-bottom: 2px solid #BFDBFE; padding-bottom: .4rem; margin: 1.2rem 0 .8rem;
}
.chip {
    display: inline-block; background: #EFF6FF; color: #1D4ED8;
    border: 1px solid #BFDBFE; border-radius: 9999px;
    font-size: .72rem; padding: .1rem .55rem; margin: .15rem;
}
.badge-green  { background:#D1FAE5; color:#065F46; border-color:#6EE7B7; }
.badge-orange { background:#FEF3C7; color:#92400E; border-color:#FCD34D; }
.badge-red    { background:#FEE2E2; color:#991B1B; border-color:#FCA5A5; }

.stProgress > div > div > div > div { background-color: #3B82F6 !important; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# 常量 & 预编译正则
# ─────────────────────────────────────────────────────────────────────────────
PERIOD_TIMES = {
    1:  ("08:00","08:50"),  2:  ("09:00","09:50"),
    3:  ("10:10","11:00"),  4:  ("11:10","12:00"),
    5:  ("13:30","14:20"),  6:  ("14:30","15:20"),
    7:  ("15:30","16:20"),  8:  ("16:30","17:20"),
    9:  ("17:30","18:20"),  10: ("18:30","19:20"),
    11: ("19:30","20:20"),  12: ("20:30","21:20"),
}
ALL_PERIODS  = list(range(1, 13))
TOTAL_WEEKS  = 17
DAY_MAP  = {"周一":0,"周二":1,"周三":2,"周四":3,"周五":4,"周六":5,"周日":6}
DAY_NAMES = ["周一","周二","周三","周四","周五","周六","周日"]

SLOT_RE = re.compile(r"(周[一二三四五六日])第([\d、，,\-–]+)节\{第([\d,，\-–\s]+)周\}")
PSPLIT  = re.compile(r"[、，,]")
PRANGE  = re.compile(r"(\d+)\s*[-–]\s*(\d+)")
WSPLIT  = re.compile(r"[,，]")
WRANGE  = re.compile(r"(\d+)\s*[-–]\s*(\d+)")

CACHE_DIR = Path(tempfile.gettempdir()) / "schedule_cache"
CACHE_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────
def file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def parse_periods(period_str: str) -> list:
    """解析节次字符串，支持逗号列表（7、8、9）和范围（7-9）两种格式"""
    periods = []
    for part in PSPLIT.split(period_str):
        part = part.strip()
        if not part:
            continue
        m = PRANGE.match(part)
        if m:
            periods.extend(range(int(m.group(1)), int(m.group(2)) + 1))
        else:
            try:
                periods.append(int(part))
            except ValueError:
                pass
    return [p for p in periods if 1 <= p <= 12]
    weeks: set = set()
    for part in WSPLIT.split(week_str):
        part = part.strip()
        if not part:
            continue
        m = WRANGE.match(part)
        if m:
            weeks.update(range(int(m.group(1)), int(m.group(2)) + 1))
        else:
            try:
                weeks.add(int(part))
            except ValueError:
                pass
    return weeks & set(range(1, TOTAL_WEEKS + 1))

# ─────────────────────────────────────────────────────────────────────────────
# 逐行解析 XLSX（openpyxl 流模式，低内存）
# ─────────────────────────────────────────────────────────────────────────────
def parse_xlsx(xlsx_path: str) -> Tuple[Optional[np.ndarray], List[str]]:
    """
    解析单个 XLSX。返回 (occ[TOTAL_WEEKS,7,12], issues)。
    使用 openpyxl read_only 模式逐行读取，避免全量加载。
    结果会缓存到磁盘（基于文件 MD5）。
    """
    # ── 磁盘缓存检查 ──
    cache_key = file_md5(xlsx_path)
    cache_path = CACHE_DIR / f"{cache_key}.pkl"
    if cache_path.exists():
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            cache_path.unlink(missing_ok=True)

    issues: List[str] = []
    occ = np.zeros((TOTAL_WEEKS, 7, 12), dtype=bool)
    col_idx: Optional[int] = None

    try:
        wb = load_workbook(xlsx_path, read_only=True, data_only=True)
        ws = wb.active

        rows_iter = ws.iter_rows(values_only=True)
        # 跳过第一行（可能是合并标题行）
        try:
            next(rows_iter)
        except StopIteration:
            wb.close()
            return None, ["文件为空"]

        # 第二行寻找"上课时间"列
        try:
            header_row = next(rows_iter)
        except StopIteration:
            wb.close()
            return None, ["表头行不足"]

        for i, cell in enumerate(header_row):
            if cell is not None and "上课时间" in str(cell):
                col_idx = i
                break

        if col_idx is None:
            wb.close()
            return None, ["未找到'上课时间'列"]

        unmatched = 0
        for row in rows_iter:
            if col_idx >= len(row):
                continue
            cell = row[col_idx]
            if cell is None:
                continue
            for part in str(cell).split(";"):
                part = part.strip()
                m = SLOT_RE.search(part)
                if not m:
                    if part and re.search(r'周[一二三四五六日]', part):
                        unmatched += 1
                    continue
                day_name = m.group(1)
                if day_name not in DAY_MAP:
                    continue
                day = DAY_MAP[day_name]
                periods = parse_periods(m.group(2))
                weeks = parse_weeks(m.group(3))
                for wk in weeks:
                    for p in periods:
                        if 1 <= p <= 12:
                            occ[wk - 1, day, p - 1] = True

        wb.close()

    except Exception as e:
        return None, [f"读取失败: {e}"]

    if unmatched:
        issues.append(f"有 {unmatched} 个时间片段格式不匹配")
    total_slots = int(occ.sum())
    if total_slots == 0:
        issues.append("课表节次全为零，可能格式异常")
    elif total_slots > 600:
        issues.append(f"节次总数偏多({total_slots})，请检查格式")

    result = (occ, issues)
    try:
        with open(cache_path, "wb") as f:
            pickle.dump(result, f)
    except Exception:
        pass

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 解析 .xls（旧版 Excel，需要 xlrd >= 2.0.1）
# ─────────────────────────────────────────────────────────────────────────────
def parse_xls(xls_path: str) -> Tuple[Optional[np.ndarray], List[str]]:
    """
    解析单个 .xls 文件，逻辑与 parse_xlsx 完全一致。
    需要安装 xlrd：pip install xlrd>=2.0.1
    """
    if not _XLRD_OK:
        return None, ["缺少依赖：请运行 `pip install xlrd>=2.0.1` 后重试"]

    # ── 磁盘缓存检查 ──
    cache_key = file_md5(xls_path)
    cache_path = CACHE_DIR / f"{cache_key}.pkl"
    if cache_path.exists():
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            cache_path.unlink(missing_ok=True)

    issues: List[str] = []
    occ = np.zeros((TOTAL_WEEKS, 7, 12), dtype=bool)
    col_idx: Optional[int] = None

    try:
        wb = xlrd.open_workbook(xls_path)
        ws = wb.sheet_by_index(0)

        if ws.nrows < 2:
            return None, ["文件为空"]

        # 跳过第一行（合并标题），第二行找"上课时间"列
        header_row = [str(ws.cell_value(1, c)) for c in range(ws.ncols)]
        for i, cell in enumerate(header_row):
            if "上课时间" in cell:
                col_idx = i
                break

        if col_idx is None:
            return None, ["未找到'上课时间'列"]

        unmatched = 0
        for r in range(2, ws.nrows):
            cell_val = ws.cell_value(r, col_idx)
            if not cell_val:
                continue
            for part in str(cell_val).split(";"):
                part = part.strip()
                m = SLOT_RE.search(part)
                if not m:
                    if part and re.search(r'周[一二三四五六日]', part):
                        unmatched += 1
                    continue
                day_name = m.group(1)
                if day_name not in DAY_MAP:
                    continue
                day = DAY_MAP[day_name]
                periods = parse_periods(m.group(2))
                weeks = parse_weeks(m.group(3))
                for wk in weeks:
                    for p in periods:
                        if 1 <= p <= 12:
                            occ[wk - 1, day, p - 1] = True

    except Exception as e:
        return None, [f"读取失败: {e}"]

    if unmatched:
        issues.append(f"有 {unmatched} 个时间片段格式不匹配")
    total_slots = int(occ.sum())
    if total_slots == 0:
        issues.append("课表节次全为零，可能格式异常")
    elif total_slots > 600:
        issues.append(f"节次总数偏多({total_slots})，请检查格式")

    result = (occ, issues)
    try:
        with open(cache_path, "wb") as f:
            pickle.dump(result, f)
    except Exception:
        pass

    return result


def parse_file(file_path: str) -> Tuple[Optional[np.ndarray], List[str]]:
    """根据扩展名自动分发到对应解析器"""
    if file_path.lower().endswith(".xls"):
        return parse_xls(file_path)
    return parse_xlsx(file_path)


# ─────────────────────────────────────────────────────────────────────────────
# 异步并行解析（asyncio + ThreadPoolExecutor）
# ─────────────────────────────────────────────────────────────────────────────
async def _parse_one_async(executor, name: str, path: str):
    loop = asyncio.get_event_loop()
    occ, issues = await loop.run_in_executor(executor, parse_file, path)
    return name, occ, issues


async def parse_all_async(paths: List[Tuple[str, str]], max_workers: int = 4,
                          progress_cb=None) -> Tuple[Dict, Dict]:
    """并行解析所有文件，返回 (all_occ, all_issues)"""
    all_occ: Dict[str, np.ndarray] = {}
    all_issues: Dict[str, List[str]] = {}
    done = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = [_parse_one_async(executor, name, path) for name, path in paths]
        for coro in asyncio.as_completed(tasks):
            name, occ, issues = await coro
            done += 1
            if progress_cb:
                progress_cb(done / len(paths), f"已解析 {done}/{len(paths)}: {name}")
            if occ is not None:
                all_occ[name] = occ
            if issues:
                all_issues[name] = issues

    return all_occ, all_issues


def run_parse(paths, max_workers, progress_cb=None):
    """同步入口（Streamlit 调用）"""
    return asyncio.run(parse_all_async(paths, max_workers, progress_cb))


# ─────────────────────────────────────────────────────────────────────────────
# 稀疏矩阵 occ_sum 聚合
# ─────────────────────────────────────────────────────────────────────────────
def build_free4d(all_occ: Dict[str, np.ndarray]) -> Tuple[np.ndarray, int]:
    """
    返回 free4d[TOTAL_WEEKS, 7, 12] 和人数 n。
    内部使用 scipy COO 稀疏矩阵累加占用，节省内存。
    """
    n = len(all_occ)
    shape_flat = TOTAL_WEEKS * 7 * 12
    row_list, col_list, data_list = [], [], []

    for idx, occ in enumerate(all_occ.values()):
        coords = np.argwhere(occ)           # (K, 3)
        flat   = coords[:, 0] * 84 + coords[:, 1] * 12 + coords[:, 2]
        row_list.append(flat)
        col_list.append(np.full(len(flat), idx, dtype=np.int32))
        data_list.append(np.ones(len(flat), dtype=np.int8))

    if row_list:
        rows = np.concatenate(row_list)
        cols = np.concatenate(col_list)
        data = np.concatenate(data_list)
        sp = sparse.coo_matrix((data, (rows, cols)), shape=(shape_flat, n))
        occ_sum = np.array(sp.sum(axis=1)).reshape(TOTAL_WEEKS, 7, 12)
    else:
        occ_sum = np.zeros((TOTAL_WEEKS, 7, 12), dtype=int)

    free4d = n - occ_sum
    return free4d.astype(int), n


# ─────────────────────────────────────────────────────────────────────────────
# ── 可视化函数（Plotly，中文标签）────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
_COLORSCALE_MAIN = "YlGnBu"

def _layout_defaults(fig, title="", h=500):
    fig.update_layout(
        title=dict(text=title, font=dict(size=16, color="#1E3A8A", family="Inter, sans-serif")),
        height=h,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#374151"),
        margin=dict(t=60, b=40, l=40, r=30),
    )
    return fig


def chart_heatmap(free4d: np.ndarray, n: int, week: Optional[int] = None):
    """周度 / 均值热力图"""
    if week is not None:
        z = free4d[week]
        title = f"第{week+1}周 空余人数热力图（共{n}人）"
    else:
        z = free4d.mean(axis=0)
        title = f"17周平均空余人数热力图（共{n}人）"

    fig = go.Figure(go.Heatmap(
        z=z.T, x=DAY_NAMES,
        y=[f"第{p}节 {PERIOD_TIMES[p][0]}" for p in ALL_PERIODS],
        colorscale=_COLORSCALE_MAIN, zmin=0, zmax=n,
        text=np.round(z.T, 1), texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="<b>%{x}</b><br>%{y}<br>空余: %{z:.1f} 人<extra></extra>",
    ))
    fig.update_yaxes(autorange="reversed", title="节次")
    fig.update_xaxes(title="星期")
    return _layout_defaults(fig, title, h=580)


def chart_weekly_trend(free4d: np.ndarray, n: int):
    """各周空闲率趋势（柱状 + 折线）"""
    max_pos = n * 7 * 12
    pct = free4d.sum(axis=(1, 2)) / max_pos * 100
    weeks = list(range(1, TOTAL_WEEKS + 1))
    colors = ["#10B981" if p >= 60 else "#F59E0B" if p >= 40 else "#EF4444" for p in pct]

    fig = make_subplots(specs=[[{"secondary_y": False}]])
    fig.add_trace(go.Bar(
        x=weeks, y=pct, marker_color=colors, name="空闲率",
        text=[f"{p:.1f}%" for p in pct], textposition="outside",
        hovertemplate="第%{x}周<br>空闲率: %{y:.1f}%<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=weeks, y=pct, mode="lines+markers", name="趋势线",
        line=dict(color="#1D4ED8", width=2.5),
        marker=dict(size=7, color="#1D4ED8"),
    ))
    fig.add_hrect(y0=60, y1=105, fillcolor="#D1FAE5", opacity=0.2, line_width=0)
    fig.add_hrect(y0=40, y1=60,  fillcolor="#FEF3C7", opacity=0.2, line_width=0)
    fig.add_hrect(y0=0,  y1=40,  fillcolor="#FEE2E2", opacity=0.2, line_width=0)
    fig.add_hline(y=60, line_dash="dash", line_color="#059669",
                  annotation_text="优 60%", annotation_position="bottom right")
    fig.add_hline(y=40, line_dash="dash", line_color="#D97706",
                  annotation_text="良 40%", annotation_position="bottom right")
    fig.update_xaxes(
        tickmode="array", tickvals=weeks, ticktext=[f"第{w}周" for w in weeks], title="周次"
    )
    fig.update_yaxes(range=[0, 110], title="空闲率 (%)")
    return _layout_defaults(fig, f"各周整体空闲率趋势（共{n}人）", h=480)


def chart_radar(free4d: np.ndarray, week: int):
    """雷达图：各天空闲节次"""
    daily = free4d[week].sum(axis=1)
    max_p = 12
    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=list(daily) + [daily[0]],
        theta=DAY_NAMES + [DAY_NAMES[0]],
        fill="toself", name=f"第{week+1}周",
        line=dict(color="#2563EB", width=2.5),
        fillcolor="rgba(37,99,235,.25)",
    ))
    fig.add_trace(go.Scatterpolar(
        r=[max_p] * 8, theta=DAY_NAMES + [DAY_NAMES[0]],
        mode="lines", line=dict(color="#9CA3AF", dash="dot"),
        name="最大可能", showlegend=True,
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(range=[0, max_p], tickfont=dict(size=9))),
        showlegend=True, height=440,
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        title=dict(text=f"第{week+1}周 各天空闲节次分布", font=dict(size=15, color="#1E3A8A")),
        margin=dict(t=60, b=20, l=40, r=40),
    )
    return fig


def chart_fully_free_heatmap(free4d: np.ndarray, n: int):
    """全员空闲周数分布热力图"""
    fw = np.array([[int((free4d[:, d, p] == n).sum()) for p in range(12)] for d in range(7)])
    fig = go.Figure(go.Heatmap(
        z=fw.T, x=DAY_NAMES,
        y=[f"第{p}节 {PERIOD_TIMES[p][0]}" for p in ALL_PERIODS],
        colorscale="Reds", zmin=0, zmax=TOTAL_WEEKS,
        text=fw.T, texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="<b>%{x}</b><br>%{y}<br>全员空闲: %{z} 周<extra></extra>",
    ))
    fig.update_yaxes(autorange="reversed", title="节次")
    fig.update_xaxes(title="星期")
    return _layout_defaults(fig, f"各时段全员空闲周数（共{TOTAL_WEEKS}周）", h=580)


def chart_3d_surface(free4d: np.ndarray, n: int, week: Optional[int] = None):
    """3D 曲面图"""
    Z = free4d[week].T if week is not None else free4d.mean(axis=0).T
    title = f"第{week+1}周 3D空闲分布" if week is not None else "17周平均 3D空闲分布"
    fig = go.Figure(go.Surface(
        z=Z, x=DAY_NAMES, y=[f"第{p}节" for p in ALL_PERIODS],
        colorscale="Viridis",
        hovertemplate="星期: %{x}<br>节次: %{y}<br>空闲: %{z:.1f}人<extra></extra>",
    ))
    fig.update_layout(
        scene=dict(
            xaxis_title="星期", yaxis_title="节次", zaxis_title="空闲人数",
            camera=dict(eye=dict(x=1.5, y=1.5, z=1.5)),
            bgcolor="rgba(0,0,0,0)",
        ),
        height=600, paper_bgcolor="rgba(0,0,0,0)",
        title=dict(text=title, font=dict(size=15, color="#1E3A8A")),
        margin=dict(t=60, b=20),
    )
    return fig


def chart_bubble(free4d: np.ndarray, n: int):
    """气泡图：每个时间格的空闲率（横轴=星期，纵轴=节次，气泡大小=空闲率）"""
    avg = free4d.mean(axis=0)  # (7,12)
    rows = []
    for d in range(7):
        for p in range(12):
            pct = avg[d, p] / n * 100
            rows.append(dict(
                星期=DAY_NAMES[d],
                节次=f"第{p+1}节",
                节次序号=p+1,
                空闲率=pct,
                空闲人数=avg[d, p],
                颜色=("#10B981" if pct >= 70 else "#F59E0B" if pct >= 40 else "#EF4444"),
            ))
    df = pd.DataFrame(rows)
    fig = px.scatter(
        df, x="星期", y="节次序号", size="空闲率",
        color="空闲率", color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        hover_data={"星期": True, "节次": True, "空闲率": ":.1f", "空闲人数": ":.1f", "节次序号": False},
        labels={"节次序号": "节次", "空闲率": "空闲率(%)"},
        size_max=30,
        category_orders={"星期": DAY_NAMES},
    )
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(1, 13)),
        ticktext=[f"第{p}节" for p in range(1, 13)],
        autorange="reversed", title="节次",
    )
    fig.update_xaxes(title="星期")
    return _layout_defaults(fig, "时间格空闲率气泡图（气泡越大=越空闲）", h=560)


def chart_person_busyload(all_occ: Dict[str, np.ndarray]):
    """各人有课总节次分布（水平柱状图 + 箱线图）"""
    data = sorted([(name, int(occ.sum())) for name, occ in all_occ.items()], key=lambda x: x[1])
    names, vals = zip(*data) if data else ([], [])

    fig = make_subplots(
        rows=1, cols=2,
        column_widths=[0.68, 0.32],
        subplot_titles=("各人有课总节次排名", "节次分布"),
    )

    colors = ["#EF4444" if v > np.percentile(vals, 80)
              else "#F59E0B" if v > np.percentile(vals, 50)
              else "#10B981" for v in vals]

    fig.add_trace(go.Bar(
        y=list(names), x=list(vals), orientation="h",
        marker_color=colors, name="有课节次",
        hovertemplate="%{y}: %{x} 节<extra></extra>",
    ), row=1, col=1)

    fig.add_trace(go.Box(
        y=list(vals), name="分布",
        marker_color="#3B82F6", boxmean="sd",
        hovertemplate="节次: %{y}<extra></extra>",
    ), row=1, col=2)

    fig.update_yaxes(title="姓名/班级", row=1, col=1)
    fig.update_xaxes(title="有课节次", row=1, col=1)
    fig.update_yaxes(title="节次数", row=1, col=2)

    h = max(400, len(names) * 22 + 100)
    return _layout_defaults(fig, "各人课业负担分析", h=h)


def chart_grouped_bar(free4d: np.ndarray, n: int):
    """分组柱状图：不同节次段（上午/下午/晚上）各天平均空闲率"""
    avg = free4d.mean(axis=0)  # (7,12)
    groups = {"上午(1-4节)": list(range(0,4)), "下午(5-9节)": list(range(4,9)), "晚上(10-12节)": list(range(9,12))}

    fig = go.Figure()
    palette = ["#3B82F6", "#10B981", "#F59E0B"]
    for (label, idxs), color in zip(groups.items(), palette):
        pct = [avg[d, idxs].mean() / n * 100 for d in range(7)]
        fig.add_trace(go.Bar(
            name=label, x=DAY_NAMES, y=pct,
            marker_color=color,
            text=[f"{v:.0f}%" for v in pct], textposition="outside",
            hovertemplate=f"<b>{label}</b><br>%{{x}}: %{{y:.1f}}%<extra></extra>",
        ))
    fig.update_layout(
        barmode="group", yaxis=dict(range=[0, 115], title="平均空闲率 (%)"),
        xaxis_title="星期",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return _layout_defaults(fig, "各天各时段平均空闲率（分组）", h=460)


# ─────────────────────────────────────────────────────────────────────────────
# Matplotlib 图表（用于 PNG/PDF 导出，中文字体已配置）
# ─────────────────────────────────────────────────────────────────────────────
def mpl_heatmap_bytes(free4d: np.ndarray, n: int, week: Optional[int] = None,
                      fmt: str = "png") -> bytes:
    """生成 matplotlib 热力图并返回字节流（PNG 或 PDF）"""
    z = free4d[week].T if week is not None else free4d.mean(axis=0).T
    title = (f"第{week+1}周 空余热力图" if week is not None else "17周平均空余热力图")

    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(z, cmap="YlGnBu", vmin=0, vmax=n, aspect="auto")
    plt.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="空余人数")
    ax.set_xticks(range(7));  ax.set_xticklabels(DAY_NAMES, fontsize=11)
    ax.set_yticks(range(12)); ax.set_yticklabels([f"第{p}节\n{PERIOD_TIMES[p][0]}" for p in ALL_PERIODS], fontsize=8)
    ax.set_title(f"{title}（共{n}人）", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("星期"); ax.set_ylabel("节次")
    for (r, c), val in np.ndenumerate(z):
        ax.text(c, r, f"{val:.0f}", ha="center", va="center", fontsize=9,
                color="white" if val < n*0.55 else "#1a1a1a")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format=fmt, dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.read()


def mpl_trend_bytes(free4d: np.ndarray, n: int, fmt: str = "png") -> bytes:
    max_pos = n * 7 * 12
    pct = free4d.sum(axis=(1, 2)) / max_pos * 100
    weeks = list(range(1, TOTAL_WEEKS + 1))
    colors = ["#10B981" if p >= 60 else "#F59E0B" if p >= 40 else "#EF4444" for p in pct]

    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(weeks, pct, color=colors, alpha=0.85, zorder=2)
    ax.plot(weeks, pct, "o-", color="#1D4ED8", linewidth=2, markersize=6, zorder=3)
    ax.axhline(60, ls="--", color="#059669", lw=1.2, label="60% 基准")
    ax.axhline(40, ls="--", color="#D97706", lw=1.2, label="40% 基准")
    ax.set_xticks(weeks); ax.set_xticklabels([f"第{w}周" for w in weeks], rotation=45, fontsize=8)
    ax.set_ylabel("空闲率 (%)"); ax.set_ylim(0, 110)
    ax.set_title(f"各周整体空闲率趋势（共{n}人）", fontsize=13, fontweight="bold")
    ax.grid(axis="y", ls=":", alpha=0.5, zorder=1)
    ax.legend(fontsize=9)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format=fmt, dpi=150, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# Excel 多 Sheet 导出
# ─────────────────────────────────────────────────────────────────────────────
def build_excel_export(free4d: np.ndarray, n: int,
                       all_occ: Dict[str, np.ndarray]) -> bytes:
    """生成含多个 Sheet 的 Excel 文件"""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        wb  = writer.book
        hdr = wb.add_format({"bold": True, "bg_color": "#1E3A8A",
                              "font_color": "#FFFFFF", "border": 1, "align": "center"})
        num = wb.add_format({"border": 1, "align": "center", "num_format": "0.0"})
        pct_fmt = wb.add_format({"border": 1, "align": "center", "num_format": "0.0%"})
        green = wb.add_format({"border":1,"align":"center","bg_color":"#D1FAE5","num_format":"0"})
        red   = wb.add_format({"border":1,"align":"center","bg_color":"#FEE2E2","num_format":"0"})

        # ── Sheet 1: 17周平均空闲率 ──
        avg = free4d.mean(axis=0)
        df1 = pd.DataFrame(avg, index=DAY_NAMES,
                           columns=[f"第{p}节\n{PERIOD_TIMES[p][0]}" for p in ALL_PERIODS])
        df1.to_excel(writer, sheet_name="17周平均空闲人数", index=True)
        ws1 = writer.sheets["17周平均空闲人数"]
        ws1.set_column("A:A", 8)
        ws1.set_column("B:M", 10)

        # ── Sheet 2: 各周空闲率 ──
        max_pos = n * 7 * 12
        rows2 = []
        for w in range(TOTAL_WEEKS):
            free_sum = int(free4d[w].sum())
            rows2.append({
                "周次": f"第{w+1}周",
                "空闲节次总数": free_sum,
                "空闲率": free_sum / max_pos,
                "全员空闲时段数": int((free4d[w] == n).sum()),
            })
        pd.DataFrame(rows2).to_excel(writer, sheet_name="各周空闲率", index=False)

        # ── Sheet 3: 全员空闲明细 ──
        rows3 = []
        for w in range(TOTAL_WEEKS):
            for d in range(7):
                for p in range(12):
                    if free4d[w, d, p] == n:
                        rows3.append({
                            "周次": f"第{w+1}周", "星期": DAY_NAMES[d],
                            "节次": p+1,
                            "时间段": f"{PERIOD_TIMES[p+1][0]}–{PERIOD_TIMES[p+1][1]}",
                        })
        pd.DataFrame(rows3).to_excel(writer, sheet_name="全员空闲明细", index=False)

        # ── Sheet 4: 各人课业负担 ──
        rows4 = []
        for name, occ in all_occ.items():
            total_busy = int(occ.sum())
            rows4.append({
                "姓名/班级": name,
                "有课总节次": total_busy,
                "空闲总节次": TOTAL_WEEKS * 7 * 12 - total_busy,
                "空闲率": 1 - total_busy / (TOTAL_WEEKS * 7 * 12),
            })
        rows4.sort(key=lambda r: r["有课总节次"])
        pd.DataFrame(rows4).to_excel(writer, sheet_name="各人课业负担", index=False)

        # ── Sheet 5: 每周每天每节次详情 ──
        rows5 = []
        for w in range(TOTAL_WEEKS):
            for d in range(7):
                for p in range(12):
                    rows5.append({
                        "周次": w+1, "星期": DAY_NAMES[d], "节次": p+1,
                        "时间段": f"{PERIOD_TIMES[p+1][0]}–{PERIOD_TIMES[p+1][1]}",
                        "空余人数": int(free4d[w, d, p]),
                        "空余率": int(free4d[w, d, p]) / n if n > 0 else 0,
                        "全员空闲": "✓" if free4d[w, d, p] == n else "",
                    })
        pd.DataFrame(rows5).to_excel(writer, sheet_name="全量明细", index=False)

    buf.seek(0)
    return buf.read()


def build_csv_zip(free4d: np.ndarray, n: int,
                  all_occ: Dict[str, np.ndarray]) -> bytes:
    """打包多个 CSV 文件为 ZIP"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # 17周平均
        avg = free4d.mean(axis=0)
        df = pd.DataFrame(avg, index=DAY_NAMES,
                          columns=[f"第{p}节" for p in ALL_PERIODS])
        zf.writestr("17周平均空闲人数.csv", df.to_csv(encoding="utf-8-sig"))

        # 全量明细
        rows = []
        for w in range(TOTAL_WEEKS):
            for d in range(7):
                for p in range(12):
                    rows.append({
                        "周次": w+1, "星期": DAY_NAMES[d], "节次": p+1,
                        "空余人数": int(free4d[w,d,p]),
                        "空余率%": round(free4d[w,d,p]/n*100, 1) if n else 0,
                        "全员空闲": free4d[w,d,p]==n,
                    })
        zf.writestr("全量明细.csv", pd.DataFrame(rows).to_csv(index=False, encoding="utf-8-sig"))

        # 各人负担
        rows2 = [{"姓名": k, "有课节次": int(v.sum())} for k, v in all_occ.items()]
        zf.writestr("各人课业负担.csv", pd.DataFrame(rows2).to_csv(index=False, encoding="utf-8-sig"))

    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# 报告生成
# ─────────────────────────────────────────────────────────────────────────────
def generate_report(free4d: np.ndarray, n: int,
                    all_occ: Dict[str, np.ndarray],
                    issues: Dict[str, List[str]]) -> str:
    lines = [
        "# 📅 班级课表空余时间统计报告",
        "",
        f"| 项目 | 数值 |",
        f"|------|------|",
        f"| 参与人数 | **{n} 人** |",
        f"| 统计周次 | 第 1–{TOTAL_WEEKS} 周 |",
        f"| 时间范围 | 08:00–21:20（含午休段标记） |",
        "",
    ]
    if issues:
        lines += ["## ⚠️ 数据问题", ""]
        for nm, iss_list in issues.items():
            for iss in iss_list:
                lines.append(f"- **{nm}**: {iss}")
        lines.append("")

    fw = np.array([[int((free4d[:, d, p] == n).sum()) for p in range(12)] for d in range(7)])
    cands = sorted([(fw[d, p], d, p+1) for d in range(7) for p in range(12)], reverse=True)
    lines += ["## 🏆 全员空闲时段 TOP 10", ""]
    lines.append("| 排名 | 时段 | 时间 | 全员空闲周数 |")
    lines.append("|------|------|------|------------|")
    for rank, (wks, d, p) in enumerate(cands[:10], 1):
        t = PERIOD_TIMES[p]
        lines.append(f"| {rank} | {DAY_NAMES[d]} 第{p}节 | {t[0]}–{t[1]} | **{wks}/{TOTAL_WEEKS}** 周 |")
    lines.append("")

    max_pos = n * 7 * 12
    lines += ["## 📈 各周空闲率", ""]
    lines.append("| 周次 | 空闲率 | 进度 |")
    lines.append("|------|--------|------|")
    for w in range(TOTAL_WEEKS):
        pct = free4d[w].sum() / max_pos * 100
        bar_len = int(pct / 5)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        lines.append(f"| 第{w+1}周 | {pct:.1f}% | `{bar}` |")
    lines.append("")

    lines += ["## 🕐 各周全员共同空闲时段", ""]
    for w in range(TOTAL_WEEKS):
        slots = [f"{DAY_NAMES[d]}第{p+1}节"
                 for d in range(7) for p in range(12) if free4d[w, d, p] == n]
        if slots:
            show = "、".join(slots[:6]) + (f" …共{len(slots)}个" if len(slots) > 6 else "")
            lines.append(f"- 第{w+1}周：{show}")
        else:
            lines.append(f"- 第{w+1}周：*无全员共同空闲时段*")
    lines.append("")

    rows_p = sorted([(nm, int(occ.sum())) for nm, occ in all_occ.items()], key=lambda x: x[1])
    lines += ["## 👥 各人有课节次（升序）", ""]
    lines.append("| 姓名/班级 | 有课节次 |")
    lines.append("|-----------|---------|")
    for nm, busy in rows_p:
        lines.append(f"| {nm} | **{busy}** |")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 输入处理：ZIP 解压 + 文件夹（多文件直传）
# ─────────────────────────────────────────────────────────────────────────────
def collect_xlsx_from_zip(uploaded_zip) -> Tuple[str, List[Tuple[str, str]]]:
    """解压 ZIP，返回 (temp_dir, [(name, path)])"""
    tmp = tempfile.mkdtemp()
    zip_tmp = os.path.join(tmp, "upload.zip")
    with open(zip_tmp, "wb") as f:
        f.write(uploaded_zip.getvalue())
    with zipfile.ZipFile(zip_tmp, "r") as zf:
        for member in zf.infolist():
            try:
                member.filename = member.filename.encode("cp437").decode("gbk")
            except Exception:
                pass
            try:
                zf.extract(member, tmp)
            except Exception:
                pass
    paths = [(p.stem, str(p))
             for p in Path(tmp).rglob("*")
             if p.suffix.lower() in (".xlsx", ".xls") and not p.name.startswith("~$")]
    return tmp, paths


def collect_xlsx_from_files(uploaded_files) -> Tuple[str, List[Tuple[str, str]]]:
    """将多上传文件保存到临时目录，返回 (temp_dir, [(name, path)])"""
    tmp = tempfile.mkdtemp()
    paths = []
    for uf in uploaded_files:
        dest = os.path.join(tmp, uf.name)
        with open(dest, "wb") as f:
            f.write(uf.getvalue())
        if Path(uf.name).suffix.lower() in (".xlsx", ".xls") and not uf.name.startswith("~$"):
            paths.append((Path(uf.name).stem, dest))
    return tmp, paths


# ─────────────────────────────────────────────────────────────────────────────
# 主程序
# ─────────────────────────────────────────────────────────────────────────────
def main():
    st.markdown('<p class="main-header">📅 班级课表空余时间统计分析</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">异步解析 · 磁盘缓存 · 懒加载图表 · 多格式导出</p>', unsafe_allow_html=True)

    # ── 侧边栏 ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 📂 数据上传")
        upload_mode = st.radio(
            "上传方式",
            ["ZIP 压缩包", "多文件（文件夹/批量）"],
            horizontal=True,
        )

        uploaded_zip   = None
        uploaded_files = None

        if upload_mode == "ZIP 压缩包":
            uploaded_zip = st.file_uploader(
                "上传 ZIP 文件（内含课表 XLSX / XLS）",
                type=["zip"],
                help="可将整个文件夹压缩为 ZIP 后上传，支持中文路径",
            )
        else:
            uploaded_files = st.file_uploader(
                "批量选择课表文件（XLSX / XLS，可多选）",
                type=["xlsx", "xls"],
                accept_multiple_files=True,
                help="可框选整个文件夹中的 xlsx / xls 文件后一次性上传",
            )

        st.markdown("---")
        st.markdown("## ⚙️ 解析设置")
        max_workers = st.slider("并行线程数", 1, 8, 4, help="增加可提升速度，但占用更多内存")
        show_issues = st.checkbox("显示解析问题", value=True)

        st.markdown("---")
        st.markdown("## ℹ️ 使用说明")
        with st.expander("查看说明"):
            st.markdown("""
**上传方式**
- **ZIP**：将课表文件夹压缩为 `.zip` 上传，支持子目录
- **多文件**：直接拖入多个 `.xlsx` 或 `.xls` 文件，或整个文件夹

> ⚠️ **读取 .xls 需安装 xlrd**
> ```
> pip install xlrd>=2.0.1
> ```

**支持的时间格式**
```
周一第3、4节{第1-17周}
周三第5节{第2,4,6周}
```

**导出格式**
- 📄 Markdown 报告
- 📊 Excel（多 Sheet）
- 📦 CSV 打包 ZIP
- 🖼️ 热力图 PNG
- 📑 趋势图 PNG
            """)

    # ── 主内容 ───────────────────────────────────────────────────────────────
    has_input = (uploaded_zip is not None) or (uploaded_files and len(uploaded_files) > 0)

    if not has_input:
        st.markdown("""
        <div style="text-align:center;padding:4rem 1rem;color:#9CA3AF;">
            <div style="font-size:3rem;margin-bottom:1rem;">📂</div>
            <div style="font-size:1.2rem;font-weight:600;color:#4B5563;">从左侧上传课表文件</div>
            <div style="margin-top:.6rem;font-size:.9rem;">
                支持 ZIP 压缩包 或 直接拖入多个 XLSX / XLS 文件
            </div>
        </div>
        """, unsafe_allow_html=True)
        return

    # ── 解压 / 收集文件 ──────────────────────────────────────────────────────
    temp_dir = None
    try:
        with st.spinner("📂 收集文件中…"):
            if uploaded_zip is not None:
                temp_dir, all_paths = collect_xlsx_from_zip(uploaded_zip)
            else:
                temp_dir, all_paths = collect_xlsx_from_files(uploaded_files)

        if not all_paths:
            st.error("未找到任何有效的 .xlsx / .xls 课表文件，请检查上传内容。")
            return

        c1, c2, c3 = st.columns(3)
        c1.metric("📄 发现课表文件", len(all_paths))
        c2.metric("⚡ 解析线程", max_workers)
        c3.metric("🗄️ 缓存目录", str(CACHE_DIR)[-30:])

        # ── 异步并行解析 ─────────────────────────────────────────────────────
        prog_bar  = st.progress(0)
        prog_text = st.empty()

        def _cb(ratio, msg):
            prog_bar.progress(ratio)
            prog_text.text(msg)

        t0 = time.time()
        all_occ, all_issues = run_parse(all_paths, max_workers, _cb)
        elapsed = time.time() - t0

        prog_bar.empty()
        prog_text.empty()
        st.success(f"✅ 成功解析 **{len(all_occ)}** 份课表，耗时 {elapsed:.2f}s")

        if not all_occ:
            st.error("没有成功解析的课表，请检查文件格式。")
            return

        if show_issues and all_issues:
            with st.expander(f"⚠️ 解析问题（{len(all_issues)} 份）"):
                for nm, iss_list in all_issues.items():
                    for iss in iss_list:
                        st.warning(f"**{nm}**: {iss}")

        # ── 计算 free4d（稀疏聚合） ──────────────────────────────────────────
        free4d, n = build_free4d(all_occ)

        # ── KPI 概览 ─────────────────────────────────────────────────────────
        st.markdown('<p class="section-title">📊 数据概览</p>', unsafe_allow_html=True)
        k1, k2, k3, k4 = st.columns(4)
        total_slots = int(sum(occ.sum() for occ in all_occ.values()))
        free_rate   = (1 - total_slots / (n * TOTAL_WEEKS * 7 * 12)) * 100
        fully_free  = int((free4d == n).sum())
        avg_free_pw = free4d.mean(axis=(1,2)).mean()

        for col, (val, label, delta) in zip(
            [k1, k2, k3, k4],
            [
                (n,                "成功解析人数",   ""),
                (f"{free_rate:.1f}%", "总体空闲率",   ("🟢 较高" if free_rate>60 else "🟡 中等" if free_rate>40 else "🔴 偏低")),
                (fully_free,       "全员空闲节次总数", ""),
                (f"{avg_free_pw:.1f}", "周均空闲人次",  ""),
            ],
        ):
            col.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{val}</div>
                <div class="kpi-label">{label}</div>
                <div class="kpi-delta">{delta}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("")

        # ── 标签页（懒加载：仅在首次切换到该 Tab 时计算） ────────────────────
        if "rendered_tabs" not in st.session_state:
            st.session_state.rendered_tabs = set()

        tab_labels = [
            "📈 空闲率趋势",
            "🗓️ 周度热力图",
            "🎯 全员空闲",
            "💠 气泡图",
            "📊 负担分析",
            "📐 分组柱状",
            "🧊 3D 曲面",
            "📑 报告 & 导出",
        ]
        tabs = st.tabs(tab_labels)

        # Tab 0: 空闲率趋势
        with tabs[0]:
            st.plotly_chart(chart_weekly_trend(free4d, n), width="stretch")
            sel_w = st.selectbox("选择周次（雷达图）", range(1, TOTAL_WEEKS+1),
                                 format_func=lambda x: f"第{x}周", key="radar_week")
            st.plotly_chart(chart_radar(free4d, sel_w - 1), width="stretch")

        # Tab 1: 周度热力图
        with tabs[1]:
            opts = ["17周平均值"] + [f"第{w}周" for w in range(1, TOTAL_WEEKS+1)]
            sel = st.selectbox("查看周次", opts, key="heatmap_week")
            wk  = None if sel == "17周平均值" else int(sel[1:-1]) - 1
            st.plotly_chart(chart_heatmap(free4d, n, wk), width="stretch")

            if wk is not None:
                st.markdown(f"#### 第{wk+1}周 空闲时段明细")
                rows = []
                for d in range(7):
                    for p in range(12):
                        v = int(free4d[wk, d, p])
                        if v > 0:
                            rows.append({"星期": DAY_NAMES[d], "节次": p+1,
                                         "时间": f"{PERIOD_TIMES[p+1][0]}–{PERIOD_TIMES[p+1][1]}",
                                         "空余人数": v, "空余率": f"{v/n*100:.1f}%"})
                if rows:
                    st.dataframe(pd.DataFrame(rows), width="stretch")

        # Tab 2: 全员空闲
        with tabs[2]:
            st.plotly_chart(chart_fully_free_heatmap(free4d, n), width="stretch")
            rows = [{"周次": f"第{w+1}周", "星期": DAY_NAMES[d], "节次": p+1,
                     "时间": f"{PERIOD_TIMES[p+1][0]}–{PERIOD_TIMES[p+1][1]}"}
                    for w in range(TOTAL_WEEKS) for d in range(7) for p in range(12)
                    if free4d[w, d, p] == n]
            if rows:
                st.dataframe(pd.DataFrame(rows), width="stretch")
            else:
                st.info("无全员共同空闲时段")

        # Tab 3: 气泡图
        with tabs[3]:
            if "bubble" not in st.session_state.rendered_tabs:
                st.session_state.rendered_tabs.add("bubble")
            st.plotly_chart(chart_bubble(free4d, n), width="stretch")

        # Tab 4: 负担分析
        with tabs[4]:
            if "burden" not in st.session_state.rendered_tabs:
                st.session_state.rendered_tabs.add("burden")
            st.plotly_chart(chart_person_busyload(all_occ), width="stretch")

        # Tab 5: 分组柱状
        with tabs[5]:
            if "grouped" not in st.session_state.rendered_tabs:
                st.session_state.rendered_tabs.add("grouped")
            st.plotly_chart(chart_grouped_bar(free4d, n), width="stretch")

        # Tab 6: 3D 曲面
        with tabs[6]:
            week_options = ["17周平均"] + [f"第{w}周" for w in range(1, TOTAL_WEEKS + 1)]
            w3d_label = st.select_slider("选择周次（或查看平均）", options=week_options)
            wk3d = None if w3d_label == "17周平均" else int(w3d_label[1:-1]) - 1
            st.plotly_chart(chart_3d_surface(free4d, n, wk3d), width="stretch")

        # Tab 7: 报告 & 导出
        with tabs[7]:
            report_md = generate_report(free4d, n, all_occ, all_issues)
            st.markdown(report_md)
            st.markdown("---")
            st.markdown("### 📥 导出")

            col_a, col_b, col_c, col_d, col_e = st.columns(5)

            with col_a:
                st.download_button(
                    "📄 Markdown 报告",
                    data=report_md.encode("utf-8"),
                    file_name="课表分析报告.md",
                    mime="text/markdown",
                    width="stretch",
                )

            with col_b:
                xlsx_bytes = build_excel_export(free4d, n, all_occ)
                st.download_button(
                    "📊 Excel 多 Sheet",
                    data=xlsx_bytes,
                    file_name="课表分析数据.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width="stretch",
                )

            with col_c:
                csv_zip = build_csv_zip(free4d, n, all_occ)
                st.download_button(
                    "📦 CSV 数据包",
                    data=csv_zip,
                    file_name="课表分析CSV.zip",
                    mime="application/zip",
                    width="stretch",
                )

            with col_d:
                png_bytes = mpl_heatmap_bytes(free4d, n)
                st.download_button(
                    "🖼️ 热力图 PNG",
                    data=png_bytes,
                    file_name="热力图_17周平均.png",
                    mime="image/png",
                    width="stretch",
                )

            with col_e:
                trend_png = mpl_trend_bytes(free4d, n)
                st.download_button(
                    "📈 趋势图 PNG",
                    data=trend_png,
                    file_name="空闲率趋势图.png",
                    mime="image/png",
                    width="stretch",
                )

    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
