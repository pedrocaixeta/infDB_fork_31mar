"""
process_streets.py — Full processing pipeline for street segment processing (InfDB Version).
This module is executed from main.py with:
    import process_streets
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import networkx as nx
from collections import Counter, defaultdict
import os 

from typing import Optional, List, Tuple
from shapely.geometry import LineString, MultiLineString, Point, MultiPoint
from shapely.ops import linemerge, split as shp_split
from sqlalchemy import text


GLOBAL_JUNCTION_DEGREE = 3
ROUND_MERGE = 6
SNAP_EPS = 0.0
THROUGH_MIN_ANGLE_DEG = 175.0
PREFER_LONGEST_THROUGH = True
PHASEB_ALLOW_THROUGH = False
# =========================================================
# BASIC HELPERS
# =========================================================

def nodes_from_endpoints(gdf: gpd.GeoDataFrame, round_decimals: int = 6):
    """Extract unique endpoints after final processing."""
    pts = []
    r = round_decimals

    for geom in gdf.geometry:
        if geom is None or geom.is_empty:
            continue

        geoms = geom.geoms if isinstance(geom, MultiLineString) else [geom]

        for ln in geoms:
            cs = list(ln.coords)
            pts.append((round(cs[0][0], r), round(cs[0][1], r)))
            pts.append((round(cs[-1][0], r), round(cs[-1][1], r)))

    counts = Counter(pts)

    recs = [
        {"node_id": i + 1, "degree": counts[pt], "geom": Point(*pt)}
        for i, pt in enumerate(sorted(counts))
    ]

    return gpd.GeoDataFrame(recs, geometry="geom", crs=gdf.crs)
# =========================================================
# CLASS 0 — FAST SPLITTING AT TRUE INTERSECTIONS
# =========================================================

def _query_bulk_pairs(geoms, sidx, predicate="intersects"):
    """
    Fast bulk spatial index query, fallback to manual scanning.
    """
    try:
        left, right = sidx.query_bulk(geoms, predicate=predicate)
        mask = left < right
        return left[mask], right[mask]
    except Exception:
        # Fallback manual intersection
        L, R = [], []
        seq = list(geoms)
        for i, g in enumerate(seq):
            for j in sidx.intersection(g.bounds):
                if j <= i:
                    continue
                if predicate == "intersects" and not g.intersects(seq[j]):
                    continue
                L.append(i)
                R.append(j)
        return np.array(L), np.array(R)


def split_lines_at_nodes_and_intersections_fast(
    gdf: gpd.GeoDataFrame,
    round_decimals: int = 6,
    tol: float = 1.5
):
    """
    CLASS 0:
    Splits all LineStrings at real intersections.
    Works fully on the geom column (PostGIS default naming: 'geom').
    """

    if gdf.empty:
        return gdf

    geom_col = gdf.geometry.name  # typically "geom"

    # ---------------------------------------------------------
    # 1. Flatten multilines
    # ---------------------------------------------------------
    rows = []
    for _, r in gdf.iterrows():
        geom = r[geom_col]
        if isinstance(geom, MultiLineString):
            for part in geom.geoms:
                nr = r.copy()
                nr[geom_col] = part
                rows.append(nr)
        else:
            rows.append(r)

    flat = gpd.GeoDataFrame(
        rows,
        columns=gdf.columns,
        geometry=gdf.geometry.name,   # <--- EXPLICITLY SET GEOMETRY COLUMN
        crs=gdf.crs
    ).reset_index(drop=True)
    # ---------------------------------------------------------
    # 2. Build spatial index
    # ---------------------------------------------------------
    sidx = flat.sindex
    left, right = _query_bulk_pairs(flat[geom_col], sidx, predicate="intersects")

    inter_pts_per_line = {i: [] for i in range(len(flat))}

    # ---------------------------------------------------------
    # 3. Detect intersection points
    # ---------------------------------------------------------
    for i, j in zip(left, right):
        a = flat.at[i, geom_col]
        b = flat.at[j, geom_col]
        inter = a.intersection(b)

        if inter.is_empty:
            continue

        if inter.geom_type == "Point":
            inter_pts_per_line[i].append(inter)
            inter_pts_per_line[j].append(inter)

        elif inter.geom_type == "MultiPoint":
            pts = list(inter.geoms)
            inter_pts_per_line[i].extend(pts)
            inter_pts_per_line[j].extend(pts)

        elif inter.geom_type in ("LineString", "MultiLineString"):
            for seg in (inter.geoms if inter.geom_type == "MultiLineString" else [inter]):
                p0 = Point(seg.coords[0])
                p1 = Point(seg.coords[-1])
                inter_pts_per_line[i].extend([p0, p1])
                inter_pts_per_line[j].extend([p0, p1])

    # ---------------------------------------------------------
    # 4. Split lines
    # ---------------------------------------------------------
    out_rows = []
    for idx, row in flat.iterrows():
        ln = row[geom_col]
        cs = list(ln.coords)
        s, e = Point(cs[0]), Point(cs[-1])
        pts = inter_pts_per_line.get(idx, [])

        # Keep only interior intersection points
        pts = [
            p for p in pts
            if (p.distance(s) > tol and p.distance(e) > tol)  # exclude endpoints
            and ln.distance(p) <= tol                         # ensure on-line
        ]

        # unique by rounded coordinate
        pts = list({(round(p.x, round_decimals), round(p.y, round_decimals)): p
                    for p in pts}.values())

        if pts:
            pieces = shp_split(ln, MultiPoint(pts))
            for seg in pieces.geoms:
                nr = row.copy()
                nr[geom_col] = seg
                out_rows.append(nr)
        else:
            out_rows.append(row)

    return gpd.GeoDataFrame(
        out_rows,
        columns=flat.columns,
        geometry=geom_col
    )
# =========================================================
# CLASS 1 — CLASSIFY SEGMENTS BY ENDPOINT DEGREE
# =========================================================

def classify_by_endpoints(gdf: gpd.GeoDataFrame, round_decimals: int = 6):
    """
    CLASS 1:
    Klassifiziert jedes Segment in:
        - junction-junction
        - junction-deadend
        - deadend-deadend
        - loop
    """

    if gdf.empty:
        return gdf

    geom_col = gdf.geometry.name
    endpoints = []

    # Collect endpoints of all segments
    for geom in gdf.geometry:
        if geom is None:
            continue
        parts = [geom] if geom.geom_type == "LineString" else geom.geoms
        for line in parts:
            if not line.is_ring:
                cs = list(line.coords)
                endpoints.append((round(cs[0][0], round_decimals),
                                  round(cs[0][1], round_decimals)))
                endpoints.append((round(cs[-1][0], round_decimals),
                                  round(cs[-1][1], round_decimals)))

    # Count degrees
    counts = Counter(endpoints)

    # Build classified output
    recs = []
    for _, row in gdf.iterrows():
        geom = row[geom_col]
        parts = [geom] if geom.geom_type == "LineString" else geom.geoms

        for line in parts:
            if line.is_ring:
                seg_type = "loop"
            else:
                cs = list(line.coords)
                sp = (round(cs[0][0], round_decimals), round(cs[0][1], round_decimals))
                ep = (round(cs[-1][0], round_decimals), round(cs[-1][1], round_decimals))

                st = "junction" if counts.get(sp, 0) > 1 else "deadend"
                et = "junction" if counts.get(ep, 0) > 1 else "deadend"

                seg_type = f"{min(st, et)}-{max(st, et)}"

            attrs = row.drop(labels=[geom_col]).to_dict()
            attrs["segment_type"] = seg_type
            recs.append({**attrs, geom_col: line})

    return gpd.GeoDataFrame(
        recs,
        geometry=geom_col,
        crs=gdf.crs
)
# =========================================================
# CLASS 1.5 — deadend-junction-iter DETECTION
# =========================================================

def mark_deadend_junction_iter(
    gdf: gpd.GeoDataFrame,
    round_dec: int = 6,
    type_col: str = "segment_type"
):
    """
    CLASS 1.5:
    Iteratives Finden von deadend-junction-iter:
    - Start mit segments == junction-junction
    - Endpunkt mit degree==1 & anderer degree>=2 → wird iterativ deadend-junction-iter
    """

    if gdf.empty or type_col not in gdf.columns:
        return gdf

    geom_col = gdf.geometry.name
    work = gdf.copy()

    # Only junction-junction candidates
    eligible = work.index[
        (work[type_col] == "junction-junction")
        & (work[geom_col].geom_type == "LineString")
    ].tolist()

    if not eligible:
        return gdf

    def ends_key(ln: LineString):
        cs = ln.coords
        return ((round(cs[0][0], round_dec), round(cs[0][1], round_dec)),
                (round(cs[-1][0], round_dec), round(cs[-1][1], round_dec)))

    dej_iter = set()

    while True:
        endpoints = []
        ends_cache = {}

        for idx in eligible:
            ln = work.at[idx, geom_col]
            s, e = ends_key(ln)
            ends_cache[idx] = (s, e)
            endpoints.append(s)
            endpoints.append(e)

        deg = Counter(endpoints)

        newly = [
            idx for idx in eligible
            if (
                (deg.get(ends_cache[idx][0], 0) == 1 and deg.get(ends_cache[idx][1], 0) >= 2)
                or
                (deg.get(ends_cache[idx][1], 0) == 1 and deg.get(ends_cache[idx][0], 0) >= 2)
            )
        ]

        if not newly:
            break

        dej_iter.update(newly)
        eligible = [idx for idx in eligible if idx not in dej_iter]

    if dej_iter:
        work.loc[list(dej_iter), type_col] = "deadend-junction-iter"

    return work

# =========================================================
# MERGE-ENGINE (allgemein, wie in basemap_full_pipeline.py)
# =========================================================
def _merge_attribute_values(values):
    """
    Bestimmt den zusammengeführten Attributwert aus einer Liste/Serie von Werten.

    Logik:
    - Wenn alle Werte leer (None/NaN/""/nur Spaces) sind → None (Feld bleibt leer).
    - Wenn es sowohl leere als auch nicht-leere Werte gibt → 'mixed'.
    - Wenn alle nicht-leeren Werte gleich sind → diesen Wert übernehmen.
    - Wenn mehrere verschiedene nicht-leere Werte existieren → 'mixed'.
    """
    s = pd.Series(values)

    # Leere Werte: NaN/None oder leerer String
    is_null = s.isna() | s.astype(str).str.strip().eq("")

    # Fall 1: alles leer -> Feld bleibt leer
    if is_null.all():
        return None

    # Es gibt mindestens einen nicht-leeren Wert
    # Fall 2: Mischung aus leer und nicht-leer -> mixed
    if is_null.any() and (~is_null).any():
        return "mixed"

    # Ab hier: alle Werte sind nicht leer
    non_empty = s[~is_null]
    uniq = pd.unique(non_empty)

    # Fall 3: genau ein eindeutiger Wert -> übernehmen
    if len(uniq) == 1:
        return uniq[0]

    # Fall 4: mehrere verschiedene Werte -> mixed
    return "mixed"

def _merge_ids(values):
    """
    Merged ID column:
    - Wenn Werte schon Listen/Strings mit ';' sind, werden sie aufgetrennt.
    - Alle IDs werden gesammelt, getrimmt, dedupliziert und mit ';' verbunden.
    - Wenn gar keine ID vorhanden ist -> None.
    """
    s = pd.Series(values).dropna()
    collected = []
    for v in s:
        # falls schon "ID1;ID2" etc.
        for part in str(v).split(";"):
            part = part.strip()
            if part:
                collected.append(part)
    collected = sorted(set(collected))
    return ";".join(collected) if collected else None

def _flatten_multilines(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rows = []
    geom_col = gdf.geometry.name
    for _, r in gdf.iterrows():
        g = r[geom_col]
        if isinstance(g, MultiLineString):
            for part in g.geoms:
                nr = r.copy()
                nr[geom_col] = part
                rows.append(nr)
        else:
            rows.append(r)
    return gpd.GeoDataFrame(rows, columns=gdf.columns, geometry=geom_col, crs=gdf.crs)

def _snap_xy(x: float, y: float, eps: float):
    return (round(x / eps) * eps, round(y / eps) * eps) if eps and eps > 0 else (x, y)


def _ends_snapped(line: LineString, r: int, eps: float):
    cs = list(line.coords)
    s = _snap_xy(cs[0][0], cs[0][1], eps)
    e = _snap_xy(cs[-1][0], cs[-1][1], eps)
    return ((round(s[0], r), round(s[1], r)), (round(e[0], r), round(e[1], r)))

def _build_graph_by_type(
    sub: gpd.GeoDataFrame,
    type_col: str,
    round_decimals: int,
    snap_eps: float
):
    geom_col = sub.geometry.name
    sub = sub.copy()
    sub[["start", "end"]] = sub[geom_col].apply(
        lambda g: pd.Series(_ends_snapped(g, round_decimals, snap_eps))
    )
    G = nx.Graph()
    for idx, row in sub.iterrows():
        G.add_edge(row["start"], row["end"], index=idx, segtype=row.get(type_col))
    return sub, G

def _node_context_alltypes(
    gdf: gpd.GeoDataFrame,
    round_decimals: int,
    snap_eps: float,
    type_col: str
):
    flat = _flatten_multilines(gdf.copy())
    geom_col = flat.geometry.name
    flat[["start", "end"]] = flat[geom_col].apply(
        lambda g: pd.Series(_ends_snapped(g, round_decimals, snap_eps))
    )
    deg_all = Counter(flat["start"].tolist() + flat["end"].tolist())
    types_at = defaultdict(set)
    for _, r in flat.iterrows():
        t = r.get(type_col)
        types_at[r["start"]].add(t)
        types_at[r["end"]].add(t)
    return deg_all, types_at

def _merge_degree2_chains_fast(
    sub: gpd.GeoDataFrame,
    type_col: str,
    length_col: str,
    round_decimals: int,
    snap_eps: float,
    junction_degree: int = GLOBAL_JUNCTION_DEGREE,
    deg_all=None,
    types_at_node=None
) -> gpd.GeoDataFrame:
    if sub.empty:
        return sub

    geom_col = sub.geometry.name
    sub, G = _build_graph_by_type(sub, type_col, round_decimals, snap_eps)

    deg_sub = dict(G.degree())
    anchors = {n for n, d in deg_sub.items() if d != 2}
    if deg_all is not None:
        anchors |= {n for n, d in deg_all.items() if d >= junction_degree}
    if types_at_node is not None:
        anchors |= {n for n, s in types_at_node.items() if len({x for x in s if x is not None}) > 1}
    anchors = {n for n in anchors if n in G}

    visited = set()
    merged_feats = []

    def emit_chain(nodes: List[Tuple[float, float]], idxs: List[int]):
        part = sub.loc[idxs]
        merged_geom = linemerge(list(part[geom_col]))
        attrs = {}
        for col in sub.columns:
            if col in (geom_col, "start", "end"):
                continue

            if col == "id":
                attrs[col] = _merge_ids(part[col])
            else:
                attrs[col] = _merge_attribute_values(part[col])

        attrs[length_col] = float(merged_geom.length)
        attrs["merged_from_count"] = len(idxs)
        attrs[geom_col] = merged_geom
        merged_feats.append(attrs)

    # 1) Chains from anchors
    for a in anchors:
        for nbr in list(G.neighbors(a)):
            ekey = tuple(sorted((a, nbr)))
            if ekey in visited:
                continue
            path = [a, nbr]
            idxs = [G[a][nbr]["index"]]
            prev, cur = a, nbr
            while True:
                visited.add(tuple(sorted((prev, cur))))
                if cur in anchors:
                    break
                nxts = [n for n in G.neighbors(cur) if n != prev]
                if not nxts:
                    break
                nxt = nxts[0] if G[cur][nxts[0]]["index"] != idxs[-1] else (
                    nxts[1] if len(nxts) > 1 else None
                )
                if nxt is None:
                    break
                path.append(nxt)
                idxs.append(G[cur][nxt]["index"])
                prev, cur = cur, nxt
                if tuple(sorted((prev, cur))) in visited:
                    break
            emit_chain(path, idxs)

    # 2) Degree-2 cycles
    for u, v, data in G.edges(data=True):
        ekey = tuple(sorted((u, v)))
        if ekey in visited:
            continue
        if deg_sub.get(u, 0) == 2 and deg_sub.get(v, 0) == 2:
            path = [u, v]
            idxs = [data["index"]]
            prev, cur = u, v
            while True:
                visited.add(tuple(sorted((prev, cur))))
                nxts = [n for n in G.neighbors(cur) if n != prev]
                if not nxts:
                    break
                nxt = nxts[0]
                if tuple(sorted((cur, nxt))) in visited:
                    idxs.append(G[cur][nxt]["index"])
                    path.append(nxt)
                    break
                idxs.append(G[cur][nxt]["index"])
                path.append(nxt)
                prev, cur = cur, nxt
            emit_chain(path, idxs)

    merged_gdf = gpd.GeoDataFrame(
        merged_feats,
        geometry=geom_col,
        crs=sub.crs,
    )

    used_pairs = {tuple(sorted((u, v))) for (u, v) in visited}
    used_idx = {G[u][v]["index"] for (u, v) in used_pairs if G.has_edge(u, v)}
    untouched = sub[~sub.index.isin(list(used_idx))].copy()
    if not untouched.empty:
        untouched[length_col] = untouched[geom_col].length.astype(float)
        untouched["merged_from_count"] = 1
        untouched.drop(columns=["start", "end"], inplace=True, errors="ignore")

    final = pd.concat([merged_gdf, untouched], ignore_index=True) if not merged_gdf.empty else untouched
    return final


def _angle_deg(a, b, c):
    import math
    v1 = (a[0] - b[0], a[1] - b[1])
    v2 = (c[0] - b[0], c[1] - b[1])
    n1 = math.hypot(*v1)
    n2 = math.hypot(*v2)
    if n1 == 0 or n2 == 0:
        return 180.0
    cosx = max(-1.0, min(1.0, (v1[0]*v2[0] + v1[1]*v2[1]) / (n1*n2)))
    return math.degrees(math.acos(cosx))


def merge_linear_chains_general(
    gdf: gpd.GeoDataFrame,
    round_decimals: int,
    junction_degree: int,
    focus_type: Optional[str],
    type_col: str,
    length_col: str,
    min_angle_deg: Optional[float],
    allow_through_merge_at_junction: bool,
    through_min_angle_deg: float,
    prefer_longest_through: bool,
    snap_eps: float,
) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf

    flat = _flatten_multilines(gdf)
    geom_col = flat.geometry.name
    flat[["start", "end"]] = flat[geom_col].apply(
        lambda g: pd.Series(_ends_snapped(g, round_decimals, snap_eps))
    )
    sub = flat if focus_type is None else flat[flat[type_col] == focus_type].copy()
    if sub.empty:
        out = flat.copy()
        out[length_col] = out[geom_col].length.astype(float)
        out.drop(columns=["start", "end"], inplace=True, errors="ignore")
        return out

    ser_sub = pd.Series(sub["start"].tolist() + sub["end"].tolist())
    degree_sub = ser_sub.value_counts().to_dict()

    deg_all = Counter(flat["start"].tolist() + flat["end"].tolist())
    types_at = defaultdict(set)
    for _, r in flat.iterrows():
        t = r.get(type_col)
        types_at[r["start"]].add(t)
        types_at[r["end"]].add(t)

    def is_junction(n):
        mixed = len({x for x in types_at.get(n, set()) if x is not None}) > 1
        return (degree_sub.get(n, 0) >= junction_degree) or \
               (deg_all.get(n, 0) >= junction_degree) or mixed

    G = nx.Graph()
    for idx, row in sub.iterrows():
        G.add_edge(
            row["start"],
            row["end"],
            index=idx,
            segtype=row.get(type_col),
            length=row[geom_col].length,
        )

    def _choose_through(prev_node, cur_node, segtype):
        cands = []
        for nxt in G.neighbors(cur_node):
            if nxt == prev_node:
                continue
            if G[cur_node][nxt].get("segtype") != segtype:
                continue
            ang = _angle_deg(prev_node, cur_node, nxt)
            if ang < through_min_angle_deg:
                continue
            idx = G[cur_node][nxt]["index"]
            length = float(sub.loc[idx, geom_col].length)
            cands.append((nxt, ang, length, idx))
        if not cands:
            return None
        if prefer_longest_through:
            cands.sort(key=lambda t: (t[1], t[2]), reverse=True)
        else:
            cands.sort(key=lambda t: t[1], reverse=True)
        nxt, ang, _len, idx = cands[0]
        return nxt, idx

    visited_edges, visited_idx, merged_feats = set(), set(), []

    def walk(start, neigh, segtype):
        path = [start, neigh]
        idxs = [G[start][neigh]["index"]]
        prev, cur = start, neigh
        while True:
            if is_junction(cur):
                if not allow_through_merge_at_junction:
                    return path, idxs
                choice = _choose_through(prev, cur, segtype)
                if not choice:
                    return path, idxs
                nxt, next_idx = choice
                path.append(nxt)
                idxs.append(next_idx)
                prev, cur = cur, nxt
                continue
            nxts = [n for n in G.neighbors(cur)
                    if n != prev and G[cur][n].get("segtype") == segtype]
            if len(nxts) != 1:
                return path, idxs
            nxt = nxts[0]
            next_idx = G[cur][nxt]["index"]
            if (min_angle_deg is not None) and (_angle_deg(prev, cur, nxt) < float(min_angle_deg)):
                return path, idxs
            path.append(nxt)
            idxs.append(next_idx)
            prev, cur = cur, nxt

    for node in list(G.nodes):
        for neigh in list(G.neighbors(node)):
            ekey = tuple(sorted((node, neigh)))
            if ekey in visited_edges:
                continue
            segtype = G[node][neigh].get("segtype")
            path, idxs = walk(node, neigh, segtype)
            if not path or len(path) < 2 or not idxs:
                continue
            for i in range(len(path) - 1):
                visited_edges.add(tuple(sorted((path[i], path[i+1]))))
            visited_idx.update(idxs)
            part = sub.loc[idxs]
            merged_geom = linemerge(list(part[geom_col]))
            attrs = {}
            for col in flat.columns:
                if col in (geom_col, "start", "end"):
                    continue

                if col == "id":
                    attrs[col] = _merge_ids(part[col])
                else:
                    attrs[col] = _merge_attribute_values(part[col])
            attrs[length_col] = float(merged_geom.length)
            attrs["merged_from_count"] = len(idxs)
            attrs[geom_col] = merged_geom
            merged_feats.append(attrs)

    merged_gdf = gpd.GeoDataFrame(
        merged_feats,
        geometry=geom_col,
        crs=flat.crs,
    )
    untouched = sub[~sub.index.isin(visited_idx)].copy()
    if not untouched.empty:
        untouched[length_col] = untouched[geom_col].length.astype(float)
        untouched["merged_from_count"] = 1
        untouched.drop(columns=["start", "end"], inplace=True, errors="ignore")

    final = pd.concat([merged_gdf, untouched], ignore_index=True) if not merged_gdf.empty else untouched
    if length_col in final.columns:
        final[length_col] = final[length_col].astype(float)
    return final

def merge_linear_chains(
    gdf: gpd.GeoDataFrame,
    round_decimals: int,
    junction_degree: int,
    focus_type: Optional[str],
    type_col: str,
    length_col: str,
    stop_on_any_junction: bool,
    allow_through_merge_at_junction: bool,
    through_min_angle_deg: float,
    prefer_longest_through: bool,
    snap_eps: float,
    min_angle_deg: Optional[float] = None,
) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf
    if stop_on_any_junction and not allow_through_merge_at_junction:
        deg_all, types_at = _node_context_alltypes(gdf, round_decimals, snap_eps, type_col)
        if focus_type is None:
            out_parts = []
            for t in sorted(gdf[type_col].dropna().unique()):
                sub = gdf[gdf[type_col] == t].copy()
                out_parts.append(
                    _merge_degree2_chains_fast(
                        sub,
                        type_col,
                        length_col,
                        round_decimals,
                        snap_eps,
                        junction_degree=junction_degree,
                        deg_all=deg_all,
                        types_at_node=types_at,
                    )
                )
            return pd.concat(out_parts, ignore_index=True) if out_parts else gdf
        else:
            sub = gdf[gdf[type_col] == focus_type].copy()
            return _merge_degree2_chains_fast(
                sub,
                type_col,
                length_col,
                round_decimals,
                snap_eps,
                junction_degree=junction_degree,
                deg_all=deg_all,
                types_at_node=types_at,
            )
    return merge_linear_chains_general(
        gdf=gdf,
        round_decimals=round_decimals,
        junction_degree=junction_degree,
        focus_type=focus_type,
        type_col=type_col,
        length_col=length_col,
        min_angle_deg=min_angle_deg,
        allow_through_merge_at_junction=allow_through_merge_at_junction,
        through_min_angle_deg=through_min_angle_deg,
        prefer_longest_through=prefer_longest_through,
        snap_eps=snap_eps,
    )

# =========================================================
# PHASE A — DEJ-FAMILY MERGE (deadend-junction + iter)
# =========================================================

def merge_dej_family(gdf: gpd.GeoDataFrame, round_dec=6):
    """
    PHASE A:
    Merge aller Segmente:
        - deadend-junction
        - deadend-junction-iter
    zu einem DEJ-Family-Block,
    mit Stop an Junctions (degree ≥ 3).
    """

    if gdf.empty:
        return gdf

    geom_col = gdf.geometry.name

    # Mark all DEJ family
    work = gdf.copy()
    work["_raw_type"] = work["segment_type"]

    mask = work["_raw_type"].isin(["deadend-junction", "deadend-junction-iter"])
    work.loc[mask, "segment_type"] = "dej_family"

    # Prepare merge
    dej_df = work[work["segment_type"] == "dej_family"].copy()
    others = work[work["segment_type"] != "dej_family"].copy()

    if dej_df.empty:
        return gdf.copy()

    # Build graph
    ends = {}
    pts = []
    for idx, row in dej_df.iterrows():
        ln = row[geom_col]
        cs = ln.coords
        s = (round(cs[0][0], round_dec), round(cs[0][1], round_dec))
        e = (round(cs[-1][0], round_dec), round(cs[-1][1], round_dec))
        ends[idx] = (s, e)
        pts.append(s)
        pts.append(e)

    deg = Counter(pts)

    # Merge via DFS walk
    visited = set()
    merged_rows = []
  
    for start_idx in dej_df.index:
        if start_idx in visited:
            continue

        s0, e0 = ends[start_idx]

        # Stop at true junction
        if deg[s0] >= 3 or deg[e0] >= 3:
            visited.add(start_idx)
            merged_rows.append(dej_df.loc[start_idx])
            continue

        chain = [start_idx]
        visited.add(start_idx)
        stack = [start_idx]

        while stack:
            current = stack.pop()
            cs, ce = ends[current]

            # Find neighbors
            for nb_idx, (ns, ne) in ends.items():
                if nb_idx == current or nb_idx in visited:
                    continue
                # Share endpoint?
                if cs in (ns, ne) or ce in (ns, ne):
                    # Stop at junction
                    if deg[ns] >= 3 or deg[ne] >= 3:
                        continue
                    visited.add(nb_idx)
                    chain.append(nb_idx)
                    stack.append(nb_idx)

        # Merge geometry
        parts = [dej_df.at[i, geom_col] for i in chain]
        merged_geom = linemerge(parts)

        r0 = dej_df.loc[chain[0]].copy()
        r0[geom_col] = merged_geom
        r0["merged_from"] = chain
        r0["merged_from_count"] = len(chain)

        merged_rows.append(r0)

    merged_df = gpd.GeoDataFrame(merged_rows, geometry=geom_col, crs=gdf.crs)

    # Restore original types
    is_merged = merged_df["merged_from_count"].fillna(1) > 1
    merged_df["segment_type"] = np.where(
        is_merged,
        "deadend-junction",
        merged_df["_raw_type"]
    )

    merged_df = merged_df.drop(columns=["_raw_type"], errors="ignore")

    return pd.concat([merged_df, others], ignore_index=True)
# =========================================================
# CLASS 2 — RE-CLASSIFY AFTER PHASE A MERGE
# =========================================================

def class2_reclassify(gdf: gpd.GeoDataFrame, round_decimals: int = 6):
    """
    CLASS 2:
    Nach PHASE A alles erneut klassifizieren:
        - junction-junction
        - junction-deadend
        - deadend-deadend
        - loop
    und length_m berechnen.
    """
    out = classify_by_endpoints(
        gdf.drop(columns=["segment_type"], errors="ignore"),
        round_decimals=round_decimals
    )

    geom_col = out.geometry.name
    out["length_m"] = out[geom_col].length.astype(float)

    # iterativ deadend-junction-iter wie in Original
    out = mark_deadend_junction_iter(out, round_dec=round_decimals)

    return out

# =========================================================
# DEADEND-JUNCTION LENGTH FILTER
# =========================================================

def apply_dej_length_filter(gdf, min_length):
    """
    Entfernt deadend-junction Segmente unterhalb der Mindestlänge.
    Entspricht dem Original-Skript (Step 7).
    """
    mask = ~(
        (gdf["segment_type"] == "deadend-junction") &
        (gdf["length_m"] < min_length)
    )
    return gdf[mask].copy()

# =========================================================
# CLASS 3 — FINAL CLASSIFICATION BEFORE PHASE B
# =========================================================

def class3_final_classify(gdf: gpd.GeoDataFrame, round_decimals: int = 6):
    """
    CLASS 3:
    Finale Re-Klassification direkt vor PHASE B.
    Identisch zu CLASS 1 & CLASS 2.
    """
    out = classify_by_endpoints(
        gdf.drop(columns=["segment_type"], errors="ignore"),
        round_decimals
    )

    geom_col = out.geometry.name
    out["length_m"] = out[geom_col].length.astype(float)

    out = mark_deadend_junction_iter(out, round_dec=round_decimals)

    return out

# =========================================================
# PHASE B — JUNCTION-JUNCTION MERGE
# =========================================================

def merge_junction_junction(gdf: gpd.GeoDataFrame, round_dec=6):
    """
    PHASE B (InfDB-Version, aber mit neuer, schneller Merge-Engine):
    - Merget alle Segmente mit segment_type == 'junction-junction'
    - Logik wie in basemap_full_pipeline (merge_linear_chains)
    """

    # Wir nehmen die aktuelle Länge-Spalte oder erzeugen sie neu
    geom_col = gdf.geometry.name
    if "length_m" not in gdf.columns:
        gdf = gdf.copy()
        gdf["length_m"] = gdf[geom_col].length.astype(float)

    merged = merge_linear_chains(
        gdf=gdf,
        round_decimals=ROUND_MERGE,
        junction_degree=GLOBAL_JUNCTION_DEGREE,
        focus_type="junction-junction",
        type_col="segment_type",
        length_col="length_m",
        stop_on_any_junction=True,
        allow_through_merge_at_junction=PHASEB_ALLOW_THROUGH,
        through_min_angle_deg=THROUGH_MIN_ANGLE_DEG,
        prefer_longest_through=PREFER_LONGEST_THROUGH,
        snap_eps=SNAP_EPS,
        min_angle_deg=None,
    )

    return merged

# =========================================================
# FINAL RE-CLASS AFTER PHASE B
# =========================================================

def final_reclass(gdf: gpd.GeoDataFrame, round_decimals=6):
    """
    Final CLASSIFICATION after all merges (PHASE A & B).
    """

    out = classify_by_endpoints(
        gdf.drop(columns=["segment_type"], errors="ignore"),
        round_decimals
    )

    geom_col = out.geometry.name
    out["length_m"] = out[geom_col].length.astype(float)

    out = mark_deadend_junction_iter(out, round_dec=round_decimals)

    return out
# =========================================================
# FINAL NODE GENERATION
# =========================================================

def generate_final_nodes(gdf: gpd.GeoDataFrame, round_decimals=6):
    geom_col = gdf.geometry.name
    pts = []

    for geom in gdf[geom_col]:
        if geom is None:
            continue

        parts = [geom] if geom.geom_type == "LineString" else geom.geoms
        for ln in parts:
            cs = list(ln.coords)
            p0 = (round(cs[0][0], round_decimals), round(cs[0][1], round_decimals))
            p1 = (round(cs[-1][0], round_decimals), round(cs[-1][1], round_decimals))
            pts.append(p0)
            pts.append(p1)

    counts = Counter(pts)

    recs = [
        {"node_id": i + 1, "degree": counts[pt], "geom": Point(*pt)}
        for i, pt in enumerate(sorted(counts))
    ]

    return gpd.GeoDataFrame(recs, geometry="geom", crs=gdf.crs)

# =========================================================
# MAIN FULL PIPELINE EXECUTION
# =========================================================

def run_process_streets(
    table_name: str,
    klasse_filter: list,
    apply_length_filter: bool,
    min_length_deadend_junction: float,
    remove_deadend_deadend: bool,
    remove_loop: bool,
    infdb
):
    """
    InfDB-Version der Basemap-Pipeline mit exakt derselben Merge-Logik
    (inkl. ID-Handling und 'mixed'-Attributen).
    """

    log = infdb.get_log()
    log.info("=== PROCESS-STREETS PIPELINE START (basemap-logic) ===")

    engine = infdb.get_db_engine()
    input_schema = infdb.get_config_value(["process-streets", "data", "input", "schema"])
    output_schema = infdb.get_config_value(["process-streets", "data", "output", "schema"])

    # NEU: objektart-Filter aus der YAML lesen
    klasse_objektart_filter = infdb.get_config_value(
        ["process-streets", "klasse_objektart_filter"]
    )
    if not isinstance(klasse_objektart_filter, dict):
        klasse_objektart_filter = {}
    if not table_name or str(table_name).strip().lower() in ("none", ""):
        table_name = infdb.get_config_value(
            ["process-streets", "data", "input", "table_name"]
        )
        log.info(f"table_name from config: {table_name}")

    full_table = f"{input_schema}.{table_name}"
    log.info(f"Loading input table: {full_table}")


    # ----------------------------------------------------------
    # LOAD INPUT
    # ----------------------------------------------------------
    gdf = gpd.read_postgis(
        sql=f"SELECT * FROM {full_table}",
        con=engine,
        geom_col="geom"
    )

    # 🔹 HIER: CRS direkt nach read_postgis setzen
    if gdf.crs is None:
        epsg = None

        # Versuch 1: EPSG aus den DB-Parametern holen
        try:
            db_params = infdb.get_db_parameters_dict()
            epsg = db_params.get("epsg", None)
        except Exception:
            epsg = None

        # Versuch 2: explizit aus der config (falls vorhanden)
        if epsg is None:
            epsg = infdb.get_config_value(["process-streets", "data", "epsg"])

        if epsg:
            gdf.set_crs(epsg=int(epsg), inplace=True)
            log.info(f"CRS gesetzt auf EPSG:{epsg} (aus InfDB)")
        else:
            log.warning("Kein EPSG in InfDB-Konfiguration gefunden, gdf.crs bleibt None!")
    else:
        log.info(f"CRS aus PostGIS übernommen: {gdf.crs}")

    if gdf.empty:
        log.warning("Input table EMPTY!")
        return {"status": "no_input_rows"}

    # Sicherstellen, dass es eine 'id'-Spalte gibt 
    if "id" not in gdf.columns:
        gdf["id"] = gdf.index.astype(str)
        log.info("No 'id' column in input -> created from index.")
    else:
        gdf["id"] = gdf["id"].astype(str)
        log.info("Using existing 'id' column (cast to str).")

    geom_col = gdf.geometry.name

    # ----------------------------------------------------------
    # STEP 1 — Klasse-Filter + optional objektart-Filter
    # ----------------------------------------------------------
    if klasse_filter:
        log.info(f"Applying klasse_filter: {klasse_filter}")
        if "klasse" in gdf.columns:
            # 1) Nach Klasse filtern
            gdf = gdf[gdf["klasse"].isin(klasse_filter)].copy()

            # 2) Optional: objektart-Filter pro Klasse aus der config
            if klasse_objektart_filter:
                if "objektart" not in gdf.columns:
                    log.warning(
                        "Column 'objektart' missing – skipping klasse_objektart_filter."
                    )
                else:
                    # Klassen, für die ein spezieller objektart-Filter existiert
                    classes_with_obj_filter = set(klasse_objektart_filter.keys())

                    # Basis: alle Zeilen ohne speziellen objektart-Filter bleiben drin
                    mask = ~gdf["klasse"].isin(classes_with_obj_filter)

                    # Für jede Klasse mit definierter objektart-Liste
                    for klasse_name, allowed_objektarten in klasse_objektart_filter.items():
                        mask |= (
                            (gdf["klasse"] == klasse_name)
                            & gdf["objektart"].isin(allowed_objektarten)
                        )

                    gdf = gdf[mask].copy()
        else:
            log.warning("Column 'klasse' missing – skipping klasse_filter.")
    else:
        log.info("No klasse_filter configured – skipping STEP 1.")

    # Debug nach STEP 1:
    if "klasse" in gdf.columns:
        log.info(f"Unique klassen after STEP 1: {gdf['klasse'].unique()}")
    if "klasse" in gdf.columns and "objektart" in gdf.columns:
        bundes = gdf[gdf["klasse"] == "Bundesstraße"]
        if not bundes.empty:
            log.info(
                "Objektart for Bundesstraße after STEP 1:\n%s",
                bundes["objektart"].value_counts().to_string()
            )

 

    # ----------------------------------------------------------
    # STEP 2 — Split Lines (echte Schnittpunkte)
    # ----------------------------------------------------------
    log.info("Splitting intersections (CLASS_0)...")
    gdf_split = split_lines_at_nodes_and_intersections_fast(gdf)

    # ----------------------------------------------------------
    # STEP 3 — CLASS 1 (inkl. deadend-junction-iter)
    # ----------------------------------------------------------
    log.info("Classifying CLASS_1...")
    class1 = classify_by_endpoints(gdf_split)
    class1 = mark_deadend_junction_iter(class1, round_dec=6, type_col="segment_type")

    # ----------------------------------------------------------
    # STEP 4 — PHASE A: DEJ-family (DEJ + DEJ-iter), stop at junctions
    #          → merged werden zu 'deadend-junction'
    # ----------------------------------------------------------
    log.info("Phase A: DEJ-family merge (DEJ + DEJ-iter, stop at junctions)...")
    phaseA_input = class1.copy()
    phaseA_input["_raw_type"] = phaseA_input["segment_type"]

    dej_mask = phaseA_input["_raw_type"].isin({"deadend-junction", "deadend-junction-iter"})
    phaseA_norm = phaseA_input.copy()
    phaseA_norm.loc[dej_mask, "segment_type"] = "dej_family"

    dej_family_merged = merge_linear_chains(
        gdf=phaseA_norm,
        round_decimals=ROUND_MERGE,
        junction_degree=GLOBAL_JUNCTION_DEGREE,
        focus_type="dej_family",
        type_col="segment_type",
        length_col="length_m",
        stop_on_any_junction=True,
        allow_through_merge_at_junction=False,
        through_min_angle_deg=180.0,
        prefer_longest_through=False,
        snap_eps=SNAP_EPS,
    )

    is_mergedA = dej_family_merged.get("merged_from_count", 1).fillna(1).astype(int) > 1
    dej_family_merged["segment_type"] = np.where(
        is_mergedA,
        "deadend-junction",
        dej_family_merged["_raw_type"]
    )
    dej_family_merged = dej_family_merged.drop(columns=["_raw_type"], errors="ignore")

    others_class1 = class1[~dej_mask].copy()
    after_phase_a = pd.concat([dej_family_merged, others_class1], ignore_index=True)

    # ----------------------------------------------------------
    # STEP 5 — CLASS 2 (Reclass + Länge)
    # ----------------------------------------------------------
    log.info("CLASS_2: reclassify + length...")
    class2 = classify_by_endpoints(
        after_phase_a.drop(columns=["segment_type"], errors="ignore"),
        round_decimals=6
    )
    class2["length_m"] = class2[geom_col].length.astype(float)
    class2 = mark_deadend_junction_iter(class2, round_dec=6, type_col="segment_type")

    # ----------------------------------------------------------
    # STEP 6 — Deadend-Junction Length Filter
    #          (nur deadend-junction, wie im Original)
    # ----------------------------------------------------------
    filtered = class2.copy()
    if apply_length_filter:
        thr = float(min_length_deadend_junction)
        log.info(f"Applying DEJ length filter (only 'deadend-junction' < {thr} m)...")
        mask_dej_short = (filtered["segment_type"] == "deadend-junction") & (filtered["length_m"] < thr)
        removed = int(mask_dej_short.sum())
        filtered = filtered[~mask_dej_short].copy()
        log.info(f"Removed {removed} short deadend-junction segments; remaining {len(filtered)}")
    else:
        log.info("No DEJ length filter applied.")

    # ----------------------------------------------------------
    # STEP 7 — CLASS 3 (nur Reclass, keine extra Länge-Logik)
    # ----------------------------------------------------------
    log.info("CLASS_3: final pre-merge reclassification...")
    class3 = classify_by_endpoints(
        filtered.drop(columns=["segment_type"], errors="ignore"),
        round_decimals=6
    )
    class3 = mark_deadend_junction_iter(class3, round_dec=6, type_col="segment_type")
    # ----------------------------------------------------------
    # STEP 7b — SECOND DEJ LENGTH FILTER (after CLASS_3)
    # ----------------------------------------------------------
    if apply_length_filter:
        thr = float(min_length_deadend_junction)

        # Falls length_m hier noch nicht existiert, zur Sicherheit berechnen
        if "length_m" not in class3.columns:
            class3["length_m"] = class3[geom_col].length.astype(float)

        log.info(
            f"Applying SECOND DEJ length filter after CLASS_3 "
            f"(only 'deadend-junction' < {thr} m)..."
        )
        mask_dej_short2 = (
            (class3["segment_type"] == "deadend-junction")
            & (class3["length_m"] < thr)
        )
        removed2 = int(mask_dej_short2.sum())
        class3 = class3[~mask_dej_short2].copy()
        log.info(
            f"Removed {removed2} additional short deadend-junction segments "
            f"after CLASS_3; remaining {len(class3)}"
        )
    else:
        log.info("No second DEJ length filter after CLASS_3 (apply_length_filter=False).")

    # ----------------------------------------------------------
    # STEP 8 — Post-CLASS_3 A): Merge DEJ-family (DEJ + DEJ-iter)
    #            über degree==2 → merged → 'deadend-junction'
    # ----------------------------------------------------------
    log.info("Post-CLASS_3 A): Merge DEJ-family (DEJ + DEJ-iter) across deg==2...")
    phaseA = class3.copy()
    phaseA["_raw_type"] = phaseA["segment_type"]
    phaseA["seg_raw"] = phaseA["_raw_type"]
    dej_family_mask = phaseA["_raw_type"].isin({"deadend-junction", "deadend-junction-iter"})
    phaseA.loc[dej_family_mask, "segment_type"] = "dej_family"

    dej_family_merged2 = merge_linear_chains(
        gdf=phaseA,
        round_decimals=ROUND_MERGE,
        junction_degree=GLOBAL_JUNCTION_DEGREE,
        focus_type="dej_family",
        type_col="segment_type",
        length_col="length_m",
        stop_on_any_junction=True,
        allow_through_merge_at_junction=False,
        through_min_angle_deg=180.0,
        prefer_longest_through=False,
        snap_eps=SNAP_EPS,
    )

    is_mergedA2 = dej_family_merged2.get("merged_from_count", 1).fillna(1).astype(int) > 1
    dej_family_merged2["segment_type"] = np.where(
        is_mergedA2,
        "deadend-junction",
        dej_family_merged2.get("seg_raw")
    )
    dej_family_merged2 = dej_family_merged2.drop(columns=["seg_raw"], errors="ignore")

    othersA = class3[~dej_family_mask].copy()
    after_A = pd.concat([dej_family_merged2, othersA], ignore_index=True)

    # ----------------------------------------------------------
    # STEP 9 — Post-CLASS_3 B): Merge JJ-family (JJ + DEJ-iter)
    #            nach JJ-Regeln → merged → 'junction-junction'
    # ----------------------------------------------------------
    log.info("Post-CLASS_3 B): Merge JJ-family (JJ + DEJ-iter) by JJ rules...")
    phaseB = after_A.copy()
    phaseB["_raw_type"] = phaseB["segment_type"]
    phaseB["seg_raw"] = phaseB["_raw_type"]
    jj_family_mask = phaseB["_raw_type"].isin({"junction-junction", "deadend-junction-iter"})
    phaseB.loc[jj_family_mask, "segment_type"] = "jj_family"

    jj_merged = merge_linear_chains(
        gdf=phaseB,
        round_decimals=ROUND_MERGE,
        junction_degree=GLOBAL_JUNCTION_DEGREE,
        focus_type="jj_family",
        type_col="segment_type",
        length_col="length_m",
        stop_on_any_junction=True,
        allow_through_merge_at_junction=PHASEB_ALLOW_THROUGH,
        through_min_angle_deg=THROUGH_MIN_ANGLE_DEG,
        prefer_longest_through=PREFER_LONGEST_THROUGH,
        snap_eps=SNAP_EPS,
    )

    is_mergedB = jj_merged.get("merged_from_count", 1).fillna(1).astype(int) > 1
    jj_merged["segment_type"] = np.where(
        is_mergedB,
        "junction-junction",
        jj_merged.get("seg_raw")
    )
    jj_merged = jj_merged.drop(columns=["seg_raw"], errors="ignore")

    othersB = after_A[~jj_family_mask].copy()
    final = pd.concat([jj_merged, othersB], ignore_index=True)

    # Hilfsspalte entfernen
    final = final.drop(columns=["_raw_type"], errors="ignore")

    # ----------------------------------------------------------
    # STEP 10 — FINAL_RECLASS (true reclassification)
    # ----------------------------------------------------------
    log.info("FINAL_RECLASS: true reclassification after all merges...")
    final_true = classify_by_endpoints(
        final.drop(columns=["segment_type"], errors="ignore"),
        round_decimals=6
    )
    final_true = mark_deadend_junction_iter(final_true, round_dec=6, type_col="segment_type")
    final_true["length_m"] = final_true[geom_col].length.astype(float)

    # Optional: deadend-deadend und loop entfernen
    final_out = final_true.copy()

    if remove_deadend_deadend:
        log.info("Removing 'deadend-deadend' segments in final output...")
        before = len(final_out)
        final_out = final_out[final_out["segment_type"] != "deadend-deadend"].copy()
        removed = before - len(final_out)
        log.info("Removed %d deadend-deadend segments in final output.", removed)

    # NEU: loop-Segmente entfernen
    if remove_loop:
        log.info("Removing 'loop' segments in final output...")
        before = len(final_out)
        final_out = final_out[final_out["segment_type"] != "loop"].copy()
        removed = before - len(final_out)
        log.info("Removed %d loop segments in final output.", removed)


    # CRS für final_out sicherstellen
    if final_out.crs is None:
        final_out.set_crs(gdf.crs, inplace=True)
        log.info(f"Set CRS on final_out to {gdf.crs}")

    # ----------------------------------------------------------
    # STEP 11 — Generate final nodes
    # ----------------------------------------------------------
    log.info("Generating final nodes from final_out...")
    nodes = generate_final_nodes(final_out)

    # CRS für nodes sicherstellen
    if nodes.crs is None:
        nodes.set_crs(gdf.crs, inplace=True)
        log.info(f"Set CRS on nodes to {gdf.crs}")

    # STEP 12 – SAVE TO POSTGIS
    segments_table_name = infdb.get_config_value(
        ["process-streets", "data", "output", "segments_table"]
    )
    nodes_table_name = infdb.get_config_value(
        ["process-streets", "data", "output",  "nodes_table"]
    )

    # Falls in der YAML nichts gesetzt ist -> Fallback
    if not segments_table_name:
        segments_table_name = "segments"
    if not nodes_table_name:
        nodes_table_name = "nodes"

    seg_table = f"{output_schema}.{segments_table_name}"
    node_table = f"{output_schema}.{nodes_table_name}"

    # Sicherstellen, dass das Ausgabeschema existiert
    try:
        log.info(f"Ensuring schema '{output_schema}' exists...")
        with engine.begin() as conn:
            conn.execute(
                text(f'CREATE SCHEMA IF NOT EXISTS "{output_schema}"')
            )
        log.info(f"Schema '{output_schema}' exists or was created.")
    except Exception as e:
        log.warning(
            f"Could not ensure schema '{output_schema}' exists "
            f"(maybe missing rights): {e}"
        )
  
    log.info(f"Writing segments to infdb → {seg_table}")
    final_out.to_postgis(
        segments_table_name,
        engine,
        if_exists="replace",
        schema=output_schema,
        index=False,
    )

    log.info(f"Writing nodes to infdb → {node_table}")
    nodes.to_postgis(
        nodes_table_name,
        engine,
        if_exists="replace",
        schema=output_schema,
        index=False,
    )

    # ----------------------------------------------------------
    # STEP 12b — OPTIONAL: write GeoJSON to local filesystem
    # ----------------------------------------------------------
    file_export_status = infdb.get_config_value(
        ["process-streets", "data", "output", "file_export", "status"]
    )
    output_dir = infdb.get_config_value(
        ["process-streets", "data", "output", "file_export", "output_dir"]
    )

    segments_geojson_name = infdb.get_config_value(
        ["process-streets", "data", "output", "file_export", "segments_geojson"]
    )
    nodes_geojson_name = infdb.get_config_value(
        ["process-streets", "data", "output", "file_export", "nodes_geojson"]
    )

    # Falls nichts gesetzt ist, aus Tabellennamen ableiten (optional)
    if not segments_geojson_name:
        segments_geojson_name = f"{segments_table_name}.geojson"
    if not nodes_geojson_name:
        nodes_geojson_name = f"{nodes_table_name}.geojson"

    # Wenn Export deaktiviert ist oder kein output_dir: überspringen
    if not output_dir or str(file_export_status).strip().lower() not in ("active", "on", "true", "1"):
        log.info("GeoJSON export inactive or no output_dir configured – skipping GeoJSON export.")
    else:
        os.makedirs(output_dir, exist_ok=True)

        seg_geojson = os.path.join(output_dir, segments_geojson_name)
        nodes_geojson = os.path.join(output_dir, nodes_geojson_name)

        log.info(f"Writing segments GeoJSON → {seg_geojson}")
        final_out.to_file(seg_geojson, driver="GeoJSON")

        log.info(f"Writing nodes GeoJSON → {nodes_geojson}")
        nodes.to_file(nodes_geojson, driver="GeoJSON")

    # ----------------------------------------------------------
    # DONE
    # ----------------------------------------------------------
    log.info("=== PROCESS-STREETS COMPLETE ===")

    return {
        "status": "success",
        "segments_out": len(final_out),
        "nodes_out": len(nodes),
        "segments_table": seg_table,
        "nodes_table": node_table
    }
  
# =========================================================
# WRAPPER CALLED FROM main.py
# =========================================================

def main(table_name, klasse_filter, apply_length_filter,
         min_length_deadend_junction, remove_deadend_deadend, remove_loop, infdb):

    def to_bool(x):
        return str(x).strip().lower() in ("true", "1", "yes", "y", "ja")

    apply_length_filter = to_bool(apply_length_filter)
    remove_deadend_deadend = to_bool(remove_deadend_deadend)
    remove_loop = to_bool(remove_loop)

    # NEU: sicherstellen, dass es ein float ist
    try:
        min_length_deadend_junction = float(min_length_deadend_junction)
    except Exception:
        # notfalls Default, damit es nicht crasht
        min_length_deadend_junction = 10.0

    if isinstance(klasse_filter, str):
        klasse_list = [k.strip() for k in klasse_filter.split(",") if k.strip()]
    elif isinstance(klasse_filter, list):
        klasse_list = [k.strip() for k in klasse_filter]
    else:
        klasse_list = None

    return run_process_streets(
        table_name,
        klasse_list,
        apply_length_filter,
        min_length_deadend_junction,
        remove_deadend_deadend,
        remove_loop,
        infdb
    )
