"""
corpus_dashboard.py
Generates a self-contained HTML dashboard from corpus statistics JSON files.
Optionally incorporates train/test overlap statistics.

Usage:
    python corpus_dashboard.py stats.json
    python corpus_dashboard.py stats.json --overlap overlap_stats.json
    python corpus_dashboard.py stats.json --overlap overlap_stats.json --output report.html --open
"""

import argparse
import json
import math
import re
import sys
import webbrowser
from pathlib import Path


# ── Palette ───────────────────────────────────────────────────────────────────

PALETTE = [
    "#7F77DD", "#378ADD", "#1D9E75", "#D85A30",
    "#639922", "#D4537E", "#BA7517", "#E24B4A", "#888780",
]

# Semantic colours for the four overlap levels
OV_COLS = {
    "token":   "#888780",   # gray   — background vocabulary
    "men_tok": "#1D9E75",   # teal   — mention tokens
    "mention": "#D85A30",   # coral  — exact surface forms
    "ident":   "#7F77DD",   # purple — concept identifiers
}
BAR_SCALE = 0.65            # Jaccard ceiling for inline bar widths


# ── Corpus statistics helpers ─────────────────────────────────────────────────

def _get(data: list, metric: str, field: str = "value", default=None):
    for item in data:
        if item.get("metric_name") == metric:
            v = item.get(field, item.get("value", default))
            if v is None:
                return default
            if isinstance(v, float) and math.isnan(v):
                return default
            return v
    return default


def _stat(data: list, metric: str, stat: str, default=None):
    val = _get(data, metric)
    if not isinstance(val, dict):
        return default
    v = val.get(stat, default)
    if v is None:
        return default
    try:
        if math.isnan(float(v)):
            return default
    except (TypeError, ValueError):
        pass
    return v


def _entropy(data: list) -> float:
    dist = _get(data, "label_distribution") or {}
    probs = [v for v in dist.values() if v and v > 0]
    return -sum(p * math.log2(p) for p in probs)


def _id_info(data: list) -> dict:
    dist = _get(data, "identifier_resource_distribution") or {}
    named = [k for k in dist if k not in ("null", "<NIL>", None)]
    null_frac = dist.get("null", 0) + dist.get("<NIL>", 0)
    if not named:
        return dict(has_ids=False, partial=False, label="none", css_class="no")
    if null_frac > 0.05:
        return dict(has_ids=True, partial=True,
                    label=f"{', '.join(named)} (partial)", css_class="part")
    return dict(has_ids=True, partial=False,
                label=", ".join(named), css_class="yes")


def _total_ann(data: list) -> int:
    details = _get(data, "label_distribution", "details") or {}
    counts  = details.get("counts", {})
    if counts:
        return sum(counts.values())
    apd = _stat(data, "annotations_per_document_stats", "mean", 0)
    dc  = _get(data, "document_count", default=0)
    return int(round(apd * dc))


def summarise(name: str, data: list) -> dict:
    ld   = _get(data, "label_distribution") or {}
    info = _id_info(data)
    return dict(
        name        = name.replace("_corpus", "").replace("_", "-"),
        raw_name    = name,
        doc_count   = _get(data, "document_count", default=0),
        token_count = _get(data, "token_count", default=0),
        n_types     = len(ld),
        types       = list(ld.keys()),
        entropy     = round(_entropy(data), 2),
        total_ann   = _total_ann(data),
        ann_per_doc = round(_stat(data, "annotations_per_document_stats",   "mean", 0), 2),
        men_per_doc = round(_stat(data, "unique_mentions_per_document_stats","mean", 0), 2),
        ids_per_doc = round(_stat(data, "unique_identifiers_per_document_stats","mean", 0), 2),
        ambiguity   = round(_stat(data, "ambiguity_degree_stats", "mean", 1.0), 3),
        variation   = _stat(data, "variation_degree_stats", "mean"),
        id_vocab    = info["label"],
        id_class    = info["css_class"],
        has_ids     = info["has_ids"],
        overlap     = None,         # filled in later by attach_overlaps()
    )


# ── Overlap helpers ───────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Normalize a name to lowercase alphanumeric for cross-file matching."""
    s = s.lower()
    for suf in ("_corpus", "_train", "_test", "_dev"):
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    return re.sub(r"[^a-z0-9]", "", s)


def _corpus_from_key(key: str) -> str:
    """Extract corpus name from overlap key '(AnatEM_train, AnatEM_test)'."""
    m = re.match(r"\((\w+?)_(?:train|test|dev)", key)
    return m.group(1) if m else key.strip("()")


def _ov_val(metrics: list, name: str):
    for m in metrics:
        if m["metric_name"] == name:
            v = m.get("value")
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            return v
    return None


def _split_sizes(metrics: list) -> tuple:
    for m in metrics:
        if m["metric_name"] == "token_overlap":
            d = m.get("details", {})
            tr = next((v for k, v in d.items() if "train" in k.lower()), 0)
            te = next((v for k, v in d.items()
                       if "test" in k.lower() or "dev" in k.lower()), 0)
            return int(tr), int(te)
    return 0, 0


def load_overlaps(path: str) -> dict:
    """Parse overlap JSON → dict keyed by normalized corpus name."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    result = {}
    for key, metrics in raw.items():
        norm_key = _norm(_corpus_from_key(key))
        tr, te   = _split_sizes(metrics)
        result[norm_key] = {
            "token_overlap":         _ov_val(metrics, "token_overlap"),
            "mention_token_overlap": _ov_val(metrics, "mention_token_overlap"),
            "mention_overlap":       _ov_val(metrics, "mention_overlap"),
            "identifier_overlap":    _ov_val(metrics, "identifier_overlap"),
            "train_size": tr,
            "test_size":  te,
        }
    return result


def attach_overlaps(corpora: list, overlaps: dict) -> None:
    """Attach overlap data to each corpus dict in-place."""
    for c in corpora:
        c["overlap"] = overlaps.get(_norm(c["raw_name"]))


# ── JSON loading ──────────────────────────────────────────────────────────────

def load_corpora(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [summarise(name, data) for name, data in raw.items()]


# ── Chart data helpers ────────────────────────────────────────────────────────

def _sorted_hbar(corpora, key, colours):
    pairs = [(c["name"], c.get(key), colours[i % len(colours)])
             for i, c in enumerate(corpora)]
    pairs.sort(key=lambda x: (x[1] is None, -(x[1] or 0)))
    labels = json.dumps([p[0] for p in pairs])
    data   = json.dumps([p[1] if p[1] is not None else 0 for p in pairs])
    bg     = json.dumps([
        col if (val is not None and val > 0) else col + "33"
        for _, val, col in pairs
    ])
    return labels, data, bg


def _all_hbar(corpora, key, colours):
    labels = json.dumps([c["name"] for c in corpora])
    data   = json.dumps([c.get(key, 0) or 0 for c in corpora])
    bg     = json.dumps([colours[i % len(colours)] for i in range(len(corpora))])
    return labels, data, bg


def _variation_data(corpora, colours):
    pairs = [(c["name"], c["variation"], colours[i % len(colours)])
             for i, c in enumerate(corpora) if c["variation"] is not None]
    pairs.sort(key=lambda x: -x[1])
    return (json.dumps([p[0] for p in pairs]),
            json.dumps([round(p[1], 2) for p in pairs]),
            json.dumps([p[2] for p in pairs]))


# ── Overlap panel builders ────────────────────────────────────────────────────

def _bar_td(val, col: str) -> str:
    if val is None:
        return "<td class='na'>—</td>"
    w = min(val / BAR_SCALE, 1.0) * 100
    return (
        f"<td class='bar-cell'>"
        f"<div class='bar-wrap'>"
        f"<div class='bar-bg'><div class='bar-fill' style='width:{w:.0f}%;background:{col}'></div></div>"
        f"<span class='bar-val'>{val * 100:.1f}%</span>"
        f"</div></td>"
    )


def build_overlap_table_rows(corpora: list) -> str:
    with_ov = [c for c in corpora if c.get("overlap")]
    with_ov.sort(key=lambda c: -(c["overlap"].get("token_overlap") or 0))
    rows = []
    for c in with_ov:
        ov = c["overlap"]
        tr = f"{ov['train_size']:,}" if ov.get("train_size") else "—"
        te = f"{ov['test_size']:,}" if ov.get("test_size") else "—"
        rows.append(
            "<tr>"
            f"<td class='l'><strong>{c['name']}</strong></td>"
            f"<td>{tr} → {te}</td>"
            + _bar_td(ov.get("token_overlap"),         OV_COLS["token"])
            + _bar_td(ov.get("mention_token_overlap"),  OV_COLS["men_tok"])
            + _bar_td(ov.get("mention_overlap"),        OV_COLS["mention"])
            + _bar_td(ov.get("identifier_overlap"),     OV_COLS["ident"])
            + f"<td><span class='pill p-{c['id_class']}'>{c['id_vocab']}</span></td>"
            "</tr>"
        )
    return "\n".join(rows)


def cascade_datasets_js(corpora: list, colours: list) -> str:
    with_ov = [c for c in corpora if c.get("overlap")]
    ds = []
    for i, c in enumerate(with_ov):
        ov   = c["overlap"]
        pts  = [ov.get("token_overlap"), ov.get("mention_token_overlap"),
                ov.get("mention_overlap"), ov.get("identifier_overlap")]
        pcts = [round(v * 100, 1) if v is not None else None for v in pts]
        col  = colours[i % len(colours)]
        ds.append(
            "{"
            + f"label:{json.dumps(c['name'])},"
            + f"data:{json.dumps(pcts)},"
            + f"borderColor:{json.dumps(col)},"
            + f"backgroundColor:{json.dumps(col)},"
            + f"pointRadius:{json.dumps([4 if v is not None else 0 for v in pts])},"
            + "pointHoverRadius:[6,6,6,6],"
            + "borderWidth:2,spanGaps:false,tension:0.1"
            + "}"
        )
    return "[" + ",\n".join(ds) + "]"


def build_overlap_panels(corpora: list) -> str:
    """Return HTML strings for panels p6 and p7."""
    rows = build_overlap_table_rows(corpora)
    tc   = OV_COLS["token"]
    mc   = OV_COLS["men_tok"]
    sc   = OV_COLS["mention"]
    ic   = OV_COLS["ident"]
    return (
        f'<div class="panel" id="p6">'
        f'<div class="leg">'
        f'<span class="li"><span class="lc" style="background:{tc}"></span>Token vocabulary</span>'
        f'<span class="li"><span class="lc" style="background:{mc}"></span>Mention tokens</span>'
        f'<span class="li"><span class="lc" style="background:{sc}"></span>Mention strings</span>'
        f'<span class="li"><span class="lc" style="background:{ic}"></span>Identifiers</span>'
        f'</div>'
        f'<div style="overflow-x:auto">'
        f'<table><thead><tr>'
        f'<th class="l">Corpus</th>'
        f'<th>Split<span class="sub">train → test tokens</span></th>'
        f'<th>Token vocab<span class="sub">Jaccard</span></th>'
        f'<th>Mention tokens<span class="sub">Jaccard</span></th>'
        f'<th>Mention strings<span class="sub">Jaccard</span></th>'
        f'<th>Identifiers<span class="sub">Jaccard</span></th>'
        f'<th>ID vocab</th>'
        f'</tr></thead><tbody>'
        f'{rows}'
        f'</tbody></table></div>'
        f'<div class="fn">'
        f'All values are Jaccard similarity (intersection / union) of unique elements between splits.<br>'
        f'<strong>Token vocab</strong> — general vocabulary; '
        f'<strong>Mention tokens</strong> — tokens inside entity spans; '
        f'<strong>Mention strings</strong> — exact entity surface forms; '
        f'<strong>Identifiers</strong> — concept-level overlap (n/a without normalization).'
        f'</div></div>\n'
        f'<div class="panel" id="p7">'
        f'<div class="leg" id="cascLeg"></div>'
        f'<div class="cw" style="height:380px">'
        f'<canvas id="c7" role="img"'
        f' aria-label="Line chart: Jaccard overlap at four abstraction levels per corpus.">'
        f'Overlap cascade from token vocabulary to identifier level.</canvas>'
        f'</div>'
        f'<p class="note">Each line traces one corpus across four abstraction levels.'
        f' The consistent drop from token vocabulary to mention strings shows that general vocabulary'
        f' is always more shared than specific entity surface forms.'
        f' Lines terminating before the identifier level indicate corpora without concept normalization.'
        f' Lines that remain high at the identifier level indicate greater concept-level train-test leakage.</p>'
        f'</div>'
    )


# ── HTML assemblers ───────────────────────────────────────────────────────────

def build_legend_html(corpora: list, colours: list) -> str:
    return "".join(
        f'<span class="li"><span class="lc" style="background:{colours[i % len(colours)]}"></span>'
        f'{c["name"]}</span>'
        for i, c in enumerate(corpora)
    )


def build_table_rows(corpora: list) -> str:
    rows = []
    for c in corpora:
        var = f"{c['variation']:.2f}" if c["variation"] is not None else \
              '<span style="color:var(--color-text-tertiary)">n/a</span>'
        ids = f"{c['ids_per_doc']:.2f}" if c["has_ids"] else \
              '<span style="color:var(--color-text-tertiary)">—</span>'
        rows.append(
            "<tr>"
            f"<td class='l'><strong>{c['name']}</strong></td>"
            f"<td class='r'>{c['doc_count']:,}</td>"
            f"<td class='r'>{c['token_count']:,}</td>"
            f"<td class='r'>{c['n_types']}</td>"
            f"<td class='r'>{c['total_ann']:,}</td>"
            f"<td class='r'>{c['ann_per_doc']:.1f}</td>"
            f"<td class='r'>{c['men_per_doc']:.1f}</td>"
            f"<td class='r'>{ids}</td>"
            f"<td><span class='pill p-{c['id_class']}'>{c['id_vocab']}</span></td>"
            f"<td class='r'>{c['ambiguity']:.3f}</td>"
            f"<td class='r'>{var}</td>"
            f"<td class='r'>{c['entropy']:.2f}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def build_id_status_html(corpora: list) -> str:
    return "".join(
        f'<span style="margin-right:10px"><strong>{c["name"]}</strong> '
        f'<span class="pill p-{c["id_class"]}">{c["id_vocab"]}</span></span>'
        for c in corpora
    )


# ── HTML template ─────────────────────────────────────────────────────────────

HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Corpus Statistics Dashboard</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:system-ui,-apple-system,sans-serif;font-size:14px;
        color:#1a1a1a;background:#f8f7f4;padding:2rem}}
  h1{{font-size:20px;font-weight:500;margin-bottom:.25rem}}
  .sub-h{{font-size:13px;color:#666;margin-bottom:1.5rem}}
  .mg{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:1.5rem}}
  .mc{{background:#fff;border:.5px solid #ddd;border-radius:8px;padding:12px 14px}}
  .ml{{font-size:12px;color:#666;margin-bottom:4px}}
  .mv{{font-size:21px;font-weight:500}}
  .ms{{font-size:11px;color:#aaa;margin:2px 0 0}}
  .leg{{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px}}
  .li{{display:flex;align-items:center;gap:5px;font-size:12px;color:#555}}
  .lc{{width:10px;height:10px;border-radius:2px;flex-shrink:0}}
  .tabs{{display:flex;flex-wrap:wrap;border-bottom:1px solid #ddd;margin-bottom:1.5rem}}
  .tab{{padding:8px 14px;font-size:13px;cursor:pointer;border:none;background:none;
         color:#666;border-bottom:2px solid transparent;margin-bottom:-1px;
         font-family:system-ui,-apple-system,sans-serif}}
  .tab.sel{{color:#111;border-bottom-color:#7F77DD;font-weight:500}}
  .panel{{display:none}}.panel.sel{{display:block}}
  .cw{{position:relative;width:100%}}
  .two{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}}
  .sec{{font-size:13px;font-weight:500;margin-bottom:10px;color:#111}}
  .note{{font-size:12px;color:#666;margin-top:10px;line-height:1.6;
          border-left:2px solid #ddd;padding-left:10px}}
  .pill{{display:inline-block;font-size:11px;padding:2px 7px;border-radius:20px;font-weight:500}}
  .p-yes{{background:#d4edda;color:#155724}}
  .p-part{{background:#fff3cd;color:#856404}}
  .p-no{{background:#f8d7da;color:#721c24}}
  table{{width:100%;border-collapse:collapse;font-size:12.5px;background:#fff;
          border-radius:8px;overflow:hidden;border:.5px solid #ddd}}
  thead tr{{border-bottom:1.5px solid #ccc;background:#f1efe8}}
  th{{padding:8px 10px;font-weight:500;color:#555;white-space:nowrap;vertical-align:bottom;line-height:1.4}}
  th.l{{text-align:left}} th.r{{text-align:right}}
  th .sub{{display:block;font-size:10px;font-weight:400;color:#aaa;margin-top:2px}}
  tbody tr{{border-bottom:.5px solid #eee}}
  tbody tr:hover{{background:#fafafa}}
  td{{padding:7px 10px;white-space:nowrap}}
  td.l{{text-align:left}} td.r{{text-align:right;font-variant-numeric:tabular-nums}}
  td.na{{color:#aaa;text-align:center}}
  .bar-cell{{padding:6px 10px;vertical-align:middle;min-width:100px}}
  .bar-wrap{{display:flex;align-items:center;gap:6px}}
  .bar-bg{{flex:1;background:#e5e3dc;border-radius:2px;height:6px;overflow:hidden}}
  .bar-fill{{height:6px;border-radius:2px}}
  .bar-val{{font-size:11.5px;min-width:36px;text-align:right;font-variant-numeric:tabular-nums;color:#333}}
  .fn{{font-size:11.5px;color:#666;margin-top:1rem;line-height:1.7;
        border-top:.5px solid #ddd;padding-top:.75rem}}
  .fn sup{{font-size:9px;vertical-align:super}}
  @media(prefers-color-scheme:dark){{
    body{{color:#e8e6e0;background:#1c1c1a}}
    .mc{{background:#2a2a28;border-color:#3a3a38}}
    .mv{{color:#e8e6e0}} .ms,.ml{{color:#888}}
    .li{{color:#aaa}} table{{background:#2a2a28;border-color:#3a3a38}}
    thead tr{{background:#333330}} th{{color:#aaa}}
    tbody tr:hover{{background:#333330}}
    .tabs{{border-color:#3a3a38}} .tab{{color:#888}}
    .tab.sel{{color:#e8e6e0}}
    .note{{border-left-color:#444;color:#aaa}}
    .fn{{color:#aaa;border-top-color:#3a3a38}}
    td.na{{color:#555}} .bar-bg{{background:#3a3a38}}
    .bar-val{{color:#ccc}} .sec{{color:#e8e6e0}}
    .p-yes{{background:#0f3d1e;color:#6fcf97}}
    .p-part{{background:#3d2e00;color:#f0c040}}
    .p-no{{background:#3d0f0f;color:#e57373}}
  }}
</style>
</head>
<body>
<h1>Corpus Statistics Dashboard</h1>
<p class="sub-h">Biomedical named entity annotation corpora — comparative analysis</p>

<div class="mg">
  <div class="mc"><p class="ml">Corpora analyzed</p><p class="mv">{n_corpora}</p></div>
  <div class="mc"><p class="ml">With concept identifiers</p><p class="mv">{n_with_ids} / {n_corpora}</p></div>
  <div class="mc"><p class="ml">Ann/doc range</p><p class="mv">{ann_min} – {ann_max}</p></div>
  <div class="mc"><p class="ml">Ambiguity range</p><p class="mv">{amb_min} – {amb_max}</p></div>
</div>

<div class="leg">{legend_html}</div>

<div class="tabs" id="tabs">
  <button class="tab sel" data-p="p1">Annotation density</button>
  <button class="tab" data-p="p2">Identifier coverage</button>
  <button class="tab" data-p="p3">Difficulty indicators</button>
  <button class="tab" data-p="p4">Entity type profile</button>
  <button class="tab" data-p="p5">Summary table</button>
  {overlap_tabs}
</div>

<div class="panel sel" id="p1">
  <div class="cw" style="height:{h_ann}px">
    <canvas id="c1" role="img" aria-label="Mean annotations per document, log scale.">
      Annotation density varies widely from under 1 to over 250 per document.
    </canvas>
  </div>
  <p class="note">Log scale. NLM-Chem annotates full-text articles; BioID uses figure captions.
  Full-text scope explains high-density outliers.</p>
</div>

<div class="panel" id="p2">
  <div style="margin-bottom:12px;font-size:13px">{id_status_html}</div>
  <div class="cw" style="height:{h_ann}px">
    <canvas id="c2" role="img" aria-label="Unique identifiers per document.">
      Three corpora have no concept identifiers and cannot support entity linking evaluation.
    </canvas>
  </div>
  <p class="note">Faded bars — zero or negligible identifier coverage.
  These corpora benchmark span detection only, not entity normalization.</p>
</div>

<div class="panel" id="p3">
  <div class="two">
    <div>
      <p class="sec">Ambiguity — identifiers per mention</p>
      <div class="cw" style="height:320px">
        <canvas id="c3" role="img" aria-label="Ambiguity scores per corpus.">
          Ambiguity is low and uniform (1.00–1.07) across all corpora.
        </canvas>
      </div>
    </div>
    <div>
      <p class="sec">Variation — surface forms per concept</p>
      <div class="cw" style="height:320px">
        <canvas id="c4" role="img" aria-label="Variation scores for corpora with concept identifiers.">
          CellLink highest at 4.08; BC5CDR and NLM-Chem lowest at 2.42.
        </canvas>
      </div>
    </div>
  </div>
  <p class="note">Ambiguity near 1.0 indicates low polysemy; not a differentiating factor across
  these corpora. Variation shown only for corpora with concept-level identifiers.</p>
</div>

<div class="panel" id="p4">
  <div class="two">
    <div>
      <p class="sec">Distinct entity type labels</p>
      <div class="cw" style="height:320px">
        <canvas id="c5" role="img" aria-label="Number of distinct entity type labels per corpus.">
          AnatEM has 12 types; four corpora annotate a single entity type.
        </canvas>
      </div>
    </div>
    <div>
      <p class="sec">Label entropy (bits)</p>
      <div class="cw" style="height:320px">
        <canvas id="c6" role="img" aria-label="Shannon entropy of label distributions.">
          Single-entity corpora have 0 bits; AnatEM highest at 2.84 bits.
        </canvas>
      </div>
    </div>
  </div>
  <p class="note">Entropy = 0 for single-entity corpora. Higher entropy indicates more
  balanced coverage across entity types.</p>
</div>

<div class="panel" id="p5">
  <div style="overflow-x:auto">
  <table>
  <thead>
    <tr>
      <th class="l">Corpus</th>
      <th class="r">Docs</th>
      <th class="r">Tokens</th>
      <th class="r">Types</th>
      <th class="r">Total ann.</th>
      <th class="r">Ann/doc</th>
      <th class="r">Men/doc</th>
      <th class="r">IDs/doc</th>
      <th>ID vocabulary</th>
      <th class="r">Ambiguity<sup>a</sup></th>
      <th class="r">Variation<sup>b</sup></th>
      <th class="r">Entropy<sup>c</sup></th>
    </tr>
  </thead>
  <tbody>
  {table_rows}
  </tbody>
  </table>
  </div>
  <div class="fn">
    <sup>a</sup> <strong>Ambiguity</strong> — mean concept identifiers per unique mention string.<br>
    <sup>b</sup> <strong>Variation</strong> — mean surface forms per concept identifier; only for corpora with IDs.<br>
    <sup>c</sup> <strong>Entropy</strong> — Shannon entropy of label distribution in bits; 0 = single entity type.
  </div>
</div>

{overlap_panels}

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const dk = matchMedia('(prefers-color-scheme:dark)').matches;
const tc = dk ? 'rgba(255,255,255,0.55)' : 'rgba(0,0,0,0.45)';
const gc = dk ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.05)';

function hbar(el, labels, data, bg, xLabel, xOpts={{}}) {{
  return new Chart(el, {{
    type: 'bar',
    data: {{ labels, datasets: [{{ data, backgroundColor: bg, borderWidth:0, borderRadius:3 }}] }},
    options: {{
      responsive:true, maintainAspectRatio:false, indexAxis:'y',
      plugins: {{ legend:{{display:false}},
        tooltip:{{ callbacks:{{ label: ctx => ` ${{ctx.parsed.x.toFixed(2)}}` }} }} }},
      scales: {{
        x: {{ ...xOpts,
              title:{{display:true,text:xLabel,color:tc,font:{{size:11}}}},
              ticks:{{color:tc,font:{{size:11}}}}, grid:{{color:gc}} }},
        y: {{ ticks:{{color:tc,font:{{size:12}}}}, grid:{{color:gc}} }}
      }}
    }}
  }});
}}

new Chart('c1', {{
  type: 'bar',
  data: {{ labels:{c1_labels}, datasets:[{{ data:{c1_data}, backgroundColor:{c1_bg},
    borderWidth:0, borderRadius:3 }}] }},
  options: {{
    responsive:true, maintainAspectRatio:false, indexAxis:'y',
    plugins: {{ legend:{{display:false}},
      tooltip:{{ callbacks:{{ label: ctx => ` ${{ctx.parsed.x.toFixed(1)}}` }} }} }},
    scales: {{
      x: {{ type:'logarithmic',
             title:{{display:true,text:'Mean annotations per document (log scale)',color:tc,font:{{size:11}}}},
             ticks:{{color:tc,font:{{size:11}},callback: v=>[0.1,1,10,100,1000].includes(v)?v:''}},
             grid:{{color:gc}} }},
      y: {{ ticks:{{color:tc,font:{{size:12}}}}, grid:{{color:gc}} }}
    }}
  }}
}});

const inited = {{}};

function initC2() {{
  hbar('c2',{c2_labels},{c2_data},{c2_bg},'Mean unique identifiers per document');
}}
function initC3() {{
  new Chart('c3', {{
    type:'bar',
    data:{{ labels:{c3_labels}, datasets:[{{ data:{c3_data}, backgroundColor:{c3_bg},
      borderWidth:0, borderRadius:3 }}] }},
    options:{{
      responsive:true, maintainAspectRatio:false, indexAxis:'y',
      plugins:{{ legend:{{display:false}},
        tooltip:{{ callbacks:{{ label: ctx=>` ${{ctx.parsed.x.toFixed(3)}}` }} }} }},
      scales:{{
        x:{{ min:{amb_min_scale}, max:{amb_max_scale},
              title:{{display:true,text:'Mean identifiers per mention',color:tc,font:{{size:11}}}},
              ticks:{{color:tc,font:{{size:11}},callback:v=>v.toFixed(2)}}, grid:{{color:gc}} }},
        y:{{ ticks:{{color:tc,font:{{size:11}}}}, grid:{{color:gc}} }}
      }}
    }}
  }});
}}
function initC4() {{
  hbar('c4',{c4_labels},{c4_data},{c4_bg},'Mean surface forms per concept');
}}
function initC5() {{
  hbar('c5',{c5_labels},{c5_data},{c5_bg},'Distinct entity type labels',
    {{ticks:{{stepSize:1,color:tc,font:{{size:11}}}}}});
}}
function initC6() {{
  hbar('c6',{c6_labels},{c6_data},{c6_bg},'Shannon entropy (bits)');
}}

const cascadeDatasets = {cascade_datasets};

function initC7() {{
  if (!cascadeDatasets.length) return;
  const leg = document.getElementById('cascLeg');
  if (leg) leg.innerHTML = cascadeDatasets.map(d =>
    `<span class="li"><span class="lc" style="background:${{d.borderColor}}"></span>${{d.label}}</span>`
  ).join('');
  new Chart('c7', {{
    type: 'line',
    data: {{
      labels: ['Token vocab','Mention tokens','Mention strings','Identifiers'],
      datasets: cascadeDatasets
    }},
    options: {{
      responsive:true, maintainAspectRatio:false,
      plugins:{{ legend:{{display:false}},
        tooltip:{{ callbacks:{{ label: ctx =>
          ` ${{ctx.dataset.label}}: ${{ctx.parsed.y!==null ? ctx.parsed.y.toFixed(1)+'%' : 'n/a'}}` }} }} }},
      scales:{{
        x:{{ ticks:{{color:tc,font:{{size:12}}}}, grid:{{color:gc}} }},
        y:{{ min:0, max:65,
               title:{{display:true,text:'Jaccard overlap (%)',color:tc,font:{{size:11}}}},
               ticks:{{color:tc,font:{{size:11}},callback:v=>v+'%'}},
               grid:{{color:gc}} }}
      }}
    }}
  }});
}}

const panels = {{
  p2: initC2,
  p3: () => {{ initC3(); initC4(); }},
  p4: () => {{ initC5(); initC6(); }},
  p7: initC7,
}};

document.getElementById('tabs').addEventListener('click', e => {{
  const btn = e.target.closest('.tab');
  if (!btn) return;
  document.querySelectorAll('.tab').forEach(b => b.classList.remove('sel'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('sel'));
  btn.classList.add('sel');
  const pid = btn.dataset.p;
  document.getElementById(pid).classList.add('sel');
  if (panels[pid] && !inited[pid]) {{ inited[pid]=true; panels[pid](); }}
}});
</script>
</body>
</html>
"""


# ── Main build ────────────────────────────────────────────────────────────────

def build_html(corpora: list) -> str:
    colours   = PALETTE[:]
    n         = len(corpora)
    has_ov    = any(c.get("overlap") for c in corpora)

    n_with_ids = sum(1 for c in corpora if c["has_ids"])
    ann_vals   = [c["ann_per_doc"] for c in corpora]
    amb_vals   = [c["ambiguity"]   for c in corpora]
    h_ann      = max(300, n * 40 + 80)

    c1_l, c1_d, c1_b = _sorted_hbar(corpora, "ann_per_doc", colours)
    c2_l, c2_d, c2_b = _sorted_hbar(corpora, "ids_per_doc", colours)
    c3_l, c3_d, c3_b = _all_hbar(corpora, "ambiguity", colours)
    c4_l, c4_d, c4_b = _variation_data(corpora, colours)
    c5_l, c5_d, c5_b = _sorted_hbar(corpora, "n_types",  colours)
    c6_l, c6_d, c6_b = _sorted_hbar(corpora, "entropy",  colours)

    amb_lo = round(max(0.95, min(amb_vals) - 0.02), 2)
    amb_hi = round(max(amb_vals) + 0.02, 2)

    if has_ov:
        overlap_tabs   = ('\n  <button class="tab" data-p="p6">Train-test overlap</button>'
                          '\n  <button class="tab" data-p="p7">Cascade view</button>')
        overlap_panels = build_overlap_panels(corpora)
        cascade_ds     = cascade_datasets_js(corpora, colours)
    else:
        overlap_tabs   = ""
        overlap_panels = ""
        cascade_ds     = "[]"

    return HTML.format(
        n_corpora       = n,
        n_with_ids      = n_with_ids,
        ann_min         = f"{min(ann_vals):.1f}",
        ann_max         = f"{max(ann_vals):.1f}",
        amb_min         = f"{min(amb_vals):.2f}",
        amb_max         = f"{max(amb_vals):.2f}",
        amb_min_scale   = amb_lo,
        amb_max_scale   = amb_hi,
        h_ann           = h_ann,
        legend_html     = build_legend_html(corpora, colours),
        id_status_html  = build_id_status_html(corpora),
        table_rows      = build_table_rows(corpora),
        overlap_tabs    = overlap_tabs,
        overlap_panels  = overlap_panels,
        cascade_datasets= cascade_ds,
        c1_labels=c1_l, c1_data=c1_d, c1_bg=c1_b,
        c2_labels=c2_l, c2_data=c2_d, c2_bg=c2_b,
        c3_labels=c3_l, c3_data=c3_d, c3_bg=c3_b,
        c4_labels=c4_l, c4_data=c4_d, c4_bg=c4_b,
        c5_labels=c5_l, c5_data=c5_d, c5_bg=c5_b,
        c6_labels=c6_l, c6_data=c6_d, c6_bg=c6_b,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate an HTML corpus statistics dashboard."
    )
    parser.add_argument("input",
                        help="Corpus statistics JSON file")
    parser.add_argument("--overlap", "-v", default=None, metavar="FILE",
                        help="Optional train/test overlap statistics JSON file")
    parser.add_argument("--output", "-o", default=None,
                        help="Output HTML path (default: <input stem>_dashboard.html)")
    parser.add_argument("--open", action="store_true",
                        help="Open the generated file in the default browser")
    args = parser.parse_args()

    in_path  = Path(args.input)
    out_path = Path(args.output) if args.output else \
               in_path.with_name(in_path.stem + "_dashboard.html")

    print(f"Loading stats:   {in_path}")
    try:
        corpora = load_corpora(str(in_path))
    except FileNotFoundError:
        print(f"Error: file not found — {in_path}", file=sys.stderr); sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON — {e}", file=sys.stderr); sys.exit(1)

    if args.overlap:
        ov_path = Path(args.overlap)
        print(f"Loading overlap: {ov_path}")
        try:
            overlaps = load_overlaps(str(ov_path))
            attach_overlaps(corpora, overlaps)
            matched = sum(1 for c in corpora if c.get("overlap"))
            print(f"Overlap matched: {matched} / {len(corpora)} corpora")
        except FileNotFoundError:
            print(f"Warning: overlap file not found — {ov_path}", file=sys.stderr)
        except json.JSONDecodeError as e:
            print(f"Warning: invalid overlap JSON — {e}", file=sys.stderr)

    print(f"Corpora:         {len(corpora)} ({', '.join(c['name'] for c in corpora)})")
    html = build_html(corpora)
    out_path.write_text(html, encoding="utf-8")
    print(f"Written:         {out_path}")

    if args.open:
        webbrowser.open(out_path.resolve().as_uri())


if __name__ == "__main__":
    main()
