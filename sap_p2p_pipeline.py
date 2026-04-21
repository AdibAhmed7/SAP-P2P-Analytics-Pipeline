"""
SAP MM — Procure-to-Pay (P2P) Analytics Pipeline
=================================================
A Python simulation of the SAP MM P2P data pipeline, replicating the
relational structure of four core SAP MM/FI tables:

    EKKO  — Purchase Order Header
    EKPO  — Purchase Order Item
    MSEG  — Material Document (Goods Movements)
    RBKP  — Invoice Receipt Header (Accounts Payable)

Author  : KIIT SAP Program — B6 Batch
Program : SAP Data Analytics | ExcelR & KIIT University
Domain  : SAP MM · ERP Systems · Procurement Analytics
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# 0.  REPRODUCIBILITY SEED
# ─────────────────────────────────────────────────────────────────────────────
np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# 1.  DATA GENERATION — Simulate SAP MM Tables
# ─────────────────────────────────────────────────────────────────────────────

N_PO = 300          # Number of Purchase Orders (EKKO rows)
GR_RATE   = 0.87    # 87 % of POs get a Goods Receipt
INV_RATE  = 0.82    # 82 % of GR POs get an Invoice posted
SLA_DAYS  = 7       # Procurement SLA: PO → GR within 7 days

# Reference master data
VENDORS = {
    "V001": "Bharat Raw Materials Pvt. Ltd.",
    "V002": "IndoSteel Traders",
    "V003": "Asian Polymers Ltd.",
    "V004": "National Electrical Supplies",
    "V005": "Prime Office Solutions",
}

MATERIALS = {
    "RM-STEEL":    {"desc": "Cold Rolled Steel Sheet",    "price_range": (75,  95)},
    "RM-POLYMER":  {"desc": "Industrial Grade Polymer",   "price_range": (45,  65)},
    "RM-COPPER":   {"desc": "Copper Wire Coil",           "price_range": (420, 480)},
    "MRO-LUBRIC":  {"desc": "Industrial Lubricant (L)",   "price_range": (18,  28)},
    "MRO-SAFETY":  {"desc": "Safety Equipment Kit",       "price_range": (850, 1200)},
    "PKG-CARTON":  {"desc": "Corrugated Carton Box",      "price_range": (12,  20)},
    "ELE-MOTOR":   {"desc": "Electric Motor 5HP",         "price_range": (3200, 4500)},
    "MRO-SPARE":   {"desc": "Machine Spare Parts",        "price_range": (200, 800)},
}

PURCH_ORGS = ["PO01", "PO02"]
PLANTS     = ["IN01", "IN02"]
PO_TYPES   = ["NB", "NB", "NB", "UB", "FO"]   # NB=standard weighted

# ── EKKO: Purchase Order Header ──────────────────────────────────────────────
po_ids = [f"45{str(i).zfill(8)}" for i in range(1, N_PO + 1)]

ekko = pd.DataFrame({
    "EBELN":  po_ids,
    "LIFNR":  np.random.choice(list(VENDORS.keys()), N_PO),
    "EKORG":  np.random.choice(PURCH_ORGS, N_PO),
    "WERKS":  np.random.choice(PLANTS, N_PO),
    "BSART":  np.random.choice(PO_TYPES, N_PO),
    "BEDAT":  pd.date_range("2024-01-01", periods=N_PO, freq="1D"),
    "NETWR":  np.zeros(N_PO),   # filled after items
    "WAERS":  "INR",
})
# Shuffle dates more naturally
ekko["BEDAT"] = pd.to_datetime("2024-01-01") + pd.to_timedelta(
    np.random.randint(0, 365, N_PO), unit="D"
)
ekko = ekko.sort_values("BEDAT").reset_index(drop=True)
ekko["EBELN"] = [f"45{str(i).zfill(8)}" for i in range(1, N_PO + 1)]

# ── EKPO: Purchase Order Item (1:N with EKKO) ────────────────────────────────
ekpo_rows = []
mat_keys = list(MATERIALS.keys())

for _, row in ekko.iterrows():
    n_items = np.random.choice([1, 2, 3, 4], p=[0.45, 0.30, 0.15, 0.10])
    chosen_mats = np.random.choice(mat_keys, n_items, replace=False)
    for pos_idx, mat in enumerate(chosen_mats, start=1):
        lo, hi = MATERIALS[mat]["price_range"]
        unit_price = round(np.random.uniform(lo, hi), 2)
        qty = np.random.randint(10, 200)
        ekpo_rows.append({
            "EBELN":  row["EBELN"],
            "EBELP":  str(pos_idx * 10).zfill(5),  # line item: 00010, 00020 …
            "MATNR":  mat,
            "MATKL":  mat.split("-")[0],
            "MENGE":  qty,
            "MEINS":  "KG" if mat.startswith("RM") else "EA",
            "NETPR":  unit_price,
            "NETWR":  round(unit_price * qty, 2),
            "WERKS":  row["WERKS"],
        })

ekpo = pd.DataFrame(ekpo_rows)

# Back-fill EKKO.NETWR = sum of its item values
po_totals = ekpo.groupby("EBELN")["NETWR"].sum()
ekko["NETWR"] = ekko["EBELN"].map(po_totals)

# ── MSEG: Material Document / Goods Receipt (movement type 101) ──────────────
# ~87 % of POs get a GR; lead time = PO date + random 1-25 days
gr_mask = np.random.rand(N_PO) < GR_RATE
gr_po_ids = ekko.loc[gr_mask, "EBELN"].values
gr_po_dates = ekko.loc[gr_mask, "BEDAT"].values

mseg_rows = []
for ebeln, po_date in zip(gr_po_ids, gr_po_dates):
    lead_days = int(np.random.choice(
        range(1, 26),
        p=np.array([8,10,12,14,16,14,12,10,8,7,6,5,5,5,4,4,3,3,3,2,2,2,2,1,1],
                   dtype=float) / 159
    ))
    gr_date = pd.Timestamp(po_date) + pd.Timedelta(days=lead_days)
    items = ekpo[ekpo["EBELN"] == ebeln]
    for _, item in items.iterrows():
        mseg_rows.append({
            "MBLNR":   f"50{str(np.random.randint(10000000, 99999999))}",
            "EBELN":   ebeln,
            "EBELP":   item["EBELP"],
            "MATNR":   item["MATNR"],
            "MENGE":   item["MENGE"],
            "BWART":   "101",           # GR for PO
            "BUDAT":   gr_date,         # Posting date = GR date
            "WERKS":   item["WERKS"],
        })

mseg = pd.DataFrame(mseg_rows)

# ── RBKP: Invoice Receipt Header (Logistics Invoice Verification / MIRO) ─────
# ~82 % of GR POs get an invoice (RBKP = header of accounting document)
gr_po_set = set(gr_po_ids)
inv_candidates = [p for p in gr_po_ids if np.random.rand() < INV_RATE]

rbkp_rows = []
for ebeln in inv_candidates:
    po_date = ekko.loc[ekko["EBELN"] == ebeln, "BEDAT"].values[0]
    gr_date = mseg.loc[mseg["EBELN"] == ebeln, "BUDAT"].values[0]
    inv_date = pd.Timestamp(gr_date) + pd.Timedelta(days=np.random.randint(1, 8))
    po_value = ekko.loc[ekko["EBELN"] == ebeln, "NETWR"].values[0]
    rbkp_rows.append({
        "BELNR":  f"51{str(np.random.randint(10000000, 99999999))}",
        "EBELN":  ebeln,               # reference to PO (EKKO)
        "BLDAT":  inv_date,            # invoice date
        "BUDAT":  inv_date,            # posting date
        "NETWR":  round(po_value * np.random.uniform(0.97, 1.02), 2),  # minor variance
        "WAERS":  "INR",
        "BSTAT":  np.random.choice(["", "", "", "A"], p=[0.7, 0.1, 0.1, 0.1]),
        # "" = posted, "A" = parked/blocked
    })

rbkp = pd.DataFrame(rbkp_rows)

# ─────────────────────────────────────────────────────────────────────────────
# 2.  DATA PROCESSING — Join Strategy (replicating SAP relational logic)
# ─────────────────────────────────────────────────────────────────────────────
#
# Step 1: EKKO inner-join EKPO on EBELN         → valid PO + items
# Step 2: result LEFT-join MSEG on EBELN/EBELP  → preserves POs without GR
# Step 3: result LEFT-join RBKP on EBELN        → preserves GR without invoice

# Step 1: Header + Items (inner — a PO must have at least one item)
df = ekko.merge(ekpo, on="EBELN", how="inner", suffixes=("_HDR", "_ITM"))

# Step 2: Attach Goods Receipt (left — preserves open POs)
mseg_key = mseg[["EBELN", "EBELP", "BUDAT", "BWART"]].rename(
    columns={"BUDAT": "GR_DATE", "BWART": "MOVE_TYPE"}
)
df = df.merge(mseg_key, on=["EBELN", "EBELP"], how="left")

# Step 3: Attach Invoice (left — preserves GR without invoice)
rbkp_key = rbkp[["EBELN", "BLDAT", "NETWR", "BSTAT"]].rename(
    columns={"BLDAT": "INV_DATE", "NETWR": "INV_AMOUNT", "BSTAT": "INV_STATUS"}
)
df = df.merge(rbkp_key, on="EBELN", how="left")

# ─────────────────────────────────────────────────────────────────────────────
# 3.  DERIVED KPIs
# ─────────────────────────────────────────────────────────────────────────────

# Lead time: PO creation → Goods Receipt (calendar days)
df["LEAD_TIME_DAYS"] = (df["GR_DATE"] - df["BEDAT"]).dt.days

# Boolean status flags (equivalent of SAP document status VBUK/VBUP in SD,
# or EKBE.BEWTP (PO history) in MM)
df["IS_GR_POSTED"]  = df["GR_DATE"].notna()    # False = open PO
df["IS_INVOICED"]   = df["INV_DATE"].notna()    # False = GR done, invoice pending
df["IS_SLA_MET"]    = df["LEAD_TIME_DAYS"] <= SLA_DAYS

# SLA zone classification (for histogram colouring)
def sla_zone(days):
    if pd.isna(days):
        return "open"
    if days <= 7:
        return "on_time"
    if days <= 14:
        return "at_risk"
    return "breach"

df["SLA_ZONE"] = df["LEAD_TIME_DAYS"].apply(sla_zone)

# ── Aggregate KPIs ────────────────────────────────────────────────────────────
total_po_value   = ekko["NETWR"].sum()
invoiced_amount  = rbkp["NETWR"].sum()
open_po_exposure = total_po_value - invoiced_amount
n_pos            = len(ekko)
n_items          = len(ekpo)
gr_rate_actual   = df["IS_GR_POSTED"].mean() * 100
delivered        = df[df["IS_GR_POSTED"]]
avg_lead_time    = delivered["LEAD_TIME_DAYS"].mean()
on_time_rate     = (delivered["IS_SLA_MET"].sum() / len(delivered)) * 100

print("=" * 60)
print("  SAP MM — P2P Analytics Pipeline  |  KPI Summary")
print("=" * 60)
print(f"  Purchase Orders Generated    : {n_pos}")
print(f"  Total Line Items             : {n_items}")
print(f"  GR Completion Rate           : {gr_rate_actual:.1f}%")
print(f"  Avg. PO-to-GR Lead Time      : {avg_lead_time:.1f} days")
print(f"  On-Time GR Rate (≤ {SLA_DAYS} days)   : {on_time_rate:.1f}%")
print(f"  Total PO Value               : ₹{total_po_value/1e7:.2f} Cr")
print(f"  Invoiced Amount              : ₹{invoiced_amount/1e7:.2f} Cr")
print(f"  Open AP Exposure             : ₹{open_po_exposure/1e7:.2f} Cr")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────────────
# 4.  DASHBOARD — 3 Charts (dark theme, matching reference project style)
# ─────────────────────────────────────────────────────────────────────────────

BG      = "#0D1117"
PANEL   = "#161B22"
GRID_C  = "#21262D"
TEXT    = "#E6EDF3"
MUTED   = "#8B949E"
GREEN   = "#3FB950"
AMBER   = "#D29922"
RED     = "#F85149"
BLUE    = "#58A6FF"
CYAN    = "#79C0FF"
ORANGE  = "#FF7B72"

def fmt_cr(x, pos):
    """Axis formatter: ₹ with Cr/M suffix."""
    if abs(x) >= 1e7:
        return f"₹{x/1e7:.0f}Cr"
    if abs(x) >= 1e6:
        return f"₹{x/1e6:.0f}M"
    return f"₹{x/1e3:.0f}K"

plt.rcParams.update({
    "figure.facecolor":  BG,
    "axes.facecolor":    PANEL,
    "axes.edgecolor":    GRID_C,
    "axes.labelcolor":   TEXT,
    "axes.titlecolor":   TEXT,
    "xtick.color":       MUTED,
    "ytick.color":       MUTED,
    "text.color":        TEXT,
    "grid.color":        GRID_C,
    "grid.linestyle":    "--",
    "grid.linewidth":    0.5,
    "font.family":       "DejaVu Sans",
})

fig = plt.figure(figsize=(18, 20), facecolor=BG)
fig.suptitle(
    "SAP MM — Procure-to-Pay (P2P) Analytics Dashboard",
    fontsize=19, fontweight="bold", color=TEXT,
    y=0.98, fontfamily="DejaVu Sans"
)

gs = gridspec.GridSpec(
    3, 2,
    figure=fig,
    hspace=0.48, wspace=0.32,
    top=0.93, bottom=0.06,
    left=0.08, right=0.97
)

# ── Chart 1: Spend by Material (EKPO.NETWR aggregation) ─────────────────────
ax1 = fig.add_subplot(gs[0, 0])

mat_spend = (
    ekpo.groupby("MATNR")["NETWR"]
    .sum()
    .sort_values(ascending=True)
)
mat_labels = [m.replace("RM-", "").replace("MRO-", "").replace("PKG-", "").replace("ELE-", "")
              for m in mat_spend.index]
bar_colors = [BLUE, CYAN, GREEN, AMBER, ORANGE, RED,
              "#B392F0", "#56D364"][:len(mat_spend)]

bars = ax1.barh(mat_labels, mat_spend.values, color=bar_colors, height=0.65, zorder=3)
ax1.set_title("Spend by Material (EKPO.NETWR)", fontsize=11, fontweight="bold",
              color=TEXT, pad=10)
ax1.set_xlabel("Procurement Spend (₹)", fontsize=9, color=MUTED)
ax1.xaxis.set_major_formatter(FuncFormatter(fmt_cr))
ax1.grid(axis="x", zorder=0)
ax1.tick_params(axis="both", labelsize=8)
for bar, val in zip(bars, mat_spend.values):
    ax1.text(val + mat_spend.max() * 0.01, bar.get_y() + bar.get_height() / 2,
             f"₹{val/1e6:.1f}M", va="center", fontsize=7.5, color=TEXT)

# ── Chart 2: PO-to-GR Lead Time Distribution (histogram with SLA zones) ─────
ax2 = fig.add_subplot(gs[0, 1])

lt_data   = delivered["LEAD_TIME_DAYS"].dropna()
bins      = range(1, 28)
zone_map  = {d: sla_zone(d) for d in range(1, 27)}
color_map = {"on_time": GREEN, "at_risk": AMBER, "breach": RED}

# Draw each bin coloured by SLA zone
for b in range(1, 27):
    count = ((lt_data >= b) & (lt_data < b + 1)).sum()
    col   = color_map[zone_map[b]]
    ax2.bar(b, count, width=0.85, color=col, alpha=0.85, zorder=3)

ax2.axvline(avg_lead_time, color="white",  linestyle="--", linewidth=1.2, label=f"Mean: {avg_lead_time:.1f}d")
ax2.axvline(SLA_DAYS,      color=GREEN,    linestyle=":",  linewidth=1.0, label=f"SLA: {SLA_DAYS}d")
ax2.axvline(14,            color=AMBER,    linestyle=":",  linewidth=1.0, label="Warning: 14d")

ax2.set_title("PO-to-GR Lead Time Distribution\n(EKKO.BEDAT → MSEG.BUDAT)",
              fontsize=11, fontweight="bold", color=TEXT, pad=10)
ax2.set_xlabel("Lead Time (Days from PO to Goods Receipt)", fontsize=9, color=MUTED)
ax2.set_ylabel("Number of POs", fontsize=9, color=MUTED)
ax2.legend(fontsize=8, facecolor=PANEL, edgecolor=GRID_C, labelcolor=TEXT,
           loc="upper left")
ax2.text(0.98, 0.95, f"On-Time: {on_time_rate:.1f}%",
         transform=ax2.transAxes, ha="right", va="top",
         fontsize=9, color=GREEN, fontweight="bold")
ax2.grid(axis="y", zorder=0)
ax2.tick_params(axis="both", labelsize=8)

# ── Chart 3: Monthly Spend Trend — PO Value & Volume (dual axis) ─────────────
ax3 = fig.add_subplot(gs[1, :])

ekko["MONTH"] = ekko["BEDAT"].dt.to_period("M")
monthly = (
    ekko.groupby("MONTH")
    .agg(po_value=("NETWR", "sum"), po_count=("EBELN", "count"))
    .reset_index()
)
monthly["MONTH_STR"] = monthly["MONTH"].astype(str)
monthly["ROLLING_AVG"] = monthly["po_value"].rolling(3, min_periods=1).mean()

ax3b = ax3.twinx()

ax3b.bar(monthly["MONTH_STR"], monthly["po_count"],
         color=GREEN, alpha=0.3, label="PO Volume", zorder=2)
ax3.plot(monthly["MONTH_STR"], monthly["po_value"],
         color=BLUE, marker="o", linewidth=2, markersize=5,
         label="PO Value (₹M)", zorder=4)
ax3.plot(monthly["MONTH_STR"], monthly["ROLLING_AVG"],
         color=ORANGE, linestyle="--", linewidth=1.5,
         label="3M Rolling Avg", zorder=3)

ax3.set_title("Monthly Procurement Trend — PO Value & Volume (2024)\n"
              "Source: EKKO.BEDAT + EKPO.NETWR",
              fontsize=11, fontweight="bold", color=TEXT, pad=10)
ax3.set_ylabel("Procurement Value (₹)", fontsize=9, color=BLUE)
ax3b.set_ylabel("PO Count", fontsize=9, color=GREEN)
ax3.yaxis.set_major_formatter(FuncFormatter(fmt_cr))
ax3.tick_params(axis="x", rotation=45, labelsize=8)
ax3.tick_params(axis="y", labelsize=8)
ax3b.tick_params(axis="y", labelsize=8)
ax3.grid(axis="y", zorder=0)

lines1, labels1 = ax3.get_legend_handles_labels()
lines2, labels2 = ax3b.get_legend_handles_labels()
ax3.legend(lines1 + lines2, labels1 + labels2,
           fontsize=8, facecolor=PANEL, edgecolor=GRID_C, labelcolor=TEXT,
           loc="upper left")

# ── Chart 4: Vendor-wise PO Value & GR Rate ──────────────────────────────────
ax4 = fig.add_subplot(gs[2, 0])

vendor_po = ekko.groupby("LIFNR")["NETWR"].sum()
vendor_gr = (
    ekko.merge(mseg[["EBELN"]].drop_duplicates(), on="EBELN", how="left")
    .assign(HAS_GR=lambda x: x["EBELN"].isin(mseg["EBELN"]))
    .groupby("LIFNR")["HAS_GR"].mean() * 100
)
vdf = pd.DataFrame({"spend": vendor_po, "gr_rate": vendor_gr}).dropna()
vdf["name"] = vdf.index.map(lambda x: VENDORS.get(x, x).split()[0])

x_pos = np.arange(len(vdf))
bars_v = ax4.bar(x_pos, vdf["spend"].values, color=CYAN, alpha=0.8, zorder=3, label="PO Value")
ax4b = ax4.twinx()
ax4b.plot(x_pos, vdf["gr_rate"].values, color=ORANGE, marker="D",
          linewidth=1.8, markersize=6, label="GR Rate %", zorder=4)

ax4.set_xticks(x_pos)
ax4.set_xticklabels(vdf["name"].values, rotation=15, ha="right", fontsize=8)
ax4.set_title("Vendor Spend vs. GR Completion Rate\n(EKKO.LIFNR + MSEG match)",
              fontsize=11, fontweight="bold", color=TEXT, pad=10)
ax4.set_ylabel("PO Value (₹)", fontsize=9, color=CYAN)
ax4b.set_ylabel("GR Completion Rate (%)", fontsize=9, color=ORANGE)
ax4.yaxis.set_major_formatter(FuncFormatter(fmt_cr))
ax4b.set_ylim(0, 110)
ax4.grid(axis="y", zorder=0)
ax4.tick_params(labelsize=8)
ax4b.tick_params(labelsize=8)

l1, lb1 = ax4.get_legend_handles_labels()
l2, lb2 = ax4b.get_legend_handles_labels()
ax4.legend(l1+l2, lb1+lb2, fontsize=8, facecolor=PANEL,
           edgecolor=GRID_C, labelcolor=TEXT, loc="upper right")

# ── Chart 5: Invoice Matching Status (3-way match health) ────────────────────
ax5 = fig.add_subplot(gs[2, 1])

# Count POs by status: fully cleared / GR done not invoiced / open PO
po_status = ekko.copy()
po_status["HAS_GR"]  = po_status["EBELN"].isin(mseg["EBELN"].unique())
po_status["HAS_INV"] = po_status["EBELN"].isin(rbkp["EBELN"].unique())

fully_cleared  = (po_status["HAS_GR"]  & po_status["HAS_INV"]).sum()
gr_no_inv      = (po_status["HAS_GR"]  & ~po_status["HAS_INV"]).sum()
open_po        = (~po_status["HAS_GR"]).sum()

labels_pie = [
    f"Fully Processed\n(GR + Invoice)\n{fully_cleared} POs",
    f"GR Done,\nNo Invoice\n{gr_no_inv} POs",
    f"Open PO\n(No GR)\n{open_po} POs",
]
sizes  = [fully_cleared, gr_no_inv, open_po]
colors_pie = [GREEN, AMBER, RED]
explode = [0.03, 0.03, 0.03]

wedges, texts, autotexts = ax5.pie(
    sizes, labels=labels_pie, colors=colors_pie,
    autopct="%1.1f%%", startangle=140, explode=explode,
    textprops={"color": TEXT, "fontsize": 8},
    wedgeprops={"linewidth": 1, "edgecolor": PANEL},
    pctdistance=0.72,
)
for at in autotexts:
    at.set_fontsize(9)
    at.set_fontweight("bold")
    at.set_color(BG)

ax5.set_title("3-Way Match Status — Invoice Processing Health\n(EKKO → MSEG → RBKP)",
              fontsize=11, fontweight="bold", color=TEXT, pad=10)

# ── Footer ────────────────────────────────────────────────────────────────────
fig.text(0.5, 0.02,
         "Simulated SAP MM Tables: EKKO · EKPO · MSEG · RBKP  |  P2P Pipeline Simulation  "
         "|  All values in INR",
         ha="center", fontsize=8, color=MUTED, style="italic")

plt.savefig("sap_p2p_dashboard.png", dpi=150, bbox_inches="tight",
            facecolor=BG, edgecolor="none")
plt.close()

print("\n  Dashboard saved → sap_p2p_dashboard.png")
print("  Pipeline complete.")
