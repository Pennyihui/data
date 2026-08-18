# -*- coding: utf-8 -*-
"""
plot_crowding.py — 拥挤度历史可视化
====================================

图1: funding_cross_section_history.png
  资金费率截面历史(2019-09 至今): BTC价格 / 中位费率+平均绝对费率 /
  多空拥挤占比 / BTC百分位与z分数
图2: oi_cross_section.png
  OI 截面(最近~21天): 总OI价值 / BTC占比 / BTC百分位
输出目录: data_new/additional/visualization/
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

A = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_new", "additional")
VIS = os.path.join(A, "visualization")
os.makedirs(VIS, exist_ok=True)


def plot_funding_history():
    df = pd.read_csv(os.path.join(A, "merged_4h_research_crosssection.csv"),
                     parse_dates=["time"])
    df = df[df["median"].notna()]
    fig, axes = plt.subplots(4, 1, figsize=(14, 12), sharex=True,
                             gridspec_kw={"height_ratios": [2, 1, 1, 1]})

    axes[0].plot(df["time"], df["close"], lw=0.8, color="tab:blue")
    axes[0].set_yscale("log")
    axes[0].set_title("BTC price (log)")
    axes[0].grid(alpha=0.3)

    axes[1].plot(df["time"], df["median"] * 1e4, lw=0.8, color="tab:green",
                 label="median funding (bp)")
    axes[1].plot(df["time"], df["mean_abs"] * 1e4, lw=0.8, color="tab:red",
                 label="mean |funding| (bp)")
    axes[1].axhline(0, color="k", lw=0.5)
    axes[1].legend(loc="upper left", fontsize=8)
    axes[1].set_title("Funding cross-section: median & mean|funding|")
    axes[1].grid(alpha=0.3)

    axes[2].plot(df["time"], df["pct_long_crowded"], lw=0.8, color="tab:orange",
                 label="long crowded (>+0.01%)")
    axes[2].plot(df["time"], df["pct_short_crowded"], lw=0.8, color="tab:purple",
                 label="short crowded (<-0.01%)")
    axes[2].legend(loc="upper left", fontsize=8)
    axes[2].set_title("Share of crowded symbols")
    axes[2].grid(alpha=0.3)

    axes[3].plot(df["time"], df["btc_pctile"], lw=0.8, color="tab:cyan",
                 label="BTC percentile")
    axes[3].plot(df["time"], df["btc_z"], lw=0.8, color="tab:red",
                 label="BTC z-score")
    axes[3].axhline(0.5, color="k", lw=0.5, ls="--")
    axes[3].legend(loc="upper left", fontsize=8)
    axes[3].set_title("BTC position in cross-section")
    axes[3].grid(alpha=0.3)

    fig.suptitle("Cross-coin funding crowding (100 perpetuals, 8h)", fontsize=13)
    fig.tight_layout()
    p = os.path.join(VIS, "funding_cross_section_history.png")
    fig.savefig(p, dpi=110)
    plt.close(fig)
    print("saved:", p)


def plot_oi_cross_section():
    df = pd.read_csv(os.path.join(A, "open_interest_cross_section_1h.csv"),
                     parse_dates=["time"])
    fig, axes = plt.subplots(3, 1, figsize=(14, 9), sharex=True)

    axes[0].plot(df["time"], df["total_oi_value"] / 1e9, lw=1, color="tab:blue")
    axes[0].set_title("Total open interest value across 100 perpetuals (USD B)")
    axes[0].grid(alpha=0.3)

    axes[1].plot(df["time"], df["btc_share"] * 100, lw=1, color="tab:green")
    axes[1].set_title("BTC share of total OI (%)")
    axes[1].grid(alpha=0.3)

    axes[2].plot(df["time"], df["btc_pctile"], lw=1, color="tab:orange")
    axes[2].axhline(0.5, color="k", lw=0.5, ls="--")
    axes[2].set_title("BTC OI percentile in cross-section")
    axes[2].grid(alpha=0.3)

    fig.suptitle("Cross-coin open interest (100 perpetuals, 1h)", fontsize=13)
    fig.tight_layout()
    p = os.path.join(VIS, "oi_cross_section.png")
    fig.savefig(p, dpi=110)
    plt.close(fig)
    print("saved:", p)


if __name__ == "__main__":
    plot_funding_history()
    plot_oi_cross_section()
