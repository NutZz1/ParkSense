"""Confusion matrices of the two ML models side by side (test set), for the evaluation slide.

Reads outputs/metrics/confusion_<model>.csv and model_comparison.csv; writes outputs/figures/fig15_confusion_rf_vs_gbt.png.
Usage: python viz/confusion_side_by_side.py
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
M, FIG = ROOT / "outputs/metrics", ROOT / "outputs/figures"
OUT = FIG / "fig15_confusion_rf_vs_gbt.png"
MODELS = [("Random Forest", "confusion_random_forest.csv", "Blues"),
          ("Gradient-Boosted Trees", "confusion_gradient_boosted_trees.csv", "Greens")]

comp = pd.read_csv(M / "model_comparison.csv").set_index("model")
plt.rcParams.update({"font.size": 11, "axes.titleweight": "bold"})
fig, axes = plt.subplots(1, 2, figsize=(12, 5.2))
for ax, (name, f, cmap) in zip(axes, MODELS):
    cm = pd.read_csv(M / f, index_col=0)
    v = cm.values
    pct = v / v.sum() * 100
    tags = [["TN", "FP"], ["FN", "TP"]]
    lab = np.array([[f"{tags[i][j]}\n{v[i, j]:,}\n({pct[i, j]:.2f}%)" for j in range(2)] for i in range(2)])
    # colour by share of each actual class, so the small "full" row is not washed out by the large "not full" row
    sns.heatmap(v / v.sum(axis=1, keepdims=True), annot=lab, fmt="", cmap=cmap, vmin=0, vmax=1, cbar=False, ax=ax,
                xticklabels=["Predicted\nnot full", "Predicted\nfull"], yticklabels=["Actual\nnot full", "Actual\nfull"],
                linewidths=1, linecolor="white", annot_kws={"fontsize": 12})
    r = comp.loc[name]
    ax.set_title(f"{name} (threshold {float(r.threshold):.2f})", pad=10)
    ax.set_xlabel(f"Accuracy {r.accuracy:.2%} · Precision {r.precision:.2%} · Recall {r.recall:.2%} · F1 {r.f1:.4f}",
                  labelpad=12, fontsize=10)
    ax.tick_params(axis="y", rotation=0)
fig.suptitle(f"Confusion matrices on the test set ({int(comp.loc['Random Forest'].n):,} rows, 5 unseen days)",
             fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig(OUT, dpi=300)
print(f"saved {OUT}")
