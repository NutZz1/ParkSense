# Trained models

Saved with PySpark 3.5.9 (Spark ML `PipelineModel`). Target: carpark nearly full (<= 10% of car lots free) 30 minutes ahead.

| Folder / file | What it is |
|---|---|
| `best_model/` | **Deployed model**: Random Forest pipeline (StringIndexer + OneHotEncoder + VectorAssembler + RandomForestClassifier, 60 trees, max depth 10). The tree weights are in `stages/*/data/` |
| `rf_30/` | Random Forest from the model comparison (same as `best_model`) |
| `gbt_30/` | Gradient-Boosted Trees from the comparison |
| `te_chd_30/`, `te_ch_30/` | Target-encoding tables (how often each carpark was full by hour x weekday / by hour, training days only), used as features `te_chd`, `te_ch` |
| `best_model_meta.json` | Algorithm, params, **decision threshold 0.89**, feature list, train/val/test split, model version |
| `models_meta_30.json` | Metadata of both compared models |

## Load it

```python
from pyspark.ml import PipelineModel
model = PipelineModel.load("models/best_model")
# input: a DataFrame with the 30 numeric + 2 categorical features listed in best_model_meta.json
#        (built by pyspark_jobs/features.py -> build_features)
# output: column "probability"; predict "full" when probability[1] >= 0.89 (the threshold in the meta file)
```

`best_model_meta.json` stores absolute paths (`/home/hduser/parksense-sg/models/...`) from the machine it was trained on.
If you clone the repo elsewhere, either clone it to that path or edit `model_path` and `te_tables` in the meta file.
Test results: accuracy 0.9852, precision 0.8547, recall 0.8657, F1 0.8602 (`outputs/metrics/model_comparison.csv`).
