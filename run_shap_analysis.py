"""
SHAP Explainability Analysis
Run this to generate SHAP values for the model card
"""

import polars as pl
import numpy as np
import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

print("Loading data and model...")

# Load data
df_train = pl.read_csv("application_train_fully_imputed.csv", infer_schema_length=None)
print(f"✅ Data loaded: {df_train.shape}")

# Prepare features
feature_cols = [col for col in df_train.columns if col not in ["SK_ID_CURR", "TARGET"]]
X = df_train.select(feature_cols)
y = df_train.get_column("TARGET").to_numpy()

# Encode
cat_cols = [col for col in X.columns if X[col].dtype == pl.Utf8]
X_pd = X.to_pandas()
X_encoded = pd.get_dummies(X_pd, columns=cat_cols, drop_first=True)
print(f"✅ Features encoded: {X_encoded.shape[1]}")

# Split
X_train, X_val, y_train, y_val = train_test_split(
    X_encoded, y, test_size=0.2, random_state=42, stratify=y
)
print(f"✅ Validation set: {X_val.shape[0]:,} samples")

# Load model
with open('rf_model_enhanced.pkl', 'rb') as f:
    rf_model = pickle.load(f)
print(f"✅ Model loaded")

# Take 1000-row sample for SHAP (for speed)
np.random.seed(42)
sample_idx = np.random.choice(len(X_val), size=1000, replace=False)
X_sample = X_val.iloc[sample_idx]
y_sample = y_val[sample_idx]

print(f"\n✅ SHAP sample: 1,000 rows")
print(f"   Default rate: {y_sample.mean()*100:.2f}%")

# Install and import SHAP
try:
    import shap
    print("✅ SHAP already installed")
except ImportError:
    print("Installing SHAP...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "shap", "-q"])
    import shap
    print("✅ SHAP installed")

# Create SHAP explainer
print("\nCalculating SHAP values (this may take 2-3 minutes)...")
explainer = shap.TreeExplainer(rf_model)
shap_values = explainer.shap_values(X_sample)

# For binary classification, shap_values is [class_0, class_1]
# We want class 1 (default)
shap_values_default = shap_values[1]

print("✅ SHAP values calculated")

# Calculate mean absolute SHAP values
mean_abs_shap = np.abs(shap_values_default).mean(axis=0)

# Create importance dataframe
shap_importance = pd.DataFrame({
    'feature': X_sample.columns,
    'mean_abs_shap': mean_abs_shap
}).sort_values('mean_abs_shap', ascending=False)

# Display top 20
top_20 = shap_importance.head(20).reset_index(drop=True)
top_20.index = top_20.index + 1

print("\n" + "="*70)
print("TOP 20 MOST IMPORTANT FEATURES")
print("="*70)
print(top_20.to_string())
print("="*70)

# Save SHAP summary plot
plt.figure(figsize=(10, 8))
shap.summary_plot(
    shap_values_default, 
    X_sample,
    plot_type="dot",
    show=False,
    max_display=20
)
plt.title("SHAP Feature Importance - Top 20 Features", fontsize=14, pad=20)
plt.tight_layout()
plt.savefig('shap_summary_plot.png', dpi=150, bbox_inches='tight')
print("\n✅ SHAP plot saved: shap_summary_plot.png")

# Save SHAP bar plot
plt.figure(figsize=(10, 8))
shap.summary_plot(
    shap_values_default, 
    X_sample,
    plot_type="bar",
    show=False,
    max_display=20
)
plt.title("SHAP Feature Importance - Mean |SHAP|", fontsize=14, pad=20)
plt.tight_layout()
plt.savefig('shap_bar_plot.png', dpi=150, bbox_inches='tight')
print("✅ SHAP bar plot saved: shap_bar_plot.png")

# Save results
import json
shap_results = {
    'top_20_features': top_20['feature'].tolist(),
    'top_20_importance': top_20['mean_abs_shap'].tolist(),
    'sample_size': 1000,
    'sample_default_rate': float(y_sample.mean())
}

with open('shap_results.json', 'w') as f:
    json.dump(shap_results, f, indent=2)

print("✅ Results saved: shap_results.json")
print("\nDone! Now you can add these results to your model card.")
