# Home Credit Default Risk Prediction

**Completed by:** Samantha Huang  
**Course:** MSBA Capstone 1 (IS 6850)  
**Project Description:** Predictive modeling to assess default risk for Home Credit loan applications

---

## 📋 Project Overview

This project aims to predict whether a loan applicant will default on their loan using Home Credit's application data and supplementary credit history. The analysis includes comprehensive exploratory data analysis (EDA), data preparation, feature engineering, and predictive modeling.

---

## 📁 Project Structure

```
home-credit-project-MSBA-capstone1-1/
├── README.md                                    # This file
├── home_credit_eda.qmd                          # Exploratory Data Analysis (Quarto)
├── home_credit_modeling.qmd                     # Machine Learning Models (Python/Quarto)
├── MODEL_CARD.qmd                               # ⭐ Comprehensive Model Documentation (2,047 lines)
├── Data_Preparation_and_Feature_Engineering.R  # Data prep functions
├── data/                                        # Raw data files (not tracked)
│   ├── application_train.csv
│   ├── application_test.csv
│   ├── bureau.csv
│   ├── previous_application.csv
│   ├── installments_payments.csv
│   └── credit_card_balance.csv
├── output/                                      # Generated outputs
│   ├── rf_model_enhanced.pkl                    # Best performing model (65.7 MB)
│   ├── submission_rf_enhanced.csv               # Kaggle submission file
│   └── MODEL_CARD.html                          # ⭐ Rendered model card (coming soon)
└── processed_data/                              # Prepared datasets
    └── application_train_fully_imputed.csv      # Training data with full imputation
```

---

## 📊 Data Files

### Main Dataset
- **application_train.csv**: Training data with loan applications and target variable (default indicator)
- **application_test.csv**: Test data for final predictions

### Supplementary Datasets
- **bureau.csv**: Credit history from other financial institutions
- **previous_application.csv**: Previous Home Credit applications
- **installments_payments.csv**: Payment history for previous loans
- **credit_card_balance.csv**: Monthly credit card balance data
- **POS_CASH_balance.csv**: Point-of-sale and cash loans balance

---

## 🔍 Key Findings from EDA

### Target Variable
- **Class imbalance**: ~8% default rate (highly imbalanced)
- Requires careful handling in modeling (SMOTE, class weights, etc.)

### Important Predictors
1. **External Sources (EXT_SOURCE_1, 2, 3)**: Strong predictors with significant missing values
2. **Credit amount**: Higher amounts slightly associated with lower default
3. **Income**: Higher income correlates with lower default risk
4. **Age**: Younger applicants have higher default rates
5. **Employment tenure**: Longer employment history indicates stability

### Data Quality Issues
- **DAYS_EMPLOYED anomaly**: 365,243 represents "retired/unemployed" → converted to NA
- **Missing values**: Varying rates across features (handled strategically)
- **Outliers**: Extreme values in income and family size retained for modeling

---

## 🛠️ Data Preparation & Feature Engineering

### Script: `Data_Preparation_and_Feature_Engineering.R`

This script contains **reusable R functions** for data preparation, organized into 5 sections:

#### **Section 1: Aggregate Supplementary Data**
- `aggregate_bureau()`: Bureau credit history features
- `aggregate_previous_applications()`: Previous loan application features  
- `aggregate_installments()`: Payment history features

#### **Section 2: Data Quality Fixes**
- `fix_data_quality()`: Fixes DAYS_EMPLOYED anomaly (365,243 → NA)

#### **Section 3: Missing Data Handling** (No Data Leakage!)
- `compute_imputation_values(train_data)`: Computes medians from **training data only**
- `apply_missing_value_imputation(data, impute_values)`: Applies train statistics to any dataset

**Strategy:**
- EXT_SOURCE_1, 2, 3: **Keep as NA** (tree-based models handle natively)
- AMT_ANNUITY, CNT_FAM_MEMBERS, DAYS_LAST_PHONE_CHANGE: Fill with **train median**
- OWN_CAR_AGE: NA → 0 (no car)
- AMT_GOODS_PRICE: Use AMT_CREDIT when missing
- Supplementary features: NA → 0 (no history)

#### **Section 4: Feature Engineering**
- `engineer_features()`: Creates new features from cleaned data
  - Time conversions (age, employment years)
  - Financial ratios (debt-to-income, credit utilization)
  - External source combinations
  - Risk indicators

#### **Section 5: Main Pipeline**
- `prepare_data_pipeline()`: Orchestrates entire workflow

---

## 🚀 Usage

### 1. Load the Functions

```r
library(tidyverse)
source("Data_Preparation_and_Feature_Engineering.R")
```

### 2. Prepare Training Data

```r
# Load raw training data
train <- read_csv("application_train.csv")

# Run full pipeline (returns list with data + imputation values)
train_result <- prepare_data_pipeline(
  app_data = train,
  data_path = ".",  # Path to supplementary CSVs
  impute_values = NULL,  # NULL = training mode
  include_bureau = TRUE,
  include_prev_app = TRUE,
  include_installments = TRUE
)

# Extract prepared data and imputation values
train_prepared <- train_result$data
train_impute_vals <- train_result$impute_values
```

### 3. Prepare Test Data (No Data Leakage!)

```r
# Load raw test data
test <- read_csv("application_test.csv")

# Run pipeline using TRAIN imputation values
test_result <- prepare_data_pipeline(
  app_data = test,
  data_path = ".",
  impute_values = train_impute_vals,  # ← Use train values!
  include_bureau = TRUE,
  include_prev_app = TRUE,
  include_installments = TRUE
)

# Extract prepared test data
test_prepared <- test_result$data
```

### 4. Save Prepared Data

```r
# Save for modeling
write_csv(train_prepared, "train_prepared.csv")
write_csv(test_prepared, "test_prepared.csv")

# Save imputation values for reproducibility
saveRDS(train_impute_vals, "imputation_values.rds")
```

---

## 📈 Features Created

### Supplementary Aggregates (~30-40 features)
- Bureau: Credit counts, active vs. closed, overdue amounts, debt ratios
- Previous apps: Approval rates, refusal history, credit comparisons
- Installments: Late payment rates, payment trends

### Engineered Features (~15-20 features)
- AGE_YEARS, EMPLOYMENT_YEARS
- CREDIT_INCOME_RATIO, ANNUITY_INCOME_RATIO
- INCOME_PER_PERSON, CREDIT_GOODS_RATIO
- EXT_SOURCE_MEAN, EXT_SOURCE_PRODUCT, EXT_SOURCE_WEIGHTED
- DOCS_SUBMITTED, REGION_RATING_MATCH
- Risk flags (HIGH_CREDIT_BURDEN, LOW_EXT_SOURCE, etc.)

### Combined Features (~5-10 features)
- BUREAU_DEBT_TO_INCOME
- BUREAU_ACTIVE_CREDIT_RATIO
- PREV_CREDIT_TO_CURRENT

**Total:** ~50-70 new features added to original dataset

---

## 🤖 Predictive Modeling

### Notebook: `home_credit_modeling.qmd`

This notebook contains the complete machine learning pipeline, from baseline establishment to final model selection and Kaggle submission.

### Models Evaluated

#### **1. Baseline: Majority Class Classifier**
- **Strategy**: Always predict class 0 (Repaid)
- **Performance**: 91.91% accuracy, AUC = 0.50
- **Purpose**: Establish minimum performance threshold

#### **2. Logistic Regression**
- **Configuration**: Balanced class weights, 308 features (after one-hot encoding)
- **Performance**: ~92.2% accuracy, AUC ~0.6-0.7
- **Finding**: Improved over baseline but limited by linear assumptions

#### **3. Random Forest (Default Parameters)**
- **Configuration**: 100 trees, max_depth=20, balanced class weights
- **Performance**: AUC ~0.76
- **Key Analysis**: Identified top 20 most important features

#### **4. Class Imbalance Strategy Comparison**
Evaluated three approaches to handle the 8% default rate:
- **Class Weight (Balanced)**: Fast training, effective performance ✅
- **SMOTE (Over-sampling)**: Slower, improved recall for defaults
- **Random Under-sampling**: Fastest, but loses information

**Winner**: Class weight method provided the best balance

#### **5. Random Forest (Hyperparameter Tuned)**
- **Tuning Method**: RandomizedSearchCV (3-fold CV, 20 iterations)
- **Search Space**: n_estimators, max_depth, min_samples_split/leaf, max_features, bootstrap
- **Performance**: AUC = 0.7624
- **Training**: Full dataset (246,008 samples)

#### **6. Random Forest Enhanced (Final Model)** ⭐
- **Enhancement**: Added 23 credit card features from `credit_card_balance.csv`
  - Utilization metrics (CC_AVG_UTILIZATION, CC_HIGH_UTIL_PCT)
  - Delinquency indicators (CC_DPD_COUNT, CC_LATE_COUNT, CC_MAX_DPD)
  - Balance trends (CC_BALANCE_TREND)
  - Payment behavior (CC_PAYMENT_RATIO, CC_ATM_RATE, CC_DRAWING_RATE)
- **Total Features**: 331 (308 original + 23 credit card)
- **Performance**: 
  - **AUC = 0.7650** (best performance)
  - Caught 2,632 out of 4,965 defaults in test set
  - Improvement: +0.0026 AUC over tuned baseline
- **Key Insight**: Credit card behavior adds predictive power beyond application data

### Final Model Selection

**Selected Model**: Random Forest Enhanced with Credit Card Features

**Why This Model?**
1. **Best Predictive Performance**: Highest AUC (0.7650) across all models tested
2. **Meaningful Improvement**: Credit card features provided incremental lift over baseline
3. **Robust to Class Imbalance**: Balanced class weights effectively handled 8% default rate
4. **Interpretable**: Feature importance analysis reveals key risk drivers
5. **Kaggle Score**: 0.76145 (validated on test set)

**Top Predictive Features**:
- EXT_SOURCE_2, EXT_SOURCE_3 (external credit scores)
- CC_HIGH_UTIL_PCT (credit card high utilization frequency)
- CC_DRAWING_RATE (credit drawing behavior)
- DAYS_BIRTH (age), DAYS_EMPLOYED (employment tenure)
- CC_PAYMENT_RATIO (payment discipline)

### Model Deliverables

- **Saved Model**: `rf_model_enhanced.pkl` (65.7 MB)
- **Submission File**: `submission_rf_enhanced.csv` (48,744 predictions)
- **Kaggle Score**: 0.76145

---

## 📝 Key Design Principles

✅ **No Data Leakage**: Test data uses training statistics for imputation  
✅ **Reusable Functions**: All logic organized as callable functions  
✅ **Works on Both Datasets**: Same pipeline for train and test  
✅ **EDA-Driven**: Reflects insights from exploratory analysis  
✅ **Well-Documented**: Inline comments and function descriptions  
✅ **Production-Ready**: Proper error handling and informative messages

---

## 🔧 Requirements

### R Packages (Data Preparation & EDA)
```r
library(tidyverse)  # Data manipulation (dplyr, tidyr, readr)
library(stringr)    # String operations
```

### Python Packages (Modeling)
```python
polars              # Fast data manipulation
numpy               # Numerical computing
pandas              # Data structures
plotnine            # Visualization (ggplot2 for Python)
scikit-learn        # Machine learning models and metrics
imbalanced-learn    # SMOTE and imbalance handling
```

---

## 📄 Model Card (NEW!)

### Comprehensive Documentation: `MODEL_CARD.qmd`

A complete model card documenting the Random Forest Enhanced model for production deployment, regulatory compliance, and stakeholder communication.

#### **Executive Summary**
- **Recommendation**: Deploy at threshold 0.35
- **Expected Impact**: $10-15M annual profit improvement
- **Approval Rate**: 85% (vs 75% at conservative 0.5 threshold)
- **Fairness**: Passes 80% rule for gender (1.000) and education (0.888)

#### **9 Comprehensive Sections**:

1. **Model Details** (Line 257)
   - Architecture: Random Forest with 331 features
   - Training: 246K samples, 8% default rate, balanced class weights
   - Validation AUC: 0.7650 | Kaggle AUC: 0.7615

2. **Intended Use** (Line 369)
   - Users: Credit analysts, loan officers, risk managers
   - Decisions: Screening, pricing, portfolio monitoring
   - Limitations: NOT for sole decisioning or different loan products

3. **Performance Metrics** (Line 477)
   - Detailed confusion matrix analysis
   - ROC curve visualization
   - Warning: 0.5 threshold is for reporting only

4. **Decision Threshold Analysis** (Line 707)
   - Research-based lending economics (Federal Reserve, Basel III)
   - Optimal threshold: **0.35** (derived from 6.3:1 cost ratio)
   - Sensitivity analysis showing business outcomes at different thresholds

5. **Model Explainability** (Line 879)
   - Top 20 predictive features via SHAP analysis
   - Credit card features: 6 of top 20 (validates enhancement)
   - Transparency limitations: EXT_SOURCE black boxes

6. **Adverse Action Mapping** (Line 1056)
   - ECOA/FCRA compliant translations
   - "EXT_SOURCE_2 (low)" → "Limited external credit history"
   - Sample adverse action notice
   - Implementation guidance for loan officers

7. **Fairness Analysis** (Line 1307)
   - Gender: Perfect equality (85.0% approval for both)
   - Education: 80.0%-90.1% range, passes 80% rule (0.888 ratio)
   - Disparities justified by actual risk differences (3.8%-10.5% default rates)
   - Regulatory compliance: ECOA, Fair Housing Act, Disparate Impact Doctrine

8. **Limitations and Risks** (Line 1593)
   - Data limitations: Historical (2018), geographic scope, missing alternative credit data
   - Model constraints: External score dependency, class imbalance effects
   - Behavioral factors: Cannot predict life events, intentionality
   - Implementation risks: Threshold sensitivity, model drift, adversarial gaming
   - Ethical considerations: Feedback loops, cannot address root causes

9. **Executive Summary** (Line 22)
   - One-page summary for senior leadership
   - Business case, financial impact, implementation requirements
   - Critical caveats (🔴 High priority: external score dependency, model drift)
   - Success criteria for first 6 months

#### **Key Numbers**:
| Metric | Value |
|--------|-------|
| **Optimal Threshold** | 0.35 |
| **Approval Rate** | 85% |
| **Model AUC** | 0.7615 (Kaggle) |
| **Annual Profit Lift** | $10-15M estimated |
| **Fairness (Gender)** | 1.000 ratio (perfect) |
| **Fairness (Education)** | 0.888 ratio (pass) |

#### **Citations**:
- Federal Reserve Bank of Kansas City (2019). "Consumer Lending Profitability in the Digital Age"
- Basel III banking regulations for Loss Given Default (LGD)
- McKinsey & Company (2020). "The Future of Consumer Lending in Emerging Markets"
- ECOA, FCRA, Fair Housing Act compliance

#### **File Details**:
- **Length**: 2,047 lines (9 complete sections)
- **Format**: Quarto document (.qmd)
- **Output**: HTML with code hidden, outputs displayed
- **Status**: ✅ Complete and ready for submission

---

## 📚 Progress Tracker

1. ✅ Exploratory Data Analysis (EDA)
2. ✅ Data Preparation & Feature Engineering
3. ✅ Model Development (Logistic Regression, Random Forest, Hyperparameter Tuning)
4. ✅ Model Evaluation & Validation (Class Imbalance Strategies, Feature Enhancement)
5. ✅ Kaggle Submission (Best Model: AUC = 0.76145)
6. ✅ **Model Card Documentation (9 sections, 2,047 lines)** ⭐ NEW!
7. ⬜ Final Presentation

---

## 📧 Contact

**Samantha Huang**  
MSBA Program - Capstone 1  
IS 6850

---

*Last Updated: March 6, 2026*
