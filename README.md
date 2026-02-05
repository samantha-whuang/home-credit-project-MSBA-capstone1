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
├── Data_Preparation_and_Feature_Engineering.R  # Data prep functions
├── data/                                        # Raw data files (not tracked)
│   ├── application_train.csv
│   ├── application_test.csv
│   ├── bureau.csv
│   ├── previous_application.csv
│   └── installments_payments.csv
└── output/                                      # Generated outputs
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

## 📝 Key Design Principles

✅ **No Data Leakage**: Test data uses training statistics for imputation  
✅ **Reusable Functions**: All logic organized as callable functions  
✅ **Works on Both Datasets**: Same pipeline for train and test  
✅ **EDA-Driven**: Reflects insights from exploratory analysis  
✅ **Well-Documented**: Inline comments and function descriptions  
✅ **Production-Ready**: Proper error handling and informative messages

---

## 🔧 Requirements

```r
# Required R packages
library(tidyverse)  # Data manipulation (dplyr, tidyr, readr)
library(stringr)    # String operations
```

---

## 📚 Next Steps

1. ✅ Exploratory Data Analysis (EDA)
2. ✅ Data Preparation & Feature Engineering
3. ⬜ Model Development (Logistic Regression, Random Forest, XGBoost)
4. ⬜ Model Evaluation & Validation
5. ⬜ Final Report & Presentation

---

## 📧 Contact

**Samantha Huang**  
MSBA Program - Capstone 1  
IS 6850

---

*Last Updated: February 4, 2026*
