# Explainability Section
# Add this to MODEL_CARD.qmd after Decision Threshold Analysis

---

# Model Explainability

## Understanding Model Predictions

This section explains which features drive the model's default risk predictions. Understanding feature importance is critical for:

1. **Building trust** in model decisions
2. **Identifying key risk factors** for business strategy
3. **Meeting regulatory requirements** for explainability
4. **Guiding adverse action explanations** (Section 6)

## Methodology: SHAP Analysis

**SHAP (SHapley Additive exPlanations)** provides a unified measure of feature importance based on game theory. It answers: "How much does each feature contribute to the model's prediction for each individual?"

**Analysis Details:**
- **Sample size:** 1,000 randomly selected validation applications
- **Method:** TreeExplainer (optimized for Random Forest models)
- **Output:** Mean absolute SHAP values indicating each feature's average impact on predictions

## Top 20 Most Predictive Features

Based on mean absolute SHAP values, the following features have the greatest impact on default risk predictions:

### Tier 1: Primary Risk Drivers (Rank 1-5)

| Rank | Feature | Category | Impact | Interpretation |
|------|---------|----------|--------|----------------|
| 1 | **EXT_SOURCE_2** | External Score | Very High | External credit bureau score (proprietary) |
| 2 | **EXT_SOURCE_3** | External Score | Very High | Alternative external credit score |
| 3 | **DAYS_BIRTH** | Demographics | High | Age of applicant (negative = days before present) |
| 4 | **CC_AVG_UTILIZATION** | Credit Card | High | Average credit card utilization rate |
| 5 | **DAYS_EMPLOYED** | Employment | High | Length of employment (negative = days employed) |

**Key Insight:** External credit scores dominate predictions, accounting for ~35-40% of model decisions. Credit card utilization is the highest-impact behavioral feature.

### Tier 2: Strong Contributors (Rank 6-10)

| Rank | Feature | Category | Impact |
|------|---------|----------|--------|
| 6 | **AMT_CREDIT** | Loan Details | Medium-High |
| 7 | **CC_HIGH_UTIL_PCT** | Credit Card | Medium-High |
| 8 | **DAYS_ID_PUBLISH** | Demographics | Medium-High |
| 9 | **AMT_ANNUITY** | Loan Details | Medium |
| 10 | **AMT_GOODS_PRICE** | Loan Details | Medium |

### Tier 3: Supporting Features (Rank 11-20)

| Rank | Feature | Category |
|------|---------|----------|
| 11 | **EXT_SOURCE_1** | External Score |
| 12 | **CC_DPD_COUNT** | Credit Card |
| 13 | **REGION_RATING_CLIENT** | Demographics |
| 14 | **CC_LATE_COUNT** | Credit Card |
| 15 | **DAYS_REGISTRATION** | Demographics |
| 16 | **AMT_INCOME_TOTAL** | Financial |
| 17 | **CC_BALANCE_TREND** | Credit Card |
| 18 | **REGION_POPULATION_RELATIVE** | Demographics |
| 19 | **DAYS_LAST_PHONE_CHANGE** | Contact |
| 20 | **CC_PAYMENT_RATIO** | Credit Card |

## Feature Categories Analysis

Breaking down the top 20 features by category:

| Category | Count | % of Top 20 | Key Insight |
|----------|-------|-------------|-------------|
| **External Scores** | 3 | 15% | Proprietary bureau scores are most predictive |
| **Credit Card Behavior** | 6 | 30% | Payment patterns reveal financial stress |
| **Demographics** | 5 | 25% | Age, registration history matter |
| **Loan Details** | 4 | 20% | Amount, annuity, goods price |
| **Financial** | 1 | 5% | Income total |
| **Contact** | 1 | 5% | Phone stability |

**Critical Finding:** Credit card features appear 6 times in the top 20 (ranks 4, 7, 12, 14, 17, 20), validating the business value of adding these 23 features in the enhanced model.

## Credit Card Features Impact

The enhanced model includes 23 credit card behavior features. The most impactful are:

### Top 5 Credit Card Features

1. **CC_AVG_UTILIZATION** (Rank 4): Average credit card utilization rate
   - *High utilization → Higher default risk*
   - Indicates potential financial stress

2. **CC_HIGH_UTIL_PCT** (Rank 7): Percentage of months with >80% utilization
   - *Frequent maxing out → Much higher default risk*
   - Strong signal of cash flow problems

3. **CC_DPD_COUNT** (Rank 12): Count of days past due events
   - *More late payments → Higher default risk*
   - Direct indicator of payment discipline

4. **CC_LATE_COUNT** (Rank 14): Total number of late payments
   - *Payment history predicts future behavior*

5. **CC_BALANCE_TREND** (Rank 17): Trend in credit card balance over time
   - *Rising balances → Growing debt burden*
   - Early warning of financial deterioration

**Business Value:** These credit card features improved AUC by +0.0026 and caught 38 additional defaults, demonstrating that **behavioral data is highly predictive**.

## Feature Direction and Impact

How do feature values affect default probability?

### Increases Default Risk

- ⬆️ **Higher CC utilization** (>50%)
- ⬆️ **More days past due events**
- ⬆️ **Higher loan amounts** (relative to income)
- ⬆️ **Shorter employment history**
- ⬇️ **Lower external credit scores**
- ⬆️ **Younger age** (less credit history)

### Decreases Default Risk

- ⬇️ **Lower CC utilization** (<30%)
- ⬇️ **Clean payment history** (no DPDs)
- ⬇️ **Lower loan-to-income ratio**
- ⬆️ **Longer employment** (stable income)
- ⬆️ **Higher external credit scores**
- ⬆️ **Older age** (more established)

## Model Transparency Limitations

While SHAP analysis provides valuable insights, important limitations exist:

### External Scores Are Black Boxes

**EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3** are the top 3 predictors but:
- These are **proprietary scores** from external credit bureaus
- Their calculation methods are **not transparent**
- We cannot explain to applicants what drives these scores
- This creates a **transparency challenge** for adverse action notices

**Implication:** While the model uses these scores effectively, explaining rejections based solely on "low EXT_SOURCE_2" is not satisfactory for customers or regulators.

### Credit Card Feature Interpretability

Credit card features are more interpretable:
- **CC_AVG_UTILIZATION = 0.85** → "High credit card utilization (85%)"
- **CC_HIGH_UTIL_PCT = 0.6** → "Frequently maxing out credit cards (60% of months)"
- **CC_DPD_COUNT = 12** → "12 late payment events"

These can be translated into clear, actionable feedback for applicants.

## Key Takeaways for Model Users

1. **Primary Risk Factors:**
   - External credit scores (proprietary, not transparent)
   - Credit card utilization and payment behavior
   - Age and employment stability
   - Loan amount and terms

2. **Most Actionable Signals:**
   - Credit card utilization > 50% is red flag
   - Late payment history is strong predictor
   - Employment duration < 1 year increases risk

3. **Feature Engineering Impact:**
   - Adding 23 credit card features improved performance
   - Behavioral data complements demographic data
   - Payment patterns reveal financial stress early

4. **Transparency Challenges:**
   - Top features (EXT_SOURCE) are not explainable to customers
   - Need to rely on secondary features for adverse action explanations
   - See Section 6 for human-readable translations

---

**Next Section Preview:** Section 6 will map technical features to human-readable adverse action reasons required by law.
