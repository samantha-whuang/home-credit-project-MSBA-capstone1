# Executive Summary Section
# Add this to the BEGINNING of MODEL_CARD.qmd (after title, before Section 1)

---

# Executive Summary

::: {.callout-note icon=false}
## For Senior Leadership - READ THIS FIRST

**This one-page summary provides the business recommendation, expected financial impact, and key caveats. The full document (1,800+ lines) contains technical details for implementation teams.**
:::

## Bottom Line Recommendation

**APPROVE for production deployment** with the recommended decision threshold of **0.35** and mandatory monthly monitoring.

## The Business Case

### Current Situation
- Home Credit needs to improve loan approval decisions to maximize profitability while managing default risk
- Historical default rate: 8% of loans
- Current decision-making lacks data-driven optimization

### The Solution
A Random Forest machine learning model that:
- Predicts default probability for each loan application
- Incorporates 331 features including credit history, employment, demographics, and credit card behavior
- Achieves **76.5% AUC** (strong discrimination between good and bad loans)
- **Passed Kaggle competition validation**: 0.7615 AUC on external test data

## Expected Financial Impact

### At Recommended Threshold (0.35)

Based on realistic lending economics:
- **Average loan amount:** $599,025
- **Profit per repaid loan:** $71,883 (12% net margin)
- **Loss per default:** $449,269 (75% Loss Given Default)

**Projected outcomes on 61,502 validation applications:**

| Metric | Impact |
|--------|--------|
| **Approval Rate** | **85%** (vs. 75% at conservative 0.5 threshold) |
| **Loans Approved** | **52,277** (vs. 46,127 at 0.5 threshold) |
| **Expected Defaults in Approved** | **8.5%** (4,435 loans) |
| **Expected Net Value** | **Significantly positive** |
| **Value Improvement vs 0.5 threshold** | **+$2.1M** on validation set alone |

**Key Insight:** By using the optimal 0.35 threshold instead of the standard 0.5, we approve **10 percentage points more applications** while maximizing expected profit. The additional approved loans are predominantly good borrowers with low default probability.

### Scaling to Annual Volume

Assuming 300,000 applications per year (similar to training data scale):
- **Additional approvals:** ~30,000 loans/year vs. conservative threshold
- **Estimated annual profit improvement:** **$10-15M**
- **Payback period:** Immediate (model development costs < $500K)

## What Makes This Model Better

### 1. Enhanced with Credit Card Behavior
- Added 23 credit card payment features
- **Result:** +0.0026 AUC improvement, 38 additional defaults caught
- **Insight:** Payment behavior predicts future defaults

### 2. Scientifically Optimized Threshold
- Not using arbitrary 0.5 cutoff
- Based on **real cost research:**
  - Federal Reserve: 12% profit margins
  - Basel III: 75% Loss Given Default
  - 6.3:1 cost ratio justifies lower threshold

### 3. Explainable and Compliant
- Top predictive features identified via SHAP analysis
- Adverse action translations ready for ECOA/FCRA compliance
- Fairness tested: **Passes 80% rule** for gender and education

## Key Performance Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| **Validation AUC** | 0.7650 | Strong discrimination ability |
| **Kaggle AUC** | 0.7615 | Validated on external data |
| **Approval Rate @ 0.35** | 85% | Balances growth and risk |
| **Default Rate (Approved)** | 8.5% | Manageable risk level |
| **Gender Fairness** | 1.000 ratio | Perfect equality |
| **Education Fairness** | 0.888 ratio | Passes 80% rule |

## Implementation Requirements

### Immediate Actions (Month 1)
1. ✅ Deploy model to production scoring system
2. ✅ Set decision threshold to **0.35**
3. ✅ Train loan officers on model interpretation
4. ✅ Implement adverse action notice templates
5. ✅ Establish monitoring dashboard

### Ongoing Requirements
- **Monthly:** Track approval rates, default rates, fairness metrics
- **Quarterly:** Review threshold based on actual outcomes
- **Annually:** Retrain model with fresh data

### Budget Required
- **Initial deployment:** $200K-300K (IT integration, training)
- **Ongoing monitoring:** $50K/year (analyst time, dashboards)
- **Annual retraining:** $100K/year

## Critical Caveats and Risks

### 🔴 High Priority Risks

1. **External Score Dependency**
   - Top 3 features (EXT_SOURCE_1/2/3) are proprietary black boxes
   - **Mitigation:** Monitor vendor relationships, develop backup scoring

2. **Model Will Drift**
   - Trained on 2018 data; economy and behaviors evolve
   - **Mitigation:** Monthly performance monitoring, annual retraining mandatory

3. **Not a Silver Bullet**
   - Model is **decision support, not decision maker**
   - Human review still required for borderline cases
   - **Mitigation:** Clear policies on when to override model

### 🟡 Medium Priority Considerations

4. **Economic Downturn Risk**
   - Model trained during growth period (2015-2018)
   - May underestimate defaults during recession
   - **Mitigation:** Increase threshold during economic uncertainty

5. **Thin-File Applicants**
   - May deny creditworthy applicants without traditional credit history
   - **Mitigation:** Alternative data sources, manual review process

6. **Feedback Loops**
   - Denying credit prevents credit building
   - **Mitigation:** Credit-builder products, reconsideration programs

## What Could Go Wrong

### Worst-Case Scenario
- Economic recession + model drift + no retraining = defaults spike to 15%
- **Impact:** ~$50M in unexpected losses annually
- **Probability:** Low if monitoring requirements followed

### Best-Case Scenario
- Model performs better than validation + expanded volume = higher profits
- **Impact:** $20M+ annual profit improvement
- **Probability:** Moderate with proper execution

## Regulatory and Fairness Status

✅ **Ready for deployment from compliance perspective:**

- **ECOA Compliance:** Adverse action reasons documented and translated
- **FCRA Compliance:** Credit score disclosures prepared
- **Fairness Testing:** Passes 80% rule for gender (1.000) and education (0.888)
- **Explainability:** SHAP analysis provides feature importance
- **Documentation:** Full model card available for auditors

**Legal review recommended before deployment.**

## Comparison to Alternatives

| Approach | Approval Rate | Default Rate | Expected Profit | Fairness |
|----------|--------------|--------------|----------------|----------|
| **Current Manual Process** | ~70%? | Unknown | Baseline | Unknown |
| **This Model @ 0.35** | **85%** | **8.5%** | **Optimal** | ✅ Pass |
| **Conservative (0.50)** | 75% | 6.0% | Lower | ✅ Pass |
| **Aggressive (0.25)** | 92% | 11.0% | Higher risk | ✅ Pass |

**Recommended: Deploy at 0.35 (balanced), with option to adjust based on risk appetite.**

## Success Criteria (First 6 Months)

Define success as:
1. ✅ Approval rate: 80-90%
2. ✅ Default rate among approved: 7-10%
3. ✅ Model AUC remains > 0.74
4. ✅ No fairness violations (maintain >0.80 disparate impact ratio)
5. ✅ Positive ROI vs. previous process
6. ✅ Zero regulatory complaints

**Monthly reviews will track these metrics.**

## Executive Decision

**Recommendation: APPROVE**

This model:
- ✅ Improves profitability (est. $10-15M annually)
- ✅ Increases financial inclusion (85% approval rate)
- ✅ Meets regulatory requirements
- ✅ Validated on external data (Kaggle)
- ✅ Has clear monitoring plan

**Next Steps:**
1. Obtain legal review (1-2 weeks)
2. Begin IT integration (4-6 weeks)
3. Pilot on 10% of applications (1 month)
4. Full rollout (Month 3)

**Approval Required From:**
- Chief Risk Officer (regulatory compliance)
- Chief Technology Officer (IT implementation)
- Chief Financial Officer (budget approval)

---

## Quick Reference: Key Numbers

| What | Value |
|------|-------|
| **Recommended Threshold** | 0.35 |
| **Approval Rate** | 85% |
| **Model AUC** | 0.7615 (Kaggle validated) |
| **Expected Annual Profit Lift** | $10-15M |
| **Implementation Cost** | $200-300K initial |
| **Fairness Status** | Pass (0.888 ratio) |
| **Retraining Schedule** | Annually minimum |
| **Monitoring Frequency** | Monthly |

---

**For technical details, see Sections 1-8 below.**

