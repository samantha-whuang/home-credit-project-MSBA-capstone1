# Adverse Action Mapping Section
# Add this to MODEL_CARD.qmd after Model Explainability

---

# Adverse Action Mapping

## Legal Requirement for Explainability

Under the **Equal Credit Opportunity Act (ECOA)** and **Fair Credit Reporting Act (FCRA)**, lenders must provide applicants with clear, specific, and understandable reasons when denying credit. Technical feature names like "EXT_SOURCE_2" or "CC_AVG_UTILIZATION" do not meet this legal standard.

This section translates the model's top predictive features into human-readable adverse action reasons that:
1. ✅ Comply with federal lending regulations
2. ✅ Are understandable to applicants without technical expertise
3. ✅ Provide actionable information when possible
4. ✅ Maintain accuracy in representing the model's decision factors

## Adverse Action Reason Mapping

### Tier 1: Primary Risk Factors

| Technical Feature | Adverse Action Reason | Additional Context |
|-------------------|----------------------|-------------------|
| **EXT_SOURCE_2 (low)** | "Limited external credit history" | Proprietary credit bureau score indicates insufficient credit track record |
| **EXT_SOURCE_3 (low)** | "Insufficient credit bureau assessment" | Alternative credit evaluation shows elevated risk |
| **DAYS_BIRTH (low value = younger)** | "Limited credit history due to age" | Younger applicants typically have shorter credit histories |
| **CC_AVG_UTILIZATION (high)** | "High credit card utilization indicating financial stress" | Using >50% of available credit suggests cash flow challenges |
| **DAYS_EMPLOYED (low value = short employment)** | "Insufficient employment history" | Short employment duration indicates less income stability |

### Tier 2: Strong Contributing Factors

| Technical Feature | Adverse Action Reason | Additional Context |
|-------------------|----------------------|-------------------|
| **AMT_CREDIT (high)** | "Loan amount too high relative to financial profile" | Requested amount exceeds prudent lending limits for applicant's situation |
| **CC_HIGH_UTIL_PCT (high)** | "Frequent credit card over-utilization" | Regularly maxing out credit cards (>80% utilization) indicates financial stress |
| **DAYS_ID_PUBLISH (recent)** | "Recently issued identification document" | New ID may indicate address instability or identity verification concerns |
| **AMT_ANNUITY (high)** | "Monthly payment amount too high relative to income" | Proposed payment may exceed sustainable debt-to-income ratio |
| **AMT_GOODS_PRICE (high)** | "Purchase amount exceeds typical lending range" | Goods price is outside normal parameters for applicant profile |

### Tier 3: Supporting Risk Factors

| Technical Feature | Adverse Action Reason |
|-------------------|----------------------|
| **EXT_SOURCE_1 (low)** | "Limited external credit history" |
| **CC_DPD_COUNT (high)** | "Multiple instances of late credit card payments" |
| **REGION_RATING_CLIENT (low)** | "Geographic area risk assessment" |
| **CC_LATE_COUNT (high)** | "History of late payments on credit accounts" |
| **DAYS_REGISTRATION (recent)** | "Recent residential registration indicating limited address stability" |
| **AMT_INCOME_TOTAL (low)** | "Insufficient income relative to loan amount" |
| **CC_BALANCE_TREND (increasing)** | "Rising credit card debt burden" |
| **REGION_POPULATION_RELATIVE (low)** | "Geographic and economic risk factors" |
| **DAYS_LAST_PHONE_CHANGE (recent)** | "Recent contact information changes" |
| **CC_PAYMENT_RATIO (low)** | "Low payment-to-balance ratio on credit cards" |

## Example Adverse Action Notice

When an application is denied, the notice must include the top 3-5 reasons. Here's an example:

::: {.callout-warning}
## Sample Adverse Action Notice

**Application Decision: DENIED**

We regret to inform you that your loan application has been denied. This decision was based on the following factors, listed in order of importance:

**Primary Reasons for Denial:**

1. **High credit card utilization indicating financial stress**  
   Your credit card accounts show an average utilization rate of 78%, which indicates potential difficulty managing additional debt obligations.

2. **Limited external credit history**  
   Credit bureau assessments indicate insufficient credit track record to support the requested loan amount.

3. **Insufficient employment history**  
   Your current employment duration of 8 months is below our standard requirement for this loan product.

**Additional Contributing Factors:**

4. **Multiple instances of late credit card payments**  
   Your credit history shows 8 late payment events in the past 12 months.

5. **Loan amount too high relative to financial profile**  
   The requested loan amount of $650,000 exceeds prudent lending limits given your current income and obligations.

---

**Your Rights:**
- You have the right to a free copy of your credit report from the credit bureau(s) we used
- You may dispute any inaccurate information in your credit report
- Federal law prohibits discrimination based on protected characteristics

**Contact Information:**  
For questions about this decision, please contact our Credit Review Department at 1-800-XXX-XXXX.

**Source of Information:**  
This decision was based on information from:
- Internal credit model assessment
- [Credit Bureau Names]
- Your application information
:::

## Best Practices for Adverse Action Communication

### 1. Use Clear, Plain Language

**❌ Don't Say:**
- "EXT_SOURCE_2 score below threshold"
- "DAYS_EMPLOYED negative value insufficient"
- "CC_AVG_UTILIZATION exceeds acceptable parameters"

**✅ Do Say:**
- "Limited external credit history"
- "Short employment history (less than 1 year)"
- "High credit card usage (78% of available credit)"

### 2. Provide Specific Numbers When Possible

**Better:** "High credit card utilization (78%)" vs. "High credit card utilization"

**Better:** "Employment history of 8 months" vs. "Insufficient employment history"

### 3. Rank Reasons by Importance

Always list reasons in order of their impact on the decision, as determined by SHAP values or feature importance.

### 4. Limit to Top 3-5 Reasons

Federal regulations typically require 3-5 reasons. More than 5 can be overwhelming and dilute the message.

### 5. Include Actionable Guidance (When Appropriate)

Help applicants understand what they can improve:

- "Consider reducing credit card balances below 30% utilization"
- "Establishing longer employment history may improve future applications"
- "Building additional credit history through smaller credit products"

## Challenging Cases: External Scores

### The Transparency Problem

**Challenge:** EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3 are the most important features but are proprietary "black box" scores. We cannot explain what drives them.

**Legal Requirement:** FCRA requires that if a credit score is a "key factor" in the decision, we must:
1. Disclose that a credit score was used
2. Provide the score value
3. Name the credit bureau that provided it
4. Explain the range of possible scores
5. Identify key factors that adversely affected the score

**Solution:** Use generic but truthful language:

> "Credit bureau assessment: Your credit score from [Bureau Name] was 524 out of a possible range of 0-1000. This score indicates limited positive credit history. Key factors affecting this score include: limited credit history, recent credit inquiries, and account utilization patterns."

**Important:** We translate "EXT_SOURCE_X (low)" as **"Limited external credit history"** because:
- It's truthful (low score does indicate limited/poor credit)
- It's understandable to consumers
- It meets FCRA disclosure requirements
- It avoids technical jargon

### Combining Features for Clearer Communication

Sometimes multiple related features can be combined into one clear reason:

**Instead of separate reasons:**
- "High CC_AVG_UTILIZATION"
- "High CC_HIGH_UTIL_PCT"  
- "High CC_DPD_COUNT"

**Combine as:**
- "Credit card payment patterns indicate financial stress: high utilization (78%), frequent over-utilization (60% of months), and 8 late payment events"

## Credit Card Features Translation Guide

Since credit card features are highly predictive and interpretable, here's a complete translation guide:

| Technical Feature | Plain Language | When to Use |
|-------------------|----------------|-------------|
| CC_AVG_UTILIZATION | "Average credit card utilization rate of [X]%" | Always specify percentage |
| CC_HIGH_UTIL_PCT | "Frequently maxing out credit cards ([X]% of months)" | Shows pattern, not just average |
| CC_DPD_COUNT | "Multiple late payment events ([X] instances)" | Specific count is more credible |
| CC_LATE_COUNT | "History of late payments on credit accounts" | Similar to DPD, may combine |
| CC_BALANCE_TREND | "Rising credit card debt over time" | Shows trajectory, not snapshot |
| CC_PAYMENT_RATIO | "Low payment amounts relative to balances" | Indicates minimum payments only |
| CC_DRAWING_RATE | "Frequent cash advances from credit cards" | Signals financial desperation |
| CC_MAX_DPD | "Maximum days past due: [X] days" | Most severe delinquency |
| CC_DRAWING_COUNT | "Frequent credit card advances" | Pattern of cash needs |
| CC_MONTHS_BALANCE | "Credit card data available for [X] months" | Shows observation period |

## Implementation Guidance

### For Credit Analysts and Loan Officers

When reviewing a denied application:

1. **Run the model** to get default probability and feature contributions (SHAP values)
2. **Identify top 3-5 features** with highest absolute SHAP values for this specific applicant
3. **Translate using this mapping guide** to generate adverse action reasons
4. **Rank in order** of SHAP importance
5. **Review for accuracy and fairness** before sending to applicant
6. **Add specific values** from applicant's data (e.g., "78% utilization" not just "high utilization")

### For Automated Systems

If generating notices automatically:

```python
# Pseudocode for automated adverse action generation
def generate_adverse_action_reasons(shap_values, feature_names, feature_values, n_reasons=4):
    # Get top N features by absolute SHAP value
    top_features = get_top_n_features(shap_values, n_reasons)
    
    reasons = []
    for feature, shap_val, actual_value in top_features:
        # Look up translation from mapping table
        plain_reason = ADVERSE_ACTION_MAP[feature]
        
        # Add specific value if available
        if feature.startswith('CC_'):
            plain_reason += f" ({actual_value:.1%})"
        
        reasons.append(plain_reason)
    
    return reasons
```

### Quality Assurance Checklist

Before sending adverse action notices, verify:

- ☐ Reasons are listed in order of importance (by SHAP value)
- ☐ Language is clear and free of technical jargon
- ☐ Specific values are included when available (e.g., percentages, counts)
- ☐ 3-5 reasons provided (not too few, not too many)
- ☐ Reasons are truthful and directly related to model features
- ☐ Notice includes required legal disclosures (credit bureau info, rights, contact info)
- ☐ No discriminatory language or references to protected characteristics

## Regulatory Compliance Notes

**Equal Credit Opportunity Act (ECOA) Requirements:**
- Must provide specific reasons for denial
- Cannot use vague terms like "insufficient credit" alone
- Must be sent within 30 days of application
- Cannot discriminate based on protected characteristics

**Fair Credit Reporting Act (FCRA) Requirements:**
- Must disclose if credit report was used
- Must provide credit score if it was a "key factor"
- Must name credit bureaus used
- Applicant has right to free credit report

**This mapping ensures compliance** while maintaining model accuracy and transparency.

---

**Next Section Preview:** Section 7 will analyze model fairness across demographic groups (gender, education) to identify potential disparities.
