# Limitations and Risks Section
# Add this to MODEL_CARD.qmd after Fairness Analysis

---

# Limitations and Risks

## Purpose of This Section

No predictive model is perfect. This section provides an honest assessment of:
1. **Known limitations** of the model's design and training data
2. **Failure modes** where predictions may be unreliable
3. **Missing data** that could improve accuracy
4. **Risks** to consider before deployment
5. **Mitigation strategies** to address these limitations

**Transparency about limitations builds trust and enables responsible use.**

## Category 1: Data Limitations

### 1.1 Training Data is Historical (Through 2018)

**Limitation:**
- Model trained on loan applications from **2015-2018**
- Economic conditions, lending practices, and borrower behaviors have changed since then
- COVID-19 pandemic (2020-2021) fundamentally altered financial patterns

**Where model might fail:**
- Unprecedented economic events (recessions, inflation spikes)
- Shifts in credit behavior (e.g., increased use of buy-now-pay-later services)
- New fraud patterns not present in historical data
- Changes in employment markets (gig economy, remote work)

**Mitigation:**
- **Retrain annually** with fresh data
- **Monitor performance drift** monthly
- **Increase threshold** during economic uncertainty
- **Flag applications** from emerging industries for manual review

### 1.2 Geographic Scope Limitations

**Limitation:**
- Trained on Home Credit's specific operating regions (primarily emerging markets)
- May not generalize to:
  - Different countries with different credit cultures
  - Developed markets with mature credit systems
  - Regions with different regulatory frameworks

**Where model might fail:**
- Applications from new geographic markets
- Immigrants from different credit systems
- Markets with significantly different base default rates

**Mitigation:**
- **Do not deploy** in new markets without retraining
- **Collect local data** before expanding geographically
- **Benchmark performance** in new regions before full rollout

### 1.3 Limited Credit History Data

**Limitation:**
- External scores (EXT_SOURCE_1/2/3) are proprietary black boxes
- Missing alternative credit data:
  - Utility payment history
  - Rent payment history
  - Bank account behavior
  - Informal credit (family loans, community lending)

**Where model might fail:**
- **Credit invisibles** - People with no traditional credit history
- **Thin-file applicants** - Very limited credit data
- **Recent immigrants** - No local credit history
- **Young adults** - Haven't built credit yet

**Impact:** May deny creditworthy applicants who lack traditional credit markers but have strong alternative signals.

**Mitigation:**
- **Alternative data sources** - Partner with utility, telecom providers
- **Cash flow underwriting** - Bank account transaction analysis
- **Manual review** for thin-file applicants with high income
- **Graduated products** - Small starter loans to build credit history

### 1.4 Missing Macroeconomic Context

**Limitation:**
- Model does not incorporate real-time economic indicators:
  - Unemployment rates
  - Inflation
  - Interest rate environment
  - Regional economic shocks

**Where model might fail:**
- Economic downturns (model trained during growth period)
- Localized economic crises (factory closures, natural disasters)
- Rapid inflation affecting debt-to-income ratios

**Mitigation:**
- **Human oversight** during economic volatility
- **Dynamic thresholds** adjusted for macro conditions
- **Regular recalibration** as economy evolves

### 1.5 Class Imbalance Effects

**Limitation:**
- Only **8% of training data are defaults**
- Model better at identifying repayments than defaults
- Minority class (defaults) may have less reliable predictions

**Where model might fail:**
- **Rare default patterns** - Unusual combinations of features leading to default
- **False negatives** - Approving defaults that don't match common patterns
- **Edge cases** in the default class

**Evidence in confusion matrix:**
- Recall on defaults: 53% (catches only half of defaults at 0.5 threshold)
- Model prioritizes avoiding false positives over catching all defaults

**Mitigation:**
- **Optimal threshold (0.35)** improves default detection
- **SMOTE or upsampling** in future model iterations
- **Ensemble with specialized default detector** for borderline cases

## Category 2: Model Architecture Limitations

### 2.1 Random Forest Interpretability Constraints

**Limitation:**
- Random Forest is more interpretable than deep learning but still:
  - Cannot fully explain interaction effects
  - Averages across 100 trees (no single decision path)
  - SHAP analysis is approximate, not exact

**Where model might fail:**
- Explaining complex, multi-feature interactions to regulators
- Justifying individual decisions with high certainty
- Debugging unexpected predictions

**Mitigation:**
- Use SHAP analysis for post-hoc interpretability
- Maintain LIME explanations for individual cases
- Keep simpler logistic regression as interpretability benchmark

### 2.2 Feature Engineering Dependencies

**Limitation:**
- Model relies on **186 engineered features** from:
  - Bureau credit history
  - Previous loan applications
  - Installment payment patterns
  
- If upstream data quality degrades, model breaks
- Feature engineering pipeline complexity creates maintenance burden

**Where model might fail:**
- Data pipeline failures (missing joins, incorrect aggregations)
- Changes to source data schemas
- New data sources with different formats

**Mitigation:**
- **Automated data quality checks** before scoring
- **Feature monitoring** - Alert if feature distributions shift
- **Fallback rules** if critical features are missing

### 2.3 No Explicit Time-Series Modeling

**Limitation:**
- Model treats credit history as static aggregates
- Doesn't capture:
  - Trajectory of financial health (improving vs. deteriorating)
  - Recent events vs. old history
  - Seasonal patterns

**Where model might fail:**
- Applicants recently recovering from financial hardship (old bad data dominates)
- Applicants with recent negative trend (aggregates mask deterioration)
- Seasonal workers with variable income

**Mitigation:**
- Add recency-weighted features
- Include trend features (already have CC_BALANCE_TREND)
- Consider LSTM/RNN models in future versions

## Category 3: External Dependencies and Black Boxes

### 3.1 External Credit Scores Are Opaque

**Critical limitation:**
- **EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3** are the top 3 predictors
- These are **proprietary scores** - we don't know how they're calculated
- We're dependent on external vendors

**Risks:**
1. **Vendor changes algorithm** → Model performance degrades silently
2. **Vendor bias** → We inherit their discrimination
3. **Vendor failure** → Lose access to critical features
4. **Unexplainable to customers** → Adverse action challenges

**Where model might fail:**
- External score provider changes methodology
- Score becomes unavailable for certain applicants
- Regulator demands full transparency (we can't provide it)

**Mitigation:**
- **Monitor external score distributions** - Detect vendor changes
- **Diversify vendors** - Don't rely on single provider
- **Build internal credit score** as backup
- **Document vendor contracts** - Require change notifications

### 3.2 Data Freshness Risks

**Limitation:**
- Credit bureau data may be 30-90 days old
- Credit card features aggregated over past months
- Employment data self-reported (not real-time verified)

**Where model might fail:**
- Recent job loss (data shows employed)
- Recent credit event not yet in bureau data
- Fraud (fake employment information)

**Mitigation:**
- **Real-time income verification** via bank connections
- **Recency checks** - Flag if last update > 60 days old
- **Fraud detection layer** separate from default prediction

## Category 4: Behavioral and Psychological Factors

### 4.1 Cannot Predict Life Events

**Limitation:**
- Model has no data on future events:
  - Illness or injury
  - Divorce or family breakdown
  - Job loss
  - Natural disasters
  
**Where model might fail:**
- Life event causes default for previously low-risk borrower
- Model cannot predict "acts of God"

**Mitigation:**
- **Insurance products** - Payment protection insurance
- **Hardship programs** - Allow temporary payment deferrals
- **Acknowledge uncertainty** - Even low-probability predictions aren't zero

### 4.2 Intentionality Not Captured

**Limitation:**
- Cannot distinguish between:
  - **Can't pay** (financial inability)
  - **Won't pay** (strategic default)
  
**Where model might fail:**
- Wealthy individuals strategically defaulting
- Applicants with intent to commit fraud (application fraud vs. default prediction)

**Mitigation:**
- **Separate fraud model** for intent detection
- **Income verification** reduces strategic default
- **Legal recourse** for strategic defaulters

### 4.3 Motivation and Life Changes

**Limitation:**
- Model based on past behavior, not future motivation
- Cannot capture:
  - Career advancements in progress
  - Improved financial literacy
  - Life changes (marriage, inheritance)

**Where model might fail:**
- Denying applicants who have genuinely reformed financial habits
- Young professionals about to receive promotions/bonuses

**Mitigation:**
- **Update credit data frequently** - Capture improvements faster
- **Manual review** of borderline cases with narrative explanations
- **Reconsideration process** - Allow applicants to appeal with new information

## Category 5: Implementation and Operational Risks

### 5.1 Threshold Sensitivity

**Risk:**
- Optimal threshold (0.35) based on specific cost assumptions
- If costs change, threshold becomes suboptimal

**Scenarios causing problems:**
- Recovery rates improve → Optimal threshold shifts down (approve more)
- Interest rates change → Profit margins shift
- Competition increases → Need to approve more to maintain volume

**Mitigation:**
- **Quarterly threshold review** based on actual financial outcomes
- **Sensitivity analysis** - Test multiple thresholds
- **A/B testing** - Pilot threshold changes on small segments

### 5.2 Model Drift Over Time

**Risk:**
- Applicant population evolves
- Economic conditions change
- Competitor actions alter applicant pool (adverse selection)

**Where model might fail:**
- Performance degrades silently over 12-24 months
- Calibration becomes inaccurate (predicted 8% default → actual 12%)

**Mitigation:**
- **Monthly performance monitoring** - Track AUC, default rates, calibration
- **Automated alerts** - Trigger when performance drops >2%
- **Champion/challenger framework** - Test new models continuously

### 5.3 Human-AI Interaction Risks

**Risk:**
- Loan officers may:
  - Over-rely on model (automation bias)
  - Ignore model when they shouldn't (overconfidence bias)
  - Use model scores inappropriately

**Where this causes problems:**
- Loan officer approves high-risk case due to personal relationship
- Loan officer denies low-risk case due to discriminatory bias (model said approve, officer overrides)

**Mitigation:**
- **Override tracking** - Monitor when humans deviate from model
- **Training** - Educate users on when to trust vs. question model
- **Escalation rules** - Require justification for large overrides

### 5.4 Adversarial Gaming

**Risk:**
- Applicants learn what model looks for
- Attempt to game the system:
  - Timing applications strategically
  - Manipulating credit utilization before applying
  - Providing false information

**Where model might fail:**
- Application fraud not detected
- Synthetic identities
- Coordinated fraud rings

**Mitigation:**
- **Fraud detection system** separate from default prediction
- **Behavior anomaly detection** - Flag unusual patterns
- **Document verification** - Validate claimed information
- **Keep model details confidential** - Don't publish feature weights

## Category 6: Ethical and Social Limitations

### 6.1 Cannot Address Root Causes

**Fundamental limitation:**
- Model predicts default risk but doesn't address **why** people default
- Cannot fix:
  - Systemic poverty
  - Educational inequality
  - Lack of financial literacy
  - Economic inequality

**Where this matters:**
- Denying credit perpetuates exclusion
- Model optimizes profit, not social welfare

**Mitigation:**
- **Financial inclusion programs** - Partner with nonprofits
- **Credit builder products** - Help denied applicants improve
- **Financial education** - Offer resources to all applicants
- **Consider broader mission** - Balance profit with social responsibility

### 6.2 Feedback Loop Risk

**Risk:**
- Denying credit prevents credit building
- Creates self-fulfilling prophecy:
  1. No credit history → Denied
  2. Denied → Cannot build credit
  3. Still no credit history → Still denied

**Where model might fail:**
- Perpetuating exclusion of already underserved groups
- Preventing social mobility

**Mitigation:**
- **Alternative credit products** - Secured cards, microloans
- **Second-look programs** - Periodic reconsideration
- **Alternative data** - Expand definition of creditworthiness

## Summary: Known Failure Modes

| Failure Mode | Likelihood | Impact | Mitigation Priority |
|--------------|-----------|--------|-------------------|
| **Economic downturn** | Medium | High | 🔴 High - Monthly monitoring |
| **External score change** | Medium | High | 🔴 High - Vendor monitoring |
| **Model drift** | High | Medium | 🔴 High - Performance tracking |
| **Thin-file applicants** | High | Medium | 🟡 Medium - Alternative data |
| **Life events** | High | Low | 🟢 Low - Inherent uncertainty |
| **Strategic gaming** | Low | Medium | 🟡 Medium - Fraud detection |
| **Feedback loops** | Medium | High | 🔴 High - Financial inclusion programs |

## What Data Is Missing?

### High-Value Missing Data

1. **Real-time income verification**
   - Current: Self-reported, unverified
   - Ideal: Bank account analysis, employer API

2. **Alternative credit data**
   - Utility payments
   - Rent payment history
   - Mobile phone payments
   - Savings account behavior

3. **Macroeconomic indicators**
   - Local unemployment rates
   - Industry-specific economic indicators
   - Real-time inflation adjustments

4. **Social/behavioral signals**
   - Financial literacy assessment
   - Savings rate/emergency fund
   - Debt management behavior
   - Budget adherence

5. **Explanatory narratives**
   - Why past late payments occurred
   - Extenuating circumstances
   - Life situation changes

### Future Enhancement Opportunities

- **Open banking data** - Real-time account transaction analysis
- **Psychometric assessments** - Financial personality profiling
- **Social network analysis** (if legally permitted and ethical)
- **Machine learning on unstructured data** - Employment verification documents, bank statements

## Responsible Use Guidelines

Given these limitations, responsible use requires:

1. ✅ **Never use as sole decision factor** - Human review required
2. ✅ **Monitor continuously** - Performance, fairness, drift
3. ✅ **Update regularly** - Annual retraining minimum
4. ✅ **Document limitations** - Inform all users
5. ✅ **Provide recourse** - Appeal process for denied applicants
6. ✅ **Consider broader impact** - Balance profit with social responsibility
7. ✅ **Stay humble** - Acknowledge uncertainty in predictions

**Remember:** A good model used responsibly is better than a perfect model used carelessly.

---

**Next Section Preview:** Section 9 will provide an Executive Summary for senior leadership.
