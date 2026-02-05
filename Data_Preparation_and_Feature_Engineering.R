# ==============================================================================
# Home Credit Default Risk - Data Preparation and Feature Engineering
# Author: Samantha Huang
# Date: February 3, 2026
# Description: Reusable functions for data cleaning, feature engineering, and
#              preparation of train and test datasets based on EDA insights.
#              
# Pipeline Order:
#   1. Aggregate supplementary data
#   2. Clean data quality issues (DAYS_EMPLOYED anomaly, etc.)
#   3. Handle missing values
#   4. Engineer new features
# ==============================================================================

library(tidyverse)

# ==============================================================================
# SECTION 1: AGGREGATE SUPPLEMENTARY DATA
# ==============================================================================

# ------------------------------------------------------------------------------
# 1.1 Bureau Data Aggregation
# ------------------------------------------------------------------------------

#' Aggregate Bureau Credit History Data to Loan Level
#'
#' Creates loan-level features from credit bureau data including:
#' - Count of prior credits (total, active, closed)
#' - Overdue amounts and flags
#' - Debt ratios and credit utilization
#' - Credit age and recency metrics
#' 
#' Based on data dictionary:
#' - CREDIT_ACTIVE: Status of credit (Active, Closed, Sold, Bad debt)
#' - CREDIT_DAY_OVERDUE: Days past due at application time
#' - AMT_CREDIT_SUM: Current credit amount
#' - AMT_CREDIT_SUM_DEBT: Current debt amount
#' - AMT_CREDIT_SUM_OVERDUE: Current overdue amount
#'
#' @param bureau Data frame of bureau.csv
#' @return Data frame with one row per SK_ID_CURR with aggregated features
#' @examples
#' bureau <- read_csv("bureau.csv")
#' bureau_agg <- aggregate_bureau(bureau)
aggregate_bureau <- function(bureau) {
  
  message("Aggregating bureau data...")
  
  bureau_agg <- bureau |>
    group_by(SK_ID_CURR) |>
    summarise(
      # Count features - How many credits does client have in bureau?
      BUREAU_CREDIT_COUNT = n(),  # Total number of prior credits in bureau
      BUREAU_ACTIVE_COUNT = sum(CREDIT_ACTIVE == "Active", na.rm = TRUE),
      BUREAU_CLOSED_COUNT = sum(CREDIT_ACTIVE == "Closed", na.rm = TRUE),
      BUREAU_BAD_DEBT_COUNT = sum(CREDIT_ACTIVE == "Bad debt", na.rm = TRUE),
      BUREAU_SOLD_COUNT = sum(CREDIT_ACTIVE == "Sold", na.rm = TRUE),
      
      # Overdue features - Payment difficulties in credit history
      BUREAU_MAX_OVERDUE_DAYS = max(CREDIT_DAY_OVERDUE, na.rm = TRUE),
      BUREAU_AVG_OVERDUE_DAYS = mean(CREDIT_DAY_OVERDUE, na.rm = TRUE),
      BUREAU_HAS_OVERDUE = as.integer(any(CREDIT_DAY_OVERDUE > 0, na.rm = TRUE)),
      
      # Debt levels - Current outstanding amounts
      BUREAU_TOTAL_DEBT = sum(AMT_CREDIT_SUM_DEBT, na.rm = TRUE),
      BUREAU_AVG_DEBT = mean(AMT_CREDIT_SUM_DEBT, na.rm = TRUE),
      BUREAU_MAX_DEBT = max(AMT_CREDIT_SUM_DEBT, na.rm = TRUE),
      
      # Overdue amounts - How much is currently overdue?
      BUREAU_TOTAL_OVERDUE = sum(AMT_CREDIT_SUM_OVERDUE, na.rm = TRUE),
      BUREAU_MAX_OVERDUE_AMT = max(AMT_CREDIT_SUM_OVERDUE, na.rm = TRUE),
      
      # Average credit sum
      BUREAU_AVG_CREDIT_SUM = mean(AMT_CREDIT_SUM, na.rm = TRUE),
      
      # Credit age metrics - DAYS_CREDIT is negative (days before current app)
      BUREAU_OLDEST_CREDIT_DAYS = min(DAYS_CREDIT, na.rm = TRUE),  # Most negative = oldest
      BUREAU_NEWEST_CREDIT_DAYS = max(DAYS_CREDIT, na.rm = TRUE),  # Least negative = newest
      BUREAU_AVG_CREDIT_AGE_DAYS = mean(DAYS_CREDIT, na.rm = TRUE),
      
      # Credit prolongation - Indicator of refinancing/restructuring
      BUREAU_TOTAL_PROLONGED = sum(CNT_CREDIT_PROLONG, na.rm = TRUE),
      BUREAU_HAS_PROLONGED = as.integer(any(CNT_CREDIT_PROLONG > 0, na.rm = TRUE)),
      
      .groups = "drop"
    ) |>
    # Convert -Inf and Inf to NA (from max/min on empty sets)
    mutate(across(where(is.numeric), ~if_else(is.infinite(.x), NA_real_, .x)))
  
  message(sprintf("  ✓ Created %d bureau features for %d clients", 
                  ncol(bureau_agg) - 1, nrow(bureau_agg)))
  
  return(bureau_agg)
}


# ------------------------------------------------------------------------------
# 1.2 Previous Application Data Aggregation
# ------------------------------------------------------------------------------

#' Aggregate Previous Application History to Loan Level
#'
#' Creates loan-level features from previous Home Credit applications:
#' - Count of applications (total, approved, refused, cancelled)
#' - Approval rate and refusal rate
#' - Credit amount patterns (requested vs approved)
#' - Down payment behavior
#' 
#' Based on data dictionary:
#' - NAME_CONTRACT_STATUS: approved, cancelled, refused, unused offer
#' - CODE_REJECT_REASON: Why application was rejected
#' - AMT_APPLICATION: Requested amount
#' - AMT_CREDIT: Approved amount
#'
#' @param prev_app Data frame of previous_application.csv
#' @return Data frame with one row per SK_ID_CURR with aggregated features
#' @examples
#' prev_app <- read_csv("previous_application.csv")
#' prev_agg <- aggregate_previous_applications(prev_app)
aggregate_previous_applications <- function(prev_app) {
  
  message("Aggregating previous applications data...")
  
  prev_agg <- prev_app |>
    group_by(SK_ID_CURR) |>
    summarise(
      # Application counts by status
      PREV_APP_COUNT = n(),
      PREV_APP_APPROVED = sum(NAME_CONTRACT_STATUS == "Approved", na.rm = TRUE),
      PREV_APP_REFUSED = sum(NAME_CONTRACT_STATUS == "Refused", na.rm = TRUE),
      PREV_APP_CANCELLED = sum(NAME_CONTRACT_STATUS == "Canceled", na.rm = TRUE),
      PREV_APP_UNUSED = sum(NAME_CONTRACT_STATUS == "Unused offer", na.rm = TRUE),
      
      # Approval rate - Key indicator of creditworthiness
      PREV_APP_APPROVAL_RATE = sum(NAME_CONTRACT_STATUS == "Approved", na.rm = TRUE) / n(),
      
      # Refusal rate - Risk indicator
      PREV_APP_REFUSAL_RATE = sum(NAME_CONTRACT_STATUS == "Refused", na.rm = TRUE) / n(),
      
      # Amount features - Requested vs approved amounts
      PREV_APP_AVG_REQUESTED = mean(AMT_APPLICATION, na.rm = TRUE),
      PREV_APP_AVG_APPROVED = mean(AMT_CREDIT[NAME_CONTRACT_STATUS == "Approved"], na.rm = TRUE),
      PREV_APP_MAX_REQUESTED = max(AMT_APPLICATION, na.rm = TRUE),
      
      # Down payment behavior - Financial discipline indicator
      PREV_APP_AVG_DOWN_PAYMENT = mean(AMT_DOWN_PAYMENT, na.rm = TRUE),
      PREV_APP_AVG_DOWN_PAYMENT_RATE = mean(RATE_DOWN_PAYMENT, na.rm = TRUE),
      
      # Decision timing - Most recent decision (closest to 0)
      PREV_APP_LAST_DECISION_DAYS = max(DAYS_DECISION, na.rm = TRUE),
      
      .groups = "drop"
    ) |>
    # Convert -Inf and Inf to NA
    mutate(across(where(is.numeric), ~if_else(is.infinite(.x), NA_real_, .x)))
  
  message(sprintf("  ✓ Created %d previous application features for %d clients", 
                  ncol(prev_agg) - 1, nrow(prev_agg)))
  
  return(prev_agg)
}


# ------------------------------------------------------------------------------
# 1.3 Installments Payments Data Aggregation
# ------------------------------------------------------------------------------

#' Aggregate Installment Payment History to Loan Level
#'
#' Creates loan-level features from payment behavior:
#' - Late payment percentages and patterns
#' - Payment amounts (actual vs expected)
#' - Payment trends over time
#' 
#' Based on data dictionary:
#' - DAYS_INSTALMENT: When payment was supposed to be made
#' - DAYS_ENTRY_PAYMENT: When payment was actually made
#' - AMT_INSTALMENT: Expected payment amount
#' - AMT_PAYMENT: Actual payment amount
#'
#' @param installments Data frame of installments_payments.csv
#' @return Data frame with one row per SK_ID_CURR with aggregated features
#' @examples
#' installments <- read_csv("installments_payments.csv")
#' install_agg <- aggregate_installments(installments)
aggregate_installments <- function(installments) {
  
  message("Aggregating installments payments data...")
  
  install_agg <- installments |>
    mutate(
      # Calculate payment delay (positive = late, negative = early)
      PAYMENT_DELAY_DAYS = DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT,
      
      # Payment difference (negative = underpaid, positive = overpaid)
      PAYMENT_DIFF = AMT_PAYMENT - AMT_INSTALMENT,
      
      # Late payment flag (paid more than 0 days late)
      IS_LATE = as.integer(PAYMENT_DELAY_DAYS > 0),
      
      # Underpayment flag (allowing $1 tolerance for rounding)
      IS_UNDERPAID = as.integer(PAYMENT_DIFF < -1)
    ) |>
    group_by(SK_ID_CURR) |>
    summarise(
      # Payment counts
      INSTALL_PAYMENT_COUNT = n(),
      
      # Late payment statistics - Key default predictor
      INSTALL_LATE_PAYMENT_COUNT = sum(IS_LATE, na.rm = TRUE),
      INSTALL_LATE_PAYMENT_PCT = mean(IS_LATE, na.rm = TRUE),
      INSTALL_MAX_DELAY_DAYS = max(PAYMENT_DELAY_DAYS, na.rm = TRUE),
      INSTALL_AVG_DELAY_DAYS = mean(PAYMENT_DELAY_DAYS, na.rm = TRUE),
      
      # Underpayment statistics - Financial difficulty indicator
      INSTALL_UNDERPAID_COUNT = sum(IS_UNDERPAID, na.rm = TRUE),
      INSTALL_UNDERPAID_PCT = mean(IS_UNDERPAID, na.rm = TRUE),
      INSTALL_AVG_PAYMENT_DIFF = mean(PAYMENT_DIFF, na.rm = TRUE),
      
      # Payment amount patterns
      INSTALL_AVG_PAYMENT_AMT = mean(AMT_PAYMENT, na.rm = TRUE),
      INSTALL_AVG_EXPECTED_AMT = mean(AMT_INSTALMENT, na.rm = TRUE),
      
      # Payment ratio - Overall payment performance (1.0 = paid exactly as expected)
      INSTALL_PAYMENT_RATIO = sum(AMT_PAYMENT, na.rm = TRUE) / sum(AMT_INSTALMENT, na.rm = TRUE),
      
      .groups = "drop"
    ) |>
    # Convert -Inf and Inf to NA
    mutate(across(where(is.numeric), ~if_else(is.infinite(.x), NA_real_, .x)))
  
  message(sprintf("  ✓ Created %d installment features for %d clients", 
                  ncol(install_agg) - 1, nrow(install_agg)))
  
  return(install_agg)
}


# ==============================================================================
# SECTION 2: DATA QUALITY FIXES
# ==============================================================================

#' Fix Data Quality Issues Identified in EDA
#'
#' Addresses known data quality problems:
#' - DAYS_EMPLOYED anomaly: 365,243 days (1000 years) is placeholder for 
#'   pensioners/unemployed. Replace with NA.
#' - Other anomalies as identified in EDA
#'
#' From EDA: 55,374 records (18.01%) have this anomaly, primarily pensioners.
#'
#' @param data Data frame (train or test)
#' @return Data frame with quality issues fixed
#' @examples
#' train_cleaned <- fix_data_quality(train)
fix_data_quality <- function(data) {
  
  message("\nFixing data quality issues...")
  
  result <- data
  
  # Fix DAYS_EMPLOYED anomaly
  # EDA finding: 365243 is a placeholder for non-employed (pensioners, unemployed)
  anomaly_count <- sum(result$DAYS_EMPLOYED == 365243, na.rm = TRUE)
  
  if (anomaly_count > 0) {
    result <- result |>
      mutate(
        DAYS_EMPLOYED = if_else(
          DAYS_EMPLOYED == 365243, 
          NA_real_,  # Replace with NA
          DAYS_EMPLOYED
        )
      )
    
    message(sprintf("  ✓ Fixed DAYS_EMPLOYED: Replaced %d anomalous values (365,243) with NA", 
                    anomaly_count))
  } else {
    message("  ✓ No DAYS_EMPLOYED anomalies found")
  }
  
  # Could add other data quality fixes here as needed
  
  return(result)
}


# ==============================================================================
# SECTION 3: MISSING DATA HANDLING
# ==============================================================================

#' Compute Imputation Values from Training Data
#' 
#' Computes median values and strategies from training data to be used for 
#' imputing both train and test datasets (prevents data leakage)
#' 
#' Per EDA findings:
#' - EXT_SOURCE_1, 2, 3: Keep as NA (tree-based models handle natively)
#' - AMT_ANNUITY, CNT_FAM_MEMBERS, DAYS_LAST_PHONE_CHANGE: Fill with median
#' - OWN_CAR_AGE: NA means no car → 0
#' - AMT_GOODS_PRICE: Use AMT_CREDIT when missing
#' 
#' @param train_data Training dataset
#' @return List of imputation values to be used with apply_missing_value_imputation()
#' @examples
#' impute_vals <- compute_imputation_values(train)
compute_imputation_values <- function(train_data) {
  
  impute_values <- list(
    # These will be filled with medians from TRAINING data only
    amt_annuity_median = median(train_data$AMT_ANNUITY, na.rm = TRUE),
    cnt_fam_members_median = median(train_data$CNT_FAM_MEMBERS, na.rm = TRUE),
    days_last_phone_change_median = median(train_data$DAYS_LAST_PHONE_CHANGE, na.rm = TRUE),
    
    # OWN_CAR_AGE: NA means no car
    own_car_age_fill = 0,
    
    # AMT_GOODS_PRICE: Use AMT_CREDIT when NA
    amt_goods_price_strategy = "use_credit"
  )
  
  message("\nComputed imputation values from training data:")
  message(sprintf("  AMT_ANNUITY median: %.2f", impute_values$amt_annuity_median))
  message(sprintf("  CNT_FAM_MEMBERS median: %.2f", impute_values$cnt_fam_members_median))
  message(sprintf("  DAYS_LAST_PHONE_CHANGE median: %.2f", impute_values$days_last_phone_change_median))
  
  return(impute_values)
}


#' Apply Missing Value Imputation
#' 
#' Handles missing values using pre-computed imputation values from training data.
#' Creates missing value indicators and fills NAs with appropriate strategies.
#' 
#' NOTE: EXT_SOURCE_1, 2, 3 are kept as NA (per EDA) - tree-based models handle NA natively
#' 
#' @param data Dataset to impute (train or test)
#' @param impute_values List of imputation values from compute_imputation_values()
#' @return Dataset with missing values handled
#' @examples
#' impute_vals <- compute_imputation_values(train)
#' train <- apply_missing_value_imputation(train, impute_vals)
#' test <- apply_missing_value_imputation(test, impute_vals)
apply_missing_value_imputation <- function(data, impute_values) {
  
  message("\nApplying missing value imputation...")
  result <- data
  
  # ------------------------------------------------------------------
  # Step 1: Create missing value indicators (before imputation!)
  # ------------------------------------------------------------------
  
  message("  Creating missing value indicators...")
  
  # External source missing indicators (keep EXT_SOURCE as NA per EDA)
  if ("EXT_SOURCE_1" %in% names(result)) {
    result$EXT_SOURCE_1_MISSING <- as.integer(is.na(result$EXT_SOURCE_1))
  }
  if ("EXT_SOURCE_2" %in% names(result)) {
    result$EXT_SOURCE_2_MISSING <- as.integer(is.na(result$EXT_SOURCE_2))
  }
  if ("EXT_SOURCE_3" %in% names(result)) {
    result$EXT_SOURCE_3_MISSING <- as.integer(is.na(result$EXT_SOURCE_3))
  }
  
  # NOTE: EXT_SOURCE_1, 2, 3 are kept as NA (per EDA decision)
  # Tree-based models handle NA natively - no imputation needed
  
  # Employment and car ownership missing indicators
  if ("DAYS_EMPLOYED" %in% names(result)) {
    result$DAYS_EMPLOYED_MISSING <- as.integer(is.na(result$DAYS_EMPLOYED))
  }
  if ("OWN_CAR_AGE" %in% names(result)) {
    result$OWN_CAR_AGE_MISSING <- as.integer(is.na(result$OWN_CAR_AGE))
  }
  
  # ------------------------------------------------------------------
  # Step 2: Strategic imputation using TRAIN statistics
  # ------------------------------------------------------------------
  
  message("  Applying strategic imputation...")
  
  # Fill with TRAIN medians (negligible missingness per EDA)
  if ("AMT_ANNUITY" %in% names(result)) {
    na_count <- sum(is.na(result$AMT_ANNUITY))
    result$AMT_ANNUITY[is.na(result$AMT_ANNUITY)] <- impute_values$amt_annuity_median
    if (na_count > 0) {
      message(sprintf("    ✓ AMT_ANNUITY: %d NAs → %.2f (train median)", 
                      na_count, impute_values$amt_annuity_median))
    }
  }
  
  if ("CNT_FAM_MEMBERS" %in% names(result)) {
    na_count <- sum(is.na(result$CNT_FAM_MEMBERS))
    result$CNT_FAM_MEMBERS[is.na(result$CNT_FAM_MEMBERS)] <- impute_values$cnt_fam_members_median
    if (na_count > 0) {
      message(sprintf("    ✓ CNT_FAM_MEMBERS: %d NAs → %.2f (train median)", 
                      na_count, impute_values$cnt_fam_members_median))
    }
  }
  
  if ("DAYS_LAST_PHONE_CHANGE" %in% names(result)) {
    na_count <- sum(is.na(result$DAYS_LAST_PHONE_CHANGE))
    result$DAYS_LAST_PHONE_CHANGE[is.na(result$DAYS_LAST_PHONE_CHANGE)] <- 
      impute_values$days_last_phone_change_median
    if (na_count > 0) {
      message(sprintf("    ✓ DAYS_LAST_PHONE_CHANGE: %d NAs → %.2f (train median)", 
                      na_count, impute_values$days_last_phone_change_median))
    }
  }
  
  # OWN_CAR_AGE: NA means doesn't own a car
  if ("OWN_CAR_AGE" %in% names(result)) {
    na_count <- sum(is.na(result$OWN_CAR_AGE))
    result$OWN_CAR_AGE[is.na(result$OWN_CAR_AGE)] <- impute_values$own_car_age_fill
    if (na_count > 0) {
      message(sprintf("    ✓ OWN_CAR_AGE: %d NAs → 0 (no car)", na_count))
    }
  }
  
  # AMT_GOODS_PRICE: Use AMT_CREDIT when missing
  if ("AMT_GOODS_PRICE" %in% names(result) && "AMT_CREDIT" %in% names(result)) {
    na_idx <- is.na(result$AMT_GOODS_PRICE)
    na_count <- sum(na_idx)
    result$AMT_GOODS_PRICE[na_idx] <- result$AMT_CREDIT[na_idx]
    if (na_count > 0) {
      message(sprintf("    ✓ AMT_GOODS_PRICE: %d NAs → AMT_CREDIT", na_count))
    }
  }
  
  # Supplementary features: NA → 0 (no credit history)
  supp_cols <- grep("^(BUREAU|PREV|INSTALL)_", names(result), value = TRUE)
  if (length(supp_cols) > 0) {
    for (col in supp_cols) {
      if (any(is.na(result[[col]]))) {
        result[[col]][is.na(result[[col]])] <- 0
      }
    }
    message(sprintf("    ✓ Supplementary features: NA → 0 (%d features)", length(supp_cols)))
  }
  
  message(sprintf("  ✓ Missing value imputation complete. Total features: %d\n", ncol(result)))
  
  return(result)
}


# ==============================================================================
# SECTION 4: FEATURE ENGINEERING
# ==============================================================================

#' Engineer New Features from Cleaned Data
#'
#' Creates new features through:
#' - Transformations (days to years)
#' - Financial ratios (debt-to-income, etc.)
#' - Combinations of existing features
#' - Domain-specific indicators
#'
#' NOTE: This should be run AFTER data quality fixes and missing value handling.
#'
#' @param data Cleaned data frame
#' @return Data frame with engineered features added
#' @examples
#' train_features <- engineer_features(train_cleaned)
engineer_features <- function(data) {
  
  message("\nEngineering features...")
  original_cols <- ncol(data)
  
  result <- data |>
    mutate(
      # ------------------------------------------------------------------
      # Time-based features (convert days to years for interpretability)
      # ------------------------------------------------------------------
      AGE_YEARS = abs(DAYS_BIRTH) / 365.25,
      
      # EMPLOYMENT_YEARS: Handle NA (from our fix) - these are non-employed
      EMPLOYMENT_YEARS = if_else(
        !is.na(DAYS_EMPLOYED),
        abs(DAYS_EMPLOYED) / 365.25,
        0  # Non-employed = 0 years
      ),
      
      # ------------------------------------------------------------------
      # Financial ratios - Debt burden indicators (key from EDA)
      # ------------------------------------------------------------------
      CREDIT_INCOME_RATIO = AMT_CREDIT / AMT_INCOME_TOTAL,
      ANNUITY_INCOME_RATIO = AMT_ANNUITY / AMT_INCOME_TOTAL,
      CREDIT_ANNUITY_RATIO = AMT_CREDIT / AMT_ANNUITY,
      
      # Income per family member - Resource availability
      INCOME_PER_PERSON = AMT_INCOME_TOTAL / CNT_FAM_MEMBERS,
      
      # Credit-to-goods ratio (ratio > 1 = borrowing more than goods worth)
      CREDIT_GOODS_RATIO = if_else(
        AMT_GOODS_PRICE > 0,
        AMT_CREDIT / AMT_GOODS_PRICE,
        NA_real_
      ),
      
      # ------------------------------------------------------------------
      # External source combinations (EDA: strongest predictors)
      # ------------------------------------------------------------------
      # Mean of available external sources
      EXT_SOURCE_MEAN = rowMeans(
        pick(starts_with("EXT_SOURCE")), 
        na.rm = TRUE
      ),
      
      # Product of external sources (multiplicative interaction)
      EXT_SOURCE_PRODUCT = if_else(
        !is.na(EXT_SOURCE_1) & !is.na(EXT_SOURCE_2) & !is.na(EXT_SOURCE_3),
        EXT_SOURCE_1 * EXT_SOURCE_2 * EXT_SOURCE_3,
        NA_real_
      ),
      
      # ------------------------------------------------------------------
      # Document and contact features
      # ------------------------------------------------------------------
      # Count of documents submitted (more docs = more verification)
      DOCUMENTS_SUBMITTED = rowSums(
        pick(starts_with("FLAG_DOCUMENT")), 
        na.rm = TRUE
      ),
      
      # Count of contact methods provided
      CONTACT_METHODS = FLAG_MOBIL + FLAG_EMP_PHONE + FLAG_WORK_PHONE + 
                        FLAG_PHONE + FLAG_EMAIL,
      
      # ------------------------------------------------------------------
      # Regional features
      # ------------------------------------------------------------------
      # Region rating match (lives and works in same rated area)
      REGION_RATING_MATCH = if_else(
        REGION_RATING_CLIENT == REGION_RATING_CLIENT_W_CITY,
        1L, 0L
      ),
      
      # Address consistency (multiple address mismatch flags)
      ADDRESS_MISMATCH_COUNT = REG_REGION_NOT_LIVE_REGION + 
                               REG_REGION_NOT_WORK_REGION + 
                               LIVE_REGION_NOT_WORK_REGION,
      
      # ------------------------------------------------------------------
      # Risk indicators (binary flags based on EDA insights)
      # ------------------------------------------------------------------
      HIGH_CREDIT_BURDEN = if_else(CREDIT_INCOME_RATIO > 2, 1L, 0L),
      VERY_HIGH_CREDIT_BURDEN = if_else(CREDIT_INCOME_RATIO > 3, 1L, 0L),
      HIGH_ANNUITY_BURDEN = if_else(ANNUITY_INCOME_RATIO > 0.5, 1L, 0L),
      
      # External source risk (low scores = higher risk)
      LOW_EXT_SOURCE = if_else(
        !is.na(EXT_SOURCE_2) & EXT_SOURCE_2 < 0.2, 
        1L, 0L
      ),
      
      # Demographic risk factors
      YOUNG_APPLICANT = if_else(AGE_YEARS < 25, 1L, 0L),
      IS_MALE = if_else(CODE_GENDER == "M", 1L, 0L),
      HAS_CAR = if_else(FLAG_OWN_CAR == "Y", 1L, 0L),
      HAS_REALTY = if_else(FLAG_OWN_REALTY == "Y", 1L, 0L),
      
      # Employment status
      IS_UNEMPLOYED_PENSIONER = if_else(MISSING_DAYS_EMPLOYED == 1, 1L, 0L)
    )
  
  new_features <- ncol(result) - original_cols
  message(sprintf("  ✓ Created %d engineered features", new_features))
  
  return(result)
}


#' Create Combined Features from Main and Supplementary Data
#'
#' Creates features that combine main application data with aggregated
#' supplementary data. Run this AFTER merging supplementary aggregates.
#'
#' @param data Data frame with both main and supplementary features
#' @return Data frame with combined features added
#' @examples
#' train_combined <- engineer_combined_features(train)
engineer_combined_features <- function(data) {
  
  message("\nEngineering combined features...")
  original_cols <- ncol(data)
  
  result <- data
  
  # Bureau-based combined features
  if ("BUREAU_TOTAL_DEBT" %in% names(data)) {
    result <- result |>
      mutate(
        # Bureau debt to current income ratio
        BUREAU_DEBT_INCOME_RATIO = if_else(
          BUREAU_TOTAL_DEBT > 0,
          BUREAU_TOTAL_DEBT / AMT_INCOME_TOTAL,
          0
        ),
        
        # Active credit ratio (active / total bureau credits)
        BUREAU_ACTIVE_RATIO = if_else(
          BUREAU_CREDIT_COUNT > 0,
          BUREAU_ACTIVE_COUNT / BUREAU_CREDIT_COUNT,
          0
        ),
        
        # Overdue debt ratio
        BUREAU_OVERDUE_DEBT_RATIO = if_else(
          BUREAU_TOTAL_DEBT > 0,
          BUREAU_TOTAL_OVERDUE / BUREAU_TOTAL_DEBT,
          0
        )
      )
    message("  ✓ Created 3 bureau combined features")
  }
  
  # Previous application combined features
  if ("PREV_APP_AVG_REQUESTED" %in% names(data)) {
    result <- result |>
      mutate(
        # Previous credit requested to current credit ratio
        PREV_CREDIT_CURRENT_RATIO = if_else(
          PREV_APP_AVG_REQUESTED > 0,
          AMT_CREDIT / PREV_APP_AVG_REQUESTED,
          NA_real_
        ),
        
        # Approval history risk
        HIGH_REFUSAL_RATE = if_else(
          PREV_APP_REFUSAL_RATE > 0.5,
          1L, 0L
        )
      )
    message("  ✓ Created 2 previous application combined features")
  }
  
  # Installment combined features
  if ("INSTALL_LATE_PAYMENT_PCT" %in% names(data)) {
    result <- result |>
      mutate(
        # Payment behavior risk
        POOR_PAYMENT_HISTORY = if_else(
          INSTALL_LATE_PAYMENT_PCT > 0.1,  # More than 10% late
          1L, 0L
        )
      )
    message("  ✓ Created 1 installment combined feature")
  }
  
  new_features <- ncol(result) - original_cols
  message(sprintf("  Total combined features: %d", new_features))
  
  return(result)
}


# ==============================================================================
# SECTION 5: MAIN PIPELINE FUNCTION
# ==============================================================================

#' Complete Data Preparation and Feature Engineering Pipeline
#'
#' Orchestrates the full pipeline in correct order:
#' 1. Load and aggregate supplementary data
#' 2. Fix data quality issues (DAYS_EMPLOYED anomaly)
#' 3. Handle missing values (using imputation values from training)
#' 4. Engineer features from main data
#' 5. Engineer combined features
#'
#' NOTE: For proper train/test handling:
#' - First run on TRAINING data with impute_values = NULL
#' - Save the returned impute_values
#' - Then run on TEST data with the saved impute_values
#'
#' @param app_data Main application data (train or test)
#' @param data_path Path to directory containing supplementary CSV files
#' @param impute_values Imputation values from training (NULL for train, required for test)
#' @param include_bureau Include bureau features (default: TRUE)
#' @param include_prev_app Include previous application features (default: TRUE)
#' @param include_installments Include installment features (default: TRUE)
#' @return List with 'data' (prepared dataframe) and 'impute_values' (for test set)
#' @examples
#' # Step 1: Prepare training data
#' train <- read_csv("application_train.csv")
#' train_result <- prepare_data_pipeline(train, data_path = ".", impute_values = NULL)
#' train_prepared <- train_result$data
#' 
#' # Step 2: Prepare test data using train's imputation values
#' test <- read_csv("application_test.csv")
#' test_result <- prepare_data_pipeline(test, data_path = ".", 
#'                                       impute_values = train_result$impute_values)
#' test_prepared <- test_result$data
prepare_data_pipeline <- function(
    app_data,
    data_path = ".",
    impute_values = NULL,
    include_bureau = TRUE,
    include_prev_app = TRUE,
    include_installments = TRUE
) {
  
  is_training <- is.null(impute_values)
  dataset_type <- if (is_training) "TRAINING" else "TEST"
  
  message("\n" , rep("=", 70))
  message(sprintf("HOME CREDIT - DATA PREP & FEATURE ENG PIPELINE (%s)", dataset_type))
  message(rep("=", 70))
  message(sprintf("Starting with: %d rows × %d columns", 
                  nrow(app_data), ncol(app_data)))
  message(rep("=", 70))
  
  result <- app_data
  start_cols <- ncol(app_data)
  
  # --------------------------------------------------------------------------
  # STAGE 1: Aggregate and merge supplementary data
  # --------------------------------------------------------------------------
  message("\n[STAGE 1/5] AGGREGATING SUPPLEMENTARY DATA")
  message(rep("-", 70))
  
  # Bureau
  if (include_bureau) {
    message("\nProcessing bureau.csv...")
    bureau <- read_csv(file.path(data_path, "bureau.csv"), 
                       show_col_types = FALSE)
    bureau_agg <- aggregate_bureau(bureau)
    result <- result |> left_join(bureau_agg, by = "SK_ID_CURR")
  }
  
  # Previous applications
  if (include_prev_app) {
    message("\nProcessing previous_application.csv...")
    prev_app <- read_csv(file.path(data_path, "previous_application.csv"), 
                         show_col_types = FALSE)
    prev_agg <- aggregate_previous_applications(prev_app)
    result <- result |> left_join(prev_agg, by = "SK_ID_CURR")
  }
  
  # Installments
  if (include_installments) {
    message("\nProcessing installments_payments.csv...")
    installments <- read_csv(file.path(data_path, "installments_payments.csv"), 
                             show_col_types = FALSE)
    install_agg <- aggregate_installments(installments)
    result <- result |> left_join(install_agg, by = "SK_ID_CURR")
  }
  
  supp_cols_added <- ncol(result) - start_cols
  message(sprintf("\n✅ Stage 1 complete: %d supplementary features added", supp_cols_added))
  
  # --------------------------------------------------------------------------
  # STAGE 2: Fix data quality issues
  # --------------------------------------------------------------------------
  message("\n\n[STAGE 2/5] FIXING DATA QUALITY ISSUES")
  message(rep("-", 70))
  
  result <- fix_data_quality(result)
  message("\n✅ Stage 2 complete: Data quality issues fixed")
  
  # --------------------------------------------------------------------------
  # STAGE 3: Handle missing values
  # --------------------------------------------------------------------------
  message("\n\n[STAGE 3/5] HANDLING MISSING VALUES")
  message(rep("-", 70))
  
  before_missing <- ncol(result)
  
  # If training data, compute imputation values first
  if (is_training) {
    message("Computing imputation values from TRAINING data...")
    impute_values <- compute_imputation_values(result)
  } else {
    message("Using pre-computed imputation values from TRAINING data...")
  }
  
  # Apply imputation
  result <- apply_missing_value_imputation(result, impute_values)
  missing_cols_added <- ncol(result) - before_missing
  message(sprintf("✅ Stage 3 complete: %d missing indicator flags created", missing_cols_added))
  
  # --------------------------------------------------------------------------
  # STAGE 4: Engineer features
  # --------------------------------------------------------------------------
  message("\n\n[STAGE 4/5] ENGINEERING FEATURES")
  message(rep("-", 70))
  
  before_eng <- ncol(result)
  result <- engineer_features(result)
  eng_cols_added <- ncol(result) - before_eng
  message(sprintf("\n✅ Stage 4 complete: %d engineered features created", eng_cols_added))
  
  # --------------------------------------------------------------------------
  # STAGE 5: Create combined features
  # --------------------------------------------------------------------------
  message("\n\n[STAGE 5/5] ENGINEERING COMBINED FEATURES")
  message(rep("-", 70))
  
  before_comb <- ncol(result)
  result <- engineer_combined_features(result)
  comb_cols_added <- ncol(result) - before_comb
  message(sprintf("\n✅ Stage 5 complete: %d combined features created", comb_cols_added))
  
  # --------------------------------------------------------------------------
  # Final summary
  # --------------------------------------------------------------------------
  total_new_features <- ncol(result) - start_cols
  
  message("\n\n", rep("=", 70))
  message("PIPELINE COMPLETE! ✨")
  message(rep("=", 70))
  message(sprintf("Final dataset: %d rows × %d columns", nrow(result), ncol(result)))
  message(sprintf("\nNew features created: %d", total_new_features))
  message(sprintf("  • Supplementary aggregates: %d", supp_cols_added))
  message(sprintf("  • Missing indicators: %d", missing_cols_added))
  message(sprintf("  • Engineered features: %d", eng_cols_added))
  message(sprintf("  • Combined features: %d", comb_cols_added))
  message(rep("=", 70), "\n")
  
  # Return both the data and imputation values
  return(list(
    data = result,
    impute_values = impute_values
  ))
}


# ==============================================================================
# END OF SCRIPT
# ==============================================================================
