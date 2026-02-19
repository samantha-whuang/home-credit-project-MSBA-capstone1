# ==============================================================================
# Run Pipeline to Generate Fully Imputed Data
# ==============================================================================

library(tidyverse)
source("Data_Preparation_and_Feature_Engineering.R")

# Load raw data
message("Loading raw data...")
train <- read_csv("application_train.csv")
test <- read_csv("application_test.csv")

# Process TRAINING data with final imputation
message("\n\nPROCESSING TRAINING DATA...")
train_result <- prepare_data_pipeline(
  app_data = train,
  data_path = ".",
  impute_values = NULL,
  include_credit_card = TRUE,  # NEW: Include credit card features
  final_imputation = TRUE
)

# Process TEST data with its own medians
message("\n\nPROCESSING TEST DATA...")
test_result <- prepare_data_pipeline(
  app_data = test,
  data_path = ".",
  impute_values = NULL,  # NULL = compute from test data's own values
  include_credit_card = TRUE,  # NEW: Include credit card features
  final_imputation = TRUE
)

# Extract prepared datasets
train_prepared <- train_result$data
test_prepared <- test_result$data

# Export
message("\n\nEXPORTING FILES...")
write_csv(train_prepared, "application_train_fully_imputed.csv")
write_csv(test_prepared, "application_test_fully_imputed.csv")

# Verify
message("\n\n", rep("=", 70))
message("FINAL VERIFICATION")
message(rep("=", 70))
message(sprintf("Train: %d rows × %d cols, %d NAs", 
                nrow(train_prepared), ncol(train_prepared), sum(is.na(train_prepared))))
message(sprintf("Test: %d rows × %d cols, %d NAs", 
                nrow(test_prepared), ncol(test_prepared), sum(is.na(test_prepared))))
message(rep("=", 70))
message("\n✅ Files created:")
message("  • application_train_fully_imputed.csv")
message("  • application_test_fully_imputed.csv")
message("\nNote: Each dataset uses its own medians for imputation.")
message("Ready for modeling with logistic regression and all other algorithms!")
