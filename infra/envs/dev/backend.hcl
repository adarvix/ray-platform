# Remote state for every layer in the dev environment. The per-layer `key` is passed by
# scripts/ci/tf.sh (key = dev/<layer>.tfstate). Replace ACCOUNT_ID with the output of 00-bootstrap.
bucket       = "corpreg-dev-tfstate-ACCOUNT_ID"
region       = "ap-south-1"
encrypt      = true
use_lockfile = true # S3-native locking (Terraform >= 1.10): no DynamoDB table
