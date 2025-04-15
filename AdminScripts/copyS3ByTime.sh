#!/bin/bash

###############################################################################
# S3 Cleanup Script - Deletes files older than specified days
# Compatible with Bash 3.2.57 on macOS
# Usage: chmod +x deleteS3_OlderThanDays.sh
# sudo bash ./deleteS3_OlderThanDays.sh --days 7 my-bucket
# Example: 
# sudo bash ./deleteS3_OlderThanDays.sh --days 2 --prefix Funding_Circle s3dropbucket-aggregator-fr Assume_Admin_Role_Frankfurt
# Use DELETE_LIMIT to contain total deleted files in the run
###############################################################################




###############################################################################
# function errecho
# Outputs messages to STDERR
###############################################################################
function errecho() {
    printf "%s\n" "$*" 1>&2
}

###############################################################################
# function check_aws_credentials
# Validates AWS credentials are available
###############################################################################
function check_aws_credentials() {
    local profile=$1
    
    if [ -n "$profile" ]; then
        # Check if profile exists
        if ! aws configure list --profile "$profile" >/dev/null 2>&1; then
            errecho "Error: AWS profile '$profile' not found"
            return 1
        fi
        # Export AWS_PROFILE
        export AWS_PROFILE="$profile"
    else
        # Check environment variables
        if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
            if [ -z "$AWS_PROFILE" ]; then
                errecho "Error: AWS credentials not found. Either:"
                errecho "  - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
                errecho "  - Set AWS_PROFILE environment variable"
                errecho "  - Provide a profile name as an argument"
                return 1
            fi
        fi
    fi
    
    # Verify AWS credentials work
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        errecho "Error: Invalid AWS credentials"
        return 1
    fi
    
    return 0
}

###############################################################################
# function convert_date
# Converts date to Unix timestamp, works on both macOS and Linux
###############################################################################
function convert_date() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS version - strip timezone before converting
        local clean_date="${1%+*}"  # Remove everything after '+'
        date -j -f "%Y-%m-%dT%H:%M:%S" "$clean_date" "+%s"
    else
        # Linux version
        date -d "$1" "+%s"
    fi
}

###############################################################################
# function sleep_ms
# Sleep for specified number of milliseconds
# Works on both Linux and macOS
###############################################################################
function sleep_ms() {
    local ms=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS version (using perl because BSD sleep doesn't support fractional seconds)
        perl -e "select(undef,undef,undef,$ms/1000)"
    else
        # Linux version
        sleep "$ms"e-3
    fi
}

###############################################################################
# function copy_objects_by_time
# Copies objects from source bucket to destination bucket based on modification time
#
# Parameters:
#   $1 - Source bucket name
#   $2 - Destination bucket name
#   $3 - Number of days to look back (objects modified within this many days)
#   $4 - Delay in MS after last copy before next copy 
###############################################################################
function copy_objects_by_time() {
    local source_bucket=$1
    local dest_bucket=$2
    local days_ago=$3
    local between_copies=$4
    local current_date
    local cutoff_date
    
    # Set default value if between_copies is not provided
    if [ -z "$between_copies" ]; then
        between_copies=10000
    fi

    # Verify bucket access
    if ! aws s3api head-bucket --bucket "$source_bucket" 2>/dev/null; then
        errecho "Error: Cannot access source bucket '$source_bucket'"
        return 1
    fi
    
    if ! aws s3api head-bucket --bucket "$dest_bucket" 2>/dev/null; then
        errecho "Error: Cannot access destination bucket '$dest_bucket'"
        return 1
    fi
    
    # Get current date in seconds since epoch
    current_date=$(date "+%s")
    # Calculate cutoff date in seconds
    cutoff_date=$((current_date - (days_ago * 86400)))
    
    echo "Copying objects modified in the last $days_ago days..."
    
    # List objects and their last modified dates
    aws s3api list-objects-v2 --bucket "$source_bucket" \
        --query 'Contents[].[Key,LastModified]' --output text | while read -r key date; do
        
        # Skip if key or date is empty
        if [ -z "$key" ] || [ -z "$date" ]; then
            continue
        fi
        
        # Convert ISO 8601 date to seconds since epoch
        object_date=$(convert_date "$date")
        
        # Skip if date conversion failed
        if [ -z "$object_date" ]; then
            errecho "Warning: Could not process date for $key"
            continue
        fi
        
        # Check if object is newer than cutoff date
        if [ "$object_date" -ge "$cutoff_date" ]; then
            echo "Copying: $key"
            
            # Perform the copy operation
            if aws s3 cp "s3://$source_bucket/$key" "s3://$dest_bucket/$key"; then
                 echo "$(date '+%Y-%m-%d %H:%M:%S') - Successfully copied: $key"
                # Sleep for specified milliseconds between copies
                sleep_ms "$between_copies"
            else
                errecho "ERROR: Failed to copy $key"
            fi
        fi
    done
}

# Show usage if --help is specified
if [ "$1" == "--help" ]; then
    echo "Usage: $0 <source-bucket> <destination-bucket> <days-ago> [aws-profile] [delay_betweencopies]"
    echo "Example: $0 my-source-bucket my-dest-bucket 7 my-profile 30000"
    echo ""
    echo "AWS credentials can be provided in three ways:"
    echo "1. Environment variables (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)"
    echo "2. AWS_PROFILE environment variable"
    echo "3. Profile name as the fourth argument"
    echo ""
    echo "delay_betweencopies: Optional delay in milliseconds between copy operations (default: 10000)"
    exit 0
fi


# Validate input parameters
if [ "$#" -lt 3 ] || [ "$#" -gt 5 ]; then    # Changed from 4 to 5 to account for delay parameter
    errecho "Usage: $0 <source-bucket> <destination-bucket> <days-ago> [aws-profile] [delay_betweencopies]"
    errecho "Example: $0 my-source-bucket my-dest-bucket 7 my-profile 30000"
    errecho "Use --help for more information"
    exit 1
fi

SOURCE_BUCKET=$1
DEST_BUCKET=$2
DAYS_AGO=$3
AWS_PROFILE_ARG=$4
DELAY_BETWEENCOPIES=$5

# Validate days_ago is a positive number
if ! [[ "$DAYS_AGO" =~ ^[0-9]+$ ]]; then
    errecho "Error: Days ago must be a positive number"
    exit 1
fi

# Validate DELAY_BETWEENCOPIES is a positive number if provided
if [ -n "$DELAY_BETWEENCOPIES" ] && ! [[ "$DELAY_BETWEENCOPIES" =~ ^[0-9]+$ ]]; then
    errecho "Error: delay_betweencopies must be a positive number"
    exit 1
fi


# Check AWS credentials
if ! check_aws_credentials "$AWS_PROFILE_ARG"; then
    exit 1
fi

# Execute the copy operation
copy_objects_by_time "$SOURCE_BUCKET" "$DEST_BUCKET" "$DAYS_AGO" "$DELAY_BETWEENCOPIES"
