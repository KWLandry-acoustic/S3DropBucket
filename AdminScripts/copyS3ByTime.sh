#!/bin/bash

###############################################################################
# S3 Utility Script - Copy files in chronological order (oldest first)
# Compatible with Bash 3.2.57 on macOS
# Usage: chmod +x copyS3ByTime.sh
# ./copyS3ByTime.sh --source-bucket <bucket> --dest-bucket <bucket> --days <n> [options]
# Example: 
# ./copyS3ByTime.sh --source-bucket sourcebucket --dest-bucket destbucket --days 3 --profile default --delay 3000 --limit 10 --prefix logs/
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
# Copies objects in chronological order (oldest first) with configurable delay
#
# Parameters:
#   $1 - Source bucket name
#   $2 - Destination bucket name
#   $3 - Number of days to look back
#   $4 - Delay in milliseconds between copies
#   $5 - Limit number of files to copy (0 = no limit)
#   $6 - Object prefix filter
###############################################################################
function copy_objects_by_time() {
    local source_bucket=$1
    local dest_bucket=$2
    local days_ago=$3
    local delay_ms=${4:-1000}
    local limit=${5:-0}
    local prefix=$6
    local temp_file="/tmp/s3_objects_$$"
    
    # Verify bucket access
    if ! aws s3api head-bucket --bucket "$source_bucket" 2>/dev/null; then
        errecho "Error: Cannot access source bucket '$source_bucket'"
        return 1
    fi
    
    if ! aws s3api head-bucket --bucket "$dest_bucket" 2>/dev/null; then
        errecho "Error: Cannot access destination bucket '$dest_bucket'"
        return 1
    fi
    
    # Calculate cutoff date (files newer than this will be included)
    local current_time=$(date "+%s")
    local cutoff_date=$((current_time - (days_ago * 86400)))
    
    echo "Collecting objects from the last $days_ago days${prefix:+ with prefix '$prefix'}..."
    
    # Use aws s3 ls for better handling of large buckets
    local aws_cmd="aws s3 ls s3://$source_bucket/"
    [ -n "$prefix" ] && aws_cmd="aws s3 ls s3://$source_bucket/$prefix"
    aws_cmd="$aws_cmd --recursive --region eu-central-1"
    [ -n "$AWS_PROFILE" ] && aws_cmd="$aws_cmd --profile $AWS_PROFILE"
    
    # Get objects with timestamps using s3 ls (handles pagination automatically)
    [ "$DEBUG" = "true" ] && echo "Debug: Running command: $aws_cmd"
    eval "$aws_cmd" > "${temp_file}_raw" 2>&1
    
    # Check for AWS CLI errors
    if [ $? -ne 0 ]; then
        [ "$DEBUG" = "true" ] && echo "Debug: AWS CLI error output:" && cat "${temp_file}_raw"
        errecho "Error: AWS CLI command failed"
        rm -f "${temp_file}_raw"
        return 1
    fi
    
    local raw_count=$(wc -l < "${temp_file}_raw")
    [ "$DEBUG" = "true" ] && echo "Debug: Found $raw_count raw objects from AWS"
    [ "$DEBUG" = "true" ] && echo "Debug: Current time: $current_time ($(date))"
    [ "$DEBUG" = "true" ] && echo "Debug: Cutoff date (epoch): $cutoff_date ($(date -r $cutoff_date 2>/dev/null || date -d @$cutoff_date 2>/dev/null))"
    
    # Show first few raw entries for debugging
    [ "$DEBUG" = "true" ] && echo "Debug: First 3 raw entries:" && head -3 "${temp_file}_raw"
    
    # Check if bucket is empty
    if [ "$raw_count" -eq 0 ]; then
        echo "Bucket is empty - no objects to copy"
        echo "Copy operation completed: 0 files processed"
        rm -f "${temp_file}_raw"
        return 0
    fi
    
    # Process s3 ls output format: date time size key
    while read -r date time size key; do
        [ "$DEBUG" = "true" ] && echo "Debug: Processing: date='$date' time='$time' size='$size' key='$key'"
        # Skip empty lines
        [ -n "$key" ] && [ -n "$date" ] && [ -n "$time" ] || continue
        
        # Convert date and time to timestamp
        local datetime="${date}T${time}"
        object_date=$(convert_date "$datetime")
        
        if [ -n "$object_date" ] && [[ "$object_date" =~ ^[0-9]+$ ]]; then
            [ "$DEBUG" = "true" ] && echo "Debug: Object $key - date: $object_date, cutoff: $cutoff_date, include: $([[ $object_date -ge $cutoff_date ]] && echo yes || echo no)"
            [ "$object_date" -ge "$cutoff_date" ] && echo "$object_date $key"
        else
            [ "$DEBUG" = "true" ] && echo "Debug: Skipping object $key - invalid date conversion: '$object_date' from '$datetime'"
        fi
    done < "${temp_file}_raw" | sort -n > "$temp_file"
    
    rm -f "${temp_file}_raw"
    
    local available_files=$(wc -l < "$temp_file")
    
    # Apply limit if specified
    if [ "$limit" -gt 0 ] && [ "$available_files" -gt "$limit" ]; then
        head -n "$limit" "$temp_file" > "${temp_file}_limited"
        mv "${temp_file}_limited" "$temp_file"
        echo "Found $available_files files, copying oldest $limit files"
    else
        echo "Found $available_files files to copy in chronological order"
    fi
    
    local total_files=$(wc -l < "$temp_file")
    
    # Copy files in chronological order
    local count=0
    while read -r timestamp key; do
        count=$((count + 1))
        echo "[$count/$total_files] Copying: $key"
        
        if aws s3 cp "s3://$source_bucket/$key" "s3://$dest_bucket/$key" --quiet; then
            echo "Successfully copied: $key"
        else
            errecho "ERROR: Failed to copy $key"
        fi
        
        # Delay between copies (except for last file)
        [ "$count" -lt "$total_files" ] && sleep_ms "$delay_ms"
    done < "$temp_file"
    
    rm -f "$temp_file"
    echo "Copy operation completed: $count files processed"
}

# Parse named parameters
SOURCE_BUCKET=""
DEST_BUCKET=""
DAYS_AGO=""
AWS_PROFILE_ARG=""
DELAY_MS="1000"
LIMIT="0"
PREFIX=""
DEBUG="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source-bucket)
            SOURCE_BUCKET="$2"
            shift 2
            ;;
        --dest-bucket)
            DEST_BUCKET="$2"
            shift 2
            ;;
        --days)
            DAYS_AGO="$2"
            shift 2
            ;;
        --profile)
            AWS_PROFILE_ARG="$2"
            shift 2
            ;;
        --delay)
            DELAY_MS="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --debug)
            DEBUG="true"
            shift
            ;;
        --help)
            echo "Usage: $0 --source-bucket <bucket> --dest-bucket <bucket> --days <n> [options]"
            echo "Example: $0 --source-bucket my-source --dest-bucket my-dest --days 7 --profile default --delay 3000 --limit 10 --prefix logs/"
            echo ""
            echo "Required:"
            echo "  --source-bucket  Source S3 bucket name"
            echo "  --dest-bucket    Destination S3 bucket name"
            echo "  --days          Number of days to look back"
            echo ""
            echo "Optional:"
            echo "  --profile       AWS profile name"
            echo "  --delay         Milliseconds between copies (default: 1000)"
            echo "  --limit         Max number of oldest files to copy (default: 0 = no limit)"
            echo "  --prefix        Object key prefix filter"
            echo "  --debug         Enable debug output"
            exit 0
            ;;
        *)
            errecho "Unknown parameter: $1"
            errecho "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$SOURCE_BUCKET" ] || [ -z "$DEST_BUCKET" ] || [ -z "$DAYS_AGO" ]; then
    errecho "Error: Missing required parameters"
    errecho "Usage: $0 --source-bucket <bucket> --dest-bucket <bucket> --days <n>"
    errecho "Use --help for more information"
    exit 1
fi

# Validate parameter values
if ! [[ "$DAYS_AGO" =~ ^[0-9]+$ ]]; then
    errecho "Error: --days must be a positive number"
    exit 1
fi

if ! [[ "$DELAY_MS" =~ ^[0-9]+$ ]]; then
    errecho "Error: --delay must be a positive number"
    exit 1
fi

if ! [[ "$LIMIT" =~ ^[0-9]+$ ]]; then
    errecho "Error: --limit must be a positive number"
    exit 1
fi


# Check AWS credentials
if ! check_aws_credentials "$AWS_PROFILE_ARG"; then
    exit 1
fi

# Execute the copy operation
copy_objects_by_time "$SOURCE_BUCKET" "$DEST_BUCKET" "$DAYS_AGO" "$DELAY_MS" "$LIMIT" "$PREFIX"
