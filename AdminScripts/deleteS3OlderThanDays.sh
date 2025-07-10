#!/bin/bash

###############################################################################
# S3 Object Cleanup Script
# Deletes objects older than specified days from S3 bucket
# Usage: 
# sudo bash ./deleteS3_OlderThanDays.sh --days 2 --prefix Funding_Circle s3dropbucket-aggregator-fr Assume_Admin_Role_Frankfurt
# Use DELETE_LIMIT to contain total deleted files in the run
###############################################################################

# Configuration
MAX_RETRIES=3
PARALLEL_PROCESSES=4
MAX_BATCH_SIZE=1000
TIMEOUT_SECONDS=3600 #60 secs * 60 minutes = 1 hour 
MAX_DELETE_BATCHES=10  # Limit number of delete batches (10 batches * 1000 objects = 10000 objects max)
DELETE_LIMIT=10000     # Maximum number of objects to delete

# Global counters (no -i declaration for Bash 3.2)
total_processed=0
total_deleted=0
total_errors=0
total_newer_files=0
total_objects_seen=0
DEBUG_MODE=false

# Use a temp file for error tracking instead of associative array
error_file=$(mktemp)
trap 'rm -f $error_file' EXIT

function log_message() {
    local level=$1
    shift
    # Skip DEBUG messages unless DEBUG_MODE is enabled
    if [ "$level" = "DEBUG" ] && [ "$DEBUG_MODE" = false ]; then
        return
    fi
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [$level] $*"
}

function errecho() {
    log_message "ERROR" "$*" 1>&2
}

function track_error() {
    local key=$1
    local error=$2
    echo "${key}|||${error}" >> "$error_file"
    : $((total_errors += 1))
}

function cleanup() {
    log_message "INFO" "Cleaning up and exiting..."
    jobs -p | xargs kill 2>/dev/null || true
    rm -f "$error_file"
    exit 1
}

function check_aws_credentials() {
    local profile=$1
    
    if [ -n "$profile" ]; then
        if ! aws configure list --profile "$profile" >/dev/null 2>&1; then
            errecho "AWS profile '$profile' not found"
            return 1
        fi
        export AWS_PROFILE="$profile"
    else
        if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
            if [ -z "$AWS_PROFILE" ]; then
                errecho "AWS credentials not found"
                errecho "Please provide credentials via:"
                errecho "  - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
                errecho "  - AWS_PROFILE environment variable"
                errecho "  - AWS profile name as argument"
                return 1
            fi
        fi
    fi
    
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        errecho "Invalid AWS credentials"
        return 1
    fi
    
    return 0
}

function convert_date() {
    local date_str=$1
    if [ -z "$OSTYPE_CACHED" ]; then
        OSTYPE_CACHED=$OSTYPE
    fi
    
    # Skip if date string is empty
    if [ -z "$date_str" ]; then
        echo "0"
        return
    fi

    # Clean up the date string
    # Remove milliseconds and timezone
    date_str=$(echo "$date_str" | sed -E 's/\.[0-9]{3}Z?$//')
    
    if echo "$OSTYPE_CACHED" | grep -q "darwin"; then
        # For macOS
        if [[ "$date_str" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; then
            # Convert ISO 8601 format to timestamp
            date -j -f "%Y-%m-%dT%H:%M:%S" "$date_str" "+%s" 2>/dev/null || echo "0"
        else
            # Try alternative format with space instead of T
            date -j -f "%Y-%m-%d %H:%M:%S" "${date_str/T/ }" "+%s" 2>/dev/null || echo "0"
        fi
    else
        # For Linux and other systems
        date -d "$date_str" "+%s" 2>/dev/null || echo "0"
    fi
}

function process_batch() {
    local bucket=$1
    local batch=$2
    local dry_run=$3
    
    if [ "$dry_run" = true ]; then
        local batch_size=0
        if [ -n "$batch" ]; then
            batch_size=$(echo "$batch" | jq '.Objects | length' || echo 0)
        fi
        log_message "DRY-RUN" "Would delete batch of $batch_size objects"
        return 0
    fi

    retry_count=0
    while [ "$retry_count" -lt "$MAX_RETRIES" ]; do
        if [ -n "$batch" ] && aws s3api delete-objects --bucket "$bucket" --delete "$batch" >/dev/null 2>&1; then
            return 0
        fi
        retry_count=$((retry_count + 1))
        [ "$retry_count" -lt "$MAX_RETRIES" ] && log_message "WARN" "Batch deletion failed, attempt $retry_count of $MAX_RETRIES"
        [ "$retry_count" -lt "$MAX_RETRIES" ] && sleep $((2 ** retry_count))
    done
    
    return 1
}

function delete_old_files() {
    local bucket=$1
    local days=$2
    local dry_run=$3
    local prefix=$4
    local batch_count=0
    
    if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        errecho "Cannot access bucket '$bucket'"
        return 1
    fi

    local current_date
    current_date=$(date "+%s")
    local cutoff_date=$((current_date - (days * 86400)))
    
    log_message "INFO" "Starting cleanup of bucket: $bucket"
    log_message "INFO" "Deleting files older than $days days"
    log_message "INFO" "Maximum delete batches: $MAX_DELETE_BATCHES"
    log_message "INFO" "Maximum objects to delete: $DELETE_LIMIT"
    [ "$dry_run" = true ] && log_message "INFO" "DRY RUN MODE - No deletions will be made"
    [ -n "$prefix" ] && log_message "INFO" "Prefix filter: $prefix"

    # Initialize batch processing
    local batch_keys=""
    local batch_size=0
    local token=""

    while true; do
        if [ "$total_deleted" -ge "$DELETE_LIMIT" ]; then
            log_message "INFO" "Reached maximum delete limit ($DELETE_LIMIT objects)"
            break
        fi

        if [ "$batch_count" -ge "$MAX_DELETE_BATCHES" ]; then
            log_message "INFO" "Reached maximum number of delete batches ($MAX_DELETE_BATCHES)"
            break
        fi

        local cmd="aws s3api list-objects-v2 --bucket $bucket"
        [ -n "$prefix" ] && cmd="$cmd --prefix $prefix"
        [ -n "$token" ] && cmd="$cmd --starting-token $token"

        local response
        response=$(eval "$cmd") || continue
        
        if [ -z "$response" ]; then
            log_message "INFO" "No objects found in bucket"
            break
        fi
        
        # Debug: Show how many objects were found
        local object_count=$(echo "$response" | jq '.Contents | length' 2>/dev/null || echo 0)
        log_message "DEBUG" "Found $object_count objects in this batch"

        while IFS=$'\t' read -r key last_modified || [ -n "$key" ]; do
            if [ -z "$key" ] || [ -z "$last_modified" ]; then
                continue
            fi
            
            : $((total_objects_seen += 1))

            if [ "$total_deleted" -ge "$DELETE_LIMIT" ]; then
                log_message "INFO" "Reached maximum delete limit ($DELETE_LIMIT objects)"
                break 2
            fi

            if [ "$batch_count" -ge "$MAX_DELETE_BATCHES" ]; then
                break 2
            fi

            local object_date
            object_date=$(convert_date "$last_modified")
            
            # Debug logging
            log_message "DEBUG" "Processing: $key, LastModified: $last_modified, Converted: $object_date, Cutoff: $cutoff_date"
            
            if [ -n "$object_date" ] && [ "$object_date" -gt 0 ] 2>/dev/null; then
                if [ "$object_date" -lt "$cutoff_date" ]; then
                    log_message "DEBUG" "File qualifies for deletion: $key"
                    batch_keys="${batch_keys}{\"Key\":\"$key\"},"
                    : $((batch_size += 1))
                    : $((total_processed += 1))
                else
                    : $((total_newer_files += 1))
                    
                    if [ "$batch_size" -eq "$MAX_BATCH_SIZE" ]; then
                        batch_keys=${batch_keys%,}
                        batch_json="{\"Objects\":[$batch_keys],\"Quiet\":true}"
                        if process_batch "$bucket" "$batch_json" "$dry_run"; then
                            : $((total_deleted += batch_size))
                            : $((batch_count += 1))
                            log_message "INFO" "Completed batch $batch_count of $MAX_DELETE_BATCHES (Total deleted: $total_deleted)"
                        else
                            track_error "batch_$total_processed" "Failed to delete batch"
                        fi
                        batch_keys=""
                        batch_size=0

                        if [ "$total_deleted" -ge "$DELETE_LIMIT" ] || [ "$batch_count" -ge "$MAX_DELETE_BATCHES" ]; then
                            break 2
                        fi
                    fi
                fi
            else
                track_error "date_conversion_$total_processed" "Failed to convert LastModified date: $last_modified for key: $key"
            fi
        done < <(echo "$response" | jq -r '.Contents[]|[.Key,.LastModified]|@tsv' 2>/dev/null || echo "")

        # Process any remaining files in the batch
        if [ -n "$batch_keys" ] && [ "$batch_size" -gt 0 ] && [ "$batch_count" -lt "$MAX_DELETE_BATCHES" ] && [ "$total_deleted" -lt "$DELETE_LIMIT" ]; then
            batch_keys=${batch_keys%,}
            batch_json="{\"Objects\":[$batch_keys],\"Quiet\":true}"
            if process_batch "$bucket" "$batch_json" "$dry_run"; then
                : $((total_deleted += batch_size))
                : $((batch_count += 1))
                log_message "INFO" "Completed final batch $batch_count of $MAX_DELETE_BATCHES (Total deleted: $total_deleted)"
            else
                track_error "batch_$total_processed" "Failed to delete final batch"
            fi
            batch_keys=""
            batch_size=0
        fi

        token=$(echo "$response" | jq -r '.NextToken // empty')
        [ -z "$token" ] && break
    done
}

function print_summary() {
    echo ""
    log_message "INFO" "Cleanup Summary:"
    log_message "INFO" "  Total objects examined: $total_objects_seen"
    log_message "INFO" "  Files processed: $total_processed"
    log_message "INFO" "  Files deleted: $total_deleted"
    log_message "INFO" "  Files newer than cutoff (not deleted): $total_newer_files"
    log_message "INFO" "  Errors encountered: $total_errors"
    [ "$DRY_RUN" = true ] && log_message "INFO" "  (DRY RUN - No actual deletions performed)"
    
    if [ "$total_errors" -gt 0 ]; then
        log_message "ERROR" "Error details:"
        while IFS="|||" read -r key error; do
            [ -n "$key" ] && log_message "ERROR" "  $key: $error"
        done < "$error_file"
    fi
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Show help
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Usage: $0 [options] <bucket> [aws-profile]"
    echo ""
    echo "Options:"
    echo "  --days <n>     Delete files older than n days (default: 3)"
    echo "  --prefix <p>   Only process files with prefix p"
    echo "  --dry-run      Show what would be deleted without actually deleting"
    echo "  --debug        Enable debug logging"
    echo "  --help, -h     Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 my-bucket                    # Delete files older than 3 days"
    echo "  $0 --days 7 my-bucket          # Delete files older than 7 days"
    echo "  $0 --prefix logs/ my-bucket     # Delete files in logs/ folder"
    echo "  $0 --dry-run my-bucket         # Show what would be deleted"
    exit 0
fi

# Parse arguments
DAYS=3
DRY_RUN=false
PREFIX=""
BUCKET=""
AWS_PROFILE_ARG=""

while [ $# -gt 0 ]; do
    case $1 in
        --days)
            DAYS="$2"
            shift 2
            ;;
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --debug)
            DEBUG_MODE=true
            shift
            ;;
        -*)
            errecho "Unknown option: $1"
            errecho "Use --help for usage information"
            exit 1
            ;;
        *)
            if [ -z "$BUCKET" ]; then
                BUCKET="$1"
            else
                AWS_PROFILE_ARG="$1"
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [ -z "$BUCKET" ]; then
    errecho "No bucket specified"
    errecho "Use --help for usage information"
    exit 1
fi

if ! echo "$DAYS" | grep -q '^[0-9]\+$'; then
    errecho "Days must be a positive number"
    exit 1
fi

# Check AWS credentials
if ! check_aws_credentials "$AWS_PROFILE_ARG"; then
    exit 1
fi

# Execute main function
delete_old_files "$BUCKET" "$DAYS" "$DRY_RUN" "$PREFIX"

# Print summary
print_summary

exit 0
