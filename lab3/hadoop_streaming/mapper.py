import sys
import csv
import io

APP_ID_IDX = 1
APP_NAME_IDX = 2
REVIEW_ID_IDX = 3
VOTES_HELPFUL_IDX = 9

def clean_field(field):
    if field is None:
        return ""
    return str(field).strip().replace('\t', ' ').replace('\n', ' ').replace('\r', '')

def main():
    csv.field_size_limit(10 * 1024 * 1024)  # 10 MB
    
    input_stream = io.TextIOWrapper(
        sys.stdin.buffer, 
        encoding='utf-8', 
        errors='replace'
    )
    
    reader = csv.reader(input_stream)
    
    is_header = True
    processed = 0
    output = 0
    errors = 0
    
    for row in reader:
        processed += 1
        
        if is_header:
            is_header = False
            if len(row) > APP_ID_IDX and 'app_id' in row[APP_ID_IDX].lower():
                sys.stderr.write(f"Skipping header: {row[:5]}...\n")
                continue
        
        try:
            if len(row) <= VOTES_HELPFUL_IDX:
                errors += 1
                continue
            
            app_id = clean_field(row[APP_ID_IDX])
            app_name = clean_field(row[APP_NAME_IDX])
            review_id = clean_field(row[REVIEW_ID_IDX])
            votes_helpful_raw = clean_field(row[VOTES_HELPFUL_IDX])
            
            if not app_id or not review_id:
                errors += 1
                continue
            
            try:
                votes_helpful = int(float(votes_helpful_raw))
            except (ValueError, TypeError):
                votes_helpful = 0
            
            is_helpful = 1 if votes_helpful >= 3 else 0
            
            print(f"{app_id}\t{app_name}\t{review_id}\t{is_helpful}")
            output += 1
            
        except Exception as e:
            errors += 1
            if errors <= 10:
                sys.stderr.write(f"Error at row {processed}: {e}\n")

if __name__ == "__main__":
    main()