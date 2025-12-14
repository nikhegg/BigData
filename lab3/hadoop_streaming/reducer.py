import sys
import io

def calculate_stats(app_id, app_name, reviews):
    if not reviews:
        return None
    
    total_reviews = len(reviews)
    helpful_reviews = sum(1 for is_helpful in reviews.values() if is_helpful)
    
    if total_reviews > 0:
        percentage = round(helpful_reviews / total_reviews * 100, 2)
    else:
        percentage = 0.0
    
    return {
        'app_id': app_id,
        'app_name': app_name,
        'helpful': helpful_reviews,
        'total': total_reviews,
        'percentage': percentage
    }

def format_output(stats):
    if stats['percentage'] == int(stats['percentage']):
        percentage_str = f"{int(stats['percentage'])}"
    else:
        percentage_str = f"{stats['percentage']}"
    
    return f"{stats['app_name']} (ID{stats['app_id']}) - {stats['helpful']} ({percentage_str}%)"

def main():
    current_app_id = None
    current_app_name = None
    reviews = {}  # review_id: is_helpful (bool)
    
    all_results = []
    
    input_stream = io.TextIOWrapper(
        sys.stdin.buffer, 
        encoding='utf-8', 
        errors='replace'
    )
    
    for line in input_stream:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        if len(parts) < 4:
            continue
        
        app_id = parts[0]
        app_name = parts[1]
        review_id = parts[2]
        
        try:
            is_helpful = int(parts[3]) == 1
        except ValueError:
            continue
        
        if current_app_id is not None and app_id != current_app_id:
            stats = calculate_stats(current_app_id, current_app_name, reviews)
            if stats:
                all_results.append(stats)
            reviews = {}
        
        current_app_id = app_id
        current_app_name = app_name
        
        if review_id in reviews:
            reviews[review_id] = reviews[review_id] or is_helpful
        else:
            reviews[review_id] = is_helpful
    
    if current_app_id is not None:
        stats = calculate_stats(current_app_id, current_app_name, reviews)
        if stats:
            all_results.append(stats)
    
    all_results.sort(key=lambda x: (-x['helpful'], -x['percentage'], x['app_name']))
    
    for stats in all_results:
        print(format_output(stats))

if __name__ == "__main__":
    main()