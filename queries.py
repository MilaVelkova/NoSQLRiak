import riak 
import time
import json
import ast
import sys
from collections import defaultdict

# Try to import psutil for CPU/memory monitoring (optional)
try:
    import psutil
    import os

    HAS_PSUTIL = True
    current_process = psutil.Process(os.getpid())
except ImportError:
    HAS_PSUTIL = False
    print("⚠️  psutil not available - CPU and memory monitoring disabled")

# Connect to Riak using Protocol Buffers
print("Connecting to Riak...")
try:
    riak_db = riak.RiakClient(pb_port=8087, protocol='pbc')
    riak_db.ping()
    print("✅ Connected to Riak successfully\n")
except Exception as e:
    print(f"❌ Failed to connect to Riak: {e}")
    sys.exit(1)

bucket = riak_db.bucket('movies')
movies_bucket = riak_db.bucket('movies')
genre_bucket = riak_db.bucket('genre')
actor_bucket = riak_db.bucket('actor')
year_bucket = riak_db.bucket('year')
language_bucket = riak_db.bucket('language')
country_bucket = riak_db.bucket('country')
top_rated_bucket = riak_db.bucket('top_rated')
budget_bucket = riak_db.bucket('budget')
revenue_bucket = riak_db.bucket('revenue')
profit_bucket = riak_db.bucket('profit')
runtime_bucket = riak_db.bucket('runtime')
vote_average_bucket = riak_db.bucket('vote_average')
popularity_bucket = riak_db.bucket('popularity')


# ------------------- Helpers -------------------
def safe_float(val):
    try:
        return float(val) if val not in (None, '', 'nan') else 0.0
    except:
        return 0.0


def safe_int(val):
    try:
        return int(float(val)) if val not in (None, '', 'nan') else 0
    except:
        return 0


def load_movie(key):
    obj = movies_bucket.get(key)
    if obj.exists:
        try:
            return json.loads(obj.data) if isinstance(obj.data, str) else obj.data
        except:
            return None
    return None


def load_movies_batch(keys):
    return [m for m in (load_movie(k) for k in keys) if m]


def get_bucket_keys(bucket, key):
    obj = bucket.get(key)
    if obj.exists:
        return obj.data or []
    return []


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def timed_query(func, runs=10, *args, **kwargs):
    """Run a query multiple times and calculate average execution time, CPU, and memory"""
    times = []
    cpu_percentages = []
    memory_usages = []
    results = None

    for _ in range(runs):
        if HAS_PSUTIL:
            current_process.cpu_percent()
            mem_before = current_process.memory_info().rss / (1024 * 1024)  # MB

        start = time.time()
        results = func(*args, **kwargs)
        elapsed = time.time() - start
        times.append(elapsed)

        if HAS_PSUTIL:
            cpu_percentages.append(current_process.cpu_percent())
            memory_usages.append(current_process.memory_info().rss / (1024 * 1024))

        time.sleep(0.01)

    avg_time = sum(times) / len(times)
    avg_cpu = sum(cpu_percentages) / len(cpu_percentages) if cpu_percentages else 0
    avg_memory = sum(memory_usages) / len(memory_usages) if memory_usages else 0

    if HAS_PSUTIL:
        print(f"Average execution time: {avg_time:.6f}s | CPU: {avg_cpu:.2f}% | Memory: {avg_memory:.2f} MB")
    else:
        print(f"Average execution time: {avg_time:.6f}s")

    # Print number of movies found
    if results is not None:
        print(f"Movies found: {len(results)}")

    return results, {
        "avg_time": avg_time,
        "avg_cpu": avg_cpu,
        "avg_memory": avg_memory,
        "min_time": min(times),
        "max_time": max(times)
    }


def query_by_index_range(index_name, min_val, max_val):
    """Query Riak using secondary index range"""
    try:
        keys = bucket.get_index(index_name, min_val, max_val)
        return list(keys.results)
    except Exception as e:
        print(f"Error querying index {index_name}: {e}")
        return []


def query_by_index_exact(index_name, value):
    """Query Riak using secondary index exact match"""
    try:
        keys = bucket.get_index(index_name, value)
        return list(keys.results)
    except Exception as e:
        print(f"Error querying index {index_name}: {e}")
        return []


# ============================================================================
# SIMPLE QUERIES (4)
# ============================================================================

def simple_query_profitable_movies(min_budget=10_000_000, min_revenue_multiplier=3):
    """
    Profitable movies using actual budget & revenue values
    min_budget in original currency (not millions)
    """
    results = []

    # Iterate over budget bucket keys (still needed for efficiency)
    for budget_key in budget_bucket.get_keys():
        movie_keys = get_bucket_keys(budget_bucket, budget_key)

        # Load all movies for this bucket key
        for movie in load_movies_batch(movie_keys):
            budget = safe_float(movie.get("budget"))
            revenue = safe_float(movie.get("revenue"))

            # Filter using actual values (not bucket key)
            if budget < min_budget:
                continue
            if revenue >= budget * min_revenue_multiplier:
                results.append({
                    "id": movie.get("id"),
                    "title": movie.get("title"),
                    "budget": budget,
                    "revenue": revenue,
                    "profit": revenue - budget,
                    "roi": revenue / budget
                })

    # Sort by ROI descending
    results.sort(key=lambda x: x["roi"], reverse=True)
    return results


def simple_query_popular_recent_movies(year_start=2015, min_popularity=50, min_vote_count=1000):
    """Popular recent movies using year and popularity buckets"""
    results = []
    for year_key in year_bucket.get_keys():
        year = safe_int(year_key)
        if year < year_start:
            continue
        movie_keys = get_bucket_keys(year_bucket, year_key)
        for movie in load_movies_batch(movie_keys):
            popularity = safe_float(movie.get("popularity"))
            vote_count = safe_int(movie.get("vote_count"))
            if popularity >= min_popularity and vote_count >= min_vote_count:
                results.append({
                    "id": movie.get("id"),
                    "title": movie.get("title"),
                    "release_year": year,
                    "popularity": popularity,
                    "vote_count": vote_count
                })
    results.sort(key=lambda x: x["popularity"], reverse=True)
    return results

def simple_query_long_high_rated_movies(min_runtime=150, min_rating=7.5, year_start=2000):
    results = []
    for ru_key in runtime_bucket.get_keys():
        runtime_val = safe_int(ru_key) * 10  # FIX: scale back
        if runtime_val < min_runtime:
            continue
        movie_keys = get_bucket_keys(runtime_bucket, ru_key)
        for movie in load_movies_batch(movie_keys):
            vote_avg = safe_float(movie.get("vote_average"))
            runtime = safe_float(movie.get("runtime"))
            release_year = safe_int(movie.get("release_year"))
            if vote_avg >= min_rating and runtime >= min_runtime and release_year >= year_start:
                results.append({
                    "id": movie.get("id"),
                    "title": movie.get("title"),
                    "runtime": runtime,
                    "vote_average": vote_avg,
                    "release_year": release_year
                })
    results.sort(key=lambda x: (x["vote_average"], x["runtime"]), reverse=True)
    return results


def simple_query_spanish_blockbusters(min_budget=10, min_revenue=20, language="es"):
    """Spanish blockbusters using language & budget/revenue buckets"""
    results = []
    lang_keys = get_bucket_keys(language_bucket, language)
    for movie in load_movies_batch(lang_keys):
        budget = safe_float(movie.get("budget")) / 1e6
        revenue = safe_float(movie.get("revenue")) / 1e6
        if budget >= min_budget and revenue >= min_revenue:
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "budget": budget,
                "revenue": revenue
            })
    results.sort(key=lambda x: x["revenue"], reverse=True)
    return results


# ============================================================================
# COMPLEX QUERIES (3)
# ============================================================================

def complex_query_multi_genre(genres=["Action","Adventure","Science Fiction"], min_rating=7.0):
    """Movies belonging to multiple genres"""
    results = []
    # Find intersection of genre buckets
    genre_sets = [set(get_bucket_keys(genre_bucket, g)) for g in genres]
    common_keys = set.intersection(*genre_sets) if genre_sets else set()
    for movie in load_movies_batch(list(common_keys)):
        vote_avg = safe_float(movie.get("vote_average"))
        if vote_avg >= min_rating:
            results.append({
                "id": movie.get("id"),
                "title": movie.get("title"),
                "vote_average": vote_avg
            })
    results.sort(key=lambda x: x["vote_average"], reverse=True)
    return results

def complex_query_genre_country_language(genre="Drama", country="United States of America", language="en",
                                         min_year=2010):
    """Movies by genre, country, language, and year filter"""
    genre_keys = set(get_bucket_keys(genre_bucket, genre))
    country_keys = set(get_bucket_keys(country_bucket, country))
    lang_keys = set(get_bucket_keys(language_bucket, language))
    common_keys = genre_keys & country_keys & lang_keys
    results = []
    for movie in load_movies_batch(list(common_keys)):
        vote_avg = safe_float(movie.get("vote_average"))
        release_year = safe_int(movie.get("release_year"))
        if vote_avg == 0 or release_year < min_year:
            continue
        results.append({
            "id": movie.get("id"),
            "title": movie.get("title"),
            "vote_average": vote_avg,
            "release_year": release_year
        })
    results.sort(key=lambda x: x["vote_average"], reverse=True)
    return results


def complex_query_high_budget_profit(min_budget=50):
    """High budget and profitable movies"""
    results = []
    for budget_key in budget_bucket.get_keys():
        budget_val = safe_int(budget_key)
        if budget_val < min_budget:
            continue
        movie_keys = get_bucket_keys(budget_bucket, budget_key)
        for movie in load_movies_batch(movie_keys):
            budget = safe_float(movie.get("budget")) / 1e6
            revenue = safe_float(movie.get("revenue")) / 1e6
            if revenue > budget:
                results.append({
                    "id": movie.get("id"),
                    "title": movie.get("title"),
                    "budget": budget,
                    "revenue": revenue,
                    "profit": revenue - budget
                })
    results.sort(key=lambda x: x["profit"], reverse=True)
    return results


# ============================================================================
# AGGREGATED QUERIES (5)
# ============================================================================

def aggregate_movies_per_year():
    results = []
    for year_key in year_bucket.get_keys():
        keys = get_bucket_keys(year_bucket, year_key)
        results.append({"release_year": safe_int(year_key), "movie_count": len(keys)})
    results.sort(key=lambda x: x["release_year"], reverse=True)
    return results

def aggregate_avg_rating_per_genre():
    results = []
    for genre_key in genre_bucket.get_keys():
        keys = get_bucket_keys(genre_bucket, genre_key)
        ratings = [safe_float(load_movie(k).get("vote_average")) for k in keys]
        ratings = [r for r in ratings if r>0]
        if ratings:
            results.append({
                "genre": genre_key,
                "movie_count": len(keys),
                "avg_rating": sum(ratings)/len(ratings)
            })
    results.sort(key=lambda x: x["avg_rating"], reverse=True)
    return results

def aggregate_top_actors_by_movie_count(top_n=10):
    results = []
    for actor_key in actor_bucket.get_keys():
        keys = get_bucket_keys(actor_bucket, actor_key)
        results.append({
            "actor": actor_key,
            "movie_count": len(keys)
        })
    results.sort(key=lambda x: x["movie_count"], reverse=True)
    return results[:top_n]

def aggregate_yearly_trends():
    """Aggregate: Yearly movie industry trends using year_bucket"""

    year_data = defaultdict(lambda: {
        "count": 0,
        "ratings": [],
        "budgets": [],
        "revenues": [],
        "runtimes": [],
        "high_rated_count": 0
    })

    for year_key in year_bucket.get_keys():
        try:
            year_obj = year_bucket.get(year_key)
            movie_keys = year_obj.data or []
        except:
            continue

        for movie_key in movie_keys:
            movie_obj = movies_bucket.get(movie_key)
            if not movie_obj.exists:
                continue
            movie = json.loads(movie_obj.data) if isinstance(movie_obj.data, str) else movie_obj.data

            year = safe_int(movie.get("release_year"))
            if year < 1990:
                continue

            vote_avg = safe_float(movie.get("vote_average"))
            budget = safe_float(movie.get("budget"))
            revenue = safe_float(movie.get("revenue"))
            runtime = safe_float(movie.get("runtime"))

            year_data[year]["count"] += 1
            if vote_avg > 0:
                year_data[year]["ratings"].append(vote_avg)
                if vote_avg >= 7.0:
                    year_data[year]["high_rated_count"] += 1
            if budget > 0:
                year_data[year]["budgets"].append(budget)
            if revenue > 0:
                year_data[year]["revenues"].append(revenue)
            if runtime > 0:
                year_data[year]["runtimes"].append(runtime)

    results = []
    for year, data in year_data.items():
        results.append({
            "release_year": year,
            "movie_count": data["count"],
            "avg_rating": sum(data["ratings"]) / len(data["ratings"]) if data["ratings"] else 0,
            "avg_budget": sum(data["budgets"]) / len(data["budgets"]) if data["budgets"] else 0,
            "avg_revenue": sum(data["revenues"]) / len(data["revenues"]) if data["revenues"] else 0,
            "avg_runtime": sum(data["runtimes"]) / len(data["runtimes"]) if data["runtimes"] else 0,
            "high_rated_count": data["high_rated_count"]
        })

    results.sort(key=lambda x: x["release_year"], reverse=True)
    print(f"[Aggregate] Yearly trends: {len(results)} years")
    return results


def aggregate_genre_combinations():
    """Aggregate most common genre combinations from movies bucket"""

    from collections import defaultdict
    import ast
    import json

    combo_stats = defaultdict(lambda: {"count": 0, "ratings": []})

    movie_keys = movies_bucket.get_keys()

    for key in movie_keys:
        obj = movies_bucket.get(key)
        if not obj or not obj.data:
            continue

        movie = obj.data
        if isinstance(movie, str):
            try:
                movie = json.loads(movie)
            except:
                continue

        raw = movie.get("genres_list", "[]")

        # Safe parse list
        try:
            genres = ast.literal_eval(raw) if isinstance(raw, str) else raw
            genres = [g.strip() for g in genres if g]
        except:
            genres = []

        # Only combinations of 2+
        if len(genres) < 2:
            continue

        genres_tuple = tuple(sorted(genres))
        combo_stats[genres_tuple]["count"] += 1

        rating = movie.get("vote_average")
        if rating:
            try:
                r = float(rating)
                if r > 0:
                    combo_stats[genres_tuple]["ratings"].append(r)
            except:
                pass

    # Prepare results
    result = []
    for genres, data in combo_stats.items():
        if data["count"] >= 3:  # same as SQL HAVING COUNT(*) >= 3
            avg_rating = sum(data["ratings"]) / len(data["ratings"]) if data["ratings"] else 0
            result.append({
                "genres": list(genres),
                "movie_count": data["count"],
                "avg_rating": avg_rating
            })

    # Same ordering as SQL
    result.sort(key=lambda x: x["movie_count"], reverse=True)

    print(f"[Aggregate] Genre combinations: {len(result)} combinations")
    return result


# ============================================================================
# MAIN - Run all queries with timing
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("RIAK QUERY BENCHMARKS")
    print("=" * 80)

    # Get total keys
    print(f"\nCounting keys in Riak bucket 'movies'...")
    all_keys = list(bucket.get_keys())
    total_keys = len(all_keys)
    print(f"Total Keys in Bucket: {total_keys:,}")

    # Store results for summary
    results = {}

    print("\n" + "=" * 80)
    print("SIMPLE QUERIES (4)")
    print("=" * 80)

    _, metrics = timed_query(simple_query_profitable_movies, 10, 10_000_000, 3)
    results["Simple 1: Profitable movies"] = metrics

    _, metrics = timed_query(simple_query_popular_recent_movies, 10, 2015, 50, 1000)
    results["Simple 2: Popular recent"] = metrics

    _, metrics = timed_query(simple_query_long_high_rated_movies, 10, 150, 7.5, 2000)
    results["Simple 3: Long high-rated"] = metrics

    _, metrics = timed_query(simple_query_spanish_blockbusters, 10, 10, 20, "es")
    results["Simple 4: Spanish blockbusters"] = metrics

    print("\n" + "=" * 80)
    print("COMPLEX QUERIES (3)")
    print("=" * 80)

    _, metrics = timed_query(complex_query_multi_genre, 10, ["Action", "Adventure", "Science Fiction"], 7.0)
    results["Complex 1: Multi-genre"] = metrics

    _, metrics = timed_query(complex_query_genre_country_language, 10, "Drama", "United States of America", "en", 2010)
    results["Complex 2: Genre+Country+Lang"] = metrics

    _, metrics = timed_query(complex_query_high_budget_profit, 10, 50)
    results["Complex 3: High budget profit"] = metrics

    print("\n" + "=" * 80)
    print("AGGREGATED QUERIES (5)")
    print("=" * 80)

    _, metrics = timed_query(aggregate_movies_per_year, 10)
    results["Aggregate 1: Movies per year"] = metrics

    _, metrics = timed_query(aggregate_avg_rating_per_genre, 10)
    results["Aggregate 2: Avg rating per genre"] = metrics

    _, metrics = timed_query(aggregate_top_actors_by_movie_count, 10, 10)
    results["Aggregate 3: Top actors"] = metrics

    _, metrics = timed_query(aggregate_yearly_trends, 10)
    results["Aggregate 4: Yearly trends"] = metrics

    _, metrics = timed_query(aggregate_genre_combinations, 10)
    results["Aggregate 5: Genre combinations"] = metrics

    # Calculate category averages
    simple_results = [v for k, v in results.items() if "Simple" in k]
    complex_results = [v for k, v in results.items() if "Complex" in k]
    aggregate_results = [v for k, v in results.items() if "Aggregate" in k]

    simple_avg_time = sum([r["avg_time"] for r in simple_results]) / len(simple_results)
    simple_avg_cpu = sum([r["avg_cpu"] for r in simple_results]) / len(simple_results)
    simple_avg_memory = sum([r["avg_memory"] for r in simple_results]) / len(simple_results)

    complex_avg_time = sum([r["avg_time"] for r in complex_results]) / len(complex_results)
    complex_avg_cpu = sum([r["avg_cpu"] for r in complex_results]) / len(complex_results)
    complex_avg_memory = sum([r["avg_memory"] for r in complex_results]) / len(complex_results)

    aggregate_avg_time = sum([r["avg_time"] for r in aggregate_results]) / len(aggregate_results)
    aggregate_avg_cpu = sum([r["avg_cpu"] for r in aggregate_results]) / len(aggregate_results)
    aggregate_avg_memory = sum([r["avg_memory"] for r in aggregate_results]) / len(aggregate_results)

    overall_avg_time = sum([r["avg_time"] for r in results.values()]) / len(results)
    overall_avg_cpu = sum([r["avg_cpu"] for r in results.values()]) / len(results)
    overall_avg_memory = sum([r["avg_memory"] for r in results.values()]) / len(results)

    # Print summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY BY CATEGORY")
    print("=" * 80)

    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ SIMPLE QUERIES (4)                                                      │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {simple_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {simple_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {simple_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")

    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ COMPLEX QUERIES (3)                                                     │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {complex_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {complex_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {complex_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")

    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ AGGREGATED QUERIES (5)                                                  │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {aggregate_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {aggregate_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {aggregate_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")

    print("\n┌─────────────────────────────────────────────────────────────────────────┐")
    print("│ OVERALL (12 QUERIES)                                                    │")
    print("├─────────────────────────────────────────────────────────────────────────┤")
    print(f"│ Avg Execution Time:  {overall_avg_time:>10.6f} seconds                          │")
    print(f"│ Avg CPU Usage:       {overall_avg_cpu:>10.2f} %                                 │")
    print(f"│ Avg Memory Usage:    {overall_avg_memory:>10.2f} MB                               │")
    print("└─────────────────────────────────────────────────────────────────────────┘")

    print("\n" + "=" * 80)
    print("DETAILED RESULTS (All 12 Queries)")
    print("=" * 80)
    print(f"\n{'Query Name':<40} {'Time (s)':<12} {'CPU (%)':<10} {'Memory (MB)':<12}")
    print("-" * 80)
    for query_name, metrics in results.items():
        print(
            f"{query_name:<40} {metrics['avg_time']:<12.6f} {metrics['avg_cpu']:<10.2f} {metrics['avg_memory']:<12.2f}")

    # Save results to JSON
    output_data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "database": "Riak",
        "total_keys": total_keys,
        "category_averages": {
            "simple": {
                "avg_time": simple_avg_time,
                "avg_cpu": simple_avg_cpu,
                "avg_memory": simple_avg_memory
            },
            "complex": {
                "avg_time": complex_avg_time,
                "avg_cpu": complex_avg_cpu,
                "avg_memory": complex_avg_memory
            },
            "aggregated": {
                "avg_time": aggregate_avg_time,
                "avg_cpu": aggregate_avg_cpu,
                "avg_memory": aggregate_avg_memory
            },
            "overall": {
                "avg_time": overall_avg_time,
                "avg_cpu": overall_avg_cpu,
                "avg_memory": overall_avg_memory
            }
        },
        "detailed_results": results
    }

    output_file = f"riak_benchmark_results_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"\n✅ Results saved to: {output_file}")
    print("=" * 80)
