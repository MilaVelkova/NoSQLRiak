import riak
import json
import ast
from collections import defaultdict

# Connect to Riak
riak_db = riak.RiakClient(pb_port=8087, protocol='pbc')

# Main movies bucket
movies_bucket = riak_db.bucket('movies')

# Aggregation buckets
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

# Helper functions
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

def clean_bucket(bucket):
    for key in bucket.get_keys():
        bucket.delete(key)

# Main aggregation function
def aggregate_movies():
    print("Cleaning all aggregation buckets...")
    for b in [genre_bucket, actor_bucket, year_bucket, language_bucket, country_bucket,
              top_rated_bucket, budget_bucket, revenue_bucket, profit_bucket, runtime_bucket, vote_average_bucket, popularity_bucket]:
        clean_bucket(b)
    print("Buckets cleaned.")

    processed = 0
    for key in movies_bucket.get_keys():
        movie_obj = movies_bucket.get(key)
        if not movie_obj.exists:
            continue

        try:
            movie = json.loads(movie_obj.data) if isinstance(movie_obj.data, str) else movie_obj.data
            movie_id = str(movie.get('id'))
            movie_key = f'movie:{movie_id}'

            # Extract fields
            title = movie.get('title', '')
            year = safe_int(movie.get('release_year'))
            budget = safe_float(movie.get('budget'))
            revenue = safe_float(movie.get('revenue'))
            profit = revenue - budget if revenue > 0 else 0
            runtime = safe_float(movie.get('runtime'))
            popularity = safe_float(movie.get('popularity'))
            vote_avg = safe_float(movie.get('vote_average'))

            # Language, director, country, genres
            language = str(movie.get('original_language', '')).strip()
            director = str(movie.get('director', '')).strip()
            countries_raw = movie.get('production_countries', '')
            countries = []
            if isinstance(countries_raw, str) and countries_raw:
                if countries_raw.startswith('['):
                    try:
                        countries = [str(c).strip() for c in ast.literal_eval(countries_raw) if c]
                    except:
                        countries = []
                else:
                    countries = [c.strip() for c in countries_raw.split(',')]

            genres_raw = movie.get('genres_list', '[]')
            try:
                genres = [str(g).strip() for g in ast.literal_eval(genres_raw) if g]
            except:
                genres = []

            # Actor/ Cast extraction
            cast_set = set()
            for star in ['Star1','Star2','Star3','Star4']:
                val = movie.get(star)
                if val: cast_set.add(str(val).strip())

            cast_list_raw = movie.get('cast_list','[]')
            try:
                cast_list = [str(c).strip() for c in ast.literal_eval(cast_list_raw) if c]
                cast_set.update(cast_list)
            except:
                pass

            # ----------------- Store in buckets -----------------

            # Year
            if year > 0:
                y_bucket = year_bucket.get(str(year))
                y_keys = y_bucket.data or []
                y_keys.append(movie_key)
                y_bucket.data = list(set(y_keys))
                y_bucket.store()

            # Language
            if language:
                l_bucket = language_bucket.get(language)
                l_keys = l_bucket.data or []
                l_keys.append(movie_key)
                l_bucket.data = list(set(l_keys))
                l_bucket.store()

            # Country
            for country in countries:
                c_bucket = country_bucket.get(country)
                c_keys = c_bucket.data or []
                c_keys.append(movie_key)
                c_bucket.data = list(set(c_keys))
                c_bucket.store()

            # Genre
            for genre in genres:
                g_bucket = genre_bucket.get(genre)
                g_keys = g_bucket.data or []
                g_keys.append(movie_key)
                g_bucket.data = list(set(g_keys))
                g_bucket.store()

                # Top rated per genre
                t_bucket = top_rated_bucket.get(genre)
                t_list = t_bucket.data or []
                t_list.append((movie_key, vote_avg))
                t_list = sorted(t_list, key=lambda x:x[1], reverse=True)[:100]
                t_bucket.data = t_list
                t_bucket.store()

            # Actor / Cast
            for actor in cast_set:
                if actor:
                    a_bucket = actor_bucket.get(actor)
                    a_keys = a_bucket.data or []
                    a_keys.append(movie_key)
                    a_bucket.data = list(set(a_keys))
                    a_bucket.store()

            # Budget / Revenue / Profit / Runtime / Vote / Popularity
            if budget > 0:
                b_bucket = budget_bucket.get(str(int(budget//1e6)))  # Range in millions
                b_keys = b_bucket.data or []
                b_keys.append(movie_key)
                b_bucket.data = list(set(b_keys))
                b_bucket.store()

            if revenue > 0:
                r_bucket = revenue_bucket.get(str(int(revenue//1e6)))
                r_keys = r_bucket.data or []
                r_keys.append(movie_key)
                r_bucket.data = list(set(r_keys))
                r_bucket.store()

            if profit > 0:
                p_bucket = profit_bucket.get(str(int(profit//1e6)))
                p_keys = p_bucket.data or []
                p_keys.append(movie_key)
                p_bucket.data = list(set(p_keys))
                p_bucket.store()

            if runtime > 0:
                ru_bucket = runtime_bucket.get(str(int(runtime//10)))  # Range by 10 minutes
                ru_keys = ru_bucket.data or []
                ru_keys.append(movie_key)
                ru_bucket.data = list(set(ru_keys))
                ru_bucket.store()

            if vote_avg > 0:
                v_bucket = vote_average_bucket.get(str(int(vote_avg*10)))  # Store in 0.1 increments
                v_keys = v_bucket.data or []
                v_keys.append(movie_key)
                v_bucket.data = list(set(v_keys))
                v_bucket.store()

            if popularity > 0:
                pop_bucket = popularity_bucket.get(str(int(popularity//10)))
                pop_keys = pop_bucket.data or []
                pop_keys.append(movie_key)
                pop_bucket.data = list(set(pop_keys))
                pop_bucket.store()

            processed += 1
            if processed % 100 == 0:
                print(f"Processed {processed} movies")

        except Exception as e:
            print(f"Error processing movie {key}: {e}")
            continue

    print(f"✅ Aggregation completed. Total processed: {processed} movies.")

if __name__ == '__main__':
    aggregate_movies()
