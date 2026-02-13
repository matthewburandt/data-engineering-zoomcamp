import pandas as pd
import random
from datetime import datetime, timedelta

# Generate 10,000 movie ratings
movies = ['The Matrix', 'Inception', 'Interstellar', 'The Godfather', 
          'Pulp Fiction', 'The Dark Knight', 'Fight Club', 'Forrest Gump']

ratings = []

for i in range(10_000):
    rating = {
        'rating_id': i + 1,
        'movie_title': random.choice(movies),
        'user_id': random.randint(1, 1000),
        'rating': round(random.uniform(1.0, 5.0), 1),
        'rating_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
        'review_length': random.randint(0, 500)
    }
    ratings.append(rating)
    
df = pd.DataFrame(ratings)
df.to_csv('/app/data/movie_ratings.csv', index=False)
print(f"Generated {len(df)} movie ratings and saved to 'movie_ratings.csv'")