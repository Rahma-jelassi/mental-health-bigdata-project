"""
Reddit Data Collector for Mental Health Analysis
Academic Big Data Project - READ ONLY
"""

import praw
import json
from datetime import datetime

def collect_posts(client_id, client_secret, user_agent, subreddits, limit=50):
    """
    Collect public posts from mental health subreddits
    READ-ONLY - No posting, commenting, or user interaction
    """
    
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    
    posts = []
    subreddit = reddit.subreddit('+'.join(subreddits))
    
    for submission in subreddit.hot(limit=limit):
        post_data = {
            'id': submission.id,
            'title': submission.title,
            'text': submission.selftext,
            'subreddit': str(submission.subreddit),
            'created_utc': submission.created_utc,
            'score': submission.score,
            'num_comments': submission.num_comments,
            'timestamp': datetime.now().isoformat()
            # Note: Author is anonymized in processing pipeline
        }
        posts.append(post_data)
    
    return posts

if __name__ == "__main__":
    # Configuration loaded from secure config file
    # Not included in repository for security
    pass