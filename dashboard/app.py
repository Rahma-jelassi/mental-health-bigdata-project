"""
Dashboard Flask - Visualisation Temps Réel
Affiche les statistiques des posts Reddit analysés
"""

from flask import Flask, render_template, jsonify
import json
import os
from collections import Counter
from datetime import datetime

app = Flask(__name__)

DATA_FILE = "../data/processed_posts_realtime.json"

def load_posts():
    """Charge les posts depuis le fichier JSON"""
    try:
        if not os.path.exists(DATA_FILE):
            return []
        
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            posts = json.load(f)
        
        return posts
    except Exception as e:
        print(f"Erreur chargement: {e}")
        return []

def calculate_statistics(posts):
    """Calcule les statistiques"""
    if not posts:
        return {
            'total': 0,
            'avg_risk': 0,
            'high_risk': 0,
            'by_subreddit': {},
            'risk_distribution': {},
            'recent_posts': []
        }
    
    # Stats de base
    total = len(posts)
    avg_risk = sum(p.get('risk_score', 0) for p in posts) / total if total > 0 else 0
    high_risk = sum(1 for p in posts if p.get('risk_score', 0) >= 30)
    
    # Par subreddit
    by_subreddit = {}
    for post in posts:
        sub = post.get('subreddit', 'unknown')
        if sub not in by_subreddit:
            by_subreddit[sub] = {'count': 0, 'total_risk': 0, 'posts': []}
        by_subreddit[sub]['count'] += 1
        by_subreddit[sub]['total_risk'] += post.get('risk_score', 0)
        by_subreddit[sub]['posts'].append(post)
    
    # Moyennes par subreddit
    for sub in by_subreddit:
        by_subreddit[sub]['avg_risk'] = by_subreddit[sub]['total_risk'] / by_subreddit[sub]['count']
    
    # Distribution des scores
    risk_ranges = {'0-10': 0, '10-20': 0, '20-30': 0, '30+': 0}
    for post in posts:
        risk = post.get('risk_score', 0)
        if risk < 10:
            risk_ranges['0-10'] += 1
        elif risk < 20:
            risk_ranges['10-20'] += 1
        elif risk < 30:
            risk_ranges['20-30'] += 1
        else:
            risk_ranges['30+'] += 1
    
    # Posts récents (10 derniers)
    recent_posts = sorted(posts, key=lambda x: x.get('processed_at', ''), reverse=True)[:10]
    
    return {
        'total': total,
        'avg_risk': round(avg_risk, 2),
        'high_risk': high_risk,
        'by_subreddit': by_subreddit,
        'risk_distribution': risk_ranges,
        'recent_posts': recent_posts,
        'last_update': datetime.now().strftime('%H:%M:%S')
    }

@app.route('/')
def index():
    """Page principale"""
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    """API: Statistiques en temps réel"""
    posts = load_posts()
    stats = calculate_statistics(posts)
    return jsonify(stats)

@app.route('/api/posts')
def get_posts():
    """API: Tous les posts"""
    posts = load_posts()
    return jsonify(posts)

if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("🌐 DASHBOARD MENTAL HEALTH ANALYSIS")
    print("=" * 70)
    print("📊 URL: http://localhost:5000")
    print("🔄 Actualisation automatique toutes les 5s")
    print("⏹️  CTRL+C pour arrêter")
    print("=" * 70)
    print()
    
    app.run(debug=True, host='0.0.0.0', port=5000)