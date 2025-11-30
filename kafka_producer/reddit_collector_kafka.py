"""
Reddit Kafka Producer - Collecte en Temps Réel
Envoie chaque nouveau post vers Kafka
"""

import requests
import json
from datetime import datetime
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_kafka_producer():
    """Crée un producteur Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Attendre confirmation
            retries=3
        )
        print("✅ Connexion à Kafka réussie")
        return producer
    except Exception as e:
        print(f"❌ Erreur connexion Kafka: {e}")
        return None


def collect_and_stream(subreddits=['depression', 'Anxiety', 'mentalhealth'], 
                       interval=30):
    """
    Collecte en continu et envoie vers Kafka
    
    Args:
        subreddits: Liste des subreddits
        interval: Intervalle en secondes entre chaque collecte
    """
    
    producer = create_kafka_producer()
    if not producer:
        return
    
    headers = {
        'User-Agent': 'MentalHealthMonitor/1.0 (Academic Research)'
    }
    
    # IDs déjà vus (pour éviter doublons)
    seen_ids = set()
    
    print("=" * 70)
    print("🚀 DÉMARRAGE DU STREAMING REDDIT → KAFKA")
    print("=" * 70)
    print(f"📍 Subreddits: {', '.join(subreddits)}")
    print(f"⏱️  Intervalle: {interval}s")
    print(f"📡 Topic Kafka: reddit-posts")
    print("=" * 70)
    print("\n▶️  Streaming en cours... (CTRL+C pour arrêter)\n")
    
    total_sent = 0
    
    try:
        while True:
            for subreddit in subreddits:
                url = f'https://www.reddit.com/r/{subreddit}/new.json?limit=25'
                
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        new_posts = 0
                        
                        for post in data['data']['children']:
                            post_info = post['data']
                            post_id = post_info.get('id')
                            
                            # Éviter les doublons
                            if post_id in seen_ids:
                                continue
                            
                            seen_ids.add(post_id)
                            
                            # Préparer les données
                            post_data = {
                                'id': post_id,
                                'title': post_info.get('title'),
                                'text': post_info.get('selftext', ''),
                                'author': post_info.get('author'),
                                'subreddit': post_info.get('subreddit'),
                                'created_utc': post_info.get('created_utc'),
                                'score': post_info.get('score', 0),
                                'num_comments': post_info.get('num_comments', 0),
                                'url': post_info.get('url', ''),
                                'timestamp': datetime.now().isoformat()
                            }
                            
                            # Envoyer vers Kafka
                            future = producer.send('reddit-posts', post_data)
                            
                            try:
                                # Attendre confirmation (avec timeout)
                                record_metadata = future.get(timeout=10)
                                new_posts += 1
                                total_sent += 1
                                
                                title_short = post_data['title'][:50] + "..."
                                print(f"✉️  [{total_sent:04d}] r/{subreddit} → Kafka: {title_short}")
                                
                            except KafkaError as e:
                                print(f"❌ Erreur Kafka: {e}")
                        
                        if new_posts > 0:
                            print(f"   ✅ {new_posts} nouveaux posts de r/{subreddit}")
                    
                    elif response.status_code == 429:
                        print(f"⚠️  Rate limit atteint. Pause de 60s...")
                        time.sleep(60)
                
                except Exception as e:
                    print(f"❌ Erreur collecte r/{subreddit}: {e}")
            
            # Attendre avant la prochaine collecte
            print(f"\n⏳ Pause de {interval}s... (Total envoyés: {total_sent})")
            print("-" * 70)
            time.sleep(interval)
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du streaming...")
        print(f"📊 Total de posts envoyés: {total_sent}")
        producer.flush()
        producer.close()
        print("✅ Producteur Kafka fermé proprement")


if __name__ == "__main__":  
    print("\n" + "=" * 70)
    print("🎯 DÉMARRAGE DU COLLECTEUR REDDIT → KAFKA")
    print("=" * 70)
    print("⚠️  IMPORTANT: Kafka doit être lancé d'abord !")
    print("   Terminal 1 : Kafka tourne")
    print("   Terminal 2 : Ce script")
    print("=" * 70)
    
    input("\n▶️  Appuie sur ENTRÉE pour démarrer le streaming...\n")
    
    # Lancer le streaming
    # Collecte toutes les 30 secondes
    collect_and_stream(interval=30)