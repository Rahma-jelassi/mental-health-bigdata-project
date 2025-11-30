"""
Consumer Kafka avec Sauvegarde JSON
Sauvegarde les derniers N posts pour le dashboard
"""

from kafka import KafkaConsumer
import json
import re
from datetime import datetime
import os
from collections import deque

# Configuration
MAX_POSTS = 100  # Garder seulement les 100 derniers posts
OUTPUT_FILE = "../data/processed_posts_realtime.json"

# Mots-clés de détresse
DISTRESS_KEYWORDS = [
    'suicide', 'kill myself', 'end it all', 'no reason to live',
    'hopeless', 'worthless', 'give up', "can't go on",
    'depressed', 'anxious', 'panic', 'overwhelmed',
    'lonely', 'isolated', 'scared', 'die'
]

def clean_text(text):
    """Nettoie le texte"""
    if not text or text == "":
        return ""
    text = str(text).lower()
    text = re.sub(r'http\S+|www\S+', '', text)
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def calculate_risk_score(text):
    """Calcule le score de risque"""
    if not text:
        return 0
    
    score = 0
    text_lower = str(text).lower()
    
    for keyword in DISTRESS_KEYWORDS:
        if keyword in text_lower:
            score += 10
    
    return min(score, 100)

def save_posts(posts, filename):
    """Sauvegarde les posts dans un fichier JSON"""
    try:
        # Créer le dossier si nécessaire
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(list(posts), f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        print(f"⚠️  Erreur sauvegarde: {e}")
        return False

def main():
    print("\n" + "=" * 70)
    print("🎯 KAFKA CONSUMER - MENTAL HEALTH ANALYSIS")
    print("=" * 70)
    print("⚠️  IMPORTANT:")
    print("   Terminal 1 : Kafka")
    print("   Terminal 2 : Reddit Collector")
    print("   Terminal 3 : Ce script")
    print("=" * 70)
    print(f"📦 Sauvegarde : {MAX_POSTS} derniers posts dans {OUTPUT_FILE}")
    print(f"🗑️  CTRL+C : Supprime le fichier pour économiser l'espace")
    print("=" * 70)
    
    input("\n▶️  Appuie sur ENTRÉE pour démarrer...\n")
    
    print("=" * 70)
    print("📡 CONNEXION À KAFKA")
    print("=" * 70)
    
    try:
        consumer = KafkaConsumer(
            'reddit-posts',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='mental-health-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ Connecté à Kafka topic: reddit-posts\n")
    except Exception as e:
        print(f"❌ Erreur connexion Kafka: {e}")
        print("⚠️  Vérifie que Kafka tourne (Terminal 1)")
        return
    
    print("=" * 70)
    print("📊 TRAITEMENT DES POSTS EN TEMPS RÉEL")
    print("=" * 70)
    print("✅ En attente de nouveaux posts...")
    print("⏹️  CTRL+C pour arrêter et nettoyer\n")
    
    # Buffer circulaire pour garder seulement les N derniers posts
    posts_buffer = deque(maxlen=MAX_POSTS)
    post_count = 0
    
    try:
        for message in consumer:
            post = message.value
            post_count += 1
            
            # Extraire infos
            post_id = post.get('id', 'N/A')
            title = post.get('title', '')
            text = post.get('text', '')
            subreddit = post.get('subreddit', 'N/A')
            author = post.get('author', 'N/A')
            score = post.get('score', 0)
            num_comments = post.get('num_comments', 0)
            url = post.get('url', '')
            
            # Traitement
            combined = f"{title} {text}"
            cleaned = clean_text(combined)
            risk_score = calculate_risk_score(combined)
            
            # Préparer pour sauvegarde
            processed_post = {
                'id': post_id,
                'title': title,
                'text': text[:500] if text else "",  # Limiter taille
                'subreddit': subreddit,
                'author': author,
                'score': score,
                'num_comments': num_comments,
                'url': url,
                'risk_score': risk_score,
                'cleaned_text': cleaned[:200] if cleaned else "",
                'processed_at': datetime.now().isoformat()
            }
            
            # Ajouter au buffer (remplace automatiquement le plus vieux)
            posts_buffer.append(processed_post)
            
            # Sauvegarder
            if save_posts(posts_buffer, OUTPUT_FILE):
                saved_indicator = "💾"
            else:
                saved_indicator = "⚠️"
            
            # Affichage console
            print("-" * 70)
            print(f"📝 POST #{post_count} {saved_indicator}")
            print("-" * 70)
            print(f"🆔 ID:         {post_id}")
            print(f"📍 Subreddit:  r/{subreddit}")
            print(f"👤 Auteur:     {author}")
            print(f"📌 Titre:      {title}")
            
            if text and len(text.strip()) > 0:
                text_preview = text[:200] + "..." if len(text) > 200 else text
                print(f"📄 Texte:      {text_preview}")
            
            print(f"⚠️  Risque:     {risk_score}/100")
            print(f"👍 Score:      {score}")
            print(f"💬 Comments:   {num_comments}")
            
            if risk_score >= 30:
                print(f"🚨 ALERTE: Score de risque élevé!")
            
            print(f"⏰ Traité:     {datetime.now().strftime('%H:%M:%S')}")
            print(f"📦 Buffer:     {len(posts_buffer)}/{MAX_POSTS} posts")
            print()
            
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("⏹️  ARRÊT DU CONSUMER")
        print("=" * 70)
        print(f"📊 Total de posts traités: {post_count}")
        print(f"📦 Posts en buffer: {len(posts_buffer)}")
        
        # Demander si on veut supprimer
        print("\n🗑️  Nettoyage pour économiser l'espace...")
        
        try:
            if os.path.exists(OUTPUT_FILE):
                os.remove(OUTPUT_FILE)
                print(f"✅ Fichier {OUTPUT_FILE} supprimé")
            else:
                print(f"ℹ️  Aucun fichier à supprimer")
        except Exception as e:
            print(f"⚠️  Erreur suppression: {e}")
        
        consumer.close()
        print("✅ Consumer fermé proprement")
        print("=" * 70)

if __name__ == "__main__":
    main()