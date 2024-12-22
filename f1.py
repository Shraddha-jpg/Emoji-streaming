from flask import Flask, request, jsonify, render_template_string
from datetime import datetime
from kafka import KafkaProducer
import json
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# In-memory user storage for simplicity
users = {
    "user123": {"user_id": "user123", "password": "password123"},
    "user1": {"user_id": "user1", "password": "password1"},
    "user2": {"user_id": "user2", "password": "password2"},
    "user3": {"user_id": "user3", "password": "password3"},
    "user4": {"user_id": "user4", "password": "password4"}
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adjust if your Kafka server is hosted differently
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500  # Buffer messages for 500ms before sending to Kafka
)

# ThreadPoolExecutor for asynchronous Kafka writing
executor = ThreadPoolExecutor(max_workers=5)

# Helper function to classify emoji
def classify_emoji(emj):
    happy_emojis = {'😄', '😊', '😂', '😃', '😁'}
    sad_emojis = {'😢', '😞', '😔', '😕', '😟'}
    sports_emojis = {'🏀', '⚽', '🏆', '⚾', '🏈'}
    celebration_emojis = {'🎉', '🥳', '🎊', '🎈'}
    thumbs_up_emojis = {'👍'}
    thumbs_down_emojis = {'👎'}

    if emj in happy_emojis:
        return "happy"
    elif emj in sad_emojis:
        return "sad"
    elif emj in sports_emojis:
        return "sports"
    elif emj in celebration_emojis:
        return "celebration"
    elif emj in thumbs_up_emojis:
        return "positive"
    elif emj in thumbs_down_emojis:
        return "negative"
    else:
        return "unknown"

# Serve the HTML form with emojis
@app.route('/')
def index():
    emojis = ['😄', '😊', '😂', '😢', '😞', '🏀', '⚽', '🎉', '🥳', '👍', '👎']
    return render_template_string('''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Emoji Submission</title>
        <style>
            .emoji { font-size: 50px; cursor: pointer; margin: 5px; }
        </style>
        <script>
            function sendEmoji(emoji) {
                fetch('/send_emoji', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        user_id: 'user123',  // You can dynamically set this if needed
                        emoji: emoji,
                        timestamp: new Date().toISOString()
                    })
                })
                .then(response => response.json())
                .then(data => alert(data.message))
                .catch(error => console.error('Error:', error));
            }
        </script>
    </head>
    <body>
        <h1>Click an Emoji</h1>
        <div>
            {% for emoji in emojis %}
                <span class="emoji" onclick="sendEmoji('{{ emoji }}')">{{ emoji }}</span>
            {% endfor %}
        </div>
    </body>
    </html>
    ''', emojis=emojis)

# Handle emoji sending
@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    data = request.get_json()
    user_id = data.get('user_id')
    emoji_input = data.get('emoji')
    timestamp = data.get('timestamp')

    # Validate user
    if user_id not in users:
        return jsonify({"message": "User not found"}), 403

    # Classify emoji
    emoji_type = classify_emoji(emoji_input)

    # Write data to a file
    with open("emoji_log.txt", "a") as f:
        f.write(f"{user_id},{emoji_type},{timestamp}\n")

    # Send data to Kafka
    message = {
        "user_id": user_id,
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }
    executor.submit(producer.send, 'emoji_topic', value=message)

    return jsonify({
        "user_id": user_id,
        "message": "Emoji received",
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }), 200

if __name__ == '__main__':
    app.run(debug=True)

