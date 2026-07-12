# TxPundit — AI Pundit Telegram Bot

Real-time World Cup match alerts via TxLINE data, delivered to Telegram.

## What it does
- Connects to TxLINE odds SSE stream — detects sharp movements >5% within 60 seconds
- Connects to TxLINE scores SSE stream — detects goals, red cards, kickoff, fulltime
- Sends formatted Telegram alerts for every significant event
- Runs both streams in parallel with auto-reconnect

## Alert types
- ⚡ Sharp odds movement with direction and percentage
- ⚽ Goal with live score
- 🟥 Red card
- 🟢 Kickoff
- 🏁 Fulltime result

## Setup
```bash
pip install -r requirements.txt
cp .env.example .env
# Add TXLINE_API_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID to .env
python3 txline_pundit_bot.py
```

## Built by
NTH MOMENT — nthmoment.xyz
TxLINE World Cup Hackathon, Track 3: Consumer and Fan Experiences
