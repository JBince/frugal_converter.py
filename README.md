# Overview
A tool to convert base64 encoded Frugal messages to and from JSON for easy testing and use.

# Usage
*Decode a message*

python3 converter.py -f ./msg.b64 -d -o ./msg.json

*Encode a message, will output the Frugal message in Base64 to the terminal*

python3 converter.py -f ./msg.json -e
