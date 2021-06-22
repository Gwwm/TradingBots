import requests
import json

from config import slack_webhook


def send_slack_message(message):
    data = {'text': message}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    response = requests.post(slack_webhook, data=json.dumps(data), headers=headers)
    print(message)

    return response