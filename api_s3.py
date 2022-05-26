import requests
result = requests.get('http://api.open-notify.org/iss-now.json')
print(result)