import json
import requests

(front_url, back_url) = ("http://localhost:11024/", "http://localhost:10024/")

#  (front_url, back_url) = ("https://upload.grew.fr/", "https://gmd-upload.grew.fr/")


url = back_url + "new_corpus"

payload = {
  "schema": "SUD",
  "name": "From AG",
  "token": "",
}
files=[
  ('files',('ParisStories_2019_concoursEquitation.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_concoursEquitation.conllu','rb'),'application/octet-stream')),
  ('files',('ParisStories_2019_cuisineApproximative.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_cuisineApproximative.conllu','rb'),'application/octet-stream')),
]
headers = {}

response1 = requests.request("POST", url, headers=headers, data=payload, files=files)
json1 = json.loads (response1.text)
token = json1["data"]["token"]

print (response1.text)

print (f' ---> {front_url}?corpus={json1["data"]["session_id"]}')

input()

payload = {
  "schema": "SUD",
  "name": "From AG",
  "token": token,
}
files=[
  ('files',('ParisStories_2019_devoirPhilosophie.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_devoirPhilosophie.conllu','rb'),'application/octet-stream')),
  ('files',('ParisStories_2019_experienceFac.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_experienceFac.conllu','rb'),'application/octet-stream'))
]
headers = {}

response2 = requests.request("POST", url, headers=headers, data=payload, files=files)
json2 = json.loads (response2.text)
print(response2.text)
print (f' ---> {front_url}?corpus={json2["data"]["session_id"]}')
