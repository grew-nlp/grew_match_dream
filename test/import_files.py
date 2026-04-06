import requests

url = "http://localhost:10024/new_corpus"

payload = {}
files=[
  ('files',('ParisStories_2019_concoursEquitation.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_concoursEquitation.conllu','rb'),'application/octet-stream')),
  ('files',('ParisStories_2019_cuisineApproximative.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_cuisineApproximative.conllu','rb'),'application/octet-stream')),
  ('files',('ParisStories_2019_devoirPhilosophie.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_devoirPhilosophie.conllu','rb'),'application/octet-stream')),
  ('files',('ParisStories_2019_experienceFac.conllu',open('/Users/guillaum/github/surfacesyntacticud/SUD_French-ParisStories/ParisStories_2019_experienceFac.conllu','rb'),'application/octet-stream'))
]
headers = {}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)
