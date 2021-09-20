import ndjson
import json
import encryption

with open('data/sample.json') as f:
    data = json.load(f)

with open('data/sample.ndjson', 'w+') as f:
    for i in data:
        json.dump(i,f)
        f.write('\n')

with open('data/sample.ndjson') as f:
    reader = ndjson.reader(f)

    for post in reader:
        print(post)