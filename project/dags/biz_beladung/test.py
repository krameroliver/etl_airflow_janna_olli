import yaml
target = 'darlehen'
with open(r'../Configs/ENB/' + target + '.yaml') as file:
    documents = yaml.full_load(file)
target_fields = documents[target]['tables']['s_darlehen']['fields']
print(target_fields)