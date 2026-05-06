import json
import pickle

BertVocab = {}

with open(r'/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_icd10_with_aggregations_token2idx.json') as f:
    token2idx = json.load(f)
    print(token2idx)
idx2token = {}
for x in token2idx:
    idx2token[token2idx[x]] = x
BertVocab['token2idx'] = token2idx
BertVocab['idx2token'] = idx2token

# save to pickle.

with open(r'/home/viskymi/project/FederatedBEHRT/data/icd10/mimic_iv_icd10_with_aggregations_vocab.pkl', 'wb') as handle:
    pickle.dump(BertVocab, handle, protocol=pickle.HIGHEST_PROTOCOL)


