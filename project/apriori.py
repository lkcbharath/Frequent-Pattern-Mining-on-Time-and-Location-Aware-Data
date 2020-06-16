import datetime
from itertools import combinations, chain
from prettytable import PrettyTable
from pandas_ods_reader import read_ods
from preprocess import get_preprocessed_data
from utils import print_table, get_location_time_star_items, combine_same_itemsets_count, get_config_info

MIN_SUPPORT_VALUE = 2

def get_transactions(filename):
  test_data = get_preprocessed_data(filename)
  for i in range(len(test_data)):
    test_data[i][0] = set(test_data[i][0])
  return test_data
  

def get_hash_ids(star_items):
  one_star_location = star_items[0]
  one_star_time = star_items[1]
  zero_star_time_location = star_items[2]
  hash_ids = {}
  start_id = 1
  for time_location in zero_star_time_location:
    hash_ids[time_location] = '1' +  str(start_id)
    start_id += 1
  start_id = 1
  for location in one_star_location:
    hash_ids[(location, '*')] = '2' + str(start_id)
    start_id += 1
  for time in one_star_time:
    hash_ids[('*', time)] = '2' + str(start_id)
    start_id += 1
  hash_ids[('*', '*')] = 31
  return hash_ids

def get_rev_hash_ids(hash_ids):
  rev_hash_ids = {}
  for time_location, id in hash_ids.items():
    rev_hash_ids[id] = time_location
  return rev_hash_ids

def get_itemset_freq_in_transaction(items, transactions, location, time):
  count = 0
  for transaction in transactions:
    is_subset = True
    itemset = transaction[0]
    for item in items:
      if not item in itemset:
        is_subset = False
    if location == transaction[1] and time == transaction[2] and is_subset:
      count += 1
  return count


def get_two_items_itemsets(transactions):
  two_items_itemsets = []
  unique_two_itemset = set()
  for transaction in transactions:
    itemset, location, time = transaction[0], transaction[1], transaction[2]
    two_itemsets = combinations(list(itemset), 2)
    for two_itemset in two_itemsets:
      two_itemset = sorted(list(two_itemset), key=lambda item: int(item[3:]))
      itemset_freq_in_transaction = get_itemset_freq_in_transaction(two_itemset, transactions, location, time)
      if itemset_freq_in_transaction >= MIN_SUPPORT_VALUE:
        unique_two_itemset.add((tuple(two_itemset), itemset_freq_in_transaction, (location, time)))
  if len(unique_two_itemset) > 0:
    for item in unique_two_itemset:
      two_items_itemsets.append(item)
  return two_items_itemsets


def get_final_itemsets(base_itemset, transactions):
  next_itemset_size = 3
  terminate = False
  final_itemsets = []
  # apriori algorithm on non-hashed spatio-temporal itemsets
  while(not terminate):
    terminate = True
    next_itemsets = set()
    itemsets = base_itemset
    if len(itemsets) > 0:
      final_itemsets.append(itemsets)
    for i in range(len(itemsets)):
      for j in range(i+1, len(itemsets)):
        if itemsets[i][2] == itemsets[j][2]:
          new_itemset = list(set(itemsets[i][0] + itemsets[j][0]))
          new_itemset = sorted(new_itemset, key=lambda item: int(item[3:]))
          if len(new_itemset) == next_itemset_size:
            itemset_freq_in_transaction = get_itemset_freq_in_transaction(new_itemset, transactions, itemsets[i][2][0], itemsets[i][2][1])
            if(itemset_freq_in_transaction >= MIN_SUPPORT_VALUE):
              next_itemsets.add((tuple(new_itemset), itemset_freq_in_transaction, itemsets[i][2]))
    if len(next_itemsets) > 0:
      terminate = False
    base_itemset = list(next_itemsets)
    next_itemset_size += 1
  
  final_itemsets = list(chain.from_iterable(final_itemsets))
  return final_itemsets

def get_star_itemsets(final_itemsets, location_time_star_items):
  one_star_location = location_time_star_items[0]
  one_star_time = location_time_star_items[1]
  zero_star_itemsets = {}
  one_star_itemsets = {}
  two_star_itemsets = {}
  for itemset in final_itemsets:
    if itemset[2] in zero_star_itemsets:
      zero_star_itemsets[itemset[2]].append(itemset)
    else:
      zero_star_itemsets[itemset[2]] = [itemset]
  for location in one_star_location:
    for id, itemsets in zero_star_itemsets.items():
      if location == id[0]:
        new_id = (location, '*')
        if new_id in one_star_itemsets:
          one_star_itemsets[new_id].append(itemsets)
        else:
          one_star_itemsets[new_id] = [itemsets]
  for time in one_star_time:
    for id, itemsets in zero_star_itemsets.items():
      if time == id[1]:
        new_id = ('*', time)
        if new_id in one_star_itemsets:
          one_star_itemsets[new_id].append(itemsets)
        else:
          one_star_itemsets[new_id] = [itemsets]
  # concatenate the list of list to form single list of itemsets
  for id, itemsets in one_star_itemsets.items():
    one_star_itemsets[id] = list(chain.from_iterable(one_star_itemsets[id]))
  two_star_id = ('*', '*')
  two_star_itemsets[two_star_id] = []
  for id, itemsets in zero_star_itemsets.items():
    two_star_itemsets[two_star_id].append(itemsets)
  two_star_itemsets[two_star_id] = list(chain.from_iterable(two_star_itemsets[two_star_id]))
  for id, itemsets in one_star_itemsets.items():
    one_star_itemsets[id] = combine_same_itemsets_count(itemsets)
  for id, itemsets in two_star_itemsets.items():
    two_star_itemsets[id] = combine_same_itemsets_count(itemsets)
  return [zero_star_itemsets, one_star_itemsets, two_star_itemsets]

def main():
  global MIN_SUPPORT_VALUE
  # preprocessing
  config_data = get_config_info()
  MIN_SUPPORT_VALUE = int(config_data['min_support'])
  filename = config_data['filename']
  transactions = get_transactions(filename)
  location_time_star_items = get_location_time_star_items(transactions)
  # base itemset
  two_items_itemsets = get_two_items_itemsets(transactions)
  # running apriori on base itemset
  t1 = datetime.datetime.now()
  final_itemsets = get_final_itemsets(two_items_itemsets, transactions)
  star_itemsets = get_star_itemsets(final_itemsets, location_time_star_items)
  zero_star_itemsets = star_itemsets[0]
  one_star_itemsets = star_itemsets[1]
  two_star_itemsets = star_itemsets[2]
  t2 = datetime.datetime.now()
  total_time = t2-t1
  print(20*'*')
  print('Total time taken in (microseconds) by apriori algorithm:', total_time.microseconds)
  print('Min support value:', MIN_SUPPORT_VALUE)
  print(20*'*')
  print_table(zero_star_itemsets, 'Itemsets for CMB')
  print_table(one_star_itemsets, 'Itemsets for 1 star CMP')
  print_table(two_star_itemsets, 'Itemsets for 2 star')

  
 
main()
