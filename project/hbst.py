import datetime
from itertools import combinations, chain
from prettytable import PrettyTable
from pandas_ods_reader import read_ods
from preprocess import get_preprocessed_data
from utils import print_table, get_location_time_star_items, combine_same_itemsets_count, get_config_info

MIN_SUPPORT_VALUE = 2

def get_transactions(filename):
  test_data = get_preprocessed_data(filename)
  return test_data
  
def get_itemsets_by_hash_id(transactions, hash_ids):
  itemsets_by_hash_id = {}
  for row in transactions:
    location_time = (row[1], row[2])
    itemset = row[0]
    if hash_ids[location_time] in itemsets_by_hash_id:
      itemsets_by_hash_id[hash_ids[location_time]].append(set(itemset))
    else:
      itemsets = [set(itemset)]
      itemsets_by_hash_id[hash_ids[location_time]] = itemsets
  return itemsets_by_hash_id


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

def get_itemset_freq_in_transaction(items, itemsets_by_hash_id, id):
  count = 0
  for itemset in itemsets_by_hash_id[id]:
    is_subset = True
    for item in items:
      if not item in itemset:
        is_subset = False
        break
    if is_subset:
      count += 1
  return count


def get_two_items_itemsets_by_hash_id(itemsets_by_hash_id):
  two_items_itemsets_by_hash_id = {}
  for id, itemsets in itemsets_by_hash_id.items():
    unique_two_itemset = set()
    for itemset in itemsets:
      two_itemsets = combinations(list(itemset), 2)
      for two_itemset in two_itemsets:
        two_itemset = sorted(list(two_itemset), key=lambda item: int(item[3:]))
        itemset_freq_in_transaction = get_itemset_freq_in_transaction(two_itemset, itemsets_by_hash_id, id)
        if itemset_freq_in_transaction >= MIN_SUPPORT_VALUE:
          unique_two_itemset.add((tuple(two_itemset), itemset_freq_in_transaction))
    if len(unique_two_itemset) > 0:
      two_items_itemsets_by_hash_id[id] = list(unique_two_itemset)
  return two_items_itemsets_by_hash_id


def get_final_itemsets_by_hash_id(base_itemset, itemsets_by_hash_id):
  next_itemset_size = 3
  terminate = False
  final_itemsets_by_hash_id = {}
  # apriori algorithm on hashed spatio-temporal itemsets
  while(not terminate):
    terminate = True
    for id, itemsets in base_itemset.items():
      next_itemsets = set()
      if len(itemsets) > 0:
        if id in final_itemsets_by_hash_id:
          final_itemsets_by_hash_id[id].append(itemsets)
        else:
          final_itemsets_by_hash_id[id] = [itemsets]
      for i in range(len(itemsets)):
        for j in range(i+1, len(itemsets)):
          new_itemset = list(set(itemsets[i][0] + itemsets[j][0]))
          new_itemset = sorted(new_itemset, key=lambda item: int(item[3:]))
          if len(new_itemset) == next_itemset_size:
            itemset_freq_in_transaction = get_itemset_freq_in_transaction(new_itemset, itemsets_by_hash_id, id)
            if(itemset_freq_in_transaction >= MIN_SUPPORT_VALUE):
              next_itemsets.add((tuple(new_itemset), itemset_freq_in_transaction))
      if len(next_itemsets) > 0:
        terminate = False
      base_itemset[id] = list(next_itemsets)
    next_itemset_size += 1
 
  for id, itemsets in final_itemsets_by_hash_id.items():
    final_itemsets_by_hash_id[id] = list(chain.from_iterable(final_itemsets_by_hash_id[id]))
  return final_itemsets_by_hash_id

  
def get_star_itemsets_by_hash_id(final_itemsets_by_hash_id, location_time_star_items, hash_ids, rev_hash_ids):
  one_star_location = location_time_star_items[0]
  one_star_time = location_time_star_items[1]
  one_star_itemsets = {}
  two_star_itemsets = {}
  for location in one_star_location:
    for id, itemsets in final_itemsets_by_hash_id.items():
      if location == rev_hash_ids[id][0]:
        new_id = hash_ids[(location, '*')]
        if new_id in one_star_itemsets:
          one_star_itemsets[new_id].append(itemsets)
        else:
          one_star_itemsets[new_id] = [itemsets]
  for time in one_star_time:
    for id, itemsets in final_itemsets_by_hash_id.items():
      if time == rev_hash_ids[id][1]:
        new_id = hash_ids[('*', time)]
        if new_id in one_star_itemsets:
          one_star_itemsets[new_id].append(itemsets)
        else:
          one_star_itemsets[new_id] = [itemsets]
  # concatenate the list of list to form single list of itemsets
  for id, itemsets in one_star_itemsets.items():
    one_star_itemsets[id] = list(chain.from_iterable(one_star_itemsets[id]))
  two_star_id = hash_ids[('*', '*')]
  two_star_itemsets[two_star_id] = []
  for id, itemsets in final_itemsets_by_hash_id.items():
    two_star_itemsets[two_star_id].append(itemsets)
  two_star_itemsets[two_star_id] = list(chain.from_iterable(two_star_itemsets[two_star_id]))
  for id, itemsets in one_star_itemsets.items():
    one_star_itemsets[id] = combine_same_itemsets_count(itemsets)
  for id, itemsets in two_star_itemsets.items():
    two_star_itemsets[id] = combine_same_itemsets_count(itemsets)
  return [one_star_itemsets, two_star_itemsets]

def print_table(itemsets_by_hash_id, rev_hash_ids, title):
  print(title)
  table = PrettyTable(['ID', 'Itemsets', 'Count', 'Location', 'Time', 'Hash'])
  for index, (id, itemsets) in enumerate(itemsets_by_hash_id.items()):
    items = [itemset[0] for itemset in itemsets]
    items_freq = [itemset[1] for itemset in itemsets]
    location_time = rev_hash_ids[id]
    row = [index+1, items, items_freq, location_time[0], location_time[1], id]
    table.add_row(row)
  table._max_width = {'Itemsets': 70, 'Count': 30}
  print(table)

def main():
  global MIN_SUPPORT_VALUE
  # preprocessing
  config_data = get_config_info()
  MIN_SUPPORT_VALUE = int(config_data['min_support'])
  filename = config_data['filename']
  transactions = get_transactions(filename)
  location_time_star_items = get_location_time_star_items(transactions)
  hash_ids = get_hash_ids(location_time_star_items)
  rev_hash_ids = get_rev_hash_ids(hash_ids)
  itemsets_by_hash_id = get_itemsets_by_hash_id(transactions, hash_ids)
  # base itemset
  two_items_itemsets_by_hash_id = get_two_items_itemsets_by_hash_id(itemsets_by_hash_id)
  # running spatio temporal apriori on base itemset
  t1 = datetime.datetime.now()
  final_itemsets_by_hash_id = get_final_itemsets_by_hash_id(two_items_itemsets_by_hash_id, itemsets_by_hash_id)
  star_itemsets_by_hash_id = get_star_itemsets_by_hash_id(final_itemsets_by_hash_id, location_time_star_items, hash_ids, rev_hash_ids)
  one_star_itemsets_by_hash_id = star_itemsets_by_hash_id[0]
  two_star_itemsets_by_hash_id = star_itemsets_by_hash_id[1]
  t2 = datetime.datetime.now()
  total_time = t2-t1
  print(20*'*')
  print('Total time taken in (microseconds) by Hash Based Spatio-Temporal(HBST) algorithm:', total_time.microseconds)
  print('Min support value:', MIN_SUPPORT_VALUE)
  print(20*'*')
  print_table(final_itemsets_by_hash_id, rev_hash_ids, 'Itemsets for CMB')
  print_table(one_star_itemsets_by_hash_id, rev_hash_ids, 'Itemsets for 1 star CMP')
  print_table(two_star_itemsets_by_hash_id, rev_hash_ids, 'Itemsets for 2 star')

main()
