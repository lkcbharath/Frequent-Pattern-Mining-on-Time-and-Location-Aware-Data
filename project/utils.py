import configparser
from prettytable import PrettyTable

def get_config_info():
  config = configparser.ConfigParser()
  config.read('config.ini')
  return {'min_support': config['DEFAULT']['min_support'], 'filename': config['DEFAULT']['filename']}

def print_table(final_itemsets, title):
  print(title)
  table = PrettyTable(['ID', 'Itemsets', 'Count', 'Location', 'Time'])
  for index, (id, itemsets) in enumerate(final_itemsets.items()):
    items = [itemset[0] for itemset in itemsets]
    items_freq = [itemset[1] for itemset in itemsets]
    row = [index+1, items, items_freq, id[0], id[1]]
    table.add_row(row)
  table._max_width = {'Itemsets': 70, 'Count': 30}
  print(table)

def get_location_time_star_items(transactions):
  one_star_location = {}
  one_star_time = {}
  zero_star_time_location = {}
  for row in transactions:
    location = row[1]
    time = row[2]
    if location in one_star_location:
      one_star_location[location].add(time)
    else:
      new_set = set()
      new_set.add(time)
      one_star_location[location] = new_set
    if time in one_star_time:
      one_star_time[time].add(location)
    else:
      new_set = set()
      new_set.add(location)
      one_star_time[time] = new_set
    if not (location, time) in zero_star_time_location:
      zero_star_time_location[(location, time)] = True
  one_star_location = [location for location in one_star_location if len(one_star_location[location]) > 1]
  one_star_time = [time for time in one_star_time if len(one_star_time[time]) > 1]
  zero_star_time_location = [key for key in zero_star_time_location]
  return [one_star_location, one_star_time, zero_star_time_location]

def combine_same_itemsets_count(itemsets):
  itemset_freq = {}
  for itemset in itemsets:
    curr_itemset = tuple(sorted(itemset[0], key=lambda x: ord(x[0])))
    if curr_itemset in itemset_freq:
      itemset_freq[curr_itemset] += itemset[1]
    else:
      itemset_freq[curr_itemset] = itemset[1]
  itemsets = []
  for itemset, count in itemset_freq.items():
    itemsets.append((itemset, count))
  return itemsets

