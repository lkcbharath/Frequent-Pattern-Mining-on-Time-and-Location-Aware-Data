import csv

def get_compound_list(rows):
  so2_list = []
  no2_list = []
  pmt_list = []
  for row in rows:
    try:
      x = float(row[5])
      so2_list.append(x)
    except ValueError:
      so2_list.append(0)
    try:
      x = float(row[6])
      no2_list.append(x)
    except ValueError:
      no2_list.append(0)
    try:
      x = float(row[7])
      pmt_list.append(x)
    except ValueError:
      pmt_list.append(0)
  so2_max, so2_min = max(so2_list), min(so2_list)
  no2_max, no2_min = max(no2_list), min(no2_list)
  pmt_max, pmt_min = max(pmt_list), min(pmt_list)
  so2_part = (so2_max-so2_min)/3
  so2_lim1 = so2_min + so2_part
  so2_lim2 = so2_min + 2*so2_part
  for i in range(len(so2_list)):
    so2 = so2_list[i]
    if so2 >= so2_min and so2 < so2_lim1:
      so2_list[i] = 'so21'
    elif so2 >= so2_lim1 and so2 < so2_lim2:
      so2_list[i] = 'so22'
    else:
      so2_list[i] = 'so23'
  for i in range(len(no2_list)):
    no2 = no2_list[i]
    no2_part = (no2_max-no2_min)/3
    no2_lim1 = no2_min + no2_part
    no2_lim2 = no2_min + 2*no2_part
    if no2 >= no2_min and no2 < no2_lim1:
      no2_list[i] = 'no21'
    elif no2 >= no2_lim1 and no2 < no2_lim2:
      no2_list[i] = 'no22'
    else:
      no2_list[i] = 'no23'
  for i in range(len(pmt_list)):
    pmt = pmt_list[i]
    pmt_part = (pmt_max-pmt_min)/3
    pmt_lim1 = pmt_min + pmt_part
    pmt_lim2 = pmt_min + 2*pmt_part
    if pmt >= pmt_min and pmt < pmt_lim1:
      pmt_list[i] = 'pmt1'
    elif pmt >= pmt_lim1 and pmt < pmt_lim2:
      pmt_list[i] = 'pmt2'
    else:
      pmt_list[i] = 'pmt3'
  compound_list = []
  for i in range(len(so2_list)):
    compound_list.append([so2_list[i], no2_list[i], pmt_list[i]])
  return compound_list

def get_place_list(rows):
  location = []
  for row in rows:
    location.append(row[2])
  return location

def get_month_list(rows):
  months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'Semptember', 'October', 'November', 'December']
  return [months[int(row[0].split('/')[1])-1] for row in rows]

def get_preprocessed_data(filename):
  fptr = open(filename, 'r')
  csvread = csv.reader(fptr)
  rows = []
  for row in csvread:
    rows.append(row)
  rows = rows[1:]
  compound_list = get_compound_list(rows)
  location_list = get_place_list(rows)
  time_list = get_month_list(rows)
  test_data = []
  for i in range(len(compound_list)):
    test_data.append([compound_list[i], location_list[i], time_list[i]])
  return test_data
