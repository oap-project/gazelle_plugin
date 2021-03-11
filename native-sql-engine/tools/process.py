#!/usr/bin/python

import sys
import pprint
import re
import traceback
import copy
from collections import OrderedDict
import numpy as np
import collections

def process(input):
    try:
        return int(input)
    except:
        try:
            return float(input)
        except:
            return input

def loadResultAsDict(path):
    result = OrderedDict()
    query = ""
    table_status = 0
    table_title = []
    table = []
    try:
        with open(path, 'r') as f:
            for line in f.readlines():
                if line[0:13] == "[SPARK_QUERY]":
                    result[query] = {'title': copy.deepcopy(table_title), 'data': copy.deepcopy(table)}
                    query = re.findall('q\d+[a,b]{0,1}', line)[0]
                    table_status = -1
                    table_title = []
                    table = []
                if "--q" in line:
                    result[query] = {'title': copy.deepcopy(table_title), 'data': copy.deepcopy(table)}
                    query = re.findall('q\d+[a,b]{0,1}', line)[0]
                    table_status = 0
                    table_title = []
                    table = []
                if query == "":
                    continue
                if line[0] == '+':
                    table_status += 1
                if table_status == 2:
                    table_status = 0
                if line[0] == '|' and table_status == 1:
                    if len(table_title) != 0:
                        continue
                    table_title = [i.strip() for i in line.split('|')[1:-1]]
                    for i in range(len(table_title)):
                        table.append([])
                if line[0] == '|' and table_status == 0:
                    idx = 0
                    tmp = [process(i.strip()) for i in line.split('|')[1:-1]]
                    if tmp[0] == 'Result':
                        continue
                    for data in tmp:
                        if idx < len(table):
                          table[idx].append(data)
                          idx += 1
    except:
        track = traceback.format_exc()
        print(track)
    return result

def isEqual(x, y):
    if x == y:
        return True
    if isinstance(x, str) or isinstance(y, str):
        return False
    try:
        if abs(x - y) < 0.1:
            return True
    except:
        if np.isnan(x) or np.isnan(y):
            return True
        return False
    if np.isnan(x) or np.isnan(y):
        return True
    return False

def isRangeEqual(x, y):
    left = sortWithTypeCheck(x)
    right = sortWithTypeCheck(y)
    return left == right

def sortAsString(data):
    for value in data:
        if isinstance(value, str) == False:
            raise TypeError("sortAsString while value is %d, first row is %s", value, data[0])
    return sorted(data)

def sortAsNumber(data):
    new_data = []
    for value in data:
        if isinstance(value, str) == True and value == 'NULL':
            new_data.append(float('nan'))
        else:
            new_data.append(value)
    return sorted(new_data, key=float)

def sortWithTypeCheck(data):
    isStr = isinstance(data[0], str)
    if isStr:
        return sortAsString(data)
    else:
        return sortAsNumber(data)

def compare(t1, t2):
    isSame = True
    for query_name in t1.keys():
        if not query_name in t2:
            continue
        for col_id in range(len(t1[query_name]['title'])):
            if not t1[query_name]['title'] == t2[query_name]['title']:
                isSame = False
                skip = True
                print("%s title are different" % query_name)
                continue
        skip = False
        for col_id in range(len(t1[query_name]['data'])):
            if skip:
                break
            checkWithNoSeq = False
            for row_id in range(len(t1[query_name]['data'][col_id])):
                if not isEqual(t1[query_name]['data'][col_id][row_id], t2[query_name]['data'][col_id][row_id]):
                    if (row_id - 10) > 0:
                        start = row_id - 10
                    else:
                        start = 0
                    if (row_id + 10) < len(t1[query_name]['data'][col_id]):
                        end = row_id + 10
                    else:
                        end = len(t1[query_name]['data'][col_id])
                    if query_name in ['q18', 'q24a', 'q24b', 'q39a', 'q49', 'q73', 'q77']:
                        checkWithNoSeq = True
                    else:
                        isSame = False
                        skip = True
                        print("%s data are different in [ColId is %d, ColName: %s, rowId is %d, data is [%s], [%s]]" % (query_name, col_id, t1[query_name]['title'][col_id], row_id, t1[query_name]['data'][col_id][row_id], t2[query_name]['data'][col_id][row_id]))
                        #print(t1[query_name]['data'][col_id][start:end])
                        #print(t2[query_name]['data'][col_id][start:end])
                    break
            if checkWithNoSeq:
                left = sortWithTypeCheck(t1[query_name]['data'][col_id])
                right = sortWithTypeCheck(t2[query_name]['data'][col_id])
                for row_id in range(len(left)):
                    if not isEqual(left[row_id], right[row_id]):
                        isSame = False
                        skip = True
                        print("%s data are different in [ColId is %d, ColName: %s, RowData: [%s] vs [%s]]" % (query_name, col_id, t1[query_name]['title'][col_id], left[row_id],  right[row_id]))
                        break
    return isSame

def main():
    if len(sys.argv) < 3:
        print("expected input should as below:\npython3 process.py ${file_1} ${file_2}")
        sys.exit()
    result_1 = loadResultAsDict(sys.argv[1])
    result_2 = loadResultAsDict(sys.argv[2])
    res = compare(result_1, result_2)
    if res == True:
        print("Two Tables are same")
    else:
        print("Two Tables are different")

#    for query_name, data in result.items():
#        print("query_name is %s" % (query_name))
#        print("table is %s" % (','.join(data['title'])))
#        first_row = [i[0] for i in data['data']]
#        print first_row

if __name__ == "__main__":
    main()
