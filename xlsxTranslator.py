# -*- coding: utf-8 -*-
import xlrd
from collections import OrderedDict
import re
# from pymongo import MongoClient
# from pymongo import ASCENDING
# from config import mongoConfig
import json
# import codecs
def insertToDB( leafId ,path , materalList, dataSet ):
    # conn = MongoClient(mongoConfig['address'], mongoConfig['port'])
    # db = conn[mongoConfig['DBname']]
    # my_set = db[mongoConfig['setName']]
    # name = 'test'
    # my_set.create_index([('leafId',ASCENDING)], unique=True)
    # example: ss/ss/abc.xlsx
    temp_name = path.split('/')[-1]
    name = temp_name.split('.')[0]
    try:
        dataSet.insert({
            "leafId":leafId,
            "name": name,
            "filepath": path,
            "materals":materalList,
            })
    except BaseException as e:
        print('Error: ',e)

def mapXlsxToSchema( path, leafId, dataSet ):
    wb = xlrd.open_workbook(path)
    convert_list = []
    sh = wb.sheet_by_index(0)
    #title = sh.row_values(0)
    # One-to-one correspondence with attribute names of the DB
    title = ['num','name','submitMethod','amount','requirement','apartment','description']
    
    # use temp Dict to deal with the empty value
    pre = OrderedDict()
    for rownum in range(1, sh.nrows):
        rowvalue = sh.row_values(rownum)
        single = OrderedDict()
        if re.match("办理地点",str(rowvalue[0])):
            break
        # if type(rowvalue[0]) == float:
        #     rowvalue[0] = int(rowvalue[0])
        #not to deal with the key '序号'

        for colnum in range(1, len(rowvalue)):
            # print(title[colnum], rowvalue[colnum])
            # deal with the empty value
            # if title[colnum] == 'amount':
            #     # example : "1份" ， just store the number
            #     rowvalue[colnum] = int(rowvalue[colnum][0])
            if not rowvalue[colnum] and pre:
                single[title[colnum]] = pre[colnum]
            else:
                single[title[colnum]] = rowvalue[colnum]
            
        convert_list.append(single)
        pre = rowvalue
    # j = json.dumps(convert_list , ensure_ascii = False)
    insertToDB(leafId, path, convert_list ,dataSet )
    # with codecs.open('file.json',"w","utf-8") as f:
    #     f.write(j)
if __name__ == '__main__':
    mapXlsxToSchema('./一般情形.xlsx', '0')