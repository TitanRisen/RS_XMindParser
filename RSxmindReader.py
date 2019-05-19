# -*- coding: utf-8 -*-
import sys
import xmind
import pipes
from urllib import parse
from collections import defaultdict
import json
from queue import Queue
import re
from config import filepathConfig 
# mongodb related
from pymongo import MongoClient
from pymongo import ASCENDING
from config import mongoConfig
from xlsxTranslator import mapXlsxToSchema
'''
import zipfile
from hashlib import md5
from functools import wraps 
from xml.dom.minidom import parse, parseString
'''
#===============================有正式配置文件后删除该项========================
# config = {'filePath':'./广州市人社政务服务事项清单/'}
CONTROL_TIME = 0
#============================================================================
class RSmindReader(object):
    def __init__(self,config = {'filePath':'./'}):
        self.config = None
        if not (config.get('filePath' , None)):
            print ('the config should have the default filepath attribute')
        self.dic = self.tree()
        self.id = 0
        self.config = config
        self.rootTopic = None
        self.nounSet = {'isLeaf','link','middleComment'}
        if not self.config['filePath'][-1] == '/':
            self.config['filePath']+='/'
        ### mongodb related
        conn = MongoClient(mongoConfig['address'], mongoConfig['port'])
        db = conn[mongoConfig['DBname']]
        self.dataSet = db[mongoConfig['setName']]
        self.dataSet.create_index([('leafId',ASCENDING)], unique=True)

    def load(self,path):
        if not self.config:
            print("The config is not right.")
            return
        if not path:
            print('Enter a legal path!')
            return
        if not path[-6:] == '.xmind':
            print('please give a xmind!')
            return
        x = xmind.load(path)
        xmind_content_list = x.getData() 
        self.BFSxmind(xmind_content_list)
        if self.dic:
            self.jsonfyDFS(self.dic)

    def save(self,path = './test.json'):
        with open(path,'w',encoding = 'utf-8') as f:
            j = json.dumps(self.dic , indent = 2 , ensure_ascii = False)
            f.write(j)

    #####0.2.0 use a recursive defaultdictory struct to replece the native dict and to avoid Assignment Sentences.
    def tree(self):
        return defaultdict(self.tree)

    def nextXmind(self,link):
        pass

    def jsonfyDFS(self,node,link =''):
        '''
        to beautify the dictory decision tree and to attach the path of each excel file , 
        because can't get complete file path with BFS
        '''
        
        if node.get('isLeaf',False):
            if link and node.get('link',None) and node['link'][-5:] == '.xlsx':
                temp_links = link.split('/')[:-1]
                temp_links.append(node['link'])
                temp = '/'.join(temp_links)
                # exception : '///'
                if temp.find('///') != -1:
                    temp_links = temp.split('///')
                    temp_link = [temp_links[0]]
                    temp_link.extend(temp_links[-1].split('/')[-2:])
                    temp = '/'.join(temp_link)
                    print(temp)
                node['link'] =  temp
                node['leaf_id'] = 'leaf_%06d' % self.id
        # 0.2.0 add the mongoDB's  insertion
                mapXlsxToSchema(temp ,node['leaf_id'] , self.dataSet)
        # 0.2.0 abandon the 'link' attrbute   
                node.pop('link')
                self.id+=1
            return
            
        # if(len(node) == 2 ):
        #     for _ in node:
        #         if not _ == 'isLeaf':
        #             node.update(middleComment = node.pop(_))
        #             break

        for temp_key in node:
            #print(temp_key)
            if temp_key not in self.nounSet :
                if  node.get('link',None):
                    if(node['link'][-6:] == '.xmind'):
                        self.jsonfyDFS(node[temp_key],node['link'])
                        
                self.jsonfyDFS(node[temp_key],link)
        #can not modify the size of dic while iterations,so modify here
        if  node.get('link',None):
            if(node['link'][-6:] == '.xmind'):
                node.pop('link')
                
        return

            

    def _dealWithLink(self,curr_node, link , queue ,node_queue, xmind_content_list):
        """ to decode the url encoding.

        :param curr_node: defaultdict. the current generated node which need to deal with
        :param link : string. legal file path
        :param queue : list. store node of the native xminds.
        :param node_queue: list. stroe the generated node.
        """
        link = parse.unquote(link)
        if link[:6] == 'xmind:':
            _id = link[7:]
            for _sheet in xmind_content_list:
                if _sheet['topic']['id'] == _id:
                    for temp_topic in _sheet['topic']['topics']:
                        queue.put(temp_topic)
                        node_queue.put(curr_node[temp_topic['title']])
                        #curr_node[temp_topic['title']] #= {}
            return

        if link[:5] == 'file:':
            if link[-6:] == '.xmind':
                temp_x = xmind.load(self.config['filePath']+link[5:])
                temp_topic= temp_x.getData()[0]['topic']



                if temp_topic.get('topics' , None):
                    for child_topic in temp_topic['topics']:
                        queue.put(child_topic)
                        node_queue.put(curr_node[child_topic['title']])
                else :
                    queue.put(temp_topic)
                    node_queue.put(curr_node[temp_topic['title']])
                curr_node['link'] = self.config['filePath']+link[5:]

                #v0.2.0 need middle comment ?
                # queue.put(temp_x.getData()[0]['topic'])
                # #backup attribute
                # curr_node['middleComment'] = None  
                # temp_node = curr_node[temp_x.getData()[0]['topic']['title']]
                # node_queue.put(temp_node)
                # temp_node['link'] = self.config['filePath']+link[5:]
                return
            if link[-5:] == '.xlsx':
                #传递上一文件结点的链接有问题，考虑BFS后对列表再DFS
                curr_node['link'] = link[5:]
                curr_node['isLeaf'] = True
                return


    def BFSxmind(self,xmind_content_list):
        """ Traversal all the xmind's node and generate the simple map of all the xmind

        :param xmind_content_list: list. the sheeets of the root xmind
        """
        if xmind_content_list is None:
            print('empty workbook')
            return
        #use the list to record the visited node
        queue = Queue() 
        #####v0.2.0 , add another queue to deal with the the generated node
        node_queue = Queue()   
        rootSheet =  xmind_content_list[0]
        root_topic = rootSheet['topic']
        
        #==============================================================================
        queue.put(root_topic)
        curr_node = self.dic[root_topic['title']]
        curr_node['isLeaf'] = False
        node_queue.put(curr_node)
        self.rootTopic = curr_node
        temp_path = ''
        topicSet=set()
        while not queue.empty():
            # global CONTROL_TIME
            # CONTROL_TIME+=1
            # if CONTROL_TIME == 30:
            #     return

            cur_topic = queue.get()
            #curr_node[cur_topic['title']] #= {}
            #curr_node = curr_node[cur_topic['title']]
            curr_node = node_queue.get()
            curr_node['isLeaf'] = False
            #if there's a link , it has not topics
            if cur_topic.get('link' , None):
                self._dealWithLink(curr_node,cur_topic['link'],queue,node_queue,xmind_content_list)
                continue
            if cur_topic.get('topics' , []):
                for temp_topic in cur_topic['topics']:
                    queue.put(temp_topic)
                    #print(temp_topic['title'])
                    #curr_node[temp_topic['title']] #={}
                    node_queue.put(curr_node[temp_topic['title']])
                    # if temp_topic not in topicSet:
                    #     topicSet.add(temp_topic)
                    #     if temp_topic 
            else:
                curr_node['isLeaf'] = True
           
            




if __name__ == '__main__':
    reader = RSmindReader(config = filepathConfig)
    reader.load('./广州市人社政务服务事项清单/广州市人社政务服务事项清单.xmind')
    print('finish')
    reader.save()
    print('SAVEd')
    
 