''' converts xml or json file to a bots grammar.
    Usage eg: c:\python27\python  bots-xml2botsgrammar.py  botssys/infile/test.xml   botssys/infile/resultgrammar.py
    Try to have a 'completely filled' xml or json file.
'''
from __future__ import print_function
import os
import sys
import atexit
import copy
import logging
try:
    import json as simplejson
except ImportError:
    import simplejson
try:
    from xml.etree import cElementTree as ET
except ImportError:
    from xml.etree import ElementTree as ET
try:
    from collections import OrderedDict
except:
    from .bots_ordereddict import OrderedDict
#bots-modules
from . import botslib
from . import botsinit
from . import botsglobal
from . import inmessage
from . import outmessage
from . import node
from .botsconfig import *

#**************************************************************************************
#***classes used in inmessage for xml2botsgrammar.
#***These classes are dynamically added to inmessage
#**************************************************************************************

class jsonforgrammar(inmessage.Inmessage):
    def initfromfile(self):
        #~ self.messagegrammarread()
        self.ta_info['charset'] = 'utf-8'
        self.ta_info['checkcharsetin'] = 'strict'
        self.ta_info['checkunknownentities'] = True
        self._readcontent_edifile()

        jsonobject = simplejson.loads(self.rawinput)
        del self.rawinput
        if isinstance(jsonobject,list):
            self.root = node.Node()  #initialise empty node.
            is_repeting_data_element, self.root.children = self._dojsonlist(jsonobject,self._getrootid())   #fill root with children
            for child in self.root.children:
                if not child.record:    #sanity test: the children must have content
                    raise botslib.InMessageError(u'[J51]: No usable content.')
        elif isinstance(jsonobject,dict):
            if len(jsonobject)==1 and isinstance(jsonobject.values()[0],dict):
                # best structure: {rootid:{id2:<dict, list>}}
                self.root = self._dojsonobject(jsonobject.values()[0],jsonobject.keys()[0],object_in_list=False)
            elif len(jsonobject)==1 and isinstance(jsonobject.values()[0],list) :
                #root dict has no name; use value from grammar for rootID; {id2:<dict, list>}
                self.root = node.Node(record={'BOTSID': self._getrootid()})  #initialise empty node.
                is_repeting_data_element, self.root.children = self._dojsonlist(jsonobject.values()[0],jsonobject.keys()[0])
            else:
                #~ print self._getrootid()
                self.root = self._dojsonobject(jsonobject,self._getrootid(),object_in_list=False)
            if not self.root:
                raise botslib.InMessageError(u'[J52]: No usable content.')
        else:
            #root in JSON is neither dict or list.
            raise botslib.InMessageError(u'[J53]: JSON root must be a "list" or "object".')

    def _dojsonlist(self,jsonobject,name):
        #TODO: check for consistency!
        lijst = [] #initialise empty list, used to append a listof (converted) json objects
        is_repeting_data_element = False
        for i in jsonobject:
            if isinstance(i,dict):  #check list item is dict/object
                newnode = self._dojsonobject(i,name,object_in_list=True)
                if newnode:
                    lijst.append(newnode)
            elif isinstance(i,(basestring,int,long,float)):
                is_repeting_data_element = True
                lijst.append(i)
            elif self.ta_info['checkunknownentities']:
                raise botslib.InMessageError(u'[J54]: List content must be a "object".')
        return is_repeting_data_element,lijst

    def _dojsonobject(self,jsonobject,name,object_in_list):
        thisnode = node.Node(record={'BOTSID':name},linpos_info=object_in_list)  #initialise empty node. linpos_info indicates if in lsit or not
        #~ print(thisnode.linpos_info, thisnode.record['BOTSID']) #,node_instance.linpos_info.record
        for key,value in jsonobject.iteritems():
            if value is None:
                continue
            elif isinstance(value,basestring):  #json field; map to field in node.record
                ## for generating grammars: empty strings should generate a field
                if value and not value.isspace():   #use only if string has a value.
                    thisnode.record[key] = value
            elif isinstance(value,dict):
                newnode = self._dojsonobject(value,key,object_in_list=False)
                if newnode:
                    thisnode.append(newnode)
            elif isinstance(value,list):
                is_repeting_data_element,lijst = self._dojsonlist(value,key)
                if is_repeting_data_element:
                    thisnode.record[key] = lijst
                else:
                    thisnode.children.extend(lijst)
            elif isinstance(value,(int,long,float)):  #json field; map to field in node.record
                thisnode.record[key] = unicode(value)
            else:
                if self.ta_info['checkunknownentities']:
                    raise botslib.InMessageError(u'[J55]: Key "%(key)s" value "%(value)s": is not string, list or dict.',
                                                    {'key':key,'value':value})
                thisnode.record[key] = unicode(value)
        if len(thisnode.record)==2 and not thisnode.children:
            return None #node is empty...
        #~ thisnode.record['BOTSID']=name
        return thisnode


    def checkmessage(self,node_instance,defmessage,subtranslation=False):
        pass

    def _getrootid(self):
        return self.ta_info.get('defaultBOTSIDroot','root')   #as there is no structure in grammar, use value form syntax.


class xmlforgrammar(inmessage.Inmessage):
    ''' class for ediobjects in XML. Uses ElementTree'''
    def initfromfile(self):
        filename = botslib.abspathdata(self.ta_info['filename'])
        self.ta_info['attributemarker'] = '__'
        parser = ET.XMLParser()
        etree =  ET.ElementTree()   #ElementTree: lexes, parses, makes etree; etree is quite similar to bots-node trees but conversion is needed
        etreeroot = etree.parse(filename, parser)
        self.root = self._etree2botstree(etreeroot)  #convert etree to bots-nodes-tree

    def _use_botscontent(self,xmlnode):
        if self._is_record(xmlnode):
            if xmlnode.text is None:
                return False
            else:
                return not xmlnode.text.isspace()
        else:
            return True

    def _etree2botstree(self,xmlnode):
        newnode = node.Node(record=self._etreenode2botstreenode(xmlnode))
        for xmlchildnode in xmlnode:   #for every node in mpathtree
            if self._is_record(xmlchildnode):
                newnode.append(self._etree2botstree(xmlchildnode))           #add as a node/record
            else:
                ## remark for generating grammars: empty strings should generate a field here
                if self._use_botscontent(xmlchildnode):
                    newnode.record[xmlchildnode.tag] = '1'      #add as a field
                #convert the xml-attributes of this 'xml-field' to fields in dict with attributemarker.
                newnode.record.update((xmlchildnode.tag + self.ta_info['attributemarker'] + key, value) for key,value in xmlchildnode.items())
        return newnode

    def _etreenode2botstreenode(self,xmlnode):
        ''' build a OrderedDict from xml-node. Add BOTSID, xml-attributes (of 'record'), xmlnode.text as BOTSCONTENT.'''
        build = OrderedDict((xmlnode.tag + self.ta_info['attributemarker'] + key,value) for key,value in xmlnode.items())   #convert xml attributes to fields.
        build['BOTSID'] = xmlnode.tag
        if self._use_botscontent(xmlnode):
            build['BOTSCONTENT'] = '1'
        return build

    def _is_record(self,xmlchildnode):
        return bool(len(xmlchildnode))


class xmlforgrammar_allrecords(inmessage.Inmessage):
    ''' class for ediobjects in XML. Uses ElementTree'''
    def initfromfile(self):
        filename = botslib.abspathdata(self.ta_info['filename'])
        self.ta_info['attributemarker'] = '__'
        parser = ET.XMLParser()
        etree =  ET.ElementTree()   #ElementTree: lexes, parses, makes etree; etree is quite similar to bots-node trees but conversion is needed
        etreeroot = etree.parse(filename, parser)
        self.root = self._etree2botstree(etreeroot)  #convert etree to bots-nodes-tree

    def _etree2botstree(self,xmlnode):
        newnode = node.Node(record=self._etreenode2botstreenode(xmlnode))
        for xmlchildnode in xmlnode:   #for every node in mpathtree
            newnode.append(self._etree2botstree(xmlchildnode))           #add as a node/record
        return newnode

    def _etreenode2botstreenode(self,xmlnode):
        ''' build a OrderedDict from xml-node. Add BOTSID, xml-attributes (of 'record'), xmlnode.text as BOTSCONTENT.'''
        build = OrderedDict((xmlnode.tag + self.ta_info['attributemarker'] + key,value) for key,value in xmlnode.items())   #convert xml attributes to fields.
        build['BOTSID'] = xmlnode.tag
        if not self._is_record(xmlnode):
            build['BOTSCONTENT'] = '1'
        return build

    def _is_record(self,xmlchildnode):
        return bool(len(xmlchildnode))

#******************************************************************
#***functions for mapping******************************************
def copytree(origin,destination):
    ''' copy all nodes under origin to destination
        recursive
        removes double  occurences
    '''
    for o_childnode in origin.children:
        #if node is already there.
        for d_childnode in destination.children:
            if o_childnode.record['BOTSID'] == d_childnode.record['BOTSID']:
                d_childnode.record.update(o_childnode.record)
                copytree(o_childnode,d_childnode)
                break   #break out of for loop, contine 'for childnode in node.children'-loop
        else:
            #no occurence of same record, so:
            d_childnode = node.Node(record=dict(o_childnode.record))
            destination.append(d_childnode)
            copytree(o_childnode,d_childnode)


def tree2grammar(node_instance,structure,recorddefs):
    ''' convert tree to grammar (structure & recorddefs)
    '''
    nodeID = node_instance.record['BOTSID']
    #set max_occurence. needed for json, where max_occurence=1 is used for list of eg int.
    max_occurence = 99999 if node_instance.linpos_info else 1

    #add node to structure
    structure.append({ID:nodeID,MIN:0,MAX:max_occurence,LEVEL:[]})
    
    #add fields to recorddefs; might be already exsting recorddef for node
    if nodeID not in recorddefs:
        recorddefs[nodeID] = [] 
    for key,value in node_instance.record.items():
        new_field = [key, 'C', 256, 'AN','R' if isinstance(value,list) else 'S'] #R: repeating field, S: single, non-repeat
        if new_field not in recorddefs[nodeID]:
            recorddefs[nodeID].append(new_field)
        
    #go recursive
    for childnode in node_instance.children:
        tree2grammar(childnode,structure[-1][LEVEL],recorddefs)


def recorddefs2string(recorddefs):
    ''' convert recorddef to printable/string
    '''
    result = ''
    for tag in sorted(recorddefs):
        result += "'%s':\n    [\n"%(tag)
        for field in recorddefs[tag]:
            if field[0] in ['BOTSID','BOTSCONTENT']:
                field[1] = 'M'
                result +=  "    ['%s', '%s', %s, '%s'],\n"%(field[0],field[1],field[2],field[3])
        for field in recorddefs[tag]:
            if field[0].startswith(tag + '__'):
                result +=  "    ['%s', '%s', %s, '%s'],\n"%(field[0],field[1],field[2],field[3])
        for field in recorddefs[tag]:
            if field[0] not in ['BOTSID','BOTSIDnr','BOTSCONTENT'] and not field[0].startswith(tag + '__'):
                if field[4] == 'S':     #S: single, non-repeat
                    result += "    ['%s', '%s', %s, '%s'],\n"%(field[0],field[1],field[2],field[3])
                else:
                    result += "    ['%s', ('%s',99), %s, '%s'],\n"%(field[0],field[1],field[2],field[3])
                #~ result += "    ['%s%s', '%s', %s, '%s'],\n"%(targetNamespace,field[0],field[1],field[2],field[3])
        result += "    ],\n"
    return result


def structure2string(structure,level=0):
    ''' convert structure to printable/string
    '''
    result = ""
    for segment in structure:
        if LEVEL in segment and segment[LEVEL]:
            result += level*'    ' + "{ID:'%s',MIN:%s,MAX:%s,LEVEL:[\n"%(segment[ID],segment[MIN],segment[MAX])
            result += structure2string(segment[LEVEL],level+1)
            result += level*'    ' + "]},\n"
        else:
            result += level*'    ' + "{ID:'%s',MIN:%s,MAX:%s},\n"%(segment[ID],segment[MIN],segment[MAX])
    return result


def grammar2file(botsgrammarfilename,structure,recorddefs):
    result = '#Generated by bots open source edi translator.\nfrom bots.botsconfig import *\n\n'
    result += 'syntax = {\n    }\n\n'
    result += 'structure = [\n'
    result += structure2string(structure)
    result += ']\n\n'
    result += 'recorddefs = {\n'
    result += recorddefs2string(recorddefs)
    result +=  "}\n"

    f = open(botsgrammarfilename,'wb')
    f.write(result)
    f.close()
    print('grammar file is written:',botsgrammarfilename)


def start():
    #********command line arguments**************************
    usage = '''
    This is "%(name)s" version %(version)s, part of Bots open source edi translator (http://bots.sourceforge.net).
    Creates a grammar from an xml or json file.'
    Usage:'
        %(name)s  -c<directory>  <xml_file>  <xml_grammar_file>
    Options:
        -c<directory>      directory for configuration files (default: config).
        -a                 all xml elements as records
        -json              generate grammar from json file
        <input file>       name of the xml or json file to read
        <grammar file>     name of the grammar file to generate

    '''%{'name':os.path.basename(sys.argv[0]),'version':botsglobal.version}
    configdir = 'config'
    edifile =''
    botsgrammarfilename = ''
    ConversionType = 'xmlforgrammar'
    for arg in sys.argv[1:]:
        if arg.startswith('-c'):
            configdir = arg[2:]
            if not configdir:
                print('Error: configuration directory indicated, but no directory name.')
                sys.exit(1)
        elif arg.startswith('-a'):
            ConversionType = 'xmlforgrammar_allrecords'
        elif arg.startswith('-json'):
            ConversionType = 'jsonforgrammar'
        elif arg in ['?', '/?','-h', '--help'] or arg.startswith('-'):
            print(usage)
            sys.exit(0)
        else:
            if not edifile:
                edifile = arg
            else:
                botsgrammarfilename = arg
    if not edifile or not botsgrammarfilename:
        print('Error: both edifile and grammarfile are required.')
        sys.exit(0)
    #***end handling command line arguments*************************
    #***init     *************************
    botsinit.generalinit(configdir)     #find locating of bots, configfiles, init paths etc.
    process_name = 'xml2botsgrammar'
    botsglobal.logger = botsinit.initenginelogging(process_name)
    atexit.register(logging.shutdown)

    #monkeypatch extra classes to inmessage.py
    inmessage.jsonforgrammar = jsonforgrammar
    inmessage.xmlforgrammar_allrecords = xmlforgrammar_allrecords
    inmessage.xmlforgrammar = xmlforgrammar

    #make inmessage object: read the input file
    inn = inmessage.parse_edi_file(editype=ConversionType,messagetype='',filename=edifile)
    #parse_edi_file calls initfromfile() -> read, parse etc
    #check if errors. raises exception if infile not OK
    inn.checkforerrorlist() 
    #make outmessage object; nothing is 'filled' yet. editype='xmlnocheck', have to use something...
    out = outmessage.outmessage_init(editype='xmlnocheck',messagetype='',filename='',divtext='',topartner='')

    #***write inn-tree to out-tree. somewhat like a copy but multiple occurence of record go to one occurence in out-tree
    out.root = node.Node(record=dict(inn.root.record)) #init 'root' of out-tree
    copytree(inn.root, out.root)
    
    #***mapping is done; out-tree is made.
    #***fill structure and recorddefs using out-tree.
    structure = []
    recorddefs = {}
    tree2grammar(out.root,structure,recorddefs)

    #***and write structure,recorddefs to botsgrammarfilename
    grammar2file(botsgrammarfilename,structure,recorddefs)
    #done!


if __name__ == '__main__':
    start()
