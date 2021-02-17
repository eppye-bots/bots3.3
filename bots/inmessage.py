
import sys
import time
import codecs
try:
    from xml.etree import cElementTree as ET
except ImportError:
    from xml.etree import ElementTree as ET
import json as simplejson
#bots-modules
from . import botslib
from . import botsglobal
from . import outmessage
from . import message
from . import node
from . import grammar
from .botsconfig import *
''' Reading/lexing/parsing/splitting an edifile.'''

def parse_edi_file(**ta_info):
    ''' Read,lex, parse edi-file. Is a dispatch function for Inmessage and subclasses.
        Error handling: there are different types of errors.
        For all errors related to incoming messages: catch these.
        Try to extract the relevant information for the message.
        - unicode errors: charset is wrong.
    '''
    try:
        classtocall = globals()[ta_info['editype']]  #get inmessage class to call (subclass of Inmessage)
    except KeyError:
        raise botslib.InMessageError('Unknown editype for incoming message: %(editype)s',ta_info)
    ediobject = classtocall(ta_info)
    #read, lex, parse the incoming edi file
    #ALL errors are caught; these are 'fatal errors': processing has stopped.
    #get information from error/exception; format this into ediobject.errorfatal
    try:
        ediobject.initfromfile()
    except UnicodeError as msg:
        #~ raise botslib.MessageError('')      #UNITTEST_CORRECTION
        content = botslib.get_relevant_text_for_UnicodeError(msg)
        #msg.encoding should contain encoding, but does not (think this is not OK for UNOA, etc)
        ediobject.errorlist.append(str(botslib.InMessageError('[A59]: incoming file has not allowed characters at/after file-position %(pos)s: "%(content)s".',
                                        {'pos':msg.start,'content':content})))
    except Exception as msg:
        #~ raise botslib.MessageError('')      #UNITTEST_CORRECTION
        txt = botslib.txtexc()
        if not botsglobal.ini.getboolean('settings','debug',False):
            txt = txt.partition(': ')[2]
        ediobject.errorlist.append(txt)
    else:
        ediobject.errorfatal = False
    return ediobject

#*****************************************************************************
class Inmessage(message.Message):
    ''' abstract class for incoming ediobject (file or message).
        Can be initialised from a file or a tree.
    '''
    def __init__(self,ta_info):
        super(Inmessage,self).__init__(ta_info)
        self.lex_records = []       #init list of lex_records
        # ~self.countpos = 0           #count chars in edi file. used in _lex, plus for EDIFACT set in _sniff (as UNA is not lexed)

    def messagegrammarread(self,typeofgrammarfile):
        ''' read grammar for a message/envelope.
        '''
        self.defmessage = grammar.grammarread(self.ta_info['editype'],self.ta_info['messagetype'],typeofgrammarfile)
        botslib.updateunlessset(self.ta_info,self.defmessage.syntax)


    def initfromfile(self):
        ''' Initialisation from a edi file.
        '''
        #inn.ta_info is initialised from parsing edi file with:.
        #frompartner,topartner,filename,messagetype,testindicator,editype,charset,alt,fromchannel,frommail,tomail,idroute,command
        #inn.ta_info is initialised. defaults in grammar.py -> envelope -> messagetype
        self.messagegrammarread(typeofgrammarfile='grammars')
        #**from here: charset errors, lex errors
        self._readcontent_edifile()     #open file. variants: read with charset, read as binary & handled in sniff, only opened and read in _lex.
        self._sniff()           #some hard-coded examination of edi file; ta_info can be overruled by syntax-parameters in edi-file
        #start lexing
        self._lex()
        #lex preprocessing via user exit indicated in syntax
        preprocess_lex = self.ta_info.get('preprocess_lex',False)
        if callable(preprocess_lex):
            preprocess_lex(lex=self.lex_records,ta_info=self.ta_info)
        if hasattr(self,'rawinput'):
            del self.rawinput
        self.set_syntax_used()
        #**from here: breaking parser errors
        self.root = node.Node()  #make root Node None.
        self.iternext_lex_record = iter(self.lex_records)
        leftover = self._parse(structure_level=self.defmessage.structure,inode=self.root)
        if leftover:
            raise botslib.InMessageError('[A50] line %(line)s pos %(pos)s: Found non-valid data at end of edi file; probably a problem with separators or message structure.',
                                            {'line':leftover[0][LIN], 'pos':leftover[0][POS]})  #probably not reached with edifact/x12 because of mailbag processing.
        del self.lex_records
        #self.root is now root of a tree (of nodes).

        #**from here: non-breaking parser errors
        #check the incoming edi message with grammar. For enveloped edi-protocols like edifact, x12: check envelopes - message are already checked.
        self.checkenvelope()
        self.checkmessage(self.root,self.defmessage)
        #get queries-dict for parsed message; this is used to update in database
        if self.root.record:
            self.ta_info.update(self.root.queries)
        else:
            for childnode in self.root.children:
                self.ta_info.update(childnode.queries)
                break

    def set_syntax_used(self):
        ''' write syntax dict in self/inmessage-object
        '''
        pass

    def handleconfirm(self,ta_fromfile,routedict,error):
        ''' end of edi file handling: writing of confirmations, etc.
        '''
        pass

    def _formatfield(self,value,field_definition,structure_record,node_instance):
        ''' Format of a field is checked and converted if needed.
            Input: value (string), field definition.
            Output: the formatted value (string)
            Parameters of self.ta_info are used: triad, decimaal
            for fixed field: same handling; length is not checked.
        '''
        if field_definition[BFORMAT] == 'A':
            if len(value) > field_definition[LENGTH]:
                self.add2errorlist('[F05]%(linpos)s: Record "%(record)s" field "%(field)s" too big (max %(max)s): "%(content)s".\n'%
                                    {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value,'max':field_definition[LENGTH]})
            if len(value) < field_definition[MINLENGTH]:
                self.add2errorlist('[F06]%(linpos)s: Record "%(record)s" field "%(field)s" too small (min %(min)s): "%(content)s".\n'%
                                    {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value,'min':field_definition[MINLENGTH]})
        elif field_definition[BFORMAT] in 'DT':
            lenght = len(value)
            if field_definition[BFORMAT] == 'D':
                try:
                    if lenght == 6:
                        time.strptime(value,'%y%m%d')
                    elif lenght == 8:
                        time.strptime(value,'%Y%m%d')
                    else:
                        raise ValueError('To be catched')
                except ValueError:
                    self.add2errorlist('[F07]%(linpos)s: Record "%(record)s" date field "%(field)s" not a valid date: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            else:   #field_definition[BFORMAT] == 'T':
                try:
                    if lenght == 4:
                        time.strptime(value,'%H%M')
                    elif lenght == 6:
                        time.strptime(value,'%H%M%S')
                    elif lenght == 7 or lenght == 8:
                        time.strptime(value[0:6],'%H%M%S')
                        if not value[6:].isdigit():
                            raise ValueError('To be catched')
                    else:
                        raise ValueError('To be catched')
                except ValueError:
                    self.add2errorlist('[F08]%(linpos)s: Record "%(record)s" time field "%(field)s" not a valid time: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
        else:   #elif field_definition[BFORMAT] in 'RNI':   #numerics (R, N, I)
            if self.ta_info['lengthnumericbare']:
                chars_not_counted = '-+' + self.ta_info['decimaal']
                length = 0
                for c in value:
                    if c not in chars_not_counted:
                        length += 1
            else:
                length = len(value)
            if length > field_definition[LENGTH]:
                self.add2errorlist('[F10]%(linpos)s: Record "%(record)s" field "%(field)s" too big (max %(max)s): "%(content)s".\n'%
                                    {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value,'max':field_definition[LENGTH]})
            if length < field_definition[MINLENGTH]:
                self.add2errorlist('[F11]%(linpos)s: Record "%(record)s" field "%(field)s" too small (min %(min)s): "%(content)s".\n'%
                                    {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value,'min':field_definition[MINLENGTH]})
            if value[-1] == '-':    #if minus-sign at the end, put it in front.
                value = value[-1] + value[:-1]
            value = value.replace(self.ta_info['triad'],'')     #strip triad-separators
            value = value.replace(self.ta_info['decimaal'],'.',1) #replace decimal sign by canonical decimal sign
            if 'E' in value or 'e' in value:
                self.add2errorlist('[F09]%(linpos)s: Record "%(record)s" field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                    {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            elif field_definition[BFORMAT] == 'R':
                lendecimal = len(value.partition('.')[2])
                try:    #convert to float in order to check validity
                    valuedecimal = float(value)
                    value = '%.*F'%(lendecimal,valuedecimal)
                except:
                    self.add2errorlist('[F16]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            elif field_definition[BFORMAT] == 'N':
                lendecimal = len(value.partition('.')[2])
                if lendecimal != field_definition[DECIMALS]:
                    self.add2errorlist('[F14]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has invalid nr of decimals: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
                try:    #convert to float in order to check validity
                    valuedecimal = float(value)
                    value = '%.*F'%(lendecimal,valuedecimal)
                except:
                    self.add2errorlist('[F15]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            elif field_definition[BFORMAT] == 'I':
                if '.' in value:
                    self.add2errorlist('[F12]%(linpos)s: Record "%(record)s" field "%(field)s" has format "I" but contains decimal sign: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
                else:
                    try:    #convert to float in order to check validity
                        valuedecimal = float(value)
                        valuedecimal = valuedecimal / 10**field_definition[DECIMALS]
                        value = '%.*F'%(field_definition[DECIMALS],valuedecimal)
                    except:
                        self.add2errorlist('[F13]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                            {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
        return value

    def _parse(self,structure_level,inode):
        ''' This is the heart of the parsing of incoming messages (but not for xml, json)
            Read the lex_records one by one (self.iternext_lex_record, is an iterator)
            - parse the records.
            - identify record (lookup in structure)
            - identify fields in the record (use the record_definition from the grammar).
            - add grammar-info to records: field-tag,mpath.
            Parameters:
            - structure_level: current grammar/segmentgroup of the grammar-structure.
            - inode: parent node; all parsed records are added as children of inode
            2x recursive: SUBTRANSLATION and segmentgroups
        '''
        structure_index = 0     #keep track of where we are in the structure_level
        countnrofoccurences = 0 #number of occurences of current record in structure
        structure_end = len(structure_level)
        get_next_lex_record = True      #indicate if the next record should be fetched, or if the current_lex_record is still being parsed.
                                        #it might seem logical to test here 'current_lex_record is None', but this is already used to indicate 'no more records'.
        while True:
            if get_next_lex_record:
                try:
                    current_lex_record = next(self.iternext_lex_record)
                except StopIteration:   #catch when no more lex_record.
                    current_lex_record = None
                get_next_lex_record = False
            if current_lex_record is None or structure_level[structure_index][ID] != current_lex_record[ID][VALUE]:
                if structure_level[structure_index][MIN] and not countnrofoccurences:   #is record is required in structure_level, and countnrofoccurences==0: error;
                                                                                        #enough check here; message is validated more accurate later
                    try:
                        raise botslib.InMessageError(self.messagetypetxt + '[S50]: Line:%(line)s pos:%(pos)s record:"%(record)s": message has an error in its structure; this record is not allowed here. Scanned in message definition until mandatory record: "%(looked)s".',
                                                                            {'record':current_lex_record[ID][VALUE],'line':current_lex_record[ID][LIN],'pos':current_lex_record[ID][POS],'looked':self.mpathformat(structure_level[structure_index][MPATH])})
                    except TypeError:       #when no UNZ (edifact)
                        raise botslib.InMessageError(self.messagetypetxt + '[S51]: Missing mandatory record "%(record)s".',
                                                                            {'record':self.mpathformat(structure_level[structure_index][MPATH])})
                structure_index += 1
                if structure_index == structure_end:  #current_lex_record is not in this level. Go level up
                    #if on 'first level': give specific error
                    if current_lex_record is not None and structure_level == self.defmessage.structure:
                        raise botslib.InMessageError(self.messagetypetxt + '[S50]: Line:%(line)s pos:%(pos)s record:"%(record)s": message has an error in its structure; this record is not allowed here. Scanned in message definition until mandatory record: "%(looked)s".',
                                                                            {'record':current_lex_record[ID][VALUE],'line':current_lex_record[ID][LIN],'pos':current_lex_record[ID][POS],'looked':self.mpathformat(structure_level[structure_index-1][MPATH])})
                    return current_lex_record    #return either None (no more lex_records to parse) or the last current_lex_record (the last current_lex_record is not found in this level)
                countnrofoccurences = 0
                continue  #continue while-loop: get_next_lex_record is false as no match with structure is made; go and look at next record of structure
            #record is found in grammar
            countnrofoccurences += 1
            #make a new node
            newnode = node.Node(record=self._parsefields(current_lex_record,structure_level[structure_index]),
                                linpos_info=(current_lex_record[0][LIN],current_lex_record[0][POS]) )
            inode.append(newnode)   #append the new node as a child to current (parent)node
            if SUBTRANSLATION in structure_level[structure_index]:
                # start a SUBTRANSLATION; find the right messagetype, etc
                messagetype = newnode.enhancedget(structure_level[structure_index][SUBTRANSLATION])
                if not messagetype:
                    raise botslib.TranslationNotFoundError('Could not find SUBTRANSLATION "%(sub)s" in (sub)message.',
                                                            {'sub':structure_level[structure_index][SUBTRANSLATION]})
                messagetype = self._manipulatemessagetype(messagetype,inode)
                #read grammar
                try:
                    defmessage = grammar.grammarread(self.__class__.__name__,messagetype,typeofgrammarfile='grammars')
                except botslib.BotsImportError:
                    #could not find grammar via normal method. try if there is a user exit to find grammar.
                    raisenovalidmapping_error = True
                    if hasattr(self.defmessage.module,'getmessagetype'):
                        messagetype2 = botslib.runscript(self.defmessage.module,self.defmessage.grammarname,'getmessagetype',editype=self.__class__.__name__,messagetype=messagetype)
                        if messagetype2:
                            try:
                                defmessage = grammar.grammarread(self.__class__.__name__,messagetype2,typeofgrammarfile='grammars')
                                raisenovalidmapping_error = False
                            except botslib.BotsImportError:
                                pass
                    if raisenovalidmapping_error:
                        raise botslib.TranslationNotFoundError('No (valid) grammar for editype "%(editype)s" messagetype "%(messagetype)s".',
                                                                {'editype':self.__class__.__name__,'messagetype':messagetype})
                #grammar is read.
                self.messagecount += 1
                self.messagetypetxt = 'Message nr %(count)s, type %(type)s, '%{'count':self.messagecount,'type':messagetype}
                #go recursive; parse using subtranslation grammar and newnode as root of this message
                current_lex_record = self._parse(structure_level=defmessage.structure[0][LEVEL],inode=newnode)
                newnode.queries = {'messagetype':messagetype}       #copy messagetype into 1st segment of subtranslation (eg UNH, ST)
                newnode.queries.update(defmessage.syntax)
                #~ newnode.queries = defmessage.syntax.copy()       #if using this line instead of previous 2: gives errors eg in incoming edifact...do not understand why
                self.checkmessage(newnode,defmessage,subtranslation=True)      #check the results of the subtranslation
                #~ end SUBTRANSLATION
                self.messagetypetxt = ''
                # get_next_lex_record is still False; we are trying to match the last (not matched) record from the SUBTRANSLATION (named 'current_lex_record').
            else:
                if LEVEL in structure_level[structure_index]:        #if header, go parse segmentgroup (recursive)
                    current_lex_record = self._parse(structure_level=structure_level[structure_index][LEVEL],inode=newnode)
                    # get_next_lex_record is still False; the current_lex_record that was not matched in lower segmentgroups is still being parsed.
                else:
                    get_next_lex_record = True
                #accomodate for UNS = UNS construction
                if structure_level[structure_index][MIN] == structure_level[structure_index][MAX] == countnrofoccurences:
                    if structure_index +1 == structure_end:
                        pass
                    else:
                        structure_index += 1
                        countnrofoccurences = 0

    @staticmethod
    def _manipulatemessagetype(messagetype,inode):
        ''' default: just return messagetype. '''
        return messagetype


    def _readcontent_edifile(self):
        ''' read content of edi file to memory.
        '''
        botsglobal.logger.debug('Read edi file "%(filename)s".',self.ta_info)
        self.rawinput = botslib.readdata(filename=self.ta_info['filename'],charset=self.ta_info['charset'],errors=self.ta_info['checkcharsetin'])

    def _sniff(self):
        ''' sniffing: hard coded parsing of edi file.
            method is specified in subclasses.
        '''
        pass

    def checkenvelope(self):
        pass

    def nextmessage(self):
        ''' Passes each 'message' to the mapping script.
        '''
        #node preprocessing via user exit indicated in syntax, eg sorting
        preprocess_nodes = self.ta_info.get('preprocess_nodes',False)
        if callable(preprocess_nodes):
            preprocess_nodes(thisnode=self)
        if self.defmessage.nextmessage is not None: #if nextmessage defined in grammar: split up messages
            #first: count number of messages
            self.ta_info['total_number_of_messages'] = self.getcountoccurrences(*self.defmessage.nextmessage)
            #yield the messages, using nextmessage
            count = 0
            self.root.processqueries({},len(self.defmessage.nextmessage))
            for eachmessage in self.getloop_including_mpath(*self.defmessage.nextmessage):  #eachmessage is a list: [mpath,mpath, etc, node]
                count += 1
                ta_info = self.ta_info.copy()
                ta_info.update(eachmessage[-1].queries)
                ta_info['message_number'] = count
                ta_info['bots_accessenvelope'] = self.root   #give mappingscript access to envelope
                yield self._initmessagefromnode(eachmessage[-1],ta_info,self.syntax,eachmessage[:-1])   #eachmessage[:-1] is the incoming envelope content
            if self.defmessage.nextmessage2 is not None:        #edifact uses nextmessage2 for UNB-UNG
                #first: count number of messages
                self.ta_info['total_number_of_messages'] = self.getcountoccurrences(*self.defmessage.nextmessage2)
                #yield the messages, using nextmessage2
                self.root.processqueries({},len(self.defmessage.nextmessage2))
                count = 0
                for eachmessage in self.getloop_including_mpath(*self.defmessage.nextmessage2):  #eachmessage is a list: [mpath,mpath, etc, node]
                    count += 1
                    ta_info = self.ta_info.copy()
                    ta_info.update(eachmessage.queries[-1])
                    ta_info['message_number'] = count
                    ta_info['bots_accessenvelope'] = self.root   #give mappingscript access to envelope
                    yield self._initmessagefromnode(eachmessage[-1],ta_info,self.syntax,eachmessage[:-1])   #eachmessage[:-1] is the incoming envelope content
        elif self.defmessage.nextmessageblock is not None:  #for csv/fixed: nextmessageblock indicates which field(s) determines a message
                                                            #--> as long as the field(s) has same value, it is the same message
                                                            #note there is only one recordtype (as checked in grammar.py)
            #first: count number of messages...loop is quite simular to yield loop
            count = 0
            for line in self.root.children:
                kriterium = line.enhancedget(self.defmessage.nextmessageblock)
                if not count:
                    count = 1
                    oldkriterium = kriterium
                elif kriterium != oldkriterium:
                    count += 1
                    oldkriterium = kriterium
                else:
                    pass    #if kriterium is the same
            self.ta_info['total_number_of_messages'] = count
            #yield the messages, using nextmessageblock
            count = 0
            for line in self.root.children:
                kriterium = line.enhancedget(self.defmessage.nextmessageblock)
                if not count:
                    count = 1
                    oldkriterium = kriterium
                    newroot = node.Node()  #make new empty root node.
                elif kriterium != oldkriterium:
                    count += 1
                    oldkriterium = kriterium
                    ta_info = self.ta_info.copy()
                    ta_info.update(oldline.queries)        #update ta_info with information (from previous line) 20100905
                    ta_info['message_number'] = count
                    ta_info['bots_accessenvelope'] = self.root      #give mappingscript access to envelope
                    yield self._initmessagefromnode(newroot,ta_info,self.syntax)
                    newroot = node.Node()  #make new empty root node.
                else:
                    pass    #if kriterium is the same
                newroot.append(line)
                oldline = line #save line 20100905
            else:
                if count:   #not if count is zero (that is, if there are no lines)
                    ta_info = self.ta_info.copy()
                    ta_info.update(line.queries)        #update ta_info with information (from last line) 20100904
                    ta_info['message_number'] = count
                    ta_info['bots_accessenvelope'] = self.root       #give mappingscript access to envelope
                    yield self._initmessagefromnode(newroot,ta_info,self.syntax)
        else:   #no split up is indicated in grammar. Normally you really would...
            #if there is one root (eg xml) or if explicitly indicated: pass the while node-tree as one message.
            if self.root.record or self.ta_info.get('pass_all',False):
                ta_info = self.ta_info.copy()
                ta_info.update(self.root.queries)
                ta_info['total_number_of_messages'] = 1
                ta_info['message_number'] = 1
                ta_info['bots_accessenvelope'] = self.root   #give mappingscript access to envelop
                yield self._initmessagefromnode(self.root,ta_info,self.syntax)
            else:   #pass nodes under root one by one
                #first: count number of messages
                total_number_of_messages = len(self.root.children)
                #yield the messages
                count = 0
                for child in self.root.children:
                    count += 1
                    ta_info = self.ta_info.copy()
                    ta_info.update(child.queries)
                    ta_info['total_number_of_messages'] = total_number_of_messages
                    ta_info['message_number'] = count
                    ta_info['bots_accessenvelope'] = self.root   #give mappingscript access to envelope
                    yield self._initmessagefromnode(child,ta_info,self.syntax)


    def _canonicaltree(self,node_instance,structure):
        ''' call the _canonicaltree for Message (check min/max, sort)
            do the QUERIES in the grammar structure.
        '''
        super(Inmessage,self)._canonicaltree(node_instance,structure)
        if QUERIES in structure:
            node_instance.get_queries_from_edi(structure)

    @classmethod
    def _initmessagefromnode(cls,inode,ta_info,syntax,envelope_content=None):
        ''' initialize a inmessage-object from node in tree.
            used in nextmessage.
        '''
        messagefromnode = cls(ta_info)
        messagefromnode.root = inode
        messagefromnode.syntax = syntax
        messagefromnode.envelope_content = envelope_content   #envelope data of incoming. list of dicts. example:
        #[{'0020': 'UNB_ID', 'S003.0007': '14', 'S002.0007': '14', 'S002.0004': 'PARTNER1', 'S004.0017': '050824', 'BOTSIDnr': '1', 'S003.0010': 'PARTNER2', 'S001.0002': '3', 'S001.0001': 'UNOA', 'BOTSID': 'UNB', 'S004.0019': '1727'}]
        return messagefromnode


class fixed(Inmessage):
    ''' class for record of fixed length.'''
    def _readcontent_edifile(self):
        ''' open the edi file.
        '''
        botsglobal.logger.debug('Read edi file "%(filename)s".',self.ta_info)
        self.filehandler = botslib.opendata(filename=self.ta_info['filename'],mode='rb',charset=self.ta_info['charset'],errors=self.ta_info['checkcharsetin'])

    def _lex(self):
        ''' edi file->self.lex_records.'''
        try:
            #there is a problem with the way python reads line by line: file/line offset is not correctly reported.
            #so the error is catched here to give correct/reasonable result.
            if self.ta_info['noBOTSID']:    #if read records contain no BOTSID: add it
                botsid = self.defmessage.structure[0][ID]   #add the recordname as BOTSID
                for linenr,line in enumerate(self.filehandler, start=1):
                    if not line.isspace():
                        line = line.rstrip('\r\n')
                        self.lex_records.append([{VALUE:botsid,LIN:linenr,POS:0,FIXEDLINE:line},])    #append record to recordlist
            else:
                startrecordid = self.ta_info['startrecordID']
                endrecordid = self.ta_info['endrecordID']
                for linenr,line in enumerate(self.filehandler, start=1):
                    if not line.isspace():
                        line = line.rstrip('\r\n')
                        self.lex_records.append([{VALUE:line[startrecordid:endrecordid].strip(),LIN:linenr,POS:0,FIXEDLINE:line},])    #append record to recordlist
        except UnicodeError as msg:
            rep_linenr = locals().get('linenr',0) + 1
            content = botslib.get_relevant_text_for_UnicodeError(msg)
            raise botslib.InMessageError('Characterset problem in file. At/after line %(line)s: "%(content)s"',{'line':rep_linenr,'content':content})

    def _parsefields(self,lex_record,record_definition):
        ''' Parse fields from one fixed message-record and check length of the fixed record.
        '''
        record2build = {} #start with empty dict
        fixedrecord = lex_record[ID][FIXEDLINE]  #shortcut to fixed incoming record
        lenfixed = len(fixedrecord)
        if record_definition[FIXED_RECORD_LENGTH] != lenfixed:
            if record_definition[FIXED_RECORD_LENGTH] > lenfixed and self.ta_info['checkfixedrecordtooshort']:
                raise botslib.InMessageError('[S52] line %(line)s: Record "%(record)s" too short; is %(pos)s pos, defined is %(defpos)s pos.',
                                                line=lex_record[ID][LIN],record=lex_record[ID][VALUE],pos=lenfixed,defpos=record_definition[FIXED_RECORD_LENGTH])
            if record_definition[FIXED_RECORD_LENGTH] < lenfixed and self.ta_info['checkfixedrecordtoolong']:
                raise botslib.InMessageError('[S53] line %(line)s: Record "%(record)s" too long; is %(pos)s pos, defined is %(defpos)s pos.',
                                                line=lex_record[ID][LIN],record=lex_record[ID][VALUE],pos=lenfixed,defpos=record_definition[FIXED_RECORD_LENGTH])
        pos = 0
        for field_definition in record_definition[FIELDS]:
            if field_definition[ID] == 'BOTSID' and self.ta_info['noBOTSID']:
                record2build['BOTSID'] = lex_record[ID][VALUE]
                continue
            value = fixedrecord[pos:pos+field_definition[LENGTH]].strip()   #copy string to avoid memory problem
            if value:
                record2build[field_definition[ID]] = value
            pos += field_definition[LENGTH]
        record2build['BOTSIDnr'] = record_definition[BOTSIDNR]
        return record2build

    def _formatfield(self,value,field_definition,structure_record,node_instance):
        ''' Format of a field is checked and converted if needed.
            Input: value (string), field definition.
            Output: the formatted value (string)
            Parameters of self.ta_info are used: triad, decimaal
            for fixed field: same handling; length is not checked.
        '''
        if field_definition[BFORMAT] == 'A':
            pass
        elif field_definition[BFORMAT] in 'DT':
            lenght = len(value)
            if field_definition[BFORMAT] == 'D':
                try:
                    if lenght == 6:
                        time.strptime(value,'%y%m%d')
                    elif lenght == 8:
                        time.strptime(value,'%Y%m%d')
                    else:
                        raise ValueError('To be catched')
                except ValueError:
                    self.add2errorlist('[F07]%(linpos)s: Record "%(record)s" date field "%(field)s" not a valid date: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            else:   #if field_definition[BFORMAT] == 'T':
                try:
                    if lenght == 4:
                        time.strptime(value,'%H%M')
                    elif lenght == 6:
                        time.strptime(value,'%H%M%S')
                    elif lenght == 7 or lenght == 8:
                        time.strptime(value[0:6],'%H%M%S')
                        if not value[6:].isdigit():
                            raise ValueError('To be catched')
                    else:
                        raise ValueError('To be catched')
                except ValueError:
                    self.add2errorlist('[F08]%(linpos)s: Record "%(record)s" time field "%(field)s" not a valid time: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
        else:   #elif field_definition[BFORMAT] in 'RNI':   #numerics (R, N, I)
            if value[-1] == '-':    #if minus-sign at the end, put it in front.
                value = value[-1] + value[:-1]
            value = value.replace(self.ta_info['triad'],'')     #strip triad-separators
            value = value.replace(self.ta_info['decimaal'],'.',1) #replace decimal sign by canonical decimal sign
            if 'E' in value or 'e' in value:
                self.add2errorlist('[F09]%(linpos)s: Record "%(record)s" field "%(field)s" contains exponent: "%(content)s".\n'%
                                    {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            if field_definition[BFORMAT] == 'R':
                lendecimal = len(value.partition('.')[2])
                try:    #convert to decimal in order to check validity
                    valuedecimal = float(value)
                    value = '%.*F'%(lendecimal,valuedecimal)
                except:
                    self.add2errorlist('[F16]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            elif field_definition[BFORMAT] == 'N':
                lendecimal = len(value.partition('.')[2])
                if lendecimal != field_definition[DECIMALS]:
                    self.add2errorlist('[F14]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has invalid nr of decimals: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
                try:    #convert to decimal in order to check validity
                    valuedecimal = float(value)
                    value = '%.*F'%(lendecimal,valuedecimal)
                except:
                    self.add2errorlist('[F15]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
            elif field_definition[BFORMAT] == 'I':
                if '.' in value:
                    self.add2errorlist('[F12]%(linpos)s: Record "%(record)s" field "%(field)s" has format "I" but contains decimal sign: "%(content)s".\n'%
                                        {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
                else:
                    try:    #convert to decimal in order to check validity
                        valuedecimal = float(value)
                        valuedecimal = valuedecimal / 10**field_definition[DECIMALS]
                        value = '%.*F'%(field_definition[DECIMALS],valuedecimal)
                    except:
                        self.add2errorlist('[F13]%(linpos)s: Record "%(record)s" numeric field "%(field)s" has non-numerical content: "%(content)s".\n'%
                                            {'linpos':node_instance.linpos(),'record':self.mpathformat(structure_record[MPATH]),'field':field_definition[ID],'content':value})
        return value


class idoc(fixed):
    ''' class for idoc ediobjects.
        for incoming the same as fixed.
        SAP does strip all empty fields for record; is catered for in grammar.defaultsyntax
    '''
    pass


class var(Inmessage):
    ''' abstract class for edi-objects with records of variabele length.'''
    def _lex(self):
        ''' lexes file with variable records to list of lex_records, fields and subfields (build self.lex_records).'''
        record_sep  = self.ta_info['record_sep']
        mode_inrecord = 0  # 1 indicates: lexing in record, 0 is lexing 'between records'.
        field_sep   = self.ta_info['field_sep'] + self.ta_info['record_tag_sep']    #for tradacoms; field_sep and record_tag_sep have same function.
        sfield_sep  = self.ta_info['sfield_sep']
        rep_sep     = self.ta_info['reserve']
        strict_syntax_check = self.ta_info.get('strict_syntax_check',False)
        sfield      = 0 # 1: subfield, 0: not a subfield, 2:repeat
        quote_char  = self.ta_info['quote_char']  #typical fo csv. example with quote_char ":  ,"1523",TEXT,"123",
        mode_quote  = 0    #0=not in quote, 1=in quote
        mode_2quote = 0    #status within mode_quote. 0=just another char within quote, 1=met 2nd quote char; might be end of quote OR escaping of another quote-char.
        escape      = self.ta_info['escape']      #char after escape-char is not interpreted as separator
        mode_escape = 0    #0=not escaping, 1=escaping
        skip_char   = self.ta_info['skip_char']   #chars to ignore/skip/discard. eg edifact: if wrapped to 80pos lines and <CR/LF> at end of segment
        lex_record  = []   #gather the content of a record
        value       = ''   #gather the content of (sub)field; the current token
        valueline   = 1    #record line of token
        valuepos    = 1    #record position of token in line
        countline   = 1    #count number of lines; start with 1
        countpos    = 0    #count position/number of chars within line
        sep = field_sep + sfield_sep + record_sep + escape + rep_sep

        for char in self.rawinput:    #get next char
            if char == '\n':
                #count number lines/position; no action.
                countline += 1      #count line
                countpos = 0        #position back to 0
            else:
                countpos += 1       #position within line
            if mode_quote:
                #lexing within a quote; note that quote-char works as escape-char within a quote
                if mode_2quote:
                    mode_2quote = 0
                    if char == quote_char: #after quote-char another quote-char: used to escape quote_char:
                        value += char    #append quote_char
                        continue
                    else: #quote is ended:
                        mode_quote = 0
                        #continue parsing of this char
                elif mode_escape:        #tricky: escaping a quote char
                    mode_escape = 0
                    value += char
                    continue
                elif char == quote_char:    #either end-quote or escaping quote_char,we do not know yet
                    mode_2quote = 1
                    continue
                elif char == escape:
                    mode_escape = 1
                    continue
                else:                       #we are in quote, just append char to token
                    value += char
                    continue
            if char in skip_char:
                #char is skipped. In csv these chars could be in a quote; in eg edifact chars will be skipped, even if after escape sign.
                continue
            if not mode_inrecord:
                #get here after record-separator is found. we are 'between' records.
                #some special handling for whitespace characters; for other chars: go on lexing
                if char.isspace():  #whitespace = ' \t\n\r\v\f'....note that CRLF might be in skip_char
                    if char in field_sep and isinstance(self,csv): #exception for tab-delimited csv/excel files: if first field is not filled: first TAB is significant!
                        pass        #just go on lexing
                    elif strict_syntax_check:  #for strict checks: no spaces between records
                        raise botslib.InMessageError('[A67]: Found whitespace characters between segments. Line %(countline)s, position %(pos)s, position %(countpos)s.',{'countline':countline,'countpos':countpos})
                    else:
                        continue    #ignore whitespace character; continue for-loop with next character
                mode_inrecord = 1   #not whitespace - a new record has started
            if mode_escape:
                #in escaped_mode: char after escape sign is appended to token
                mode_escape = 0
                value += char
                continue
            if not value:
                #if no char in token: this is a new token, get line and pos for (new) token
                valueline = countline
                valuepos = countpos
            if char == quote_char and (not value or value.isspace()):
                #for csv: handle new quote value. New quote value only makes sense for new field (value is empty) or field contains only whitespace
                mode_quote = 1
                continue
            if char not in sep:
                value += char    #just a char: append char to value
                continue
            if char in field_sep:
                #end of (sub)field. Note: first field of composite is marked as 'field'
                lex_record.append({VALUE:value,SFIELD:sfield,LIN:valueline,POS:valuepos})    #write current value to lex_record
                value = ''
                sfield = 0      #new token is field
                continue
            if char == sfield_sep:
                #end of (sub)field. Note: first field of composite is marked as 'field'
                lex_record.append({VALUE:value,SFIELD:sfield,LIN:valueline,POS:valuepos})    #write current value to lex_record
                value = ''
                sfield = 1        #new token is sub-field
                continue
            if char in record_sep:      #end of record
                if strict_syntax_check and not lex_record:      #check for 'double' record seperator.
                    raise botslib.InMessageError('[A69]: Found double record seperator. Line %(countline)s, position %(pos)s, position %(countpos)s.',{'countline':countline,'countpos':countpos})
                lex_record.append({VALUE:value,SFIELD:sfield,LIN:valueline,POS:valuepos})    #write current value to lex_record
                self.lex_records.append(lex_record)                 #write lex_record to self.lex_records
                lex_record = []
                value = ''
                sfield = 0      #new token is field
                mode_inrecord = 0    #we are not in a record
                continue
            if char == escape:
                mode_escape = 1
                continue
            if char == rep_sep:
                lex_record.append({VALUE:value,SFIELD:sfield,LIN:valueline,POS:valuepos})    #write current value to lex_record
                value = ''
                sfield = 2        #new token is repeating
                continue
        #end of for-loop. all characters have been processed.
        #in a perfect world, value should always be empty now, but:
        #it appears a csv record is not always closed properly, so force the closing of the last record of csv file:
        if mode_inrecord and self.ta_info.get('allow_lastrecordnotclosedproperly',False):
            lex_record.append({VALUE:value,SFIELD:sfield,LIN:valueline,POS:valuepos})    #append element in record
            self.lex_records.append(lex_record)    #write record to recordlist
        else:
            leftover = value.strip('\x00\x1a')
            if leftover:
                raise botslib.InMessageError('[A51]: Found non-valid data at end of edi file; probably a problem with separators or message structure: "%(leftover)s".',
                                                {'leftover':leftover})

    def _parsefields(self,lex_record,record_definition):
        ''' Identify the fields in inmessage-record using the record_definition from the grammar
            Build a record (dictionary; field-IDs are unique within record) and return this.
        '''
        list_of_fields_in_record_definition = record_definition[FIELDS]
        if record_definition[ID] == 'ISA' and isinstance(self,x12):    #isa is an exception: no strip()
            is_x12_ISA = True
        else:
            is_x12_ISA = False
        record2build = {}         #record that is build from lex_record using ID's from record_definition
        tindex = -1     #elementcounter; composites count as one
        #~ tsubindex = 0     #sub-element counter within composite; for files that are OK: init when compostie is detected. This init is for error (field is lexed as subfield but is not.) 20130222: catch UnboundLocalError now
        #********loop over all fields present in this record of edi file
        #********identify the lexed fields in grammar, and build a dict with (fieldID:value)
        for lex_field in lex_record:
            value = lex_field[VALUE].strip() if not is_x12_ISA else lex_field[VALUE][:]
            #*********use info of lexer: what is preceding separator (field, sub-field, repeat)
            if not lex_field[SFIELD]:       #preceded by field-separator
                try:
                    tindex += 1                 #use next field
                    field_definition = list_of_fields_in_record_definition[tindex]
                except IndexError:
                    self.add2errorlist('[F19] line %(line)s pos %(pos)s: Record "%(record)s" too many fields in record; unknown field "%(content)s".\n'%
                                        {'content':lex_field[VALUE],'line':lex_field[LIN],'pos':lex_field[POS],'record':self.mpathformat(record_definition[MPATH])})
                    continue
                if field_definition[MAXREPEAT] == 1: #definition says: not repeating
                    if field_definition[ISFIELD]:    #definition says: field       +E+
                        if value:
                            record2build[field_definition[ID]] = value
                    else:                                      #definition says: subfield    +E:S+
                        tsubindex = 0
                        list_of_subfields_in_record_definition = list_of_fields_in_record_definition[tindex][SUBFIELDS]
                        sub_field_in_record_definition = list_of_subfields_in_record_definition[tsubindex]
                        if value:
                            record2build[sub_field_in_record_definition[ID]] = value
                else:   #definition says: repeating
                    if field_definition[ISFIELD]:      #definition says: field      +E*R+
                        record2build[field_definition[ID]] = [value]
                    else:                                        #definition says: subfield   +E:S*R:S+
                        tsubindex = 0
                        list_of_subfields_in_record_definition = list_of_fields_in_record_definition[tindex][SUBFIELDS]
                        sub_field_in_record_definition = list_of_subfields_in_record_definition[tsubindex]
                        record2build[field_definition[ID]] = [{sub_field_in_record_definition[ID]:value},]
            elif lex_field[SFIELD] == 1:    #preceded by sub-field separator
                try:
                    tsubindex += 1
                    sub_field_in_record_definition = list_of_subfields_in_record_definition[tsubindex]
                except (TypeError,UnboundLocalError):       #field has no SUBFIELDS, or unexpected subfield
                    self.add2errorlist('[F17] line %(line)s pos %(pos)s: Record "%(record)s" expect field but "%(content)s" is a subfield.\n'%
                                        {'content':lex_field[VALUE],'line':lex_field[LIN],'pos':lex_field[POS],'record':self.mpathformat(record_definition[MPATH])})
                    continue
                except IndexError:      #tsubindex is not in the subfields
                    self.add2errorlist('[F18] line %(line)s pos %(pos)s: Record "%(record)s" too many subfields in composite; unknown subfield "%(content)s".\n'%
                                          {'content':lex_field[VALUE],'line':lex_field[LIN],'pos':lex_field[POS],'record':self.mpathformat(record_definition[MPATH])})
                    continue
                if field_definition[MAXREPEAT] == 1: #definition says: not repeating   +E:S+
                    if value:
                        record2build[sub_field_in_record_definition[ID]] = value
                else:                                          #definition says: repeating       +E:S*R:S+
                    record2build[field_definition[ID]][-1][sub_field_in_record_definition[ID]] = value
            else:                         #  preceded by repeat separator
                #check if repeating!
                if field_definition[MAXREPEAT] == 1:
                    if 'ISA' == self.mpathformat(record_definition[MPATH]) and field_definition[ID] == 'ISA11':     #exception for ISA
                        pass
                    else:
                        self.add2errorlist('[F40] line %(line)s pos %(pos)s: Record "%(record)s" expect not-repeating elemen, but "%(content)s" is repeating.\n'%
                                              {'content':lex_field[VALUE],'line':lex_field[LIN],'pos':lex_field[POS],'record':self.mpathformat(record_definition[MPATH])})
                    continue

                if field_definition[ISFIELD]:      #definition says: field      +E*R+
                    record2build[field_definition[ID]].append(value)
                else:                                        #definition says: first subfield   +E:S*R:S+
                    tsubindex = 0
                    list_of_subfields_in_record_definition = list_of_fields_in_record_definition[tindex][SUBFIELDS]
                    sub_field_in_record_definition = list_of_subfields_in_record_definition[tsubindex]
                    record2build[field_definition[ID]].append({sub_field_in_record_definition[ID]:value})
        record2build['BOTSIDnr'] = record_definition[BOTSIDNR]
        return record2build

    @staticmethod
    def separatorcheck(separatorstring):
        if len(separatorstring) != len(set(separatorstring)):
            raise botslib.InMessageError('[A64]: Separator problem in edi file: same separator is used twice.')
        if ' ' in separatorstring:
            raise botslib.InMessageError('[A65]: Separator problem in edi file: space is used as separator.')
        for sep in separatorstring:
            if sep.isalnum():
                raise botslib.InMessageError('[A66]: Separator problem in edi file: separator is alfanumeric.')


class csv(var):
    ''' class for ediobjects with Comma Separated Values'''
    def _lex(self):
        super(csv,self)._lex()
        
        if self.ta_info['skip_firstline']:
            # if it is an integer, skip that many lines
            # if True, skip just the first line
            if isinstance(self.ta_info['skip_firstline'],bool):
                del self.lex_records[0]
            else:
                del self.lex_records[0:self.ta_info['skip_firstline']]

        noBOTSID = self.ta_info['noBOTSID']
        if noBOTSID: 
            # if integer, swap fields in record
            # if True, add BOTSID to record
            if isinstance(noBOTSID,bool):
                botsid = self.defmessage.structure[0][ID]   #add the recordname as BOTSID
                for lex_record in self.lex_records:
                    lex_record[0:0] = [{VALUE: botsid, POS: 0, LIN:lex_record[0][LIN], SFIELD: False}]
            else:
                for lex_record in self.lex_records:
                    botsid_record = lex_record.pop(noBOTSID)
                    lex_record[0:0] = [botsid_record]


    def set_syntax_used(self):
        self.syntax['record_sep'] = self.ta_info['record_sep']
        self.syntax['field_sep']  = self.ta_info['field_sep']
        self.syntax['quote_char'] = self.ta_info['quote_char']
        self.syntax['escape']     = self.ta_info['escape']

class excel(csv):
    def initfromfile(self):
        ''' initialisation from an excel file.
            file is first converted to csv using python module xlrd
        '''
        try:
            self.xlrd = botslib.botsbaseimport('xlrd')
        except ImportError:
            raise ImportError('Dependency failure: editype "excel" requires python library "xlrd".')
        import csv as csvlib
        try:
            import io
        except:
            import StringIO as io # Py2

        self.messagegrammarread(typeofgrammarfile='grammars')
        self.ta_info['charset'] = self.defmessage.syntax['charset']      #always use charset of edi file.
        if self.ta_info['escape']:
            doublequote = False
        else:
            doublequote = True

        botsglobal.logger.debug('Read edi file "%(filename)s".',self.ta_info)
        #xlrd reads excel file; python's csv modules write this to file-like StringIO (as utf-8); read StringIO as self.rawinput; decode this (utf-8->unicode)
        infilename = botslib.abspathdata(self.ta_info['filename'])
        try:
            xlsdata = self.read_xls(infilename)
        except:
            txt = botslib.txtexc()
            botsglobal.logger.error('Excel extraction failed, may not be an Excel file? Error:\n%(txt)s',
                                            {'txt':txt})
            raise botslib.InMessageError('Excel extraction failed, may not be an Excel file? Error:\n%(txt)s',
                                            {'txt':txt})
        rawinputfile = io.StringIO()
        csvout = csvlib.writer(rawinputfile, quotechar=self.ta_info['quote_char'], delimiter=self.ta_info['field_sep'], doublequote=doublequote, escapechar=self.ta_info['escape'])
        csvout.writerows( list(map(self.utf8ize, xlsdata)) )
        rawinputfile.seek(0)
        self.rawinput = rawinputfile.read()
        rawinputfile.close()
        self.rawinput = self.rawinput.decode('utf-8')
        #start lexing and parsing as csv
        self._lex()
        if hasattr(self,'rawinput'):
            del self.rawinput
        self.root = node.Node()  #make root Node None.
        self.iternext_lex_record = iter(self.lex_records)
        leftover = self._parse(structure_level=self.defmessage.structure,inode=self.root)
        if leftover:
            raise botslib.InMessageError('[A52]: Found non-valid data at end of excel file: "%(leftover)s".',
                                            {'leftover':leftover})
        del self.lex_records
        self.checkmessage(self.root,self.defmessage)

    def read_xls(self,infilename):
        # Read excel first sheet into a 2-d array
        book       = self.xlrd.open_workbook(infilename)
        sheet      = book.sheet_by_index(0)
        #~ formatter  = lambda(t,v): self.format_excelval(book,t,v,False)  # python3
        xlsdata = []
        for row in range(sheet.nrows):
            (types, values) = (sheet.row_types(row), sheet.row_values(row))
            xlsdata.append(list(map(formatter, list(zip(types, values)))))
        return xlsdata
    #-------------------------------------------------------------------------------
    def format_excelval(self,book,datatype,value,wanttupledate):
        #  Convert excel data for some data types
        if datatype == 2:
            if value == int(value):
                value = int(value)
        elif datatype == 3:
            datetuple = self.xlrd.xldate_as_tuple(value, book.datemode)
            value = datetuple if wanttupledate else self.tupledate_to_isodate(datetuple)
        elif datatype == 5:
            value = self.xlrd.error_text_from_code[value]
        return value
    #-------------------------------------------------------------------------------
    def tupledate_to_isodate(self,tupledate):
        # Turns a gregorian (year, month, day, hour, minute, nearest_second) into a
        # standard YYYY-MM-DDTHH:MM:SS ISO date.
        (y,m,d, hh,mm,ss) = tupledate
        nonzero = lambda n: n != 0
        datestring = '%04d-%02d-%02d'  % (y,m,d)    if list(filter(nonzero,(y,m,d))) else ''
        timestring = 'T%02d:%02d:%02d' % (hh,mm,ss) if list(filter(nonzero,(hh,mm,ss))) or not datestring else ''
        return datestring+timestring
    #-------------------------------------------------------------------------------
    def utf8ize(self,l):
        # Make string-like things into utf-8, leave other things alone
        return [str(s).encode('utf-8') if hasattr(s,'encode') else s for s in l]


class edifact(var):
    ''' class for edifact inmessage objects.'''
    @staticmethod
    def _manipulatemessagetype(messagetype,inode):
        ''' default: just return messagetype. '''
        return messagetype.replace('.','_')      #older edifact messages have eg 90.1 as version...does not match with python imports...so convert this

    def _readcontent_edifile(self):
        ''' read content of edifact file in memory.
            is read as binary. In _sniff determine charset; then decode according to charset.
        '''
        botsglobal.logger.debug('Read edifact file "%(filename)s".',self.ta_info)
        self.rawinput = botslib.readdata_bin(filename=self.ta_info['filename'])     #read as binary

    def _sniff(self):
        ''' examine the beginning of edifact file for syntax parameters and charset.
            edifact file is sniffed as binary. edifact has several charsets (UNOA, UNOC, UNOY).
            Bots assumes: UNA-string contains NO extra CR/LF. (would be absurd; combination of: multiple UNA in file & using 'blocked' edifact.)
        '''
        #check for BOM. BOM should not be there. But if it is, gave very confusing error.
        if self.rawinput.startswith(codecs.BOM_UTF8):
            raise botslib.InMessageError('[A68]: Edifact file starts with BOM.')
        #*****read first 100 bytes to do sniffing....
        rawinput = self.rawinput[0:99].decode('iso-8859-1')
        #*****find first non-whitespace character
        rawinput = rawinput.lstrip()
        #*****check if UNA
        if rawinput.startswith('UNA'):
            has_una_string = True
            #read UNA and set syntax parameters
            count = 3
            try:
                for field in ['sfield_sep','field_sep','decimaal','escape','reserve','record_sep']:
                    self.ta_info[field] = rawinput[count]
                    count += 1
            except IndexError:
                raise botslib.InMessageError('[A53]: Edifact file contains "UNA" and than garbage.')   #if file starts with <whitespace>'UNA' than has less than 6 characters?
            #UNA-string is done; loop until next not-space char
            rawinput = rawinput[count:].lstrip()
        else:
            has_una_string = False
        #*****check if there is UNB
        if not rawinput.startswith('UNB'):
            raise botslib.InMessageError('[A54]: Found no "UNB" at the start of edifact file. Probably not be edifact.')  #also: UNA too short. not possible if mailbag is used.
        #*****get separators, charset, version.
        count = 0       #as there is an UNB
        found_charset = ''
        for char in rawinput:
            if char in self.ta_info['skip_char']:
                continue
            if count <= 3:
                if count == 3:
                    found_field_sep = char
            elif count <= 7:
                found_charset += char
            elif count == 8:
                found_sfield_sep = char
            else:
                self.ta_info['version'] = char
                break
            count += 1
        else:
            raise botslib.InMessageError('[A55]: Problems with UNB-segment; too many <CR/LF>.')

        #set and/or verify separators
        if has_una_string:
            if found_field_sep != self.ta_info['field_sep'] or found_sfield_sep != self.ta_info['sfield_sep']:
                raise botslib.InMessageError('[A56]: Separators as used in edifact file are different from values as in UNA-segment.')
        else:
            if found_field_sep == '+' and found_sfield_sep == ':':     #assume standard/UNOA separators.
                self.ta_info['sfield_sep'] = ':'
                self.ta_info['field_sep'] = '+'
                self.ta_info['decimaal'] = '.'
                self.ta_info['escape'] = '?'
                self.ta_info['reserve'] = '*'
                self.ta_info['record_sep'] = "'"
            elif found_field_sep == '\x1D' and found_sfield_sep == '\x1F':     #check if UNOB separators are used...never seen this, but keep this logic
                self.ta_info['sfield_sep'] = '\x1F'
                self.ta_info['field_sep'] = '\x1D'
                self.ta_info['decimaal'] = '.'
                self.ta_info['escape'] = ''
                self.ta_info['reserve'] = '*'
                self.ta_info['record_sep'] = '\x1C'
            else:
                raise botslib.InMessageError('[A57]: Edifact file has non-standard separators. An UNA segment is required.')

        #*********** decode the file (to unicode).
        self.ta_info['charset'] = found_charset
        try:
            self.rawinput = self.rawinput.decode(found_charset,self.ta_info['checkcharsetin'])
            self.countpos = self.rawinput.find('UNB')       #import
        except LookupError:
            raise botslib.InMessageError('[A58]: Edifact file has unknown characterset "%(charset)s".',
                                            {'charset':found_charset})
        except UnicodeDecodeError as msg:
            raise botslib.InMessageError('[A59]: Edifact file has not allowed characters at/after file-position %(content)s.',
                                            {'content':msg[2]})
        #****extra checks for repetition separator
        if self.ta_info['version'] < '4':
            self.ta_info['reserve'] = ''    # repetition separator only for version >= 4.
        elif self.ta_info['reserve'] == ' ' and not self.ta_info.get('strict_syntax_check',False):
            self.ta_info['reserve'] = ''    #if version > 4 and repetition separator is space: assume this is a mistake. If strict checking: error is catched in separatorcheck.

        #****extra checks for separators
        self.separatorcheck(self.ta_info['sfield_sep'] + self.ta_info['field_sep'] + self.ta_info['decimaal'] + self.ta_info['escape'] + self.ta_info['reserve'] + self.ta_info['record_sep'])


    def checkenvelope(self):
        ''' check envelopes (UNB-UNZ counters & references, UNH-UNT counters & references etc)
        '''
        for UNB in self.getloop({'BOTSID':'UNB'}):
            botsglobal.logmap.debug('Start parsing edifact envelopes')
            unbreference = UNB.get({'BOTSID':'UNB','0020':None})
            unzreference = UNB.get({'BOTSID':'UNB'},{'BOTSID':'UNZ','0020':None})
            if unbreference and unzreference and unbreference != unzreference:
                self.add2errorlist('[E01]: UNB-reference is "%(unbreference)s"; should be equal to UNZ-reference "%(unzreference)s".\n'%{'unbreference':unbreference,'unzreference':unzreference})
            unzcount = UNB.get({'BOTSID':'UNB'},{'BOTSID':'UNZ','0036':None})
            messagecount = len(UNB.children) - 1
            try:
                if int(unzcount) != messagecount:
                    self.add2errorlist('[E02]: Count of messages in UNZ is %(unzcount)s; should be equal to number of messages %(messagecount)s.\n'%{'unzcount':unzcount,'messagecount':messagecount})
            except:
                self.add2errorlist('[E03]: Count of messages in UNZ is invalid: "%(count)s".\n'%{'count':unzcount})
            for nodeunh in UNB.getloop({'BOTSID':'UNB'},{'BOTSID':'UNH'}):
                unhreference = nodeunh.get({'BOTSID':'UNH','0062':None})
                untreference = nodeunh.get({'BOTSID':'UNH'},{'BOTSID':'UNT','0062':None})
                if unhreference and untreference and unhreference != untreference:
                    self.add2errorlist('[E04]: UNH-reference is "%(unhreference)s"; should be equal to UNT-reference "%(untreference)s".\n'%{'unhreference':unhreference,'untreference':untreference})
                untcount = nodeunh.get({'BOTSID':'UNH'},{'BOTSID':'UNT','0074':None})
                segmentcount = nodeunh.getcount()
                try:
                    if int(untcount) != segmentcount:
                        self.add2errorlist('[E05]: Segmentcount in UNT is %(untcount)s; should be equal to number of segments %(segmentcount)s.\n'%{'untcount':untcount,'segmentcount':segmentcount})
                except:
                    self.add2errorlist('[E06]: Count of segments in UNT is invalid: "%(count)s".\n'%{'count':untcount})
            for nodeung in UNB.getloop({'BOTSID':'UNB'},{'BOTSID':'UNG'}):
                ungreference = nodeung.get({'BOTSID':'UNG','0048':None})
                unereference = nodeung.get({'BOTSID':'UNG'},{'BOTSID':'UNE','0048':None})
                if ungreference and unereference and ungreference != unereference:
                    self.add2errorlist('[E07]: UNG-reference is "%(ungreference)s"; should be equal to UNE-reference "%(unereference)s".\n'%{'ungreference':ungreference,'unereference':unereference})
                unecount = nodeung.get({'BOTSID':'UNG'},{'BOTSID':'UNE','0060':None})
                groupcount = len(nodeung.children) - 1
                try:
                    if int(unecount) != groupcount:
                        self.add2errorlist('[E08]: Groupcount in UNE is %(unecount)s; should be equal to number of groups %(groupcount)s.\n'%{'unecount':unecount,'groupcount':groupcount})
                except:
                    self.add2errorlist('[E09]: Groupcount in UNE is invalid: "%(count)s".\n'%{'count':unecount})
                for nodeunh in nodeung.getloop({'BOTSID':'UNG'},{'BOTSID':'UNH'}):
                    unhreference = nodeunh.get({'BOTSID':'UNH','0062':None})
                    untreference = nodeunh.get({'BOTSID':'UNH'},{'BOTSID':'UNT','0062':None})
                    if unhreference and untreference and unhreference != untreference:
                        self.add2errorlist('[E10]: UNH-reference is "%(unhreference)s"; should be equal to UNT-reference "%(untreference)s".\n'%{'unhreference':unhreference,'untreference':untreference})
                    untcount = nodeunh.get({'BOTSID':'UNH'},{'BOTSID':'UNT','0074':None})
                    segmentcount = nodeunh.getcount()
                    try:
                        if int(untcount) != segmentcount:
                            self.add2errorlist('[E11]: Segmentcount in UNT is %(untcount)s; should be equal to number of segments %(segmentcount)s.\n'%{'untcount':untcount,'segmentcount':segmentcount})
                    except:
                        self.add2errorlist('[E12]: Count of segments in UNT is invalid: "%(count)s".\n'%{'count':untcount})
            botsglobal.logmap.debug('Parsing edifact envelopes is OK')

    def handleconfirm(self,ta_fromfile,routedict,error):
        ''' done at end of edifact file handling.
            generates CONTRL messages (or not)
        '''
        #for fatal errors there is no decent node tree
        if self.errorfatal:
            return
        #check if there are any 'send-edifact-CONTRL' confirmrules.
        confirmtype = 'send-edifact-CONTRL'
        if not botslib.globalcheckconfirmrules(confirmtype):
            return
        editype = 'edifact'
        AcknowledgeCode = '7' if not error else '4'
        #copy fields from UNB received to UNB of CONTRL to send
        for UNB in self.getloop({'BOTSID':'UNB'}):
            nr_message_to_confirm = 0
            messages_not_confirm = []
            for nodeunh in UNB.getloop({'BOTSID':'UNB'},{'BOTSID':'UNH'}):
                messagetype = nodeunh.queries['messagetype']
                #no CONTRL for CONTRL or APERAK message; check if CONTRL should be send via confirmrules
                if messagetype[:6] in ['CONTRL','APERAK'] or not botslib.checkconfirmrules(confirmtype,idroute=self.ta_info['idroute'],idchannel=self.ta_info['fromchannel'],
                                                                                                frompartner=sender,topartner=receiver,messagetype=messagetype):
                    messages_not_confirm.append(nodeunh)
                else:
                    nr_message_to_confirm += 1
            if not nr_message_to_confirm:
                continue
            #remove message not to be confirmed from tree (is destructive, but this is end of file processing anyway.
            for message_not_confirm in messages_not_confirm:
                UNB.children.remove(message_not_confirm)
            #check if there is a user mappingscript
            tscript,toeditype,tomessagetype = botslib.lookup_translation(fromeditype=editype,frommessagetype='CONTRL',frompartner=receiver,topartner=sender,alt='')
            if not tscript:
                tomessagetype = 'CONTRL22UNEAN002'  #default messagetype for CONTRL
                translationscript = None
            else:
                translationscript,scriptfilename = botslib.botsimport('mappings',editype,tscript)  #import the mappingscript
            #generate CONTRL-message. One received interchange->one CONTRL-message
            reference = str(botslib.unique('messagecounter'))
            ta_confirmation = ta_fromfile.copyta(status=TRANSLATED)
            filename = str(ta_confirmation.idta)
            out = outmessage.outmessage_init(editype=editype,messagetype=tomessagetype,filename=filename,reference=reference,statust=OK)    #make outmessage object
            out.ta_info['frompartner'] = inn.ta_info['topartner']   #reverse!
            out.ta_info['topartner'] = inn.ta_info['frompartner']       #reverse!
            if translationscript and hasattr(translationscript,'main'):
                botslib.runscript(translationscript,scriptfilename,'main',inn=self,out=out,routedict=routedict,ta_fromfile=ta_fromfile)
            else:
                #default mapping script for CONTRL
                #write UCI for UNB (envelope)
                out.put({'BOTSID':'UNH','0062':reference,'S009.0065':'CONTRL','S009.0052':'2','S009.0054':'2','S009.0051':'UN','S009.0057':'EAN002'})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','0083':AcknowledgeCode})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','0020':UNB.get({'BOTSID':'UNB','0020':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S002.0004':UNB.get({'BOTSID':'UNB','S002.0004':None})})     #not reverse!
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S002.0007':UNB.get({'BOTSID':'UNB','S002.0007':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S002.0008':UNB.get({'BOTSID':'UNB','S002.0008':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S002.0042':UNB.get({'BOTSID':'UNB','S002.0042':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S003.0010':UNB.get({'BOTSID':'UNB','S003.0010':None})})   #not reverse!
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S003.0007':UNB.get({'BOTSID':'UNB','S003.0007':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S003.0014':UNB.get({'BOTSID':'UNB','S003.0014':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UCI','S003.0046':UNB.get({'BOTSID':'UNB','S003.0046':None})})
                #write UCM for each UNH (message)
                for nodeunh in UNB.getloop({'BOTSID':'UNB'},{'BOTSID':'UNH'}):
                    lou = out.putloop({'BOTSID':'UNH'},{'BOTSID':'UCM'})
                    lou.put({'BOTSID':'UCM','0083':AcknowledgeCode})
                    lou.put({'BOTSID':'UCM','0062':nodeunh.get({'BOTSID':'UNH','0062':None})})
                    lou.put({'BOTSID':'UCM','S009.0065':nodeunh.get({'BOTSID':'UNH','S009.0065':None})})
                    lou.put({'BOTSID':'UCM','S009.0052':nodeunh.get({'BOTSID':'UNH','S009.0052':None})})
                    lou.put({'BOTSID':'UCM','S009.0054':nodeunh.get({'BOTSID':'UNH','S009.0054':None})})
                    lou.put({'BOTSID':'UCM','S009.0051':nodeunh.get({'BOTSID':'UNH','S009.0051':None})})
                    lou.put({'BOTSID':'UCM','S009.0057':nodeunh.get({'BOTSID':'UNH','S009.0057':None})})
                out.put({'BOTSID':'UNH'},{'BOTSID':'UNT','0074':out.getcount()+1,'0062':reference})  #last line (counts the segments produced in out-message)
                #try to run the user mapping script fuction 'change' (after the default mapping); 'chagne' fucntion recieves the tree as written by default mapping, function can change tree.
                if translationscript and hasattr(translationscript,'change'):
                    botslib.runscript(translationscript,scriptfilename,'change',inn=self,out=out,routedict=routedict,ta_fromfile=ta_fromfile)
            #write tomessage (result of translation)
            out.writeall()
            botsglobal.logger.debug('Send edifact confirmation (CONTRL) route "%(route)s" fromchannel "%(fromchannel)s" frompartner "%(frompartner)s" topartner "%(topartner)s".',
                                    {'route':self.ta_info['idroute'],'fromchannel':self.ta_info['fromchannel'],'frompartner':receiver,'topartner':sender})
            self.ta_info.update(confirmtype=confirmtype,confirmed=True,confirmasked = True,confirmidta=ta_confirmation.idta)  #this info is used in transform.py to update the ta.....ugly...
            ta_confirmation.update(**out.ta_info)    #update ta for confirmation

    def try_to_retrieve_info(self):
        ''' when edi-file is not correct, (try to) get info about eg partnerID's in message
            for now: look around in lexed record
        '''
        if hasattr(self,'lex_records'):
            for lex_record in self.lex_records:
                if lex_record[0][VALUE] == 'UNB':
                    count_fields = 0
                    for field in lex_record:
                        if not field[SFIELD]:  #if field (not subfield etc)
                            count_fields += 1
                            if count_fields == 3:
                                self.ta_info['frompartner'] = field[VALUE]
                            elif count_fields == 4:
                                self.ta_info['topartner'] = field[VALUE]
                            elif count_fields == 6:
                                self.ta_info['reference'] = field[VALUE]
                                return
                    return

    def set_syntax_used(self):
        self.syntax['record_sep']  = self.ta_info['record_sep']
        self.syntax['field_sep']   = self.ta_info['field_sep']
        self.syntax['sfield_sep']   = self.ta_info['sfield_sep']
        self.syntax['reserve']   = self.ta_info['reserve']
        self.syntax['escape']   = self.ta_info['escape']


class x12(var):
    ''' class for x12 inmessage objects.'''
    @staticmethod
    def _manipulatemessagetype(messagetype,inode):
        ''' x12 also needs field from GS record to identify correct messagetype '''
        return messagetype +  inode.record.get('GS08','')

    def _sniff(self):
        ''' examine a file for syntax parameters and correctness of protocol
            eg parse ISA, get charset and version
        '''
        count = 0
        version = ''
        recordID = ''
        rawinput = self.rawinput[:200].lstrip()
        for char in rawinput:
            if char in '\r\n' and count != 105: #pos 105: is record_sep, could be \r\n
                continue
            count += 1
            if count <= 3:
                recordID += char
            elif count == 4:
                self.ta_info['field_sep'] = char
                if recordID != 'ISA':
                    raise botslib.InMessageError('[A60]: Expect "ISA", found "%(content)s". Probably no x12?',
                                                {'content':self.rawinput[:7]})   #not with mailbag
            elif count in [7,18,21,32,35,51,54,70]:   #extra checks for fixed ISA.
                if char != self.ta_info['field_sep']:
                    raise botslib.InMessageError('[A63]: Non-valid ISA header; position %(pos)s of ISA is "%(foundchar)s", expect here element separator "%(field_sep)s".',
                                                    {'pos':str(count),'foundchar':char,'field_sep':self.ta_info['field_sep']})
            elif count == 83:
                self.ta_info['reserve'] = char
            elif count < 85:
                continue
            elif count <= 89:
                version += char
            elif count == 105:
                self.ta_info['sfield_sep'] = char
            elif count == 106:
                self.ta_info['record_sep'] = char
                break
        else:
            #count never reached 106.
            raise botslib.InMessageError('[A62]: Expect X12 file but envelope is not right.')
        #****extra checks for repetition separator. Note: reserve=repeating separator.
        #for ISA version >= 00403 'reserve' is used as repeat separator. However some senders use ISA version >= 00403 but do not use repeats. Than reserve char is eg 'U' (as in older ISA versions).
        #Correct this error: if the char is alfanumeric assume wrong usage (and do not use repeat sep.)
        if version < '00403':
            self.ta_info['reserve'] = ''
        elif self.ta_info['reserve'].isalnum() and not self.ta_info.get('strict_syntax_check',False):
            self.ta_info['reserve'] = ''    #if version >= '00403' and repetition separator is alphanum and no strict checking: assume mistake. If strict checking: error is catched in separatorcheck.

        #if <CR> is segment terminator: cannot be in the skip_char-string
        self.ta_info['skip_char'] = self.ta_info['skip_char'].replace(self.ta_info['record_sep'],'')

        #****extra checks for separators
        self.separatorcheck(self.ta_info['sfield_sep'] + self.ta_info['field_sep'] + self.ta_info['reserve'] + self.ta_info['record_sep'])


    def checkenvelope(self):
        ''' check envelopes, gather information to generate 997 '''
        for nodeisa in self.getloop({'BOTSID':'ISA'}):
            botsglobal.logmap.debug('Start parsing X12 envelopes')
            isareference = nodeisa.get({'BOTSID':'ISA','ISA13':None})
            ieareference = nodeisa.get({'BOTSID':'ISA'},{'BOTSID':'IEA','IEA02':None})
            if isareference and ieareference and isareference != ieareference:
                self.add2errorlist('[E13]: ISA-reference is "%(isareference)s"; should be equal to IEA-reference "%(ieareference)s".\n'%{'isareference':isareference,'ieareference':ieareference})
            ieacount = nodeisa.get({'BOTSID':'ISA'},{'BOTSID':'IEA','IEA01':None})
            groupcount = nodeisa.getcountoccurrences({'BOTSID':'ISA'},{'BOTSID':'GS'})
            try:
                if int(ieacount) != groupcount:
                    self.add2errorlist('[E14]: Count in IEA-IEA01 is %(ieacount)s; should be equal to number of groups %(groupcount)s.\n'%{'ieacount':ieacount,'groupcount':groupcount})
            except:
                self.add2errorlist('[E15]: Count of messages in IEA is invalid: "%(count)s".\n'%{'count':ieacount})
            for nodegs in nodeisa.getloop({'BOTSID':'ISA'},{'BOTSID':'GS'}):
                gsreference = nodegs.get({'BOTSID':'GS','GS06':None})
                gereference = nodegs.get({'BOTSID':'GS'},{'BOTSID':'GE','GE02':None})
                if gsreference and gereference and gsreference != gereference:
                    self.add2errorlist('[E16]: GS-reference is "%(gsreference)s"; should be equal to GE-reference "%(gereference)s".\n'%{'gsreference':gsreference,'gereference':gereference})
                gecount = nodegs.get({'BOTSID':'GS'},{'BOTSID':'GE','GE01':None})
                messagecount = len(nodegs.children) - 1
                try:
                    if int(gecount) != messagecount:
                        self.add2errorlist('[E17]: Count in GE-GE01 is %(gecount)s; should be equal to number of transactions: %(messagecount)s.\n'%{'gecount':gecount,'messagecount':messagecount})
                except:
                    self.add2errorlist('[E18]: Count of messages in GE is invalid: "%(count)s".\n'%{'count':gecount})
                for nodest in nodegs.getloop({'BOTSID':'GS'},{'BOTSID':'ST'}):
                    streference = nodest.get({'BOTSID':'ST','ST02':None})
                    sereference = nodest.get({'BOTSID':'ST'},{'BOTSID':'SE','SE02':None})
                    #referencefields are numerical; should I compare values??
                    if streference and sereference and streference != sereference:
                        self.add2errorlist('[E19]: ST-reference is "%(streference)s"; should be equal to SE-reference "%(sereference)s".\n'%{'streference':streference,'sereference':sereference})
                    secount = nodest.get({'BOTSID':'ST'},{'BOTSID':'SE','SE01':None})
                    segmentcount = nodest.getcount()
                    try:
                        if int(secount) != segmentcount:
                            self.add2errorlist('[E20]: Count in SE-SE01 is %(secount)s; should be equal to number of segments %(segmentcount)s.\n'%{'secount':secount,'segmentcount':segmentcount})
                    except:
                        self.add2errorlist('[E21]: Count of segments in SE is invalid: "%(count)s".\n'%{'count':secount})
            botsglobal.logmap.debug('Parsing X12 envelopes is OK')

    def try_to_retrieve_info(self):
        ''' when edi-file is not correct, (try to) get info about eg partnerID's in message
            for now: look around in lexed record
        '''
        if hasattr(self,'lex_records'):
            for lex_record in self.lex_records:
                if lex_record[0][VALUE] == 'ISA':
                    count_fields = 0
                    for field in lex_record:
                        count_fields += 1
                        if count_fields == 7:
                            self.ta_info['frompartner'] = field[VALUE]
                        elif count_fields == 9:
                            self.ta_info['topartner'] = field[VALUE]
                        elif count_fields == 15:
                            self.ta_info['reference'] = field[VALUE]
                            return
                    return

    def handleconfirm(self,ta_fromfile,routedict,error):
        ''' at end of edi file handling:
            send 997 messages (or not)
        '''
        #for fatal errors there is no decent node tree
        if self.errorfatal:
            return
        #check if there are any 'send-x12-997' confirmrules.
        confirmtype = 'send-x12-997'
        if not botslib.globalcheckconfirmrules(confirmtype):
            return          #global check...less usefull for x12 than for edifact
        editype = 'x12' #self.__class__.__name__
        AcknowledgeCode = 'A' if not error else 'R'
        for GS in self.getloop({'BOTSID':'ISA'},{'BOTSID':'GS'}):
            if GS.get({'BOTSID':'GS','GS01':None}) == 'FA': #do not generate 997 for 997
                continue
            
            sender = nodegs._queries.get('frompartner') 
            receiver = nodegs._queries.get('topartner')
            #there is: messagetype/messageversion received; messagetype/messageversion send as ack (997)
            #always send back same messageversion.
            confirm_GS = False
            for nodest in GS.getloop({'BOTSID':'GS'},{'BOTSID':'ST'}):
                if botslib.checkconfirmrules(confirmtype,idroute=self.ta_info['idroute'],idchannel=self.ta_info['fromchannel'],
                                                    frompartner=sender,topartner=receiver,messagetype=nodest.queries['messagetype']):
                    confirm_GS = True
                break
            if not confirm_GS:
                continue       #do not generate 997

            #check if there is a user mappingscript
            tscript,toeditype,tomessagetype = botslib.lookup_translation(fromeditype=editype,frommessagetype='997',frompartner=receiver,topartner=sender,alt='')
            if tscript:
                translationscript,scriptfilename = botslib.botsimport('mappings',editype,tscript)  #import the mappingscript
            else:
                from_message_version = GS.get({'BOTSID':'GS','GS08':None}) or '004010'
                tomessagetype = '997' + from_message_version    #use same version 997 as in GS08 of received message
                translationscript = None
                
            #generate 997 (one per GS-GE)
            reference = str(botslib.unique('messagecounter')).zfill(4)    #20120411: use zfill as messagescounter can be <1000, ST02 field is min 4 positions
            ta_confirmation = ta_fromfile.copyta(status=TRANSLATED)
            filename = str(ta_confirmation.idta)
            out = outmessage.outmessage_init(editype=editype,messagetype=tomessagetype,filename=filename,reference=reference,statust=OK)    #make outmessage object
            out.ta_info['frompartner'] = receiver   #reversed!
            out.ta_info['topartner'] = sender       #reversed!
            if translationscript and hasattr(translationscript,'main'):
                botslib.runscript(translationscript,scriptfilename,'main',inn=GS,out=out,routedict=routedict,ta_fromfile=ta_fromfile)
            else:
                #default mapping script for 997nodegs
                #write AK1/AK9 for GS (envelope)
                out.put({'BOTSID':'ST','ST01':'997','ST02':reference})
                out.put({'BOTSID':'ST'},{'BOTSID':'AK1','AK101':GS.get({'BOTSID':'GS','GS01':None}),'AK102':GS.get({'BOTSID':'GS','GS06':None})})
                gecount = GS.get({'BOTSID':'GS'},{'BOTSID':'GE','GE01':None})
                out.put({'BOTSID':'ST'},{'BOTSID':'AK9','AK901':AcknowledgeCode,'AK902':gecount,'AK903':gecount,'AK904':gecount})
                #write AK2 for each ST (message)
                for ST in GS.getloop({'BOTSID':'GS'},{'BOTSID':'ST'}):
                    AK2 = out.putloop({'BOTSID':'ST'},{'BOTSID':'AK2'})
                    AK2.put({'BOTSID':'AK2','AK201':ST.get({'BOTSID':'ST','ST01':None}),'AK202':ST.get({'BOTSID':'ST','ST02':None})})
                    AK2.put({'BOTSID':'AK2'},{'BOTSID':'AK5','AK501':AcknowledgeCode})
                out.put({'BOTSID':'ST'},{'BOTSID':'SE','SE01':out.getcount()+1,'SE02':reference})  #last line (counts the segments produced in out-message)
                #try to run the user mapping script function 'change' (after the default mapping); 'change' function recieves the tree as written by default mapping, function can change tree.
                if translationscript and hasattr(translationscript,'change'):
                    botslib.runscript(translationscript,scriptfilename,'change',inn=GS,out=out,routedict=routedict,ta_fromfile=ta_fromfile)
            #write tomessage (result of translation)
            # envelope_content = [{},{'BOTSID':'GS','GS08':message_version_received}]
            #~ syntax = self.syntax.copy()
            # out.syntax = self.syntax             #syntax is used by writing 997 message
            # info_from_mapping = {'envelope_content':envelope_content,'syntax':self.syntax}
            # out.ta_info['rsrv5'] = simplejson.dumps(info_from_mapping, ensure_ascii=False)   #syntax, envelope_content saved, used in enveloping
            out.writeall()   #write resulting 997
            botsglobal.logger.debug('Send x12 confirmation (997) route "%(route)s" fromchannel "%(fromchannel)s" frompartner "%(frompartner)s" topartner "%(topartner)s".',
                    {'route':self.ta_info['idroute'],'fromchannel':self.ta_info['fromchannel'],'frompartner':receiver,'topartner':sender})
            self.ta_info.update(confirmtype=confirmtype,confirmed=True,confirmasked = True,confirmidta=ta_confirmation.idta)  #this info is used in transform.py to update the ta.....ugly...
            ta_confirmation.update(**out.ta_info)    #update ta for confirmation

    def set_syntax_used(self):
        self.syntax['record_sep'] = self.ta_info['record_sep']
        self.syntax['field_sep']  = self.ta_info['field_sep']
        self.syntax['sfield_sep'] = self.ta_info['sfield_sep']
        self.syntax['reserve']    = self.ta_info['reserve']
    #~ @classmethod
    #~ def _initmessagefromnode(cls,inode,ta_info,envelope_content=None):
        #~ ''' x12 subclass: removes spaces from x12 envelope (ISA!)
        #~ '''
        #~ for key,value in envelope_content[0].items():
            #~ envelope_content[0][key] = value.strip()
        #~ for key,value in envelope_content[1].items():
            #~ envelope_content[1][key] = value.strip()
        #~ return Inmessage._initmessagefromnode(inode,ta_info,envelope_content)



class tradacoms(var):
    def checkenvelope(self):
        for nodestx in self.getloop({'BOTSID':'STX'}):
            botsglobal.logmap.debug('Start parsing tradacoms envelopes')
            endcount = nodestx.get({'BOTSID':'STX'},{'BOTSID':'END','NMST':None})
            messagecount = len(nodestx.children) - 1
            try:
                if int(endcount) != messagecount:
                    self.add2errorlist('[E22]: Count in END is %(endcount)s; should be equal to number of messages %(messagecount)s.\n'%{'endcount':endcount,'messagecount':messagecount})
            except:
                self.add2errorlist('[E23]: Count of messages in END is invalid: "%(count)s".\n'%{'count':endcount})
            firstmessage = True
            for nodemhd in nodestx.getloop({'BOTSID':'STX'},{'BOTSID':'MHD'}):
                if firstmessage:
                    nodestx.queries = {'messagetype':nodemhd.queries['messagetype']}
                    firstmessage = False
                mtrcount = nodemhd.get({'BOTSID':'MHD'},{'BOTSID':'MTR','NOSG':None})
                segmentcount = nodemhd.getcount()
                try:
                    if int(mtrcount) != segmentcount:
                        self.add2errorlist('[E24]: Count in MTR is %(mtrcount)s; should be equal to number of segments %(segmentcount)s.\n'%{'mtrcount':mtrcount,'segmentcount':segmentcount})
                except:
                    self.add2errorlist('[E25]: Count of segments in MTR is invalid: "%(count)s".\n'%{'count':mtrcount})
            botsglobal.logmap.debug('Parsing tradacoms envelopes is OK')


class xml(Inmessage):
    ''' class for ediobjects in XML. Uses ElementTree'''
    def initfromfile(self):
        botsglobal.logger.debug('Read edi file "%(filename)s".',self.ta_info)
        filename = botslib.abspathdata(self.ta_info['filename'])

        if self.ta_info['messagetype'] == 'mailbag':
            # the messagetype is not know.
            # bots reads file usersys/grammars/xml/mailbag.py, and uses 'mailbagsearch' to determine the messagetype
            # mailbagsearch is a list, containing python dicts. Dict consist of 'xpath', 'messagetype' and (optionally) 'content'.
            # 'xpath' is a xpath to use on xml-file (using elementtree xpath functionality)
            # if found, and 'content' in the dict; if 'content' is equal to value found by xpath-search, then set messagetype.
            # if found, and no 'content' in the dict; set messagetype.
            try:
                module,grammarname = botslib.botsimport('grammars','xml','mailbag')
                mailbagsearch = getattr(module, 'mailbagsearch')
            except AttributeError:
                botsglobal.logger.error('Missing mailbagsearch in mailbag definitions for xml.')
                raise
            except botslib.BotsImportError:
                botsglobal.logger.error('Missing mailbag definitions for xml, should be there.')
                raise
            parser = ET.XMLParser()
            try:
                extra_character_entity = getattr(module, 'extra_character_entity')
                for key,value in list(extra_character_entity.items()):
                    parser.entity[key] = value
            except AttributeError:
                pass    #there is no extra_character_entity in the mailbag definitions, is OK.
            etree =  ET.ElementTree()   #ElementTree: lexes, parses, makes etree; etree is quite similar to bots-node trees but conversion is needed
            etreeroot = etree.parse(filename, parser)
            for item in mailbagsearch:
                if 'xpath' not in item or 'messagetype' not in item:
                    raise botslib.InMessageError('Invalid search parameters in xml mailbag.')
                found = etree.find(item['xpath'])
                if found is not None:
                    if 'content' in item and found.text != item['content']:
                        continue
                    self.ta_info['messagetype'] = item['messagetype']
                    break
            else:
                raise botslib.InMessageError('Could not find right xml messagetype for mailbag.')

            self.messagegrammarread(typeofgrammarfile='grammars')
        else:
            self.messagegrammarread(typeofgrammarfile='grammars')
            parser = ET.XMLParser()
            for key,value in list(self.ta_info['extra_character_entity'].items()):
                parser.entity[key] = value
            etree =  ET.ElementTree()   #ElementTree: lexes, parses, makes etree; etree is quite similar to bots-node trees but conversion is needed
            etreeroot = etree.parse(filename, parser)
        self._handle_empty(etreeroot)
        self.stackinit()
        self.root = self._etree2botstree(etreeroot)  #convert etree to bots-nodes-tree
        self.checkmessage(self.root,self.defmessage)
        self.ta_info.update(self.root.queries)

    def _handle_empty(self,xmlnode):
        if xmlnode.text:
            xmlnode.text = xmlnode.text.strip()
        for key,value in list(xmlnode.items()):
            xmlnode.attrib[key] = value.strip()
        for xmlchildnode in xmlnode:   #for every node in mpathtree
            self._handle_empty(xmlchildnode)

    def _etree2botstree(self,xmlnode):
        ''' recursive. '''
        newnode = node.Node(record=self._etreenode2botstreenode(xmlnode))   #make new node, use fields
        for xmlchildnode in xmlnode:   #for every node in mpathtree
            entitytype = self._entitytype(xmlchildnode)
            if not entitytype:  #is a field, or unknown that looks like a field
                if xmlchildnode.text:       #if xml element has content, add as field
                    newnode.record[xmlchildnode.tag] = xmlchildnode.text      #add as a field
                #convert the xml-attributes of this 'xml-filed' to fields in dict with attributemarker.
                newnode.record.update((xmlchildnode.tag + self.ta_info['attributemarker'] + key, value) for key,value in list(xmlchildnode.items()) if value)
            elif entitytype == 1:  #childnode is a record according to grammar
                newnode.append(self._etree2botstree(xmlchildnode))           #go recursive and add child (with children) as a node/record
                self.stack.pop()    #handled the xmlnode, so remove it from the stack
            else:   #is a record, but not in grammar
                if self.ta_info['checkunknownentities']:
                    self.add2errorlist('[S02]%(linpos)s: Unknown xml-tag "%(recordunkown)s" (within "%(record)s") in message.\n'%
                                        {'linpos':newnode.linpos(),'recordunkown':xmlchildnode.tag,'record':newnode.record['BOTSID']})
                continue
        return newnode  #return the new node

    def _etreenode2botstreenode(self,xmlnode):
        ''' build a basic dict from xml-node. Add BOTSID, xml-attributes (of 'record'), xmlnode.text as BOTSCONTENT.'''
        build = dict((xmlnode.tag + self.ta_info['attributemarker'] + key,value) for key,value in list(xmlnode.items()) if value)   #convert xml attributes to fields.
        build['BOTSID'] = xmlnode.tag
        if xmlnode.text:
            build['BOTSCONTENT'] = xmlnode.text
        return build

    def _entitytype(self,xmlchildnode):
        ''' check if xmlchildnode is field (or record)'''
        structure_level = self.stack[-1]
        if LEVEL in structure_level:
            for structure_record in structure_level[LEVEL]:   #find xmlchildnode in structure
                if xmlchildnode.tag == structure_record[ID]:
                    self.stack.append(structure_record)
                    return 1
        #tag not in structure. Check for children; Return 2 if has children
        if len(xmlchildnode):
            return 2
        return 0

    def stackinit(self):
        self.stack = [self.defmessage.structure[0],]     #stack to track where we are in stucture of grammar

class xmlnocheck(xml):
    ''' class for ediobjects in XML. Uses ElementTree'''
    def checkmessage(self,node_instance,defmessage,subtranslation=False):
        pass

    def _entitytype(self,xmlchildnode):
        if len(xmlchildnode):
            self.stack.append(0)
            return 1
        return 0

    def stackinit(self):
        self.stack = [0,]     #stack to track where we are in structure of grammar

class json(Inmessage):
    def initfromfile(self):
        self.messagegrammarread(typeofgrammarfile='grammars')
        name_root_dict_according_to_grammar = self._getrootid()
        self._readcontent_edifile()
        jsonobject = simplejson.loads(self.rawinput)
        del self.rawinput
        #several options for format...
        #examine content, determine IsNamed, IsOneMessage, convert to Node tree
        #option 2 and 5 are preferred...
        IsNamed = False
        IsOneMessage = False
        if isinstance(jsonobject,list):
            check_option = True
            for i in jsonobject:
                if not isinstance(i,dict):
                    raise botslib.InMessageError('[J56]: content of json not OK. Content is expected to be a list of objects, but is list of something else.')
                if check_option:
                    check_option = False
                    if len(i)==1 and name_root_dict_according_to_grammar in i:
                        IsNamed = True
            if IsNamed:
                # 1.List of messages, named: [{rootdict:{,,,}},{rootdict:{,,,}},]
                self.root = node.Node()  #initialise new node.
                for i in jsonobject:
                    self.root.children.append(self._dojsonobject(i[name_root_dict_according_to_grammar],name_root_dict_according_to_grammar))
            else:
                # 2. List of messages, name via grammar: [{,,,},{,,,},].
                self.root = node.Node()            #initialise new node.
                dummy,self.root.children = self._dojsonlist(jsonobject,name_root_dict_according_to_grammar)   #fill root with children
        elif isinstance(jsonobject,dict):
            if len(jsonobject)==1 and name_root_dict_according_to_grammar in jsonobject:
                #jsons with explicit named rootdict. {rootdict: <dict or list>}
                IsNamed = True
                json_content = jsonobject[name_root_dict_according_to_grammar]
                if isinstance(json_content,dict):
                    # 3. one message, named: {rootdict:{,,,}}
                    IsOneMessage = True
                    self.root = self._dojsonobject(json_content,name_root_dict_according_to_grammar)
                elif isinstance(json_content,list):
                    # 4. list of messages, named: {rootdict:[{,,,},{,,,},]}
                    self.root = node.Node()  #initialise new node.
                    dummy,self.root.children = self._dojsonlist(json_content,name_root_dict_according_to_grammar)
            else:
                # 5. one message, name via grammar: {,,,}.
                IsOneMessage = True
                self.root = self._dojsonobject(jsonobject,name_root_dict_according_to_grammar)
        else:
            raise botslib.InMessageError('[J53]: content of json not OK. Content is not a "list" or "object".')
        #check message(s) with grammar
        self.checkmessage(self.root,self.defmessage)
        if IsOneMessage:
            self.ta_info.update(self.root.queries)
        else:
            for child in self.root.children:
                self.ta_info.update(child.queries)
                break

    def _getrootid(self):
        return self.defmessage.structure[0][ID]

    def _dojsonlist(self,jsonobject,name):
        lijst = [] #initialise empty list, used to append a listof (converted) json objects
        is_repeting_data_element = False    #mostly a list will be of dicts (repeating group). But it can happen it is a list of data-elements (int, string)
        for i in jsonobject:
            if isinstance(i,dict):  #check list item is dict/object
                newnode = self._dojsonobject(i,name)
                if newnode:
                    lijst.append(newnode)
            elif isinstance(i,(str,int,float)):
                is_repeting_data_element = True
                lijst.append(i)
            #note: list within list is non-sense. A name is required, so list are always in dict (or root is a list)
            elif self.ta_info['checkunknownentities']:
                raise botslib.InMessageError('[J54]: List content must be a object, string, int, long or float - but it is not.')
        return is_repeting_data_element,lijst

    def _dojsonobject(self,jsonobject,name):
        thisnode = node.Node(record={'BOTSID':name})  #initialise new node.
        for key,value in list(jsonobject.items()):
            if value is None:
                continue
            elif isinstance(value,str):  #json field; map to field in node.record
                # for generating grammars: empty strings should generate a field ....
                if value and not value.isspace():   #use only if string has actually value.
                    thisnode.record[key] = value
            elif isinstance(value,dict):
                newnode = self._dojsonobject(value,key)     #recursion
                if newnode:
                    thisnode.append(newnode)
            elif isinstance(value,list):
                is_repeting_data_element,lijst = self._dojsonlist(value,key)
                if is_repeting_data_element:
                    thisnode.record[key] = lijst
                else:
                    thisnode.children.extend(lijst)
            elif isinstance(value,(int,float)):  #json field; map to field in node.record
                thisnode.record[key] = str(value)
            else:
                if self.ta_info['checkunknownentities']:
                    raise botslib.InMessageError('[J55]: Key "%(key)s" value "%(value)s": is not string, list or dict.',
                                                    {'key':key,'value':value})
                thisnode.record[key] = str(value)
        if len(thisnode.record)==2 and not thisnode.children:
            return None #node is empty...
        return thisnode


class jsonnocheck(json):
    def checkmessage(self,node_instance,defmessage,subtranslation=False):
        pass

    def _getrootid(self):
        return self.ta_info['defaultBOTSIDroot']   #as there is no structure in grammar, use value from syntax.


class db(Inmessage):
    ''' For database connector: reading from database.
        Communication script delivers a file with a pickled object;
        File is read, object is unpickled, object is passed to the mapping script as inn.root.
    '''
    def initfromfile(self):
        botsglobal.logger.debug('Read edi file "%(filename)s".',self.ta_info)
        self.root = botslib.readdata_pickled(filename=self.ta_info['filename'])

    def nextmessage(self):
        yield self


class raw(Inmessage):
    ''' Input file is a raw bytestream.
        File is read, and passed to mapping script as inn.root
    '''
    def initfromfile(self):
        botsglobal.logger.debug('Read edi file "%(filename)s".',self.ta_info)
        self.root = botslib.readdata_bin(filename=self.ta_info['filename'])

    def nextmessage(self):
        self.syntax = {}
        self.envelope_content = [{},{},{},]
        if isinstance(self.root,dict) and 'ta_info' in self.root:
            self.ta_info.update(self.root['ta_info'])
        yield self
