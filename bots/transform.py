import sys
import os
import copy
import collections
import unicodedata
try:
    import cPickle as pickle
except ImportError:
    import pickle
import json as simplejson
#bots-modules
from . import botslib
from . import botsglobal
from . import inmessage
from . import outmessage
from . import grammar
from .botsconfig import *
''' module contains functions to be called from user scripts. '''

#*******************************************************************************************************************
#****** functions imported from other modules. reason: user scripting uses primary transform functions *************
#*******************************************************************************************************************
from .botslib import addinfo,updateinfo,changestatustinfo,checkunique,changeq,sendbotsemail,strftime
from .envelope import mergemessages
from .communication import run


@botslib.log_session
def translate(startstatus,endstatus,routedict,rootidta):
    ''' query edifiles to be translated.
        status: FILEIN--PARSED-<SPLITUP--TRANSLATED
    '''
    try:    #see if there is a userscript that can determine the translation
        userscript,scriptname = botslib.botsimport('mappings','translation')
    except botslib.BotsImportError:       #userscript is not there; other errors like syntax errors are not catched
        userscript = scriptname = None
    #select edifiles to translate
    for rawrow in botslib.query('''SELECT idta,frompartner,topartner,filename,messagetype,testindicator,editype,charset,alt,fromchannel,filesize,frommail,tomail
                                FROM ta
                                WHERE idta>%(rootidta)s
                                AND status=%(status)s
                                AND statust=%(statust)s
                                AND idroute=%(idroute)s
                                ORDER BY idta ''',
                                {'status':startstatus,'statust':OK,'idroute':routedict['idroute'],'rootidta':rootidta}):
            row = dict(rawrow)   #convert to real dictionary
            _translate_one_file(row,routedict,endstatus,userscript,scriptname)

def _translate_one_file(row,routedict,endstatus,userscript,scriptname):
    ''' -   read, lex, parse, make tree of nodes.
        -   split up files into messages (using 'nextmessage' of grammar)
        -   get mappingscript, start mappingscript.
        -   write the results of translation (no enveloping yet)
    '''
    try:
        ta_fromfile = botslib.OldTransaction(row['idta'])
        ta_parsed = ta_fromfile.copyta(status=PARSED)
        if row['filesize'] > botsglobal.ini.getint('settings','maxfilesizeincoming',5000000):
            ta_parsed.update(filesize=row['filesize'])
            raise botslib.FileTooLargeError('File size of %(filesize)s is too big; option "maxfilesizeincoming" in bots.ini is %(maxfilesizeincoming)s.',
                                            {'filesize':row['filesize'],'maxfilesizeincoming':botsglobal.ini.getint('settings','maxfilesizeincoming',5000000)})
        botsglobal.logger.debug('Start translating file "%(filename)s" editype "%(editype)s" messagetype "%(messagetype)s".',row)
        #read whole edi-file: read, parse and made into a inmessage-object. Message is represented as a tree (inmessage.root is the root of the tree).
        #edifile.ta_info is initialised (details in parse_edi_file)
        edifile = inmessage.parse_edi_file(frompartner=row['frompartner'],
                                            topartner=row['topartner'],
                                            filename=row['filename'],
                                            messagetype=row['messagetype'],
                                            testindicator=row['testindicator'],
                                            editype=row['editype'],
                                            charset=row['charset'],
                                            alt=row['alt'],
                                            fromchannel=row['fromchannel'],
                                            frommail=row['frommail'],
                                            tomail=row['tomail'],
                                            idroute=routedict['idroute'],
                                            command=routedict['command'])
        edifile.checkforerrorlist() #no exception if infile has been lexed and parsed OK else raises an error

        if int(routedict['translateind']) == 3: #parse & passthrough; file is parsed, partners are known, no mapping, does confirm.
                                                #partners should be queried from ISA level!
            raise botslib.ParsePassthroughException('')
        #edifile.ta_info contains: init values; values from grammar, values from QUERIES
        for inn_splitup in edifile.nextmessage():   #for each message in parsed edifile (one message might get translation multiple times via 'alt'
            #edifile.ta_info was copied to inn_splitup.ta_info; information from queries is in inn_splitup.ta_info; plus nr_messages & message_sequence_number (1,2,3)
            try:
                ta_splitup = ta_parsed.copyta(status=SPLITUP,**inn_splitup.ta_info)    #copy db-ta from PARSED; write ta_info to db for each message
                inn_splitup.ta_info['idta_fromfile'] = ta_fromfile.idta     #for confirmations in userscript; the idta of incoming file
                inn_splitup.ta_info['idta'] = ta_splitup.idta               #for confirmations in userscript; the idta of 'confirming message'
                number_of_loops_with_same_alt = 0
                while True:             #more than one translation can be done via 'alt'; there is an explicit break if no more translation need to be done.
                    #find/lookup the translation************************
                    tscript,toeditype,tomessagetype = botslib.lookup_translation(fromeditype=inn_splitup.ta_info['editype'],
                                                                        frommessagetype=inn_splitup.ta_info['messagetype'],
                                                                        frompartner=inn_splitup.ta_info['frompartner'],
                                                                        topartner=inn_splitup.ta_info['topartner'],
                                                                        alt=inn_splitup.ta_info['alt'])
                    if not tscript:       #no translation found in translate table; check if can find translation via user script
                        if userscript and hasattr(userscript,'gettranslation'):
                            tscript,toeditype,tomessagetype = botslib.runscript(userscript,scriptname,'gettranslation',idroute=routedict['idroute'],message=inn_splitup)
                        if not tscript:
                            raise botslib.TranslationNotFoundError('Translation not found for editype "%(editype)s", messagetype "%(messagetype)s", frompartner "%(frompartner)s", topartner "%(topartner)s", alt "%(alt)s".',
                                                                        inn_splitup.ta_info)

                    inn_splitup.ta_info['divtext'] = tscript     #store name of mapping script for reporting (used for display in GUI).
                    #initialize new out-object*************************
                    ta_translated = ta_splitup.copyta(status=endstatus,frommail='',tomail='',cc='')     #make ta for translated message (new out-ta); explicitly erase mail-addresses
                    filename_translated = unicode(ta_translated.idta)
                    #out_translated.ta_info is initialised
                    out_translated = outmessage.outmessage_init(editype=toeditype,
                                                                messagetype=tomessagetype,
                                                                filename=filename_translated,
                                                                frompartner=inn_splitup.ta_info.get('frompartner'),
                                                                topartner=inn_splitup.ta_info.get('topartner'),
                                                                testindicator=inn_splitup.ta_info.get('testindicator'),
                                                                reference=unique('messagecounter'),
                                                                statust=OK,
                                                                divtext=tscript,
                                                                alt=inn_splitup.ta_info['alt'],
                                                                )    #make outmessage object

                    #run mapping script************************
                    botsglobal.logger.debug('Mappingscript "%(tscript)s" translates messagetype "%(messagetype)s" to messagetype "%(tomessagetype)s".',
                                            {'tscript':tscript,'messagetype':inn_splitup.ta_info['messagetype'],'tomessagetype':out_translated.ta_info['messagetype']})
                    translationscript,scriptfilename = botslib.botsimport('mappings',inn_splitup.ta_info['editype'],tscript) #get the mappingscript
                    alt_from_previous_run = inn_splitup.ta_info['alt']      #needed to check for infinite loop
                    #both inn.ta_info and out.ta_info can be written in mapping script.
                    doalttranslation = botslib.runscript(translationscript,scriptfilename,'main',inn=inn_splitup,out=out_translated)
                    botsglobal.logger.debug('Mappingscript "%(tscript)s" finished.',{'tscript':tscript})

                    #reference is indexed (in ta)
                    #manipulate botskey after mapping script:
                    if 'botskey' in inn_splitup.ta_info:
                        inn_splitup.ta_info['reference'] = inn_splitup.ta_info['botskey']
                    if 'botskey' in out_translated.ta_info:
                        out_translated.ta_info['reference'] = out_translated.ta_info['botskey']

                    #now out.ta_info is updated with information from grammar (incl partner-dependent information) and written.

                    #check the value received from the mappingscript to determine what to do in this while-loop. Handling of chained trasnlations.
                    if doalttranslation is None:
                        #translation(s) are done; handle out-message
                        handle_out_message(out_translated,ta_translated)
                        break   #break out of while loop
                    elif isinstance(doalttranslation,dict):
                        #some extended cases; a dict is returned that contains 'instructions' for some type of chained translations
                        if alt_from_previous_run == doalttranslation['alt']:
                            number_of_loops_with_same_alt += 1
                        else:
                            number_of_loops_with_same_alt = 0
                        if doalttranslation['type'] == 'out_as_inn':
                            #do chained translation: use the out-object as inn-object, new out-object
                            #use case: detected error in incoming file; use out-object to generate warning email
                            copy_out_message = copy.deepcopy(out_translated)
                            handle_out_message(copy_out_message,ta_translated)
                            inn_splitup = out_translated    #out-object is now inn-object
                            inn_splitup.ta_info['alt'] = doalttranslation['alt']   #get the alt-value for the next chained translation
                            inn_splitup.ta_info.setdefault('frompartner','')
                            inn_splitup.ta_info.setdefault('topartner','')
                            inn_splitup.ta_info.pop('statust')
                        elif doalttranslation['type'] == 'no_check_on_infinite_loop':
                            #do chained translation: allow many loops wit hsame alt-value.
                            #mapping script will have to handle this correctly.
                            number_of_loops_with_same_alt = 0
                            handle_out_message(out_translated,ta_translated)
                            inn_splitup.ta_info['alt'] = doalttranslation['alt']   #get the alt-value for the next chained translation
                        else:   #there is nothing else
                            raise botslib.BotsError('Mappingscript returned dict with an unknown "type": "%(doalttranslation)s".',{'doalttranslation':doalttranslation})
                    else:  #note: this includes alt '' (empty string)
                        if alt_from_previous_run == doalttranslation:
                            number_of_loops_with_same_alt += 1
                        else:
                            number_of_loops_with_same_alt = 0
                        #do normal chained translation: same inn-object, new out-object
                        handle_out_message(out_translated,ta_translated)
                        inn_splitup.ta_info['alt'] = doalttranslation   #get the alt-value for the next chained translation
                    if number_of_loops_with_same_alt > 10:
                        raise botslib.BotsError('Mappingscript returns same alt value over and over again (infinite loop?). Alt: "%(doalttranslation)s".',{'doalttranslation':doalttranslation})
                #end of while-loop **********************************************************************************
            #exceptions file_out-level: exception in mappingscript or writing of out-file
            except (botslib.ParsePassthroughException,botslib.KillWholeFileException):   #handle on file level (not here, on message level)
                raise
            except Exception as msg:
                #two ways to handle errors in mapping script or in writing outgoing message:
                #1. continue processing other messages in file/interchange (default in bots 3.*)
                #2. one error in file/interchange->drop all results (default in bots 2.*)
                #options to force 2 (one error -> drop whole file):
                #1. in mappin: raise KillWholeFileException
                #2. in grammar-syntax of incoming file: 'KillWholeFile' = True
                if inn_splitup.ta_info.get('KillWholeFile',False):
                    raise botslib.KillWholeFileException(msg)
                txt = botslib.txtexc()
                ta_splitup.update(statust=ERROR,errortext=txt,**inn_splitup.ta_info)   #update db. inn_splitup.ta_info could be changed by mappingscript. Is this useful?
                ta_splitup.deletechildren()
            else:
                ta_splitup.update(statust=DONE, **inn_splitup.ta_info)   #update db. inn_splitup.ta_info could be changed by mappingscript. Is this useful?

    #exceptions file_in-level
    except botslib.ParsePassthroughException:   #edi-file is OK, file is passed-through after parsing.
        ta_parsed.deletechildren()
        ta_parsed.update(statust=DONE,filesize=row['filesize'],**edifile.ta_info)   #update with info from eg queries
        ta_parsed.copyta(status=MERGED,statust=OK)          #original file goes straight to MERGED
        edifile.handleconfirm(ta_fromfile,routedict,error=False)
        botsglobal.logger.debug('Parse & passthrough for input file "%(filename)s".',row)
    except botslib.FileTooLargeError as msg:
        ta_parsed.update(statust=ERROR,errortext=unicode(msg))
        ta_parsed.deletechildren()
        botsglobal.logger.debug('Error in translating input file "%(filename)s":\n%(msg)s',{'filename':row['filename'],'msg':msg})
    except:
        txt = botslib.txtexc()
        ta_parsed.update(statust=ERROR,errortext=txt,**edifile.ta_info)
        ta_parsed.deletechildren()
        edifile.handleconfirm(ta_fromfile,routedict,error=True)
        botsglobal.logger.debug('Error in translating input file "%(filename)s":\n%(msg)s',{'filename':row['filename'],'msg':txt})
    else:
        edifile.handleconfirm(ta_fromfile,routedict,error=False)
        ta_parsed.update(statust=DONE,filesize=row['filesize'],**edifile.ta_info)
        botsglobal.logger.debug('Translated input file "%(filename)s".',row)
    finally:
        ta_fromfile.update(statust=DONE)


def handle_out_message(out_translated,ta_translated):
    if out_translated.ta_info['statust'] == DONE:    #if indicated in mappingscript the message should be discarded
        botsglobal.logger.debug('No output file because mappingscript explicitly indicated this.')
        out_translated.ta_info['filename'] = ''
        out_translated.ta_info['status'] = DISCARD
    else:
        copy_ta_info = out_translated.ta_info.copy()
        botsglobal.logger.debug('Start writing output file editype "%(editype)s" messagetype "%(messagetype)s".',out_translated.ta_info)
        #values set here bij mapping (out.ta_info) are overwritten by values in grammars. 
        #bots3.3: option to set value in mapping
        #Fixing now: make copy, overwrite over grammar values are read/updated
        out_translated.writeall()   #write result of translation.
        out_translated.ta_info.update(copy_ta_info)
        out_translated.ta_info['filesize'] = os.path.getsize(botslib.abspathdata(out_translated.ta_info['filename']))  #get filesize
        info_from_mapping = {'envelope_content':out_translated.envelope_content,'syntax':out_translated.syntax}
        out_translated.ta_info['rsrv5'] = simplejson.dumps(info_from_mapping, ensure_ascii=False)
    ta_translated.update(**out_translated.ta_info)  #update outmessage transaction with ta_info; statust = OK

#*********************************************************************
#*** utily functions for persist: store things in the bots database.
#*** this is intended as a memory stretching across messages.
#*********************************************************************
#<python thing> ->pickle-> byte stream.
#db connection: expect unicode (as storage field is text)
#so pickle output is turned into unicode first, using 'neutral' iso-8859-1
#when unpickling, have to encode again of course.
#this is upward compatible; if stored as in bots <= 3.1 is OK.
#another option would be to use JSON. Only disadvantage is that it is 'data' only (not eg date-time objects)
def persist_add(domein,botskey,value):
    ''' store persistent values in db.
    '''
    content = pickle.dumps(value,0).decode('iso-8859-1')
    try:
        botslib.changeq(''' INSERT INTO persist (domein,botskey,content)
                            VALUES   (%(domein)s,%(botskey)s,%(content)s)''',
                            {'domein':domein,'botskey':botskey,'content':content})
    except:
        raise botslib.PersistError('Failed to add for domein "%(domein)s", botskey "%(botskey)s", value "%(value)s".',
                                    {'domein':domein,'botskey':botskey,'value':value})

def persist_update(domein,botskey,value):
    ''' store persistent values in db.
    '''
    content = pickle.dumps(value,0).decode('iso-8859-1')
    botslib.changeq(''' UPDATE persist
                        SET content=%(content)s,ts=%(ts)s
                        WHERE domein=%(domein)s
                        AND botskey=%(botskey)s''',
                        {'domein':domein,'botskey':botskey,'content':content,'ts':strftime('%Y-%m-%d %H:%M:%S')})

def persist_add_update(domein,botskey,value):
    # add the record, or update it if already there.
    try:
        persist_add(domein,botskey,value)
    except:
        persist_update(domein,botskey,value)

def persist_delete(domein,botskey):
    ''' store persistent values in db.
    '''
    botslib.changeq(''' DELETE FROM persist
                        WHERE domein=%(domein)s
                        AND botskey=%(botskey)s''',
                        {'domein':domein,'botskey':botskey})

def persist_lookup(domein,botskey):
    ''' lookup persistent values in db.
    '''
    for row in botslib.query('''SELECT content
                                FROM persist
                                WHERE domein=%(domein)s
                                AND botskey=%(botskey)s''',
                                {'domein':domein,'botskey':botskey}):
        return pickle.loads(row['content'].encode('iso-8859-1'))
    return None

#*********************************************************************
#*** utily functions for codeconversion via database table ccode
#*********************************************************************
def ccode(ccodeid,leftcode,field='rightcode',safe=False):
    ''' converts code using a db-table ccode.
    '''
    for row in botslib.query('''SELECT ''' +field+ '''
                                FROM ccode
                                WHERE ccodeid_id = %(ccodeid)s
                                AND leftcode = %(leftcode)s''',
                                {'ccodeid':ccodeid,'leftcode':leftcode}):
        return row[field]
    if safe is None:
        return None
    elif safe:
        return leftcode
    else:
        raise botslib.CodeConversionError('Value "%(value)s" not in code-conversion, user table "%(table)s".',
                                            {'value':leftcode,'table':ccodeid})

def safe_ccode(ccodeid,leftcode,field='rightcode'):   #depreciated, use ccode with safe=True
    return ccode(ccodeid,leftcode,field,safe=True)

def reverse_ccode(ccodeid,rightcode,field='leftcode',safe=False):
    ''' as ccode but reversed lookup.'''
    for row in botslib.query('''SELECT ''' +field+ '''
                                FROM ccode
                                WHERE ccodeid_id = %(ccodeid)s
                                AND rightcode = %(rightcode)s''',
                                {'ccodeid':ccodeid,'rightcode':rightcode}):
        return row[field]
    if safe is None:
        return None
    elif safe:
        return rightcode
    else:
        raise botslib.CodeConversionError('Value "%(value)s" not in code-conversion, user table "%(table)s".',
                                            {'value':rightcode,'table':ccodeid})

def safe_reverse_ccode(ccodeid,rightcode,field='leftcode'):   #depreciated, use reverse_ccode with safe=True
    ''' as safe_ccode but reversed lookup.'''
    return reverse_ccode(ccodeid,rightcode,field,safe=True)

#depreciated, kept for upward compatibility
codetconversion = ccode
safecodetconversion = safe_ccode
rcodetconversion = reverse_ccode
safercodetconversion = safe_reverse_ccode

def getcodeset(ccodeid,leftcode,field='rightcode'):
    ''' Returns a list of all 'field' values in ccode with right ccodeid and leftcode.
    '''
    terug = []
    for row in botslib.query('''SELECT ''' +field+ '''
                                FROM ccode
                                WHERE ccodeid_id = %(ccodeid)s
                                AND leftcode = %(leftcode)s
                                ORDER BY id''',
                                {'ccodeid':ccodeid,'leftcode':leftcode}):
        terug.append(row[field])
    return  terug

#*********************************************************************
#*** utily functions for calculating/generating/checking EAN/GTIN/GLN
#*********************************************************************
def calceancheckdigit(ean):
    ''' input: EAN without checkdigit; returns the checkdigit'''
    try:
        if not ean.isdigit():
            raise botslib.EanError('GTIN "%(ean)s" should be string with only numericals.',{'ean':ean})
    except AttributeError:
        raise botslib.EanError('GTIN "%(ean)s" should be string, but is a "%(type)s".',{'ean':ean,'type':type(ean)})
    sum1 = sum(int(x)*3 for x in ean[-1::-2]) + sum(int(x) for x in ean[-2::-2])
    return unicode((1000-sum1)%10)

def calceancheckdigit2(ean):
    ''' just for fun: slightly different algoritm for calculating the ean checkdigit. same results; is 10% faster.
    '''
    sum1 = 0
    factor = 3
    for i in ean[-1::-1]:
        sum1 += int(i) * factor
        factor = 4 - factor         #factor flip-flops between 3 and 1...
    return unicode(((1000 - sum1) % 10))

def checkean(ean):
    ''' input: EAN; returns: True (valid EAN) of False (EAN not valid)'''
    return (ean[-1] == calceancheckdigit(ean[:-1]))

def addeancheckdigit(ean):
    ''' input: EAN without checkdigit; returns EAN with checkdigit'''
    return ean+calceancheckdigit(ean)

#*********************************************************************
#*** div utily functions for mappings
#*********************************************************************
def unique(domein,updatewith=None):
    ''' generate unique number per domain.
        uses db to keep track of last generated number.
    '''
    return unicode(botslib.unique(domein,updatewith))

def unique_runcounter(domein,updatewith=None):
    ''' as unique, but per run of bots-engine.
    '''
    return unicode(botslib.unique_runcounter(domein,updatewith))

def inn2out(inn,out):
    ''' copies inn-message to outmessage
        option 1: out.root = inn.root
                   works, super fast, no extra memory used....but not always safe (changing/deleting in inn or out changes the other
                   for most cases this works as a superfast method (if performance is a thing)
        option 2: out.root = copy.deepcopy(inn.root)
                   works, but quite slow and uses a lot of memory
        option3: use roll your own method to 'deepcopy' node tree.
                   much faster, way less memory, and safe.
    '''
    out.root = inn.root.copynode()

def useoneof(*args):
    for arg in args:
        if arg:
            return arg
    else:
        return None

def dateformat(date):
    ''' for edifact: return right format code for the date. '''
    if not date:
        return None
    if len(date) == 8:
        return '102'
    if len(date) == 12:
        return '203'
    if len(date) == 16:
        return '718'
    raise botslib.BotsError('No valid edifact date format for "%(date)s".',{'date':date})

def datemask(value,frommask,tomask):
    ''' value is formatted according as in frommask;
        returned is the value formatted according to tomask.
        example: datemask('12/31/2012','MM/DD/YYYY','YYYYMMDD') returns '20121231'
    '''
    if not value:
        return value
    convdict = collections.defaultdict(list)
    for key,val in zip(frommask,value):
        convdict[key].append(val)
    #convdict contains for example:  {'Y': ['2', '0', '1', '2'], 'M': ['1', '2'], 'D': ['3', '1'], '/': ['/', '/']}
    terug = ''
    try:
        # alternative implementation: return ''.join(convdict.get(c,[c]).pop(0) for c in tomask)     #very short, but not faster....
        for char in tomask:
            terug += convdict.get(char,[char]).pop(0)     #for this character, lookup value in convdict (a list). pop(0) this list: get first member of list, and drop it. If char not in convdict as key, use char itself.
    except:
        raise botslib.BotsError('Error in function datamask("%(value)s", "%(frommask)s", "%(tomask)s").',
                                    {'value':value,'frommask':frommask,'tomask':tomask})
    return terug

def truncate(maxpos,value):
    if value:
        return value[:maxpos]
    else:
        return value

def concat(*args,**kwargs):
    sep = kwargs.get('sep', '')     #default is '': no separator.
    terug = sep.join(arg for arg in args if arg)
    return terug if terug else None

#***lookup via database partner
def partnerlookup(value,field,field_where_value_is_searched='idpartner',safe=False):
    ''' lookup via table partner.
        lookup value is returned, exception if not there.
        when using 'field_where_value_is_searched' with other values as ='idpartner',
        partner tabel is only indexed on idpartner (so uniqueness is not guaranteerd).
        should work OK if not too many partners.
        parameter safe can be:
        - True: if not found, return value
        - False: if not found throw exception
        - None: if not found, return None
    '''
    for row in botslib.query('''SELECT ''' +field+ '''
                                FROM partner
                                WHERE '''+field_where_value_is_searched+ ''' = %(value)s
                                ''',{'value':value}):
        if row[field]:
            return row[field]
    #nothing found in partner table
    if safe is None:
        return None
    elif safe:  #if safe is True
        return value
    else:
        raise botslib.CodeConversionError('No result found for partner lookup; either partner "%(idpartner)s" does not exist or field "%(field)s" has no value.',
                                            {'idpartner':value,'field':field})

def dropdiacritics(content,charset='ascii'):
    ''' input: unicode; output: unicode
        1. try for each char if char 'fits' into <charset>
        2. if not: normalize converts to  char + seperate diacritic (or some other sequence...but that is not too interesting).
        2. encode first char of normalized sequence with ignore: non-ascii chars - including the separate diacritics - are dropped
        3. decode again to return as unicode
        Result is:
        - one char in -> zero or one char out (that is what the [0] does); checked with all unicode
        - only unicode is produced that 'fits' in indicated charset.
        - for characters with diacritics the diacritics are dropped.
        - side-effects: (1) some characters are just dropped; (2) effects like: trademark sign->T. Last one does not happen if 'NFKD' -> 'NFD'
    '''
    lijst = []
    for char in content:
        try:
            lijst.append(char.encode(charset))     #encode to latin1 bytes
        except:                                     #encoding fails (non-latin1 chars)
            lijst.append(unicodedata.normalize('NFKD', char)[0].encode(charset,'ignore'))    #try to convert by dropping diacritic
    return ''.join(lijst).decode(charset)

def chunk(sequence, size):
    ''' return generator for chunks
        input: string, list, tuple.
        uses cases:
        print list(chunk([1,2,3,4,5,6,7,8,9,10],3))     #[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
        print list(chunk('a nice example string',5))    #['a nic', 'e exa', 'mple ', 'strin', 'g']
        print list(chunk(list(chunk('a nice example string',5)),2)) [['a nic', 'e exa'], ['mple ', 'strin'], ['g']]
        print list(chunk(list(chunk('',5)),2))          #[]
        print list(chunk(list(chunk(None,5)),2))        #[]
    '''
    if sequence:
        for pos in range(0, len(sequence), size):
            yield sequence[pos:pos + size]
