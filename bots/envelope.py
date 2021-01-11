import sys
import os
import shutil
import json as simplejson
#bots-modules
from . import botslib
from . import botsglobal
from . import outmessage
from .botsconfig import *


def mergemessages(startstatus,endstatus,idroute,rootidta=None):
    ''' Merges and/or envelopes one or more messages to one file (status TRANSLATED---->MERGED).
        Attribute 'merge' indicates message should be merged with similar messages (or not).
        If merge is False: 1 message per envelope - no merging
        'merge' comes from db-ta; added in translation via from syntax of outgoing message (envelope, message, partner).
        Merge/not merge is implemented as separate loops: one for merge&envelope, another for enveloping only
    '''
    if rootidta is None:
        rootidta = botsglobal.currentrun.get_minta4query()
    #**********for messages only to envelope (no merging)
    #editype,messagetype: needed to get right envelope
    #envelope: envelope to use
    #frompartner,topartner,testindicator,charset,nrmessages: needed for envelope (edifact, x12)
    #idta: ID of the db-ta
    #filename: file to envelope
    for row in botslib.query('''SELECT editype,messagetype,envelope,frompartner,topartner,testindicator,charset,nrmessages,idta,filename,rsrv3,rsrv5
                                FROM ta
                                WHERE idta>%(rootidta)s
                                AND status=%(status)s
                                AND statust=%(statust)s
                                AND merge=%(merge)s
                                AND idroute=%(idroute)s
                                ORDER BY idta
                                ''',
                                {'rootidta':rootidta,'status':startstatus,'statust':OK,'merge':False,'idroute':idroute}):
        try:
            ta_info = dict(row)
            ta_fromfile = botslib.OldTransaction(ta_info['idta'])
            ta_tofile = ta_fromfile.copyta(status=endstatus)  #copy db_ta
            ta_info['filename'] = unicode(ta_tofile.idta)     #create filename for enveloped message
            ta_info['idroute'] = idroute
            botsglobal.logger.debug('Envelope 1 message editype: %(editype)s, messagetype: %(messagetype)s.',ta_info)
            envelope(ta_info,[row['filename']])
            ta_info['filesize'] = os.path.getsize(botslib.abspathdata(ta_info['filename']))
        except:
            txt = botslib.txtexc()
            ta_tofile.update(statust=ERROR,errortext=txt)
        else:
            ta_tofile.update(statust=OK,**ta_info)  #selection is used to update enveloped message;
        finally:
            ta_fromfile.update(statust=DONE)

    #**********for messages to merge & envelope
    #editype,messagetype: needed to get right envelope
    #envelope: envelope to use
    #rsrv3 : user defined enveloping criterium
    #frompartner,topartner,testindicator,charset,nrmessages: needed for envelope (edifact, x12)
    for row in botslib.query('''SELECT editype,messagetype,envelope,rsrv3,frompartner,topartner,testindicator,charset,rsrv5,sum(nrmessages) as nrmessages
                                FROM ta
                                WHERE idta>%(rootidta)s
                                AND status=%(status)s
                                AND statust=%(statust)s
                                AND merge=%(merge)s
                                AND idroute=%(idroute)s
                                GROUP BY editype,messagetype,envelope,rsrv3,frompartner,topartner,testindicator,charset
                                ORDER BY editype,messagetype,envelope,rsrv3,frompartner,topartner,testindicator,charset
                                ''',
                                {'rootidta':rootidta,'status':startstatus,'statust':OK,'merge':True,'idroute':idroute}):
        try:
            ta_info = dict(row)
            ta_info['idroute'] = idroute
            #do another query to gather idta's to be merged.
            filename_list = []  #list of filenames...these will be used in actual merging.
            for row2 in botslib.query('''SELECT idta,filename
                                        FROM ta
                                        WHERE idta>%(rootidta)s
                                        AND status=%(status)s
                                        AND statust=%(statust)s
                                        AND merge=%(merge)s
                                        AND editype=%(editype)s
                                        AND messagetype=%(messagetype)s
                                        AND (frompartner=%(frompartner)s OR frompartner IS NULL)
                                        AND (topartner=%(topartner)s OR topartner IS NULL)
                                        AND testindicator=%(testindicator)s
                                        AND envelope=%(envelope)s
                                        AND rsrv3=%(rsrv3)s
                                        AND charset=%(charset)s
                                        ORDER BY idta
                                        ''',
                                        {'rootidta':rootidta,'status':startstatus,'statust':OK,'merge':True,
                                        'editype':ta_info['editype'],'messagetype':ta_info['messagetype'],'frompartner':ta_info['frompartner'],
                                        'topartner':ta_info['topartner'],'testindicator':ta_info['testindicator'],'charset':ta_info['charset'],
                                        'rsrv3':ta_info['rsrv3'],'envelope':ta_info['envelope']}):
                ta_fromfile = botslib.OldTransaction(row2['idta'])             #edi message to be merged/envelope
                if not filename_list:                                          #if first time in loop: copy ta to new/merged/target ta
                    ta2_tofile = ta_fromfile.copyta(status=endstatus,parent=0) #copy db_ta; parent=0 as enveloping works via child, not parent
                    ta_info['filename'] = unicode(ta2_tofile.idta)
                ta_fromfile.update(child=ta2_tofile.idta,statust=DONE)         #add child-relation to the org ta
                filename_list.append(row2['filename'])
            botsglobal.logger.debug('Merge and envelope: editype: %(editype)s, messagetype: %(messagetype)s, %(nrmessages)s messages',ta_info)
            envelope(ta_info,filename_list)
            ta_info['filesize'] = os.path.getsize(botslib.abspathdata(ta_info['filename']))
        except:
            txt = botslib.txtexc()
            ta2_tofile.update(statust=ERROR,errortext=txt)
        else:
            ta2_tofile.update(statust=OK,**ta_info)


def envelope(ta_info,ta_list):
    ''' dispatch function for class Envelope and subclasses.
        editype, edimessage and envelope essential for enveloping.

        How is enveloping determined:
        1.  no enveloping: ta_info['envelope'] is '' (or None)
            -   file(s) is/are just copied.
            -   no user scripting for envelope.
        2.  user scripted: there is a file in bots/envelopescripts/ta_info['editype']/ta_info['envelope'].py (and has to have a class ta_info['envelope'])
            -   user exits extends/replaces default enveloping.
                syntax: grammar.editype.envelope (alt could be envelopescripts.editype.envelope; but this is inline with incoming)
                        grammar.editype.messagetype
        3.  default envelope: if ta_info['editype'] is a class in this module, use it.
                script: envelope.editype
                syntax: grammar.editype.envelope
                        grammar.editype.messagetype
    '''
    userscript = scriptname = None
    if not ta_info['envelope']:     #1. no enveloping
        classtocall = noenvelope
    else:
        try:    #check for user scripted enveloping
            userscript,scriptname = botslib.botsimport('envelopescripts',ta_info['editype'], ta_info['envelope'])
            classtocall = getattr(userscript,ta_info['envelope'],None)  #2. user scripted. If userscript does not have class ta_info['envelope']
                                                                        #no error is given - file can have other functions in it.
        except botslib.BotsImportError:
            classtocall = None      #3. default envelope
        if classtocall is None:
            try:
                classtocall = globals()[ta_info['editype']]
            except KeyError:
                raise botslib.OutMessageError('Not found envelope "%(envelope)s" for editype "%(editype)s".',ta_info)
    info_from_mapping = simplejson.loads(ta_info.get('rsrv5'))
    envelope_content = info_from_mapping['envelope_content']
    syntax = info_from_mapping['syntax']
    env = classtocall(ta_info,ta_list,userscript,scriptname,envelope_content,syntax)
    env.run()

class Envelope(object):
    ''' Base Class for enveloping; use subclasses.
    '''
    def __init__(self,ta_info,ta_list,userscript,scriptname,envelope_content,syntax):
        self.ta_info = ta_info
        self.ta_list = ta_list
        self.userscript = userscript
        self.scriptname = scriptname
        self.envelope_content = envelope_content      #dict with envelope data from mapping script
        self.syntax = syntax          #dict with syntax data from mapping script

    def _openoutenvelope(self):
        ''' make an outmessage object; read the grammar.
        '''
        #self.ta_info contains information from ta: editype, messagetype,frompartner, topartner, testindicator,charset,envelope
        self.out = outmessage.outmessage_init(**self.ta_info)    #make outmessage object.
        #read grammar for envelopesyntax. Remark: self.ta_info is not updated.
        self.out.syntax = self.syntax     #outmessage object uses this 
        self.out.messagegrammarread(typeofgrammarfile='envelope')

    def writefilelist(self,tofile):
        for filename in self.ta_list:
            fromfile = botslib.opendata(filename, 'rb',self.ta_info['charset'])
            shutil.copyfileobj(fromfile,tofile,1048576)
            fromfile.close()

    def filelist2absolutepaths(self):
        ''' utility function; some classes need absolute filenames eg for xml-including'''
        return [botslib.abspathdata(filename) for filename in self.ta_list]

    def check_envelope_partners(self):
        ''' as partners are required: check if there.
        '''
        if not self.ta_info['frompartner']:
            raise botslib.OutMessageError('In enveloping "frompartner" unknown.')
        if not self.ta_info['topartner']:
            raise botslib.OutMessageError('In enveloping "topartner" unknown.')


class noenvelope(Envelope):
    ''' Only copies the input files to one output file.'''
    def run(self):
        botslib.tryrunscript(self.userscript,self.scriptname,'ta_infocontent',ta_info=self.ta_info)
        if len(self.ta_list) > 1:
            tofile = botslib.opendata(self.ta_info['filename'],'wb',self.ta_info['charset'])
            self.writefilelist(tofile)
            tofile.close()
        else:
            self.ta_info['filename'] = self.ta_list[0]

class fixed(noenvelope):
    pass

class csv(noenvelope):
    def run(self):
        if self.ta_info['envelope'] == 'csvheader':
            #~ Adds first line to csv files with fieldnames; than write files.
            self._openoutenvelope()
            botslib.tryrunscript(self.userscript,self.scriptname,'ta_infocontent',ta_info=self.ta_info)

            tofile = botslib.opendata(self.ta_info['filename'],'wb',self.ta_info['charset'])
            headers = dict((field[ID],field[ID]) for field in self.out.defmessage.structure[0][FIELDS])
            self.out.put(headers)
            self.out.tree2records(self.out.root)
            tofile.write(self.out.record2string(self.out.lex_records[0:1]))
            self.writefilelist(tofile)
            tofile.close()
        else:
            super(csv,self).run()

class edifact(Envelope):
    '''
        partners: 
        1. partner from database -> find syntax
        2. order of priority in suing partner data from differnt sources:
            1. envelope_content (from mapping)
            2. syntax
            3. via database (ta_info)
    '''
    def run(self):
        self.check_envelope_partners() 
        #read grammars, including partner syntax. Partners from database (in ta_info) are used to find partner syntax
        self._openoutenvelope()
        self.ta_info.update(self.out.ta_info)
        #user exit to change ta_info
        botslib.tryrunscript(self.userscript,self.scriptname,'ta_infocontent',ta_info=self.ta_info)
        #frompartner
        UNBsender = self.envelope_content[0].get('S002.0004') or self.ta_info.get('S002.0004') or self.ta_info['frompartner']
        UNBsender_qualifier = self.envelope_content[0].get('S002.0007') or self.ta_info['UNB.S002.0007']
        #topartner
        UNBreceiver = self.envelope_content[0].get('S003.0010') or self.ta_info.get('S003.0010') or self.ta_info['topartner']
        UNBreceiver_qualifier = self.envelope_content[0].get('S003.0007') or self.ta_info['UNB.S003.0007']
        #version dependent enveloping
        self.ta_info['version'] = self.envelope_content[0].get('S001.0002') or self.ta_info['version']
        if self.ta_info['version'] < '4':
            senddate = botslib.strftime('%y%m%d')
            reserve = ' '
        else:
            senddate = botslib.strftime('%Y%m%d')
            reserve = self.ta_info['reserve']

        #UNB reference: set from mapping or (counter per sender or receiver)
        self.ta_info['reference'] = self.envelope_content[0].get('0020') or unicode(botslib.unique('unbcounter_' + UNBsender if not botsglobal.ini.getboolean('settings','interchangecontrolperpartner',False) else UNBreceiver ))
        #testindicator:
        if self.envelope_content[0].get('0035') and self.envelope_content[0].get('0035') != '0':  #1. set from mapping
            testindicator = '1'
        elif self.ta_info['testindicator'] and self.ta_info['testindicator'] != '0':        #2. set from ta/database
            testindicator = '1'
        elif self.ta_info['UNB.0035'] != '0':                                               #3. set from syntax
            testindicator = '1'
        else:                                                                               #4. default: no test
            testindicator = ''
        #build the envelope tree/tree
        self.out.put({'BOTSID':'UNB',
                        'S001.0001':self.envelope_content[0].get('S001.0001') or self.ta_info['charset'],
                        'S001.0002':self.envelope_content[0].get('S001.0002') or self.ta_info['version'],
                        'S001.0080':self.envelope_content[0].get('S001.0080') or self.ta_info['UNB.S001.0080'],
                        'S001.0133':self.envelope_content[0].get('S001.0133') or self.ta_info['UNB.S001.0133'],
                        'S002.0004':UNBsender,
                        'S002.0007':UNBsender_qualifier,
                        'S002.0008':self.envelope_content[0].get('S002.0008') or self.ta_info['UNB.S002.0008'],
                        'S002.0042':self.envelope_content[0].get('S002.0042') or self.ta_info['UNB.S002.0042'],
                        'S003.0010':UNBreceiver,
                        'S003.0007':UNBreceiver_qualifier,
                        'S003.0014':self.envelope_content[0].get('S003.0014') or self.ta_info['UNB.S003.0014'],
                        'S003.0046':self.envelope_content[0].get('S003.0046') or self.ta_info['UNB.S003.0046'],
                        'S004.0017':self.envelope_content[0].get('S004.0017') or senddate,
                        'S004.0019':self.envelope_content[0].get('S004.0019') or botslib.strftime('%H%M'),
                        '0020':     self.ta_info['reference'],
                        'S005.0022':self.envelope_content[0].get('S005.0022') or self.ta_info['UNB.S005.0022'],
                        'S005.0025':self.envelope_content[0].get('S005.0025') or self.ta_info['UNB.S005.0025'],
                        '0026':     self.envelope_content[0].get('0026') or self.ta_info['UNB.0026'],
                        '0029':     self.envelope_content[0].get('0029') or self.ta_info['UNB.0029'],
                        '0031':     self.envelope_content[0].get('0031') or self.ta_info['UNB.0031'],
                        '0032':     self.envelope_content[0].get('0032') or self.ta_info['UNB.0032'],
                        '0035':     testindicator,
                        })
        self.out.put({'BOTSID':'UNB'},{'BOTSID':'UNZ','0036':self.ta_info['nrmessages'],'0020':self.ta_info['reference']})
        #user exit to change data in tree/segments
        botslib.tryrunscript(self.userscript,self.scriptname,'envelopecontent',ta_info=self.ta_info,out=self.out)
        #convert tree to segments
        self.out.checkmessage(self.out.root,self.out.defmessage)
        self.out.checkforerrorlist()
        self.out.tree2records(self.out.root)
        #write to file:
        tofile = botslib.opendata(self.ta_info['filename'],'wb',self.ta_info['charset'])
        if self.ta_info['forceUNA'] or self.ta_info['charset'] != 'UNOA':   #write UNA, hardcoded.
            tofile.write('UNA'+self.ta_info['sfield_sep']+self.ta_info['field_sep']+self.ta_info['decimaal']+self.ta_info['escape']+ reserve +self.ta_info['record_sep']+self.ta_info['add_crlfafterrecord_sep'])
        tofile.write(self.out.record2string(self.out.lex_records[0:1]))     #write UNB
        self.writefilelist(tofile)                                          #write edifact messages
        tofile.write(self.out.record2string(self.out.lex_records[1:2]))     #write UNZ
        tofile.close()


class tradacoms(Envelope):
    ''' Generate STX and END segment; fill with appropriate data, write to interchange file.'''
    def run(self):
        #determine partnrIDs. either from mapping (via self.envelope_content) or database (via self.ta_info). Check: partnerIDs are required
        self.check_envelope_partners()
        self._openoutenvelope()
        self.ta_info.update(self.out.ta_info)
        botslib.tryrunscript(self.userscript,self.scriptname,'ta_infocontent',ta_info=self.ta_info)
        #prepare data for envelope
        if botsglobal.ini.getboolean('settings','interchangecontrolperpartner',False):
            self.ta_info['reference'] = unicode(botslib.unique('stxcounter_' + self.ta_info['topartner']))
        else:
            self.ta_info['reference'] = unicode(botslib.unique('stxcounter_' + self.ta_info['frompartner']))
        #build the envelope segments (that is, the tree from which the segments will be generated)
        self.out.put({'BOTSID':'STX',
                        'STDS1':self.ta_info['STX.STDS1'],
                        'STDS2':self.ta_info['STX.STDS2'],
                        'FROM.01':self.ta_info['frompartner'],
                        'UNTO.01':self.ta_info['topartner'],
                        'TRDT.01':botslib.strftime('%y%m%d'),
                        'TRDT.02':botslib.strftime('%H%M%S'),
                        'SNRF':self.ta_info['reference']})
        if self.ta_info['STX.FROM.02']:
            self.out.put({'BOTSID':'STX','FROM.02':self.ta_info['STX.FROM.02']})
        if self.ta_info['STX.UNTO.02']:
            self.out.put({'BOTSID':'STX','UNTO.02':self.ta_info['STX.UNTO.02']})
        if self.ta_info['STX.APRF']:
            self.out.put({'BOTSID':'STX','APRF':self.ta_info['STX.APRF']})
        if self.ta_info['STX.PRCD']:
            self.out.put({'BOTSID':'STX','PRCD':self.ta_info['STX.PRCD']})
        self.out.put({'BOTSID':'STX'},{'BOTSID':'END','NMST':self.ta_info['nrmessages']})  #dummy segment; is not used
        #user exit
        botslib.tryrunscript(self.userscript,self.scriptname,'envelopecontent',ta_info=self.ta_info,out=self.out)
        #convert the tree into segments; here only the STX is written (first segment)
        self.out.checkmessage(self.out.root,self.out.defmessage)
        self.out.checkforerrorlist()
        self.out.tree2records(self.out.root)

        #start doing the actual writing:
        tofile = botslib.opendata(self.ta_info['filename'],'wb',self.ta_info['charset'])
        tofile.write(self.out.record2string(self.out.lex_records[0:1]))
        self.writefilelist(tofile)
        tofile.write(self.out.record2string(self.out.lex_records[1:2]))
        tofile.close()


class templatehtml(Envelope):
    ''' class for outputting edi as html (browser, email).
        Uses a genshi-template for the enveloping/merging.
    '''
    def run(self):
        try:
            from genshi.template import TemplateLoader
        except:
            raise ImportError('Dependency failure: editype "templatehtml" requires python library "genshi".')
        self._openoutenvelope()
        self.ta_info.update(self.out.ta_info)
        botslib.tryrunscript(self.userscript,self.scriptname,'ta_infocontent',ta_info=self.ta_info)
        if not self.ta_info['envelope-template']:
            raise botslib.OutMessageError('While enveloping in "%(editype)s.%(messagetype)s": syntax option "envelope-template" not filled; is required.',
                                            self.ta_info)
        templatefile = botslib.abspath(self.__class__.__name__,self.ta_info['envelope-template'])
        ta_list = self.filelist2absolutepaths()
        try:
            botsglobal.logger.debug('Start writing envelope to file "%(filename)s".',self.ta_info)
            loader = TemplateLoader(auto_reload=False)
            tmpl = loader.load(templatefile)
        except:
            txt = botslib.txtexc()
            raise botslib.OutMessageError('While enveloping in "%(editype)s.%(messagetype)s", error:\n%(txt)s',
                                        {'editype':self.ta_info['editype'],'messagetype':self.ta_info['messagetype'],'txt':txt})
        try:
            filehandler = botslib.opendata_bin(self.ta_info['filename'],'wb')
            stream = tmpl.generate(data=ta_list)
            stream.render(method='xhtml',encoding=self.ta_info['charset'],out=filehandler)
        except:
            txt = botslib.txtexc()
            raise botslib.OutMessageError('While enveloping in "%(editype)s.%(messagetype)s", error:\n%(txt)s',
                                        {'editype':self.ta_info['editype'],'messagetype':self.ta_info['messagetype'],'txt':txt})
        finally:
            filehandler.close()

class x12(Envelope):
    ''' Generate envelope segments; fill with appropriate data, write to interchange-file.
        partners: 
        1. partner from database -> find syntax
        2. order of priority in suing partner data from differnt sources:
            1. envelope_content (from mapping)
            2. syntax
            3. via database (ta_info)
    '''
    def run(self):
        self.check_envelope_partners() 
        #read grammars, including partner syntax. Partners from database (in ta_info) are used to find partner syntax
        self._openoutenvelope()         
        self.ta_info.update(self.out.ta_info)
        #user exit to change ta_info
        botslib.tryrunscript(self.userscript,self.scriptname,'ta_infocontent',ta_info=self.ta_info)
        #test indicator. Options: (--same as partners--)
        if self.ta_info['testindicator'] and self.ta_info['testindicator'] != '0':    #value from db/mapping; '0' is default value: if set in db and not default: 
            testindicator = self.envelope_content[0].get('ISA15') or self.ta_info['testindicator']
        else:
            testindicator = self.envelope_content[0].get('ISA15') or self.ta_info['ISA15']
        #frompartner
        ISAsender_qualifier = self.envelope_content[0].get('ISA05') or self.ta_info['ISA05']
        ISAsender = self.envelope_content[0].get('ISA06') or self.ta_info.get('ISA06') or self.ta_info['frompartner']
        GS02sender = self.envelope_content[1].get('GS02') or self.ta_info.get('GS02') or self.ta_info['frompartner']
        #topartner
        ISAreceiver_qualifier = self.envelope_content[0].get('ISA07') or self.ta_info['ISA07']
        ISAreceiver = self.envelope_content[0].get('ISA06') or self.ta_info.get('ISA08') or self.ta_info['topartner']
        GS03receiver = self.envelope_content[1].get('GS03') or self.ta_info.get('GS03') or self.ta_info['frompartner']
        #ISA/GS reference: set from mapping or (counter per sender or receiver)
        self.ta_info['reference'] = self.envelope_content[0].get('ISA13') or unicode(botslib.unique('isacounter_' + self.ta_info['topartner'] if botsglobal.ini.getboolean('settings','interchangecontrolperpartner',False) else frompartner))
        #date and time
        senddate = botslib.strftime('%Y%m%d')
        sendtime = botslib.strftime('%H%M')
        #version
        version = self.envelope_content[0].get('ISA12') or self.ta_info['version']
        #build the envelope segments (generate tree from which the segments will be generated)
        self.out.put({'BOTSID':'ISA',
                        'ISA01':self.envelope_content[0].get('ISA01') or self.ta_info['ISA01'],
                        'ISA02':self.envelope_content[0].get('ISA02') or self.ta_info['ISA02'],
                        'ISA03':self.envelope_content[0].get('ISA03') or self.ta_info['ISA03'],
                        'ISA04':self.envelope_content[0].get('ISA04') or self.ta_info['ISA04'],
                        'ISA05':ISAsender_qualifier,
                        'ISA06':ISAsender.ljust(15),     #add spaces; is fixed length
                        'ISA07':ISAreceiver_qualifier,
                        'ISA08':ISAreceiver.ljust(15),    #add spaces; is fixed length
                        'ISA09':self.envelope_content[0].get('ISA09') or senddate[2:],
                        'ISA10':self.envelope_content[0].get('ISA10') or sendtime,
                        'ISA11':self.envelope_content[0].get('ISA11') or self.ta_info['ISA11'],      #if ISA version > 00403, replaced by repertion separator (below, hardcoded)
                        'ISA12':version,
                        'ISA13':self.ta_info['reference'],
                        'ISA14':self.envelope_content[0].get('ISA14') or self.ta_info['ISA14'],
                        'ISA15':testindicator},
                        strip=False)         #MIND: strip=False: ISA fields should not be stripped as it is fixed-length
        self.out.put({'BOTSID':'ISA'},{'BOTSID':'IEA','IEA01':'1','IEA02':self.ta_info['reference']})
        
        gs06reference = self.envelope_content[1].get('GS06') or self.ta_info['reference']
        gs08messagetype = self.envelope_content[1].get('GS08') or self.ta_info['messagetype'][3:]       #GS08 is message version + extension. so: 850004010VICS -> 004010VICS
        self.out.put({'BOTSID':'ISA'},{'BOTSID':'GS',
                                        'GS01':self.envelope_content[1].get('GS01') or self.ta_info['functionalgroup'],
                                        'GS02':GS02sender,
                                        'GS03':GS03receiver,
                                        'GS04':self.envelope_content[1].get('GS04') or senddate if gs08messagetype[:6] >= '004010' else senddate[2:],
                                        'GS05':self.envelope_content[1].get('GS05') or sendtime,
                                        'GS06':gs06reference,
                                        'GS07':self.envelope_content[1].get('GS07') or self.ta_info['GS07'],
                                        'GS08':gs08messagetype,
                                        })
        self.out.put({'BOTSID':'ISA'},{'BOTSID':'GS'},{'BOTSID':'GE','GE01':self.ta_info['nrmessages'],'GE02':gs06reference})
        
        #user exit to change data in tree/segments
        botslib.tryrunscript(self.userscript,self.scriptname,'envelopecontent',ta_info=self.ta_info,out=self.out)
        #convert the tree into segments; here only the UNB is written (first segment)
        self.out.checkmessage(self.out.root,self.out.defmessage)
        self.out.checkforerrorlist()
        self.out.tree2records(self.out.root)
        #start doing the actual writing:
        tofile = botslib.opendata(self.ta_info['filename'],'wb',self.ta_info['charset'])
        isa_string = self.out.record2string(self.out.lex_records[0:1])
        
        #ISA has separators at certain positions. Not possible in Bots (can not use sep as data). So: hardcoded.
        #SHOULD hardcode read/write ISA...
        if version < '00403':
            isa_string = isa_string[:103] + self.ta_info['field_sep'] + self.ta_info['sfield_sep'] + isa_string[103:]
        else:
            isa_string = isa_string[:82] + self.ta_info['reserve'] + isa_string[83:103] + self.ta_info['field_sep'] + self.ta_info['sfield_sep'] + isa_string[103:]
        tofile.write(isa_string)                                            #write ISA
        tofile.write(self.out.record2string(self.out.lex_records[1:2]))     #write GS
        self.writefilelist(tofile)
        tofile.write(self.out.record2string(self.out.lex_records[2:]))      #write GE and IEA
        tofile.close()


class jsonnocheck(noenvelope):
    pass

class json(noenvelope):
    pass

class xmlnocheck(noenvelope):
    pass

class xml(noenvelope):
    pass

class db(noenvelope):
    pass

class raw(noenvelope):
    pass
