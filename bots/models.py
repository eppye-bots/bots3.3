import sys
import urllib.parse
import os
import re
from django.db import models
from django.core.validators import validate_integer
from django.core.exceptions import ValidationError
from django.utils.encoding import python_2_unicode_compatible
from . import botsglobal
from . import validate_email
''' Declare database tabels.
    Django is not always perfect in generating db - but improving ;-)).
    The generated database can be manipulated SQL. see bots/sql/*.
'''
#***Declare constants, mostly codelists.**********************************************
DEFAULT_ENTRY = ('','---------')
STATUST = [
    (0, 'Open'),
    (1, 'Error'),
    (2, 'Stuck'),
    (3, 'Done'),
    (4, 'Resend'),
    (5, 'No retry'),
    ]
STATUS = [
    (1,'Process'),
    (3,'Discarded'),
    (200,'Received'),
    (220,'Infile'),
    (310,'Parsed'),
    (320,'Document-in'),
    (330,'Document-out'),
    (400,'Merged'),
    (500,'Outfile'),
    (520,'Send'),
    ]
EDITYPES = [
    #~ DEFAULT_ENTRY,
    ('csv', 'csv'),
    ('database', 'database (old)'),
    ('db', 'db'),
    ('edifact', 'edifact'),
    ('email-confirmation','email-confirmation'),
    ('excel','excel (only incoming)'),
    ('fixed', 'fixed'),
    ('idoc', 'idoc'),
    ('json', 'json'),
    ('jsonnocheck', 'jsonnocheck'),
    ('mailbag', 'mailbag'),
    ('raw', 'raw'),
    ('templatehtml', 'template-html'),
    ('tradacoms', 'tradacoms'),
    ('xml', 'xml'),
    ('xmlnocheck', 'xmlnocheck'),
    ('x12', 'x12'),
    ]
INOROUT = (
    ('in', 'in'),
    ('out', 'out'),
    )
CHANNELTYPE = (     #Note: in communication.py these channeltypes are converted to channeltype to use in acceptance tests.
    ('file', 'file'),
    ('smtp', 'smtp'),
    ('smtps', 'smtps'),
    ('smtpstarttls', 'smtpstarttls'),
    ('pop3', 'pop3'),
    ('pop3s', 'pop3s'),
    ('pop3apop', 'pop3apop'),
    ('http', 'http'),
    ('https', 'https'),
    ('imap4', 'imap4'),
    ('imap4s', 'imap4s'),
    ('ftp', 'ftp'),
    ('ftps', 'ftps (explicit)'),
    ('ftpis', 'ftps (implicit)'),
    ('sftp', 'sftp (ssh)'),
    ('xmlrpc', 'xmlrpc'),
    ('mimefile', 'mimefile'),
    ('trash', 'trash/discard'),
    ('communicationscript', 'communicationscript'),
    ('db', 'db'),
    ('database', 'database (old)'),
    )
CONFIRMTYPE = [
    ('ask-email-MDN','ask an email confirmation (MDN) when sending'),
    ('send-email-MDN','send an email confirmation (MDN) when receiving'),
    ('ask-x12-997','ask a x12 confirmation (997) when sending'),
    ('send-x12-997','send a x12 confirmation (997) when receiving'),
    ('ask-edifact-CONTRL','ask an edifact confirmation (CONTRL) when sending'),
    ('send-edifact-CONTRL','send an edifact confirmation (CONTRL) when receiving'),
    ]
RULETYPE = (
    ('all','Confirm all'),
    ('route','Route'),
    ('channel','Channel'),
    ('frompartner','Frompartner'),
    ('topartner','Topartner'),
    ('messagetype','Messagetype'),
    )
ENCODE_MIME = (
    ('always','Base64'),
    ('never','Never'),
    ('ascii','Base64 if not ascii'),
    )
EDI_AS_ATTACHMENT = (
    ('attachment','As attachment'),
    ('body','In body of email'),
    )
ENCODE_ZIP_IN = (
    (1,'Always unzip file'),
    (2,'If zip-file: unzip'),
    )
ENCODE_ZIP_OUT = (
    (1,'Always zip'),
    )
TRANSLATETYPES = (
    (0,'Nothing'),
    (1,'Translate'),
    (2,'Pass-through'),
    (3,'Parse & Pass-through'),
    )
CONFIRMTYPELIST = [DEFAULT_ENTRY] + CONFIRMTYPE
EDITYPESLIST = [DEFAULT_ENTRY] + EDITYPES

#***Functions that produced codelists.**********************************************
def getroutelist():     #needed because the routeid is needed (and this is not theprimary key
    return [DEFAULT_ENTRY]+[(l,l) for l in routes.objects.values_list('idroute', flat=True).order_by('idroute').distinct() ]

def getinmessagetypes():
    return [DEFAULT_ENTRY]+[(l,l) for l in translate.objects.values_list('frommessagetype', flat=True).order_by('frommessagetype').distinct() ]
def getoutmessagetypes():
    return [DEFAULT_ENTRY]+[(l,l) for l in translate.objects.values_list('tomessagetype', flat=True).order_by('tomessagetype').distinct() ]
def getallmessagetypes():
    return [DEFAULT_ENTRY]+[(l,l) for l in sorted(set(list(translate.objects.values_list('tomessagetype', flat=True).all()) + list(translate.objects.values_list('frommessagetype', flat=True).all()) )) ]
def getpartners():
    return [DEFAULT_ENTRY]+[(l,'%s (%s)'%(l,n)) for (l,n) in partner.objects.values_list('idpartner','name').filter(active=True)]
def getfromchannels():
    return [DEFAULT_ENTRY]+[(l,'%s (%s)'%(l,t)) for (l,t) in channel.objects.values_list('idchannel','type').filter(inorout='in')]
def gettochannels():
    return [DEFAULT_ENTRY]+[(l,'%s (%s)'%(l,t)) for (l,t) in channel.objects.values_list('idchannel','type').filter(inorout='out')]

#***Database tables that produced codelists.**********************************************
class StripCharField(models.CharField):
    ''' strip values before saving to database. this is not default in django #%^&*'''
    def get_prep_value(self, value,*args,**kwargs):
        ''' Convert Python objects (value) to query values (returned)
        '''
        if isinstance(value, str):
            return value.strip()
        else:
            return value

def multiple_email_validator(value):
    ''' Problems with validating email adresses:
        django's email validating is to strict. (eg if quoted user part, space is not allowed).
        use case: x400 via IPmail (x400 addresses are used in email-addresses).
        Use re-expressions to get this better/conform email standards.
    '''
    if botsglobal.ini.getboolean('webserver','validate_email_address',True):      #email validation can be disabled via bots.ini
        emails = re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)',value)    #split emails
        for email in emails:
            if not validate_email.validate_email_address(email):
                raise ValidationError('Enter valid e-mail address(es) separated by commas.', code='invalid')

def script_link1(script,linktext):
    ''' if script exists return a plain text name as link; else return "no" icon, plain text name
        used in translate (all scripts should exist, missing script is an error).
    '''
    script = script.replace('.',os.sep,script.count('.')-1) # allow mapping script subdirs
    if os.path.exists(script):
        return '<a href="/srcfiler/?src=%s" target="_blank">%s</a>'%(urllib.parse.quote(script),linktext)
    else:
        return '<img src="/media/admin/img/icon-no.svg"></img> %s'%linktext

def script_link2(script):
    ''' if script exists return "yes" icon + view link; else return "no" icon
        used in routes, channels (scripts are optional)
    '''
    if os.path.exists(script):
        return '<a class="nowrap" href="/srcfiler/?src=%s" target="_blank"><img src="/media/admin/img/icon-yes.svg"></img> view</a>'%urllib.parse.quote(script)
    else:
        return '<img src="/media/admin/img/icon-no.svg"></img>'


class MultipleEmailField(models.CharField):
    default_validators = [multiple_email_validator]
    description = 'One or more e-mail address(es),separated by ",".'
class TextAsInteger(models.CharField):
    default_validators = [validate_integer]

#***********************************************************************************
#******** written by webserver ********************************************************
#***********************************************************************************
@python_2_unicode_compatible
class confirmrule(models.Model):
    #~ id = models.IntegerField(primary_key=True)
    active = models.BooleanField(default=False)
    confirmtype = StripCharField(max_length=35,choices=CONFIRMTYPE)
    ruletype = StripCharField(max_length=35,choices=RULETYPE)
    negativerule = models.BooleanField(default=False,help_text='Use to exclude. Bots first checks positive rules, than negative rules. Eg include certain channel, exclude partner XXX.')
    frompartner = models.ForeignKey('partner',related_name='cfrompartner',null=True,on_delete=models.CASCADE,blank=True,limit_choices_to = {'isgroup': False})
    topartner = models.ForeignKey('partner',related_name='ctopartner',null=True,on_delete=models.CASCADE,blank=True,limit_choices_to = {'isgroup': False})
    idroute = StripCharField(max_length=35,null=True,blank=True,verbose_name='route')
    idchannel = models.ForeignKey('channel',null=True,on_delete=models.CASCADE,blank=True,verbose_name='channel')
    editype = StripCharField(max_length=35,choices=EDITYPES,blank=True)         #20121229"is not used anymore.....editype is always clear from context.
    messagetype = StripCharField(max_length=35,blank=True,help_text='Eg "850004010" (x12) or "ORDERSD96AUN" (edifact).')
    rsrv1 = StripCharField(max_length=35,blank=True,null=True)  #added 20100501
    rsrv2 = models.IntegerField(null=True)                        #added 20100501
    def __str__(self):
        return str(self.confirmtype) + ' ' + str(self.ruletype)
    class Meta:
        db_table = 'confirmrule'
        verbose_name = 'confirm rule'
        ordering = ['confirmtype','ruletype','negativerule','frompartner','topartner','idroute','idchannel','messagetype']
@python_2_unicode_compatible
class ccodetrigger(models.Model):
    ccodeid = StripCharField(primary_key=True,max_length=35,verbose_name='Type of user code')
    ccodeid_desc = models.TextField(blank=True,null=True,verbose_name='Description')
    def __str__(self):
        return str(self.ccodeid)
    class Meta:
        db_table = 'ccodetrigger'
        verbose_name = 'user code type'
        ordering = ['ccodeid']
@python_2_unicode_compatible
class ccode(models.Model):
    #~ id = models.IntegerField(primary_key=True)     #added 20091221
    ccodeid = models.ForeignKey(ccodetrigger,on_delete=models.CASCADE,verbose_name='Type of user code')
    leftcode = StripCharField(max_length=35,db_index=True)
    rightcode = StripCharField(max_length=70,db_index=True)
    attr1 = StripCharField(max_length=70,blank=True)
    attr2 = StripCharField(max_length=35,blank=True)
    attr3 = StripCharField(max_length=35,blank=True)
    attr4 = StripCharField(max_length=35,blank=True)
    attr5 = StripCharField(max_length=35,blank=True)
    attr6 = StripCharField(max_length=35,blank=True)
    attr7 = StripCharField(max_length=35,blank=True)
    attr8 = StripCharField(max_length=35,blank=True)
    def __str__(self):
        return str(self.ccodeid) + ' ' + str(self.leftcode) + ' ' + str(self.rightcode)
    class Meta:
        db_table = 'ccode'
        verbose_name = 'user code'
        unique_together = (('ccodeid','leftcode','rightcode'),)
        ordering = ['ccodeid','leftcode']
@python_2_unicode_compatible
class channel(models.Model):
    idchannel = StripCharField(max_length=35,primary_key=True)
    inorout = StripCharField(max_length=35,choices=INOROUT,verbose_name='in/out')
    type = StripCharField(max_length=35,choices=CHANNELTYPE)        #protocol type: ftp, smtp, file, etc
    charset = StripCharField(max_length=35,default='us-ascii')     #20120828: not used anymore; in database is NOT NULL
    host = StripCharField(max_length=256,blank=True)
    port = models.PositiveIntegerField(default=0,blank=True,null=True)
    username = StripCharField(max_length=35,blank=True)
    secret = StripCharField(max_length=35,blank=True,verbose_name='password')
    starttls = models.BooleanField(default=False,verbose_name='No check from-address',help_text='Do not check if incoming "from" email addresses is known.')       #20091027: used as 'no check on "from:" email address'
    apop = models.BooleanField(default=False,verbose_name='No check to-address',help_text='Do not check if incoming "to" email addresses is known.')       #20110104: used as 'no check on "to:" email address'
    remove = models.BooleanField(default=False,help_text='Delete incoming edi files after reading.<br>Use in production else files are read again and again.')
    path = StripCharField(max_length=256,blank=True)  #different from host - in ftp both host and path are used
    filename = StripCharField(max_length=70,blank=True,help_text='Incoming: use wild-cards eg: "*.edi".<br>Outgoing: many options, see <a target="_blank" href="https://botsdocs.readthedocs.io/en/latest/configuration/channel/filenames.html">documentation</a>.<br>Advised: use "*" in filename (is replaced by unique counter per channel).<br>eg "D_*.edi" gives D_1.edi, D_2.edi, etc.')
    lockname = StripCharField(max_length=35,blank=True,verbose_name='Lock-file',help_text='Directory locking: if lock-file exists in directory, directory is locked for reading/writing.')
    syslock = models.BooleanField(default=False,verbose_name='System locks',help_text='Use system file locks for reading or writing edi files (windows, *nix).')
    parameters = StripCharField(max_length=70,blank=True,help_text='For use in user communication scripting.')
    ftpaccount = StripCharField(max_length=35,blank=True,verbose_name='ftp account',help_text='FTP accounting information; note that few systems use this.')
    ftpactive = models.BooleanField(default=False,verbose_name='ftp active mode',help_text='Passive mode is used unless this is ticked.')
    ftpbinary = models.BooleanField(default=False,verbose_name='ftp binary transfer mode',help_text='File transfers are ASCII unless this is ticked.')
    askmdn = StripCharField(max_length=17,blank=True,choices=ENCODE_MIME,verbose_name='mime encoding')     #20100703: used to indicate mime-encoding
    sendmdn = StripCharField(max_length=17,blank=True,choices=EDI_AS_ATTACHMENT,verbose_name='as body or attachment')      #20120922: for email/mime: edi file as attachment or in body
    mdnchannel = StripCharField(max_length=35,blank=True,verbose_name='Tmp-part file name',help_text='Write file than rename. Bots renames to filename without this tmp-part.<br>Eg first write "myfile.edi.tmp", tmp-part is ".tmp", rename to "myfile.edi".')      #20140113:use as tmp part of file name
    archivepath = StripCharField(max_length=256,blank=True,verbose_name='Archive path',help_text='Write edi files to an archive.<br>See <a target="_blank" href="https://botsdocs.readthedocs.io/en/latest/deployment/archiving.html#the-long-term-archive">documentation</a>. Eg: "C:/edi/archive/mychannel".')           #added 20091028
    desc = models.TextField(max_length=256,null=True,blank=True,verbose_name='Description')
    rsrv1 = TextAsInteger(max_length=35,blank=True,null=True,verbose_name='Max failures',help_text='Max number of connection failures of incommunication before this is reported as a processerror (default: direct report).')      #added 20100501 #20140315: used as max_com
    rsrv2 = models.IntegerField(null=True,blank=True,verbose_name='Max seconds',help_text='Max seconds for in-communication channel to run. Purpose: limit incoming edi files; for large volumes it is better read more often than all files in one time.')   #added 20100501. 20110906: max communication time.
    rsrv3 = models.IntegerField(null=True,blank=True,verbose_name='Max days archive',help_text='Max number of days files are kept in archive.<br>Overrules global setting in bots.ini.')   #added 20121030. #20131231: use as maxdaysarchive
    keyfile = StripCharField(max_length=256,blank=True,null=True,verbose_name='Private key file',help_text='Path to file that contains PEM formatted private key.')          #added 20121201
    certfile = StripCharField(max_length=256,blank=True,null=True,verbose_name='Certificate chain file',help_text='Path to file that contains PEM formatted certificate chain.')          #added 20121201
    testpath = StripCharField(max_length=256,blank=True,verbose_name='Acceptance test path',help_text='Path used during acceptance tests, see <a target="_blank" href="https://botsdocs.readthedocs.io/en/latest/advanced-deployment/change-management.html#isolated-acceptance-testing">documentation</a>.')           #added 20120111

    def communicationscript(self):
        return script_link2(os.path.join(botsglobal.ini.get('directories','usersysabs'),'communicationscripts', self.idchannel + '.py'))
    communicationscript.allow_tags = True
    communicationscript.short_description = 'User script'

    class Meta:
        ordering = ['idchannel']
        db_table = 'channel'
    def __str__(self):
        return self.idchannel + ' (' + self.type + ')'
@python_2_unicode_compatible
class partner(models.Model):
    idpartner = StripCharField(max_length=35,primary_key=True,verbose_name='partner identification')
    active = models.BooleanField(default=False)
    isgroup = models.BooleanField(default=False,help_text='Indicate if normal partner or a partner group. Partners can be assigned to partner groups.')
    name = StripCharField(max_length=256) #only used for user information
    mail = MultipleEmailField(max_length=256,blank=True)
    cc = MultipleEmailField(max_length=256,blank=True,help_text='Multiple CC-addresses supported (comma-separated).')
    mail2 = models.ManyToManyField(channel, through='chanpar',blank=True)
    group = models.ManyToManyField('self',db_table='partnergroup',blank=True,symmetrical=False,limit_choices_to = {'isgroup': True})
    rsrv1 = StripCharField(max_length=35,blank=True,null=True)    #added 20100501
    rsrv2 = models.IntegerField(null=True)                        #added 20100501
    name1 = StripCharField(max_length=70,blank=True,null=True)    #added 20121201
    name2 = StripCharField(max_length=70,blank=True,null=True)    #added 20121201
    name3 = StripCharField(max_length=70,blank=True,null=True)    #added 20121201
    address1 = StripCharField(max_length=70,blank=True,null=True) #added 20121201
    address2 = StripCharField(max_length=70,blank=True,null=True) #added 20121201
    address3 = StripCharField(max_length=70,blank=True,null=True) #added 20121201
    city = StripCharField(max_length=35,blank=True,null=True)     #added 20121201
    postalcode = StripCharField(max_length=17,blank=True,null=True)          #added 20121201
    countrysubdivision = StripCharField(max_length=9,blank=True,null=True)   #added 20121201
    countrycode = StripCharField(max_length=3,blank=True,null=True)          #added 20121201
    phone1 = StripCharField(max_length=17,blank=True,null=True)   #added 20121201
    phone2 = StripCharField(max_length=17,blank=True,null=True)   #added 20121201
    startdate = models.DateField(blank=True,null=True)            #added 20121201
    enddate = models.DateField(blank=True,null=True)              #added 20121201
    desc = models.TextField(blank=True,null=True,verbose_name='Description')    #added 20121201
    attr1 = StripCharField(max_length=35,blank=True,null=True,verbose_name='attr1') # user can customise verbose name
    attr2 = StripCharField(max_length=35,blank=True,null=True,verbose_name='attr2')
    attr3 = StripCharField(max_length=35,blank=True,null=True,verbose_name='attr3')
    attr4 = StripCharField(max_length=35,blank=True,null=True,verbose_name='attr4')
    attr5 = StripCharField(max_length=35,blank=True,null=True,verbose_name='attr5')
    class Meta:
        ordering = ['idpartner']
        db_table = 'partner'
    def __str__(self):
        return str(self.idpartner) + ' (' + str(self.name) + ')'
    def save(self, *args, **kwargs):
        if isinstance(self,partnergroep):
            self.isgroup = True
        else:
            self.isgroup = False
        super(partner,self).save(*args,**kwargs)

class partnergroep(partner):
    class Meta:
        proxy = True
        ordering = ['idpartner']
        db_table = 'partner'

@python_2_unicode_compatible
class chanpar(models.Model):
    #~ id = models.IntegerField(primary_key=True)     #added 20091221
    idpartner = models.ForeignKey(partner,on_delete=models.CASCADE,verbose_name='partner')
    idchannel = models.ForeignKey(channel,on_delete=models.CASCADE,verbose_name='channel')
    mail = MultipleEmailField(max_length=256)
    cc = MultipleEmailField(max_length=256,blank=True)           #added 20091111
    askmdn = models.BooleanField(default=False)     #not used anymore 20091019
    sendmdn = models.BooleanField(default=False)    #not used anymore 20091019
    class Meta:
        unique_together = (('idpartner','idchannel'),)
        ordering = ['idpartner','idchannel']
        db_table = 'chanpar'
        verbose_name = 'email address per channel'
        verbose_name_plural = 'email address per channel'
    def __str__(self):
        return str(self.idpartner) + ' ' + str(self.idchannel) + ' ' + str(self.mail)
@python_2_unicode_compatible
class translate(models.Model):
    #~ id = models.IntegerField(primary_key=True)
    active = models.BooleanField(default=False)
    fromeditype = StripCharField(max_length=35,choices=EDITYPES,help_text='Editype to translate from.')
    frommessagetype = StripCharField(max_length=35,help_text='Messagetype to translate from.')
    alt = StripCharField(max_length=35,null=False,blank=True,verbose_name='Alternative translation',help_text='Do translation only for this alternative translation.')
    frompartner = models.ForeignKey(partner,related_name='tfrompartner',null=True,blank=True,on_delete=models.PROTECT,help_text='Do translation only for this frompartner.')
    topartner = models.ForeignKey(partner,related_name='ttopartner',null=True,blank=True,on_delete=models.PROTECT,help_text='Do translation only for this topartner.')
    tscript = StripCharField(max_length=70,verbose_name='Mapping Script',help_text='Mappingscript to use in translation.')
    toeditype = StripCharField(max_length=35,choices=EDITYPES,help_text='Editype to translate to.')
    tomessagetype = StripCharField(max_length=35,help_text='Messagetype to translate to.')
    desc = models.TextField(max_length=256,null=True,blank=True,verbose_name='Description')
    rsrv1 = StripCharField(max_length=35,blank=True,null=True)  #added 20100501
    rsrv2 = models.IntegerField(null=True)                        #added 20100501

    def tscript_link(self):
        return script_link1(os.path.join(botsglobal.ini.get('directories','usersysabs'),'mappings', self.fromeditype, self.tscript + '.py'),self.tscript)
    tscript_link.allow_tags = True
    tscript_link.short_description = 'Mapping Script'

    def frommessagetype_link(self):
        if self.fromeditype in ('db','raw'): # db and raw have no grammar, so not an error
            return self.frommessagetype
        else:
            return script_link1(os.path.join(botsglobal.ini.get('directories','usersysabs'),'grammars', self.fromeditype, self.frommessagetype + '.py'),self.frommessagetype)
    frommessagetype_link.allow_tags = True
    frommessagetype_link.short_description = 'Frommessagetype'

    def tomessagetype_link(self):
        if self.toeditype in ('db','raw'): # db and raw have no grammar, so not an error
            return self.tomessagetype
        else:
            return script_link1(os.path.join(botsglobal.ini.get('directories','usersysabs'),'grammars', self.toeditype, self.tomessagetype + '.py'),self.tomessagetype)
    tomessagetype_link.allow_tags = True
    tomessagetype_link.short_description = 'Tomessagetype'

    class Meta:
        db_table = 'translate'
        verbose_name = 'translation rule'
        ordering = ['fromeditype','frommessagetype','frompartner','topartner','alt']
    def __str__(self):
        return str(self.fromeditype) + ' ' + str(self.frommessagetype) + ' ' + str(self.alt) + ' ' + str(self.frompartner) + ' ' + str(self.topartner)

@python_2_unicode_compatible
class routes(models.Model):
    #~ id = models.IntegerField(primary_key=True)
    idroute = StripCharField(max_length=35,db_index=True,help_text='Identification of route; a composite route consists of multiple parts having the same "idroute".')
    seq = models.PositiveIntegerField(default=1,verbose_name='Sequence',help_text='For routes consisting of multiple parts, this indicates the order these parts are run.')
    active = models.BooleanField(default=False,help_text='Bots-engine only uses active routes.')
    fromchannel = models.ForeignKey(channel,related_name='rfromchannel',null=True,on_delete=models.SET_NULL,blank=True,verbose_name='incoming channel',limit_choices_to = {'inorout': 'in'},help_text='Receive edi files via this communication channel.')
    fromeditype = StripCharField(max_length=35,choices=EDITYPES,blank=True,help_text='Editype of the incoming edi files.')
    frommessagetype = StripCharField(max_length=35,blank=True,help_text='Messagetype of incoming edi files. For edifact: messagetype=edifact; for x12: messagetype=x12.')
    tochannel = models.ForeignKey(channel,related_name='rtochannel',null=True,on_delete=models.SET_NULL,blank=True,verbose_name='outgoing channel',limit_choices_to = {'inorout': 'out'},help_text='Send edi files via this communication channel.')
    toeditype = StripCharField(max_length=35,choices=EDITYPES,blank=True,help_text='Filter edi files of this editype for outgoing channel.')
    tomessagetype = StripCharField(max_length=35,blank=True,help_text='Filter edi files of this messagetype for outgoing channel.')
    alt = StripCharField(max_length=35,default='',blank=True,verbose_name='Alternative translation',help_text='Use if there is more than one "translation" for the same editype and messagetype.')
    frompartner = models.ForeignKey(partner,related_name='rfrompartner',null=True,on_delete=models.SET_NULL,blank=True,limit_choices_to = {'isgroup': False},help_text='The frompartner of the incoming edi files.')
    topartner = models.ForeignKey(partner,related_name='rtopartner',null=True,on_delete=models.SET_NULL,blank=True,limit_choices_to = {'isgroup': False},help_text='The topartner of the incoming edi files.')
    frompartner_tochannel = models.ForeignKey(partner,related_name='rfrompartner_tochannel',null=True,on_delete=models.PROTECT,blank=True,help_text='Filter edi files of this partner/partnergroup for outgoing channel')
    topartner_tochannel = models.ForeignKey(partner,related_name='rtopartner_tochannel',null=True,on_delete=models.PROTECT,blank=True,help_text='Filter edi files of this partner/partnergroup for outgoing channel')
    testindicator = StripCharField(max_length=1,blank=True,help_text='Filter edi files with this test-indicator for outgoing channel.')
    translateind = models.IntegerField(default=1,choices=TRANSLATETYPES,verbose_name='translate',help_text='Indicates what to do with incoming files for this route(part).')
    notindefaultrun = models.BooleanField(default=False,blank=True,verbose_name='Not in default run',help_text='Do not use this route in a normal run. Advanced, related to scheduling specific routes or not.')
    desc = models.TextField(max_length=256,null=True,blank=True,verbose_name='Description')
    rsrv1 = StripCharField(max_length=35,blank=True,null=True)  #added 20100501
    rsrv2 = models.IntegerField(null=True,blank=True)           #added 20100501
    defer = models.BooleanField(default=False,blank=True,help_text='Set ready for communication, defer actual communication. Communication is done later in another route(-part).')                        #added 20100601
    zip_incoming = models.IntegerField(null=True,blank=True,choices=ENCODE_ZIP_IN,verbose_name='Incoming zip-file handling',help_text='Unzip received files.')  #added 20100501 #20120828: use for zip-options
    zip_outgoing = models.IntegerField(null=True,blank=True,choices=ENCODE_ZIP_OUT,verbose_name='Outgoing zip-file handling',help_text='Send files as zip-files.')                        #added 20100501

    def routescript(self):
        return script_link2(os.path.join(botsglobal.ini.get('directories','usersysabs'),'routescripts', self.idroute + '.py'))
    routescript.allow_tags = True
    routescript.short_description = 'Script'

    def indefaultrun(obj):
        return not obj.notindefaultrun
    indefaultrun.boolean = True
    indefaultrun.short_description = 'Default run'

    class Meta:
        db_table = 'routes'
        verbose_name = 'route'
        unique_together = (('idroute','seq'),)
        ordering = ['idroute','seq']
    def __str__(self):
        return str(self.idroute) + ' ' + str(self.seq)
    def translt(self):
        if self.translateind == 0:
            return '<img alt="%s" src="/media/images/icon-no.svg"></img>'%(self.get_translateind_display())
        elif self.translateind == 1:
            return '<img alt="%s" src="/media/images/icon-yes.svg"></img>'%(self.get_translateind_display())
        elif self.translateind == 2:
            return '<img alt="%s" src="/media/images/icon-arrow-blue.svg"></img>'%(self.get_translateind_display())
        elif self.translateind == 3:
            return '<img alt="%s" src="/media/images/icon-arrow-yellow.svg"></img>'%(self.get_translateind_display())
    translt.allow_tags = True
    translt.admin_order_field = 'translateind'

#***********************************************************************************
#******** written by engine ********************************************************
#***********************************************************************************
class filereport(models.Model):
    #~ id = models.IntegerField(primary_key=True)
    idta = models.IntegerField(primary_key=True)
    reportidta = models.IntegerField()
    statust = models.IntegerField(choices=STATUST)
    retransmit = models.IntegerField()
    idroute = StripCharField(max_length=35)
    fromchannel = StripCharField(max_length=35)
    tochannel = StripCharField(max_length=35)
    frompartner = StripCharField(max_length=35)
    topartner = StripCharField(max_length=35)
    frommail = StripCharField(max_length=256)
    tomail = StripCharField(max_length=256)
    ineditype = StripCharField(max_length=35,choices=EDITYPES)
    inmessagetype = StripCharField(max_length=35)
    outeditype = StripCharField(max_length=35,choices=EDITYPES)
    outmessagetype = StripCharField(max_length=35)
    incontenttype = StripCharField(max_length=35)
    outcontenttype = StripCharField(max_length=35)
    nrmessages = models.IntegerField()
    ts = models.DateTimeField(db_index=True) #copied from ta
    infilename = StripCharField(max_length=256)
    inidta = models.IntegerField(null=True)   #not used anymore
    outfilename = StripCharField(max_length=256)
    outidta = models.IntegerField()
    divtext = StripCharField(max_length=35)
    errortext = models.TextField()
    rsrv1 = StripCharField(max_length=35,blank=True,null=True)  #added 20100501; 
    rsrv2 = models.IntegerField(null=True)                        #added 20100501
    filesize = models.IntegerField(null=True)                    #added 20121030
    class Meta:
        db_table = 'filereport'
class mutex(models.Model):
    #specific SQL is used (database defaults are used)
    mutexk = models.IntegerField(primary_key=True)  #is always value '1'
    mutexer = models.IntegerField()
    ts = models.DateTimeField()         #timestamp of mutex
    class Meta:
        db_table = 'mutex'
class persist(models.Model):
    #OK, this has gone wrong. There is no primary key here, so django generates this. But there is no ID in the custom sql.
    #Django still uses the ID in sql manager. This leads to an error in snapshot plugin. Disabled this in snapshot function; to fix this really database has to be changed.
    #specific SQL is used (database defaults are used)
    domein = StripCharField(max_length=35)
    botskey = StripCharField(max_length=35)
    content = models.TextField()
    ts = models.DateTimeField()
    class Meta:
        db_table = 'persist'
        unique_together = (('domein','botskey'),)
class report(models.Model):
    idta = models.IntegerField(primary_key=True)    #rename to reportidta
    lastreceived = models.IntegerField()
    lastdone = models.IntegerField()
    lastopen = models.IntegerField()
    lastok = models.IntegerField()
    lasterror = models.IntegerField()
    send = models.IntegerField()
    processerrors = models.IntegerField()
    ts = models.DateTimeField(db_index=True)                     #copied from (runroot)ta
    type = StripCharField(max_length=35)
    status = models.BooleanField()
    rsrv1 = StripCharField(max_length=35,blank=True,null=True)  #added 20100501. 20131230: used to store the commandline for the run.
    rsrv2 = models.IntegerField(null=True)                       #added 20100501.
    filesize = models.IntegerField(null=True)                    #added 20121030: total size of messages that have been translated.
    acceptance = models.IntegerField(null=True)                            #added 20130114:
    class Meta:
        db_table = 'report'
#~ #trigger for sqlite to use local time (instead of utc). I can not add this to sqlite specific sql code, as django does not allow complex (begin ... end) sql here.
#~ CREATE TRIGGER uselocaltime  AFTER INSERT ON ta
#~ BEGIN
#~ UPDATE ta
#~ SET ts = datetime('now','localtime')
#~ WHERE idta = new.idta ;
#~ END;
class ta(models.Model):
    #specific SQL is used (database defaults are used)
    idta = models.AutoField(primary_key=True)
    statust = models.IntegerField(choices=STATUST)
    status = models.IntegerField(choices=STATUS)
    parent = models.IntegerField(db_index=True)
    child = models.IntegerField()
    script = models.IntegerField()
    idroute = StripCharField(max_length=35)
    filename = StripCharField(max_length=256)
    frompartner = StripCharField(max_length=35)
    topartner = StripCharField(max_length=35)
    fromchannel = StripCharField(max_length=35)
    tochannel = StripCharField(max_length=35)
    editype = StripCharField(max_length=35)
    messagetype = StripCharField(max_length=35)
    alt = StripCharField(max_length=35)
    divtext = StripCharField(max_length=35)           #name of translation script. #20200929: filename of attachment in email
    merge = models.BooleanField()
    nrmessages = models.IntegerField()
    testindicator = StripCharField(max_length=10)     #0:production; 1:test. Length to 1?
    reference = StripCharField(max_length=70,db_index=True)
    frommail = StripCharField(max_length=256)
    tomail = StripCharField(max_length=256)
    charset = StripCharField(max_length=35)
    statuse = models.IntegerField()                   #obsolete 20091019
    retransmit = models.BooleanField()                #20070831: only retransmit, not rereceive
    contenttype = StripCharField(max_length=35)
    errortext = models.TextField()                    #20120921: unlimited length
    ts = models.DateTimeField()
    confirmasked = models.BooleanField()              #added 20091019; confirmation asked or send
    confirmed = models.BooleanField()                 #added 20091019; is confirmation received (when asked)
    confirmtype = StripCharField(max_length=35)       #added 20091019
    confirmidta = models.IntegerField()               #added 20091019
    envelope = StripCharField(max_length=35)          #added 20091024
    botskey = StripCharField(max_length=35)           #added 20091024
    cc = StripCharField(max_length=512)               #added 20091111
    rsrv1 = StripCharField(max_length=70)             #added 20100501; 20120618: email subject; 20190108: change to 70
    rsrv2 = models.IntegerField(null=True)            #added 20100501; 20190116: indicate file is mime (1=mime)
    rsrv3 = StripCharField(max_length=35)             #added 20100501; 20131231: envelopeID to explicitly control enveloping (enveloping criterium)
    rsrv4 = models.IntegerField(null=True)            #added 20100501
    rsrv5 = StripCharField(max_length=35)             #added 20121030; 20190408: envelope data (as filled from mapping)
    filesize = models.IntegerField(null=True)         #added 20121030
    numberofresends = models.IntegerField(null=True)  #added 20121030; if all OK (no resend) this is 0
    class Meta:
        db_table = 'ta'
class uniek(models.Model):
    #specific SQL is used (database defaults are used)
    domein = StripCharField(max_length=70,primary_key=True,verbose_name='Counter domain')
    nummer = models.IntegerField(verbose_name='Last used number')
    class Meta:
        db_table = 'uniek'
        verbose_name = 'counter'
        ordering = ['domein']
