# -*- coding: utf-8 -*-
#
# Beancount importer for csv exports from Volksbank or GLS Bank.
# Author: Frank Stollmeier
# License: GNU GPLv3
#


import datetime
from beancount.core.number import D
from beancount.core import data
from beancount.core import amount
from beancount.core import position
from beancount.ingest import importer
from beancount import loader



class VolksbankImporter(importer.ImporterProtocol):
    '''An importer for CSV export from a Volksbank online banking.'''

    def __init__(self, importing_account, default_adjacent_account = "Unknown:account", target_journal = None, currency = 'EUR', flag = '!'):
        '''
        Parameters
        ----------
        importing_account:          string, name of account belonging to the csv export (one leg of the transaction)
        default_adjacent_account:   string, default account to collect the expenses (other leg of the transaction)
        target_journal:             string, optional. Filename of the target journal to guess the corresponding account names instead of using the default_adjacent_account (the other leg of the transaction)
        currency:                   string, optional. Default is 'EUR'
        flag:                       char, optional. Default is '!'
        '''
        self.account = importing_account
        self.default_adjacent_account = default_adjacent_account
        self.target_journal = target_journal
        self.currency = currency
        self.flag = flag
        self.initialize_guessing()
        self.file_format_version = None
        
    def initialize_guessing(self):
        '''Create a dictionary of the previous transactions in 'target_journal', which can be used to guess the right postings for the new transactions.'''
        self.posting_dict = dict()
        if not (self.target_journal is None):
            entries,errors,options = loader.load_file(self.target_journal)
            entries = [e for e in entries if isinstance(e, data.Transaction)]
            entries = sorted(entries, key = lambda e: e.date)
            for entry in entries:
                if not (self.account in [p.account for p in entry.postings]):
                    continue
                if entry.payee is None:
                    continue
                if len(entry.payee) == 0:
                    continue
                if not entry.payee in self.posting_dict:
                    self.posting_dict[entry.payee] = []
                self.posting_dict[entry.payee].append(entry.postings)

    def guess_postings(self, payee, total_transaction_value):
        '''Guess postings based on the previous transactions with the same payee.
        The guess is simply the most recent transaction with the same payee. If the transaction consists of multiple postings, the total_transaction_value is distributed to the postings in the same ratios as in the previous posting. 
        If there is no previous transaction with the same payee, the target account is the default_adjacent_account. 
        
        Parameters
        ----------
        payee:                      string
        total_transaction_value:    float
        '''
        new_postings = []
        if payee in self.posting_dict:
            previous_postings = self.posting_dict[payee][-1]
            accounts = []
            units = []
            for prev_posting in previous_postings:
                accounts.append(prev_posting.account)
                units.append(prev_posting.units)
                if prev_posting.account == self.account:
                    prev_posting_had_reversed_signs = 0 > float(prev_posting.units.number) * total_transaction_value
            s = sum([float(u.number) for u in units if u.number > 0])
            for account,unit in zip(accounts,units):
                share = float(unit.number) / s
                if prev_posting_had_reversed_signs:
                    share = -share
                p = data.Posting(account, amount.Amount(D(str(round(share*abs(total_transaction_value),2))), self.currency), None, None, None, None)
                new_postings.append(p)
            #move importing_account to the end of the list
            i = 0
            for j,posting in enumerate(new_postings):
                if posting.account == self.account:
                    i = j
            new_postings.append(new_postings.pop(i))
        else:
            new_postings.append(data.Posting(self.default_adjacent_account, amount.Amount(D(str(-total_transaction_value)), self.currency), None, None, None, None))
            new_postings.append(data.Posting(self.account, amount.Amount(D(str(total_transaction_value)), self.currency), None, None, None, None))
        return new_postings
        
    def identify(self, file):
        header_version1 = '"Buchungstag";"Valuta";"Auftraggeber/Zahlungsempfänger";"Empfänger/Zahlungspflichtiger";"Konto-Nr.";"IBAN";"BLZ";"BIC";"Vorgang/Verwendungszweck";"Kundenreferenz";"Währung";"Umsatz";" "'
        header_version2 = "Buchungstag;Valuta;Textschlüssel;Primanota;Zahlungsempfänger;ZahlungsempfängerKto;ZahlungsempfängerIBAN;ZahlungsempfängerBLZ;ZahlungsempfängerBIC;Vorgang/Verwendungszweck;Kundenreferenz;Währung;Umsatz;Soll/Haben"
        header_version3 = "Bezeichnung Auftragskonto;IBAN Auftragskonto;BIC Auftragskonto;Bankname Auftragskonto;Buchungstag;Valutadatum;Name Zahlungsbeteiligter;IBAN Zahlungsbeteiligter;BIC (SWIFT-Code) Zahlungsbeteiligter;Buchungstext;Verwendungszweck;Betrag;Waehrung;Saldo nach Buchung;Bemerkung;Kategorie;Steuerrelevant;Glaeubiger ID;Mandatsreferenz"
        header_version4 = "Bezeichnung Auftragskonto;IBAN Auftragskonto;BIC Auftragskonto;Bankname Auftragskonto;Buchungstag;Valutadatum;Name Zahlungsbeteiligter;IBAN Zahlungsbeteiligter;BIC (SWIFT-Code) Zahlungsbeteiligter;Buchungstext;Verwendungszweck;Betrag;Waehrung;Saldo nach Buchung;Bemerkung;Gekennzeichneter Umsatz;Glaeubiger ID;Mandatsreferenz"
        with open(file.name, "r", encoding = "ISO-8859-1") as f:
            for line in f:
                if header_version1 in line:
                    self.file_format_version = 1
                    return True
                elif header_version2 in line:
                    self.file_format_version = 2
                    return True
                elif header_version3 in line:
                    self.file_format_version = 3
                    return True
                elif header_version4 in line:
                    self.file_format_version = 4
                    return True
            print('Unable to identify file format.')
            return False

    def file_account(self, file):
        return self.account

    def extract(self, file):
        #parse csv file
        if self.file_format_version == 1:
            buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, indices, endsaldo = parse_csv_file_v1(file.name)
        elif self.file_format_version == 2:
            buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, indices, endsaldo = parse_csv_file_v2(file.name)
        elif self.file_format_version == 3:
            buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, indices, endsaldo = parse_csv_file_v3(file.name)
        elif self.file_format_version == 4:
            buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, indices, endsaldo = parse_csv_file_v3(file.name)    
        else:
            raise IOError("Unknown file format.")
        #create transactions
        entries = []
        for i in range(len(buchungstag)):
            postings = self.guess_postings(auftraggeber_empfaenger[i], float(betrag[i]) ) 
            meta = data.new_metadata(file.name, indices[i])
            txn = data.Transaction(meta, buchungstag[i], self.flag, auftraggeber_empfaenger[i], verwendungszweck[i], data.EMPTY_SET, data.EMPTY_SET, postings)
            entries.append(txn)
        #create balance
        meta = data.new_metadata(file.name, endsaldo[2])
        entries.append( data.Balance(meta, endsaldo[0] + datetime.timedelta(days=1), self.account, amount.Amount(D(endsaldo[1]), self.currency), None, None) )
        
        return entries


def convert_value(value, soll_haben):
    '''Convert number format from the CSV file and add sign +/- depending on Soll/Haben (Credit/Debit).
    Example:  convert_value('1.200,30', 'S') returns '-1200.30'.
    
    Parameters
    ----------
    value:      string, number with ',' as decimal separator and '.' as thousand-separator
    soll_haben: string, 'S' for 'Soll' and 'H' for 'Haben'
    '''
    return ('-' if ('S' in soll_haben) else '') + value.replace('.','').replace(',','.').replace('"','')

def convert_value2(value):
    '''Convert signed numbers from the CSV file. For unsigned number with Soll/Haben see convert_value().
    
    Parameters
    ----------
    value:     string, like '-4,7' or '5,21'
    '''
    return value.replace('.','').replace(',','.')

def convert_date(date):
    '''Convert date from the CSV file (format '04.10.2020') to datetime.
    
    Parameters
    ----------
    date:       string
    '''
    day,month,year = date.replace('"','').split('.')
    return datetime.date(int(year),int(month),int(day))


def parse_csv_file_v1(filename):
    '''Parse CSV file.
    
    Parameters
    ----------
    filename:   string
    '''
    buchungstag = []
    auftraggeber_empfaenger = []
    buchungstext = []
    verwendungszweck = []
    betrag = []
    kontostand = []
    endsaldo = None
    
    linenumbers = []
    header = True
    collector = ""
    linenumber = -1
    
    f = open(filename, 'r', encoding = 'iso-8859-1')
    for line in f:
        linenumber += 1
        #skip header
        if header:
            if "Valuta" in line:
                header = False  
            continue
        #skip empty lines
        if len(line) == 0:
            continue
        #Each transaction in the csv file is splitted over several lines, so we have to collect the content of several lines until the transaction is complete: 
        collector = collector + line.replace('\n',' ')
        if '"S"' in collector or '"H"' in collector:
            if "Anfangssaldo" in collector:
                pass
            elif "Endsaldo" in collector:
                result = collector.split(';')
                date = result[0]
                amount = convert_value(result[11], result[12].replace('"',''))
                endsaldo = (convert_date(date),amount, linenumber)
            else:
                values = collector.split(';')
                buchungstag.append(convert_date(values[0]))
                auftraggeber_empfaenger.append(values[3].replace('"',''))
                buchungstext.append('')
                verwendungszweck.append(values[8].replace('"',''))
                betrag.append(convert_value(values[11], collector[-3:]))
                kontostand.append(None)
                linenumbers.append(linenumber)
            collector = ""
            
    return buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, linenumbers, endsaldo

def parse_csv_file_v2(filename):
    '''Parse CSV file with the new file format they started to use at the beginning of 2022.
    
    Parameters
    ----------
    filename:   string
    '''
    buchungstag = []
    auftraggeber_empfaenger = []
    buchungstext = []
    verwendungszweck = []
    betrag = []
    kontostand = []
    endsaldo = None
    
    linenumbers = []
    header = True
    collector = ""
    linenumber = -1
    
    f = open(filename, 'r', encoding = 'iso-8859-1')
    for line in f:
        linenumber += 1
        #skip header
        if header:
            if "Valuta" in line:
                header = False  
            continue
        
        values = line.split(';')
        if len(values[0]) == 0:
            continue
        if values[10] == 'Endsaldo':
            date = convert_date(values[0])
            amount = convert_value(values[12], values[13])
            endsaldo = (date, amount, linenumber)
        elif values[10] == 'Anfangssaldo':
            continue
        else:
            buchungstag.append(convert_date(values[0]))
            auftraggeber_empfaenger.append(values[4])
            buchungstext.append('')
            verwendungszweck.append(values[9])
            betrag.append(convert_value(values[12], values[13]))
            kontostand.append(None)
            linenumbers.append(linenumber)
            
    return buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, linenumbers, endsaldo

def parse_csv_file_v3(filename):
    '''Parse CSV file with the new file format they started to use at the beginning of 2022.
    
    Parameters
    ----------
    filename:   string
    '''
    buchungstag = []
    auftraggeber_empfaenger = []
    buchungstext = []
    verwendungszweck = []
    betrag = []
    kontostand = []
    endsaldo = None
    
    linenumbers = []
    header = True
    collector = ""
    linenumber = -1
    
    f = open(filename, 'r', encoding = 'iso-8859-1')
    for line in f:
        linenumber += 1
        #skip header
        if header:
            if "Mandatsreferenz" in line:
                header = False  
            continue
        
        values = line.split(';')
        if len(values[0]) == 0:
            continue
        
        buchungstag.append(convert_date(values[4]))
        auftraggeber_empfaenger.append(values[6])
        buchungstext.append('')
        verwendungszweck.append(values[10])
        betrag.append(convert_value2(values[11]))
        kontostand.append(convert_value2(values[13]))
        linenumbers.append(linenumber)
        
    endsaldo = (buchungstag[0], kontostand[0], linenumbers[0])
            
    return buchungstag, auftraggeber_empfaenger, buchungstext, verwendungszweck, betrag, kontostand, linenumbers, endsaldo
