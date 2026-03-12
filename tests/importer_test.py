from beancount.parser import cmptest
from beancount.utils import test_utils

from beancount_importer_volksbank import VolksbankImporter


class TestVolksbankImporter(cmptest.TestCase):
    @test_utils.docfile
    def test_extract(self, filename):
        """\
Bezeichnung Auftragskonto;IBAN Auftragskonto;BIC Auftragskonto;Bankname Auftragskonto;Buchungstag;Valutadatum;Name Zahlungsbeteiligter;IBAN Zahlungsbeteiligter;BIC (SWIFT-Code) Zahlungsbeteiligter;Buchungstext;Verwendungszweck;Betrag;Waehrung;Saldo nach Buchung;Bemerkung;Gekennzeichneter Umsatz;Glaeubiger ID;Mandatsreferenz
Konto;DE01234567890123456789;BIC;Bank Name;10.03.2026;10.03.2026;Transaction 4;DE09876543210987654321;BDSYOAD;Type;Description 4;40,00;EUR;250,00;;;;
Konto;DE01234567890123456789;BIC;Bank Name;10.03.2026;10.03.2026;Transaction 3;DE09876543210987654321;BDSYOAD;Type;Description 3;30,00;EUR;210,00;;;;
Konto;DE01234567890123456789;BIC;Bank Name;10.03.2026;10.03.2026;Transaction 2;DE09876543210987654321;BDSYOAD;Type;Description 2;20,00;EUR;180,00;;;;
Konto;DE01234567890123456789;BIC;Bank Name;09.03.2026;09.03.2026;Transaction 1;DE09876543210987654321;BDSYOAD;Type;Description 1;10,00;EUR;160,00;;;;
        """

        importer = VolksbankImporter("Assets:BankAccount", default_adjacent_account="Assets:UnknownAccount")
        assert(importer.identify(filename))
        entries = importer.extract(filename, None)
        self.assertEqualEntries(
            r"""
2026-03-09 ! "Transaction 1" "Description 1"
    Assets:UnknownAccount  -10.0 EUR
    Assets:BankAccount      10.0 EUR

2026-03-10 ! "Transaction 2" "Description 2"
    Assets:UnknownAccount  -20.0 EUR
    Assets:BankAccount      20.0 EUR

2026-03-10 ! "Transaction 3" "Description 3"
    Assets:UnknownAccount  -30.0 EUR
    Assets:BankAccount      30.0 EUR

2026-03-10 ! "Transaction 4" "Description 4"
    Assets:UnknownAccount  -40.0 EUR
    Assets:BankAccount      40.0 EUR

2026-03-11 balance Assets:BankAccount                              250.00 EUR
            """,
            entries
        )
