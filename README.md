# Beancount importer for Volksbank & GLS Bank

This is an importer for [beancount](https://github.com/beancount/beancount) to convert CSV files exported from the online banking of Volksbank or GLS Bank. 

Maybe not every volksbank uses the same software, and some may use the same software but not the same version or the same configuration. Hence, this importer may work for some banks called 'Volksbank', but probably not for all of them. At the time of writing this, it also works for the GLS Bank.

For more information on the importing process of Beancount see [Beancount's documentation on importing external data](https://beancount.github.io/docs/importing_external_data.html).

# Installation
Just copy the file 'beancount_importer_volksbank.py' to a place where python can find it. 

# Configuration

Create a file config.py:
```python
import beancount_importer_volksbank

CONFIG = [
    beancount_importer_volksbank.VolksbankImporter(
        "Assets:Volksbank",
        target_journal = "journal-volksbank.beancount")
        ]
```
The parameter `target_journal` is optional. Without it, all transaction will get the position `Unknown:account`, which then needs to be replaced manually by the correct account. With `target_journal = <filename of the journal with previous transactions>`, the importer will try to guess the accounts for the new transactions by searching for the most recent previous transaction with the same payee and assuming that the new transaction goes to the same accounts. This guess may be incorrect, but in case of many repeating transactions it can significantly reduce the manual work.

The importer has some more options and parameters, see the docstring of VolksbankImporter in 'beancount_importer_volksbank.py'.

# Usage

```bash
bean-extract config.py export.csv
```
