# DSE-python

Some useful DSE python scripts or examples

## Notes on scripts

**copy_and_scamble.py**

This is a very basic script that will copy some record from one table to another scrambling the text type data it just really a idea to perhaps build upon to help a user copy a few records from a casandra table to another one and protect and condifdential data at the same time.

Some notes on errors I got:

1. I was getting the following error: cassandra.protocol.SyntaxException: <ErrorMessage code=2000 [Syntax error in CQL query] message="line 1:4101 no viable alternative at input 'token' (...subMerchantId,taxAmount,taxAmountX,taxRate,tdSis,[token],transactionChannel...)">

this is because the work "token" is a keyword, the [ ] around the field are just the server highlighting it, so Adam H mentioned:

from cassandra.metadata import protect_name
protect_name(<field name>)

2. I was seeing the following error: ValueError: Too many arguments provided to bind() (got 1507, expected 227) this is because the prepared statement doesnt like a string sequence you need either a list or tuple
