import random
import time
import string

# from database_code.functions_mongo import get_account_numbers
# from database_code.functions_cassandra import *
from functions_mongo import get_account_numbers
from functions_cassandra import add_transaction
from random import choices


"""
Funkcja wspomagająca generowanie losowej daty przelewu
"""
def str_time_prop(start, end, format, prop):
    
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    
    ptime = stime + prop * (etime - stime)
    
    return time.strftime(format, time.localtime(ptime))

"""
Generowanie losowej daty przelewu
"""
def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d', prop)

"""
Generowanie losowego tytułu przelewu
"""
def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

"""
Generowanie losowego przelewu
"""
def random_transactions():
    # pobranie istniejących numerów kont do wykonywania przelewów
    account_numbers = get_account_numbers()
    
    done = False
    
    # wylosowanie dwóch różnych kont
    while not done:
        source = str(choices(account_numbers)[0])
        target = str(choices(account_numbers)[0])
        
        if source != target:
            done = True
    
    # wygenerowanie daty
    transaction_date = random_date("2020-01-10", "2020-05-30", random.random())  
    # wygenerowanie kwoty
    amount = random.randint(10, 1000)
    #wygenerowanie tytułu
    title = random_string(6)
    
    # wygenerowanie przelewu
    add_transaction(source, target, transaction_date, amount, title)

"""
Wygenerowanie x liczby przelewów
"""
x = 1000
for i in range(0, x):
    random_transactions()