from functions_elasticsearch import es_new_registration
from functions_redis import q
# # from functions_elasticsearch import *
# # from functions_redis import *
from datetime import timedelta
from datetime import  datetime
from random import choices
import time
import random

import pymongo
myclient = pymongo.MongoClient()
mydb = myclient["bankAPP"]
users_collection = mydb["users"]
accounts_collection = mydb["accounts"]
cards_collection = mydb["cards"]

"""
Pobranie numerów kont użytkownika
"""
def get_account_numbers():
    account_numbers = []
    
    accounts = accounts_collection.find({}, {"_id": 1})
    
    for account in accounts:
        account_numbers.append(account['_id'])

    return account_numbers

"""
Pobranie numerów kont użytkownika i salda każdego z nich
"""
def get_account_number_and_balance(email):
    
    accounts = users_collection.find({"email": email},
                                     {"_id": 0, "accounts": 1})[0]['accounts']
    balances = []
    
    for account in accounts:
        balance = accounts_collection.find({"_id": account},
                                           {"_id": 0, "balance": 1})[0]
        balances.append(balance['balance'])

    return accounts, balances

"""
Pobranie salda konta o podanym numerze
"""
def check_balance(account_number):
    balance = accounts_collection.find_one({"_id": account_number},
                                           {"balance": "1"})['balance']

    return balance

"""
Aktualizacja stanu konta o podanym numerze
"""
def update_balance(amount, account_number):
    current_balance = check_balance(account_number)
    new_balance = current_balance + amount
    accounts_collection.update_one({"_id": account_number},
                                   {"$set":{"balance": new_balance}})
    
"""
Sprawdzenie poprawności danych logowania oraz czy dany użytkownik w ogóle
istnieje
"""
def log_in(email, password):
    
    user = users_collection.find_one({"email": email})
    
    if email == 'admin@a.pl' and password == 'admin123':
        return 'admin'
    elif user:
        if password == user['password']:
            return 'mozna'
        else:
            return 'haslo'
    else:
        return 'brak'
    

"""
Funkcja do losowania stanu konta podczas generacji danych
"""
def divisible_random(a,b,n):
    if b-a < n:
      raise Exception('{} is too big'.format(n))
    result = random.randint(a, b)
    while result % n != 0:
      result = random.randint(a, b)
    return result

"""
Dodawanie nowego konta
"""
def add_account(account_type):
    account_number = str(random.randint(100000, 999999))
    
    created = False
    
    # balance = divisible_random(10, 100000, 10)
    
    while not created:
        # sprawdzenie, czy konto o wylosowanym numerze znajduje się w bazie
        if accounts_collection.find_one({"_id": account_number}):
            account_number = str(random.randint(100000, 999999))
        else:
            created_at = datetime.now()
            # jeśli nie, konto zostaje dodane do kolekcji
            accounts_collection.insert_one({"_id": account_number,
                                            "type": account_type,
                                            "balance": 0,
                                            # "balance": balance,
                                            "cards": [],
                                            "created_at": created_at})
            created = True
    
    return account_number

"""
Dodawanie karty dla podanego konta, na daną chwilę karta jest tworzona z
losowymi wartościami. Domyślnie użytkownik powinien sam zdefiniować limit.
"""
def add_card(account_number):
    
    card_number = str(random.randint(10000, 99999))
    
    limits = [500, 1000, 1500, 3000]
    limit = choices(limits)
    limit = limit[0]
    
    expiry_date = datetime.now() + timedelta(days=730)
    cvv = str(random.randint(100,999))

    created = False
    
    while not created:
        # sprawdzenie, czy w bazie nie ma już karty o wylosowanym numerze
        if accounts_collection.find_one({"cards": card_number}) or cards_collection.find_one({"_id": card_number}):
            card_number = str(random.randint(10000, 99999))
        else:
            # jeśli nie ma to karta zostaje dodana do kolekcji
            cards_collection.insert_one({"_id": card_number,
                                         "limit": limit,
                                         "activated": True,
                                         "expiry_date": expiry_date,
                                         "CVV": cvv})
            # karta zostaje dopisana do kart danego konta
            accounts_collection.update_one({"_id": account_number},
                                       {"$push": {"cards": card_number}})
            created = True
    
    return card_number

"""
Zapisanie użytkownika w MongoDB
"""
def mongo_insert(pesel, name, surname, gender, password, email, phone,
                 account_number, created_at):
    
    # wydłużenie czasu, aby przedstawić działanie kolejki w Redisie
    time.sleep(60)  
    
    # dodanie użytkownika o wprowadzonych danych do bazy
    users_collection.insert_one({"_id": pesel,
                                     "name": name,
                                     "surname": surname,
                                     "gender": gender,
                                     "password": password,
                                     "email": email,
                                     "accounts": [account_number],
                                     "membership": "silver",
                                     "phone": phone,
                                     "created_at": created_at})

"""
Rejestracja nowego użytkownika
"""
def register(pesel, name, surname, gender, password, email, phone):
    # sprawdzenie, czy użytkownik o podanym PESELu lub emailu jest już w bazie
    pesel_exists = bool(users_collection.find_one({"_id": pesel}))
    email_exists = bool(users_collection.find_one({"email": email}))
    
    # jeśli nie to można zlecić wprowadzenie użytkownika do bazy
    if not pesel_exists and not email_exists:
        created_at = datetime.now()
        account_number = add_account("Główne")
        
        # wykorzystanie kolejki w Redis'ie
        q.enqueue(mongo_insert, pesel, name, surname, gender, password,
                  email, phone, account_number, created_at)
        
        # przesłanie logów do Elasticsearch'a
        es_new_registration(created_at, email, account_number)
        return True
    else:
        return False