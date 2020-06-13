import random
import time
import string

from random_pesel import RandomPESEL
from random import choices
from datetime import datetime

from functions_mongo import users_collection, add_account, add_card
from functions_elasticsearch import es_new_registration

import pandas as pd

"""
Funkcja wspomagająca generowanie losowej daty
"""
def str_time_prop(start, end, format, prop):
    
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    
    ptime = stime + prop * (etime - stime)
    
    return time.strftime(format, time.localtime(ptime))

"""
Generowanie losowej daty
"""
def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d', prop)

"""
Generowanie losowego ciągu znaków
"""
def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))



"""MONGODB DATA GENERATION"""
"""USERS COLLECTION"""

"""
Wylosowanie domeny e-mail
"""
def get_one_random_domain():  
    domains = [ "hotmail.com", "gmail.com", "aol.com", "mail.com" , "mail.kz", "yahoo.com"]
    return random.choice(domains)
    
"""
Wygenerowanie adresu e-mail
"""
def generate_random_email():

    one_name = str(random_string(5))
    one_domain = str(get_one_random_domain())         
    
    email = one_name  + "@" + one_domain
    
    return email

"""
Generowanie osoby
"""
def generate_person():
    # wygenerowanie numeru PESEL
    pesel_function = RandomPESEL()
    pesel = pesel_function.generate(min_age=18, max_age=60)
    
    # wylosowanie płci
    genders = ['Kobieta', 'Mężczyzna']
    chances = [0.4, 0.6]    
    gender = choices(genders, chances)
    gender = gender[0]
    
    # wylosowanie imienia i nazwiska w zależności od płci
    if gender == 'Kobieta':
        female_names = pd.read_csv('names.csv')['Female'].tolist()
        name = choices(female_names)
    else:
        male_names = pd.read_csv('names.csv')['Male'].tolist()
        name = choices(male_names)
    name = name[0]        
    surnames = pd.read_csv('surnames.csv')['name'].tolist()
    surname = choices(surnames)
    surname = surname[0]

    # wygenerowanie hasła    
    password = random_string(8)
    
    # wygenerowanie adresu e-mail
    email = generate_random_email()
        
    # wylosowanie członkowstwa w banku
    memberships = ['silver', 'gold', 'platinum']
    chances = [0.7, 0.2, 0.1]    
    membership = choices(memberships, chances)
    membership = membership[0]
    
    # wygenerowanie numeru telefonu
    phone = str(random.randint(100000000, 999999999))
    
    # wygenerowanie daty rejestracji
    created_at = random_date("2020-01-10", "2020-05-30", random.random())
    
    # dodanie do bazy, jeśli użytkownik jeszcze nie istnieje
    if not users_collection.find_one({"_id": pesel}) and not users_collection.find_one({"email": email}):
        account_number = add_account("Główne")
        users_collection.insert_one({"_id": pesel,
                                      "name": name,
                                      "surname": surname,
                                      "gender": gender,
                                      "password": password,
                                      "email": email,
                                      "accounts": [account_number],
                                      "membership": membership,
                                      "phone": phone,
                                      "created_at": created_at})
        es_new_registration(created_at, email, account_number)
    else:
        types = ['', 'Oszczędnościowe']
        chances = [0.5, 0.5]
        
        account_type = choices(types, chances)
        account_type = account_type[0]
        
        if account_type:
            account_number = add_account(account_type)
            if users_collection.find_one({"_id": pesel}):
                users_collection.update_one({"_id": pesel}, {"$push":{"accounts":account_number}})

"""
Pobranie istniejących kont użytkownika
"""
def get_accounts():
    accounts = list(users_collection.find({}, {"_id": 1, "accounts": 1}))
    
    return accounts

"""
Wygenerowanie karty dla konta
"""
def generate_cards():
    # wylosowanie liczby kart dla konta
    types = ['One', 'Two', '']
    chances = [0.65, 0.45, 0.05]
    
    # pobranie listy istniejących kont
    accounts = get_accounts()    
    account_numbers = []    
    for account in accounts:
        account_numbers.append(account['accounts'][0])
    
    # dodanie odpowiedniej liczby kart dla wybranego konta
    for account in account_numbers:
        how_many = choices(types, chances)
        how_many = how_many[0]
        
        if how_many == 'One':
            add_card(account)
        elif how_many == 'Two':
            add_card(account)
            add_card(account)

"""
Wygenerowanie drugiego konta dla użytkownika
"""
def add_second_account():
    
    # pobranie PESELi użytkowników
    pesels = get_accounts()    
    pesel_numbers = []   
    for pesel in pesels:
        pesel_numbers.append(pesel['_id'])
    
    # wylosowanie czy dodać konto czy nie
    yes_no = [True, False]
    chances = [0.7, 0.2]
    
    # jeśli tak to dodawane jest drugie konto
    for pesel in pesel_numbers:
        yes = choices(yes_no, chances)
        yes = yes[0]        
        if yes:
            account_number = add_account('Oszczędnościowe')
            users_collection.update_one({"_id": pesel}, {"$push":{"accounts":account_number}})

"""
Generowanie x użytkowników, kart i drugich kont
"""
people = []
start_time = datetime.now()

x = 100

for i in range(0, x):
    generate_person()
    if i%10 == 0:
        print(str(i) + ' done')
        
end_time = datetime.now()
done_in = end_time - start_time
print('Done in ' + str(done_in))     
generate_cards()
add_second_account()
    



