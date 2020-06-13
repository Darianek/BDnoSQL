from elasticsearch import  Elasticsearch

ES_HOST = {"host": "localhost", "port": 9200}
es = Elasticsearch(hosts=[ES_HOST])

INDEX_NAME = 'treebank'
# response = es.indices.create(index=INDEX_NAME)

"""
Przesłanie logów o nowej rejestracji
"""
def es_new_registration(date, email, account_number):   
    data = {
        "operation": "new_registration",
        "email": email,
        "account_number": account_number,
        "date": date
        }
    
    es.index(index=INDEX_NAME, doc_type='operations', body=data)

"""
Przesłanie logów o nowym logowaniu
"""
def es_new_login(date, email, succeeded):
    date = date.strftime("%Y-%m-%d")
    data = {
        "operation": "new_login",
        "email": email,
        "succeeded": succeeded,
        "date": date
        }
    
    es.index(index=INDEX_NAME, doc_type='operations', body=data)

"""
Przesłanie logów o nowej transakcji
"""
def es_new_transaction(date, source, target, amount, transaction_id, succeeded):
    
    data = {"operation": "new_transaction",
            "source": source,
            "target": target,
            "amount": amount,
            "succeeded": succeeded,
            "date": date,
            }
    
    es.index(index=INDEX_NAME, doc_type='operations', body=data)

"""
Wyszukanie w logach transakcji danego użytkownika
"""
def look_for_transactions(email):
    from functions_mongo import get_account_number_and_balance

    # pobranie numerów kont i ich sald dla wybranego użytkownika
    accounts, balances = get_account_number_and_balance(email)
    
    data = []
    
    for account in accounts:
        search = {
                "query": {
                        "multi_match" : {
                          "query":    account, 
                          "fields": [ "source", "target" ] 
                        }
                      }
            }
        
        # wyszukanie przelewów użytkownika zarówno przychodzących jak i
        # wychodzących ("fields": [ "source", "target" ])
        res = es.search(index=INDEX_NAME, body=search, size=9999)
        if not res['hits']['hits']:
            # jeśli nie ma wyników, zostają zwrócone odpowiednie wartości
            return False
        else:
            # jeśli sa wyniki, to zostają zapisane do zmiennej
            for record in res['hits']['hits']:
                data.append(record['_source'])
    
    return data

"""
Przeszukiwanie logów
"""
def search(what, email, operation_type, date1, date2):
    
    # sprawdzenie jaki filtr został zastosowany     
    if what == 'email':
        search={
          'query':{
              'match':{
                  what: {
                      "query": email,
                      } 
                  }
              }
          }
        
        # pobranie logów dotyczących danego użytkownika
        res = es.search(index=INDEX_NAME, body=search, size=9999)
        data = []
        autocorrect = set()
        
        if not res['hits']['hits']:
            # jeśli nie ma wyników, zostają zwrócone odpowiednie wartości
            return False, False
        else:
            # jeśli są wyniki, to zostają zapisane do zmiennej, zależnie od
            # wybranego filtra
            for record in res['hits']['hits']:
                if record['_source']['email'] == email:
                    if operation_type == 'wszystkie':
                        data.append(record['_source'])
                        new = look_for_transactions(email)
                        if new:
                            data.extend(new)
                    elif operation_type == 'rejestracja':
                        if record['_source']['operation'] == 'new_registration':
                            data.append(record['_source'])
                    elif operation_type == 'transakcje':
                        new = look_for_transactions(email)
                        if new:
                            data.extend(new)
                    elif operation_type == 'logowanie':
                        if record['_source']['operation'] == 'new_login':
                            data.append(record['_source'])
            if not data:
                # jeśli zdarzy się, że mógł zostać podany zły e-mail, to
                # zostaną pobrane, te które administrator miał na myśli
                # w celu ich zaproponowania
                for record in res['hits']['hits']:
                    autocorrect.add(record['_source']['email'])
                return False, autocorrect
            else:
                return True, data    
    elif what == 'date':
        registrations = 0
        logins_good = 0
        logins_bad = 0
        transactions = 0
        data = []
        search={
            "query": {
                    "range" : {
                        what : {
                            "gte" : date1,
                            "lte" : date2,
                        }
                    }
                }
            }
        # pobranie logów z podanego przedziału czasowego
        res = es.search(index=INDEX_NAME, body=search, size=9999)
        
        if not res['hits']['hits']:
            # jeśli nie ma wyników, zostają zwrócone odpowiednie wartości
            return False, False, False, False
        else:
            # jeśli są wyniki, to logi zostają zapisane do zmiennej
            for record in res['hits']['hits']:
                data.append(record['_source'])
        
        # aktualizacja statystyk na podstawie wcześniej zebranych logów
        for d in data:
            if d['operation'] == 'new_registration':
                registrations += 1
            elif d['operation'] == 'new_login':
                if d['succeeded']:
                    logins_good +=1
                else:
                    logins_bad += 1
            elif d['operation'] == 'new_transaction':
                transactions += 1
        
        return registrations, logins_good, logins_bad, transactions
        