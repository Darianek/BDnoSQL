from cassandra.cluster import Cluster
from functions_elasticsearch import es_new_transaction
from functions_mongo import check_balance, update_balance
# from functions_elasticsearch import es_new_transaction
# from functions_mongo import check_balance, update_balance

import pandas as pd

cluster = Cluster(['127.0.0.1', '127.0.0.2', '127.0.0.3'])
# cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('treebank')

"""
Dodanie nowej transakcji
"""
def add_transaction(source, target, transaction_date, amount, title):
    
    # pobranie stanu konta, z którego ma być wykonany przelew
    source_balance = check_balance(source)
    
    # ustalenie kolejnego id transakcji
    transaction_id = session.execute('select count(*) from transaction_by_id')[0].count + 1

    amount = int(amount)
    source = str(source)
    target = str(target)

    # sprawdzenie, czy na danym koncie jest odpowiednia ilość środków
    if source_balance < amount:
        # jeśli nie, to transakcja kończy się niepowodzeniem
        succeeded = False
    else:
        # jeśli tak, można wykonać przelew
        succeeded = True
        
        # aktualizacja stanów obu kont
        update_balance(-amount, source)
        update_balance(amount, target)
        
        # ustalenie wartości dla trzech tabel
        values_by_id = str(transaction_id) + "," + str(amount) + ",'" + source + "','" + target + "','" + title + "','" + transaction_date +"')"
        values_by_source = "'" + source + "'," + "'" + transaction_date + "'," + str(transaction_id) +")"
        values_by_target = "'" + target + "'," + "'" + transaction_date + "'," + str(transaction_id) +")"
           
        # wprowadzenie powyższych wartości do bazy
        session.execute("INSERT INTO transaction_by_id(transaction_id, amount, source, target, title, transaction_date) VALUES ("+values_by_id)
        session.execute("INSERT INTO transaction_by_source(source, transaction_date, transaction_id) VALUES (" + values_by_source)
        session.execute("INSERT INTO transaction_by_target(target, transaction_date, transaction_id) VALUES (" + values_by_target)
    
    # zapisanie nowej transakcji w logach Elasticsearch'a
    es_new_transaction(transaction_date, source, target, amount, transaction_id, succeeded)
    return succeeded

"""
Wyświetlenie historii przelewów wychodzących
"""
def show_outgoing_history(account_number):
    amounts = []
    sources = []
    targets = []
    titles = []
    transaction_dates = []
    types = []
    
    # pobranie wszystkich przelewów wychodzących dla danego konta
    command = "SELECT transaction_id FROM transaction_by_source WHERE source='"+ account_number + "' "
    records = session.execute(command)
   
    for record in records:
        transaction_id = record.transaction_id
        
        # pobranie szczegółowych informacji o każdej z transakcji
        row = session.execute("SELECT * FROM transaction_by_id where transaction_id=" + str(transaction_id))
        
        amounts.append(row[0].amount)
        sources.append(row[0].source)
        targets.append(row[0].target)
        titles.append(row[0].title)
        transaction_dates.append(row[0].transaction_date)
        types.append('outgoing')
    
    # zapisanie wyników do zmiennych
    outgoig_transactions = {"source": sources, "targets": targets,
                            "amounts": amounts, "titles": titles,
                            "transaction_dates": transaction_dates,
                            "types": types}
    df = pd.DataFrame(outgoig_transactions)
    
    # posortowanie wyników według dat
    o_df = df.sort_values(by=['transaction_dates'], ascending=False)
    outgoig_transactions = df.to_dict('records')
        
    return o_df, outgoig_transactions

"""
Wyświetlenie historii przelewów przychodzących
"""
def show_incoming_history(account_number):
    amounts = []
    sources = []
    targets = []
    titles = []
    transaction_dates = []
    types = []
    
    # pobranie wszystkich przelewów przychodzących dla danego konta
    records = session.execute("SELECT transaction_id FROM transaction_by_target WHERE target='"+ account_number + "' ")
    
    for record in records:
        transaction_id = record.transaction_id
        
        # pobranie szczegółowych informacji o każdej z transakcji
        row = session.execute("SELECT * FROM transaction_by_id where transaction_id=" + str(transaction_id))
        
        amounts.append(row[0].amount)
        sources.append(row[0].source)
        targets.append(row[0].target)
        titles.append(row[0].title)
        transaction_dates.append(row[0].transaction_date)
        types.append('incoming')
    
    # zapisanie wyników do zmiennych    
    incoming_transactions = {"source": sources, "targets": targets, "amounts": amounts, "titles": titles, "transaction_dates": transaction_dates, "types": types}
    df = pd.DataFrame(incoming_transactions)
    
    # posortowanie wyników według dat
    i_df = df.sort_values(by=['transaction_dates'], ascending=False)
    incoming_transactions = df.to_dict('records')
    
    return i_df, incoming_transactions

"""
Wyświetlenie historii wszystkich transakcji
"""
def show_whole_history(account_number):
    
    # wykorzystanie wcześniej zdefiniowanych funkcji
    o_df, outgoig_transactions = show_outgoing_history(account_number)
    i_df, incoming_transactions = show_incoming_history(account_number)
    
    whole_df = o_df.append(i_df)
    
    # posortowanie wyników według dat
    whole_df = whole_df.sort_values(by=['transaction_dates'], ascending=False)
    whole_transactions = whole_df.to_dict('records')
    
    return whole_df, whole_transactions

"""
Wyświetlenie historii wybranych przelewów
"""
def show_history(account_number, which_one):
    
    if which_one == 'whole':
        df, transactions = show_whole_history(account_number)
    elif which_one == 'incoming':
        df, transactions = show_incoming_history(account_number)
    elif which_one == 'outgoing':
        df, transactions = show_outgoing_history(account_number)
    
    return df, transactions

"""
Wyświetlenie historii wybranych przelewów
"""
def new_show_history(account_number, which_one):
    
    to_show = []
    
    if which_one == 'whole':
        df, transactions = show_whole_history(account_number)
    elif which_one == 'incoming':
        df, transactions = show_incoming_history(account_number)
    elif which_one == 'outgoing':
        df, transactions = show_outgoing_history(account_number)
        
    for item in transactions:
        to_add = (str(item['transaction_dates']), str(item['source']), str(item['targets']), str(item['titles']), str(item['amounts']))
        to_show.append(to_add)
    
    return to_show

"""
Pobranie danych dla funkcji wyświetlającej historię z filtrami
"""
def get_rows(amount1, amount2, records):
    amounts = []
    sources = []
    targets = []
    titles = []
    transaction_dates = []
    types = []
    
    for record in records:
        transaction_id = record.transaction_id
        # sprawdzenie czy wyniki mają mieścić się w przedziale kwot
        if amount1:
            row = session.execute("SELECT * FROM transaction_by_id where transaction_id=" + str(transaction_id))
            
            # jeśli tak, to szukane są tylko te, które spełniają warunek
            if int(amount1) <= int(row[0].amount) <= int(amount2):
                amounts.append(row[0].amount)
                sources.append(row[0].source)
                targets.append(row[0].target)
                titles.append(row[0].title)
                transaction_dates.append(row[0].transaction_date)
                types.append('incoming')
        else:
            # w przeciwnym razie, dodawane są wszystkie wyniki
            row = session.execute("SELECT * FROM transaction_by_id where transaction_id=" + str(transaction_id))            
            amounts.append(row[0].amount)
            sources.append(row[0].source)
            targets.append(row[0].target)
            titles.append(row[0].title)
            transaction_dates.append(row[0].transaction_date)
            types.append('incoming')
    
    # zapisanie wyników do zmiennej
    transactions = {"source": sources, "targets": targets, "amounts": amounts,
                    "titles": titles, "transaction_dates": transaction_dates,
                    "types": types}
    df = pd.DataFrame(transactions)
    # posortowanie wyników według dat
    transaction_df = df.sort_values(by=['transaction_dates'], ascending=False)
    
    return transaction_df

"""
Wyświetlenie historii według podanych filtrów
"""
def history_with_filters(account_number, selection, date1, date2, amount1, amount2):
    to_show = []
    
    # sprawdzenie który filtr został wybrany i jego zastosowanie do wyświetlenia wyników   
    if selection == 'wszystkie':
        if date1:
            table_name = "transaction_by_target"
            command_range = "SELECT transaction_id FROM " + table_name + " WHERE target='"+ account_number + "' AND transaction_date >= '" + date1 + "' AND transaction_date <= '" + date2 + "'"
            records = session.execute(command_range)
            incoming_df = get_rows(amount1, amount2, records)
            table_name = "transaction_by_source"
            command_range = "SELECT transaction_id FROM " + table_name + " WHERE source='"+ account_number + "' AND transaction_date >= '" + date1 + "' AND transaction_date <= '" + date2 + "'"
            records = session.execute(command_range)
            outgoing_df = get_rows(amount1, amount2, records)
        else:
            table_name = "transaction_by_target"
            command_all = "SELECT transaction_id FROM " + table_name + " WHERE target='"+ account_number + "' "
            records = session.execute(command_all)
            incoming_df = get_rows(amount1, amount2, records)
            table_name = "transaction_by_source"
            command_all = "SELECT transaction_id FROM " + table_name + " WHERE source='"+ account_number + "' "
            records = session.execute(command_all)
            outgoing_df = get_rows(amount1, amount2, records)
        
        whole_df = outgoing_df.append(incoming_df)
        whole_df = whole_df.sort_values(by=['transaction_dates'], ascending=False)
        transactions = whole_df.to_dict('records')
    elif selection == 'uznania':
        if date1:
            table_name = "transaction_by_target"
            command_range = "SELECT transaction_id FROM " + table_name + " WHERE target='"+ account_number + "' AND transaction_date >= '" + date1 + "' AND transaction_date <= '" + date2 + "'"
            records = session.execute(command_range)
            incoming_df = get_rows(amount1, amount2, records)
        else:
            table_name = "transaction_by_target"
            command_range = "SELECT transaction_id FROM " + table_name + " WHERE target='"+ account_number + "' "
            records = session.execute(command_range)
            incoming_df = get_rows(amount1, amount2, records)
        transactions = incoming_df.to_dict('records')
    elif selection == 'obciążenia':
        if date1:
            table_name = "transaction_by_source"
            command_range = "SELECT transaction_id FROM " + table_name + " WHERE source='"+ account_number + "' AND transaction_date >= '" + date1 + "' AND transaction_date <= '" + date2 + "'"
            records = session.execute(command_range)
            outgoing_df = get_rows(amount1, amount2, records)
        else:
            table_name = "transaction_by_source"
            command_range = "SELECT transaction_id FROM " + table_name + " WHERE source='"+ account_number + "' "
            records = session.execute(command_range)
            outgoing_df = get_rows(amount1, amount2, records)
        transactions = outgoing_df.to_dict('records')
            
    for item in transactions:
        to_add = (str(item['transaction_dates']), str(item['source']), str(item['targets']), str(item['titles']), str(item['amounts']))
        to_show.append(to_add)
    
    return to_show
    