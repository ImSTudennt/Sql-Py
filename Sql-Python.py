import psycopg2

def create_db(con):
    with con.cursor() as cur:
    
        cur.execute("""DROP TABLE people_phones;
                       DROP TABLE phones;
                       DROP TABLE people;""")

        cur.execute("""CREATE TABLE IF NOT EXISTS people(
                       id SERIAL PRIMARY KEY, 
                       name VARCHAR(40) NOT NULL,
                       surname VARCHAR(40) NOT NULL,
                       email VARCHAR(40) UNIQUE NOT NULL);""")

        cur.execute("""CREATE TABLE IF NOT EXISTS phones(
                       id SERIAL PRIMARY KEY,
                       phone VARCHAR UNIQUE NOT NULL);""")

        cur.execute("""CREATE TABLE IF NOT EXISTS people_phones(
                       id SERIAL PRIMARY KEY,
                       people_id INTEGER NOT NULL REFERENCES people(id),
                       phone_id INTEGER NOT NULL REFERENCES phones(id));""")
        con.commit()

    con.close()


def new_client(con, name, surname, email, phone=None):
    with con.cursor() as cur:
        if phone == None:
            cur.execute("""INSERT INTO people(name,surname,email)
                           VALUES(%s, %s, %s);""", (name,surname,email))
            cur.execute("""SELECT *FROM people;""")
            print(cur.fetchall())
        else:
            cur.execute("""INSERT INTO people(name,surname,email)
                           VALUES(%s, %s, %s);""", (name,surname,email))
            cur.execute("""SELECT *FROM people;""")
            print(cur.fetchall())
            cur.execute("""INSERT INTO phones(phone)
                           VALUES(%s);""", (phone,))
            cur.execute("""SELECT *FROM phones;""")
            print(cur.fetchall())
            cur.execute("""INSERT INTO people_phones(people_id,phone_id)
                           VALUES((SELECT id FROM people where email=%s),(SELECT id FROM phones WHERE phone=%s));""", (email,phone))
            cur.execute("""SELECT *FROM people_phones;""")
            print(cur.fetchall())
        con.commit()

    con.close()


def add_phone(con, people_id, phone):
    with con.cursor() as cur:
        cur.execute("""INSERT INTO phones(phone)
                       VALUES(%s);""", (phone,))
        cur.execute("""SELECT *FROM phones;""")
        print(cur.fetchall())
        cur.execute("""INSERT INTO people_phones(people_id,phone_id)
                       VALUES(%s,(SELECT id FROM phones WHERE phone=%s));""", (people_id,phone))
        cur.execute("""SELECT *FROM people_phones;""")
        print(cur.fetchall())
        con.commit()
    con.close()


def count_phones(con,people_id):
    with con.cursor() as cur:
        cur.execute("""SELECT COUNT(*) FROM people_phones WHERE people_id = %s;""", (people_id,))
        amount = cur.fetchall()[0][0]
    return amount


def change_client(con, people_id, name=None, surname=None, email=None, phone=None):
    with con.cursor() as cur:
        if name != None:
            cur.execute("""UPDATE people
                              SET name = %s
                            WHERE id = %s;""", (name,people_id))
        if surname != None:
            cur.execute("""UPDATE people
                              SET surname = %s
                            WHERE id = %s;""", (surname,people_id))
        if email != None:
            cur.execute("""UPDATE people
                              SET email = %s
                            WHERE id = %s;""", (email,people_id))
        if phone != None and count_phones(con,people_id) == 1:
            cur.execute("""UPDATE phones
                              SET phone = %s
                            WHERE id = (SELECT phone_id FROM people_phones WHERE people_id = %s);""", (phone,people_id))
            cur.execute("""SELECT *FROM phones;""")
            print(cur.fetchall())
        elif phone != None and count_phones(con,people_id) > 1:
            number = input("Какой номер вы хотите изменить?")
            cur.execute("""UPDATE phones
                              SET phone = %s
                            WHERE phone = %s;""", (phone,number))
            cur.execute("""SELECT *FROM phones;""")
            print(cur.fetchall())
        cur.execute("""SELECT *FROM people;""")
        print(cur.fetchall())
        con.commit()
    con.close()


def delete_phone(con, people_id, phone):
    with con.cursor() as cur:
        cur.execute("""DELETE FROM people_phones 
                        WHERE people_id = %s and phone_id = (SELECT id FROM phones WHERE phone = %s);""", (people_id,phone))
        cur.execute("""DELETE FROM phones WHERE phone = %s;""", (phone,))
        cur.execute("""SELECT *FROM people_phones;""")
        print(cur.fetchall())
        cur.execute("""SELECT *FROM phones;""")
        print(cur.fetchall())
        con.commit()
    con.close()


def delete_client(con, people_id):
    with con.cursor() as cur:
        cur.execute("""DELETE FROM people_phones WHERE people_id = %s;""", (people_id,))
        cur.execute("""SELECT *FROM people_phones;""")
        print(cur.fetchall())
        cur.execute("""DELETE FROM people WHERE id = %s;""", (people_id,))
        cur.execute("""SELECT *FROM people;""")
        print(cur.fetchall())
        cur.execute("""DELETE FROM phones WHERE id not in (SELECT phone_id FROM people_phones);""")
        cur.execute("""SELECT *FROM phones;""")
        print(cur.fetchall())
        con.commit()
    con.close()


def find_client(con, first_name=None, last_name=None, email=None, phone=None):
    with con.cursor() as cur:
        cur.execute("""SELECT name, surname , email , phone 
                          FROM people p
                               LEFT JOIN people_phones pp ON p.id = pp.people_id 
                               LEFT JOIN phones p2 ON p2.id = pp.phone_id
                        WHERE name = %s or surname = %s or email = %s or phone = %s;""", (first_name,last_name,email,phone))
        print(cur.fetchall())
    con.close()


if __name__ == '__main__':
    con = psycopg2.connect(database='HW', user='postgres', password='Timofei95514444')
    # create_db(con)
    # new_client(con,'Alex','Mosunov','alex6@mail.ru', '79347457766')
    # new_client(con,'Vova','Mosunov','vlad6@mail.ru')
    # new_client(con,'Peter','Meh','peter6@mail.ru', '79347467766')
    # add_phone(con,3,'8937744444')
    # change_client(con,2,'Vladimir', surname='Mosunov')
    # change_client(con,3,phone='89342354454')
    # delete_phone(con, 3, '89342354454')
    # delete_client(con,3)
    # find_client(con,first_name='Alex')

