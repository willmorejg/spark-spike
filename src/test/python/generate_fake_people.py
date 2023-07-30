#!/usr/bin/python3

from faker import Faker
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://jim:Wiomm$001@localhost:5432/insurance', echo = True)

fake = Faker()

for _ in range(500) :
    male = True
    givenName = ""
    middleName = ""
    surname = ""
    suffix = ""
    
    if fake.boolean() :
        male = False
    
    if male :
        givenName = fake.first_name_male()
    else :
        givenName = fake.first_name_female()

    if fake.boolean() :
        if male :
            middleName = fake.first_name_male()
        else :
            middleName = fake.first_name_female()
    
    surname = fake.last_name()
    

    if fake.boolean() :
        if male :
            suffix = fake.suffix_male()
        else :
            suffix = fake.suffix_female()

    print()
    print("givenName  : " + givenName)
    print("middleName : " + middleName)
    print("surname    : " + surname)
    print("suffix     : " + suffix)
    person = [givenName, middleName, surname, suffix]
    engine.execute("INSERT INTO person (given_name, middle_name, surname, suffix) VALUES(%s,%s,%s,%s)", person)

