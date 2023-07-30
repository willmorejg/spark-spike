#!/usr/bin/python3

from faker import Faker
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://jim:Wiomm$001@localhost:5432/insurance', echo = True)

fake = Faker()

for _ in range(200) :
    address = fake.street_address()

    if "Apt" in address or "Suite" in address :
        tmpAddress = address.split(" ")
        address2 = tmpAddress[-2] + " " + tmpAddress[-1]
        tmpAddress.pop()
        tmpAddress.pop()
        address = " ".join(tmpAddress)
    else :
        address2 = ""

    city = fake.city()
    state = fake.state_abbr()
    zip = fake.zipcode()
    
    if fake.boolean() :
        zip4 = '{:04d}'.format(fake.random_int(0, 9999))
    else :
        zip4 = ""

    print()
    print("address : " + address)
    print("address2: " + address2)
    print("city    : " + city)
    print("state   : " + state)
    print("zip     : " + zip)
    print("zip4    : " + zip4)
    addr = [address, address2, city, state, zip, zip4]
    engine.execute("INSERT INTO address (address1, address2, city, state, zip_code, zip_code_4) VALUES(%s,%s,%s,%s,%s,%s)", addr)

