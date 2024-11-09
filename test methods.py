def ReplaceDots(price_string):
    price_string = price_string.replace(".", "'")
    price_string = price_string.replace(",", ".")
    price_string = price_string.replace("'", ",")
    return price_string


print(ReplaceDots("Hel.ou,"))
