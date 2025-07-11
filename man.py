import cfg
from bl import business_logic_function
from popdic import populate_dictionary

def runnit():
    populate_dictionary()

    while cfg.current_contracts is not None:
        business_logic_function()

if __name__ == "__main__":
    runnit()
