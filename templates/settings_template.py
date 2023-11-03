# PARAMETERS

import streamlit as st
import pandas as pd

from database import DBMenu, DBDrinks, DBProductKeywords, DBServiceKeywords, DBAmbienceKeywords



def settings_template_doc(name_db, name_user, section):
    service_keywords = [
        'service', 'waiter', 'waitress', 'waiters', 'waitresses', 'waitstaff',
        'host', 'hostess', 'hostesses', 'hosts', 'server', 'servers', 'staff',
        'price', 'bill', 'rush', 'rushing', 'rushes', 'rushed', 'rush','birthday',
        'pricing', 'priced', 'pricey', 'expensive', 'expensiveness', 'expensiveness',
    ]

    key_words_related_to_ambience = ['atmosphere', 'ambience', 'chairs', 'tables', 'seating',
                                        'decor', 'interior', 'design', 'lighting', 'music', 'sound',
                                        'noise', 'noisy', 'quiet', 'loud', 'crowded', 'crowd', 'crowds',
                                        'crowding', 'clean', 'peaceful', 'toilets','parking','toilet',
                                        
    ]
    
    food_keywords = [
        'cocktails', 'chilli', 'chutney', 'chutneys',
        'icecream', 'ice-cream', 'ice cream', 'icecreams', 'ice-creams', 'ice creams',
        'food', 'taste', 'tastes', 'flavor', 'flavors', 'dish', 'dishes', 'menu',
        'meal', 'meals', 'portions', 'portion', 'portioned', 'portioning', 'portioned',
        'quality', 'quantity', 'quantities', 'quantitied', 'quantitied', 'quantitied', 'Biryani']

    keywords = service_keywords + key_words_related_to_ambience + food_keywords

    menu_items_lookup = [
            'Akuri',
            'B & E Naan',
            'Bacon Naan',
            'Basmati Kheer',
            'Basmati Rice',
            'Bhel',
            'Big Bombay',
            'Black Daal',
            'Bowl of Greens',
            'BS Keema Pau',
            'BS Pau Bhaji',
            'Butter Corn OTC',
            'Butter Paanch',
            'Calamari',
            'Ch. Chawal (V)',
            'Ch. Puri(V)',
            'Chck Tikka Roll',
            'Cheese Naan',
            'Chicken Biryani',
            'Chicken Ruby',
            'Chicken Tikka',
            'Chilli Br Salad',
            'Chilli Chicken',
            'Choc Pud w/chil',
            'Choc Pud w/Cinn',
            'Chocolate Chai',
            'Chole Puri',
            'Dbl Bacon Naan',
            'Egg Naan',
            'Garlic Naan',
            'Gulab Jamun',
            'GunpowderPotato',
            'House Chaat',
            'Jackfruit Birya',
            'Kachumber',
            'Keema Per Eedu',
            'Kejriwal',
            'Khichia WChundo',
            'Kulfi Malai',
            'Kulfi Mango',
            'Kulfi Pistach.',
            'Lamb Chops',
            'Lamb Raan',
            'Lamb Raan Bun',
            'Lamb Samosas',
            'Mango& Fenn',
            'Masala Prawns',
            'Mattar Paneer',
            'Murgh Malai',
            'Mutton Chp Korm',
            'Mutton Pepper F',
            'Nihari',
            'Nihari Biryani',
            'Okra Fries',
            'Paneer Roll',
            'Paneer Tikka',
            'Parsi Omelette',
            'Pineapple Tikka',
            'Plain Naan',
            'Prawn Koliwada',
            'Raita',
            'Roomali Roti',
            'S & E Naan',
            'Saus Naan',
            'Sheekh Kabab',
            'Side Beans',
            'Tandoor  Roti',
            'Uttapam Stack',
            'Vada Pau',
            'Veg. Samosas',
            'Vegan Bombay',
            'Vegan Saus Naan',
            'Wrestler Naan',
            'Xmas Feast',
            'CCT',
        ]

    drink_items_lookup = [
            'Beer',
            'Colaba Colada',
            'Mojito',
            'Mango Lassi',
            'Breakfast Lassi',
            'Coffee',
            'Americano',
            'Espresso',
            'Latte',
            '33cl Kingfisher',
            '65cl Kingfisher',
            'Aflatoon',
            'Amb. Viog Btl',
            'Bob Sauv Blanc',
            'Bolly Bellini',
            'Quinde Gls',
            'Quinde Malbec',
            'Sali Boti Full',
            'Sali Boti Half',
            'Sonia Negroni',
            'Sparkling Water',
            'Spumante Btl',
            'Spumante Gls',
            'Still Water',
            'Viceroy OF',
            'Watermelon Shbt',
            'Syrah Punctum',
            'Kohinoor Fizz',
            'Meenakshi Moj',
            'Nimbu Soda',
            'Oat Chai',
            'P. Grig Btl',
            'P. Grigio Gls',
            'Passionfr Shbt',
            'Peacock Cider',
            'Grenache Btl',
            'Grenache Gls',
            'Chai',
            'handra Bose',
            'Coloba Colada',
            'Cyrus Irani',
            'Dhoble',
            'Diet Coke 33cl',
            'Dish Esp Mtini',
            'Dishoom IPA',
            'East IndiaGim',
            'Folle Grillo Bt',
            'FolleGrillo Gls',
            'Fresh Orange J']

    options_for_classification = [
        'Welcome Not First Class',
        'Welcome Not Big Hearted',
        'Team Missed Special Occasions',
        'Team Missed Allergen / Dietary Requirement',
        'Team Service Not Big Hearted',
        'Team Service Not First Class',
        'Team Failed Service Recovery',
        'Bill Overcharged / Incorrect',
        'Overall End 2 End Poor Experience',
        'Overall Underwhelming / Average Experience',
        'Atmosphere',
        'Cleanliness / Safety Concerns',
        'Drink Delivery Slow',
        'Drinks Missing / Incorrect',
        'Drinks Average Feedback',
        'Drinks Poor Feedback',
        'Food Delivery Slow',
        'Food Missing / Incorrect',
        'Food Average Feedback',
        'Food Poor Feedback',
        'Dishoom General Offering',
        'Dishoom Design',
        'Dishoom Booking Policy / Queue',
        'Dishoom Price',
        'Dishoom Service Charge',
        'Too loud',
        'Other (No Clear Category)',
        '',
        'nan'
    ]

    db_menu_food = DBMenu()
    db_drinks_menu = DBDrinks()
    db_product_keywords = DBProductKeywords()
    db_service_key_words = DBServiceKeywords()
    db_ambience = DBAmbienceKeywords()

    expander_menu = st.expander('Menu')
    # write description
    expander_menu.write('This is the menu for food and drinks. You can add, delete and edit the menu items. You can also save the menu items to the database.')
    col_food, col_drinks = expander_menu.columns(2)

    c1,c2,c3 = col_food.columns(3)
    button_delete = c2.button('Delete all')
    if button_delete:
        db_menu_food.delete_all()

    button_populate = c1.button('Default Menu')
    if button_populate:
        db_menu_food.delete_all()
        db_menu_food.populate_db(menu_items_lookup)

    menu = col_food.data_editor(db_menu_food.view(as_df=True), num_rows='dynamic', use_container_width=True)

    button_save = c3.button('Save')
    if button_save:
        list_of_menu_items = menu['name'].tolist()
        # keep non empty values and not none
        list_of_menu_items = [item for item in list_of_menu_items if item is not None]
        list_of_menu_items = [item for item in list_of_menu_items if item != '']
        db_menu_food.save_all(list_of_menu_items)


    # drinks
    c1,c2,c3 = col_drinks.columns(3)
    button_delete = c2.button('Delete all (drinks)')
    if button_delete:
        db_drinks_menu.delete_all()

    button_populate = c1.button('Default Drinks')
    if button_populate:
        db_drinks_menu.delete_all()
        db_drinks_menu.populate_db(drink_items_lookup)


    menu_drinks = col_drinks.data_editor(db_drinks_menu.view(as_df=True), num_rows='dynamic', use_container_width=True)

    button_save = c3.button('Save (drinks)')
    if button_save:
        list_of_drink_items = menu_drinks['name'].tolist()
        # keep non empty values and not none
        list_of_drink_items = [item for item in list_of_drink_items if item is not None]
        list_of_drink_items = [item for item in list_of_drink_items if item != '']
        db_drinks_menu.save_all(list_of_drink_items)

    # product keywords
    # home icon is this -> 🏠 
    # a more informal is this -> 🏡 or this -> 🏚️ or this -> 🏘️ or this 

    expander_keywords = st.expander('Product Keywords')
    # when the Reviews contains one of this words will be classified as 'Product' related.
    expander_keywords.write('This is the list of keywords that will be used to classify the reviews as **Product** related. You can add, delete and edit the keywords. You can also save the keywords to the database.')
    c1,c2,c3 = expander_keywords.columns(3)
    button_delete = c2.button('Delete all (product keywords)')
    if button_delete:
        db_product_keywords.delete_all()

    button_populate = c1.button('Default Product Keywords')
    if button_populate:
        db_product_keywords.delete_all()
        db_product_keywords.populate_db(food_keywords)


    menu_product_keywords = expander_keywords.data_editor(db_product_keywords.view(as_df=True), num_rows='dynamic', use_container_width=True)
    button_save = c3.button('Save (product keywords)')

    if button_save:
        list_of_product_keywords = menu_product_keywords['name'].tolist()
        # keep non empty values and not none
        list_of_product_keywords = [item for item in list_of_product_keywords if item is not None]
        list_of_product_keywords = [item for item in list_of_product_keywords if item != '']
        db_product_keywords.save_all(list_of_product_keywords)

    # service keywords
    expander_service_keywords = st.expander('Service Keywords')
    # when the Reviews contains one of this words will be classified as 'Service' related.
    expander_service_keywords.write('This is the list of keywords that will be used to classify the reviews as **Service** related. You can add, delete and edit the keywords. You can also save the keywords to the database.')

    c1,c2,c3 = expander_service_keywords.columns(3)
    button_delete = c2.button('Delete all (service keywords)')
    if button_delete:
        db_service_key_words.delete_all()

    button_populate = c1.button('Default Service Keywords')
    if button_populate:
        db_service_key_words.delete_all()
        db_service_key_words.populate_db(service_keywords)


    menu_service_keywords = expander_service_keywords.data_editor(db_service_key_words.view(as_df=True), num_rows='dynamic', use_container_width=True)

    button_save = c3.button('Save (service keywords)')
    if button_save:
        list_of_service_keywords = menu_service_keywords['name'].tolist()
        # keep non empty values and not none
        list_of_service_keywords = [item for item in list_of_service_keywords if item is not None]
        list_of_service_keywords = [item for item in list_of_service_keywords if item != '']
        db_service_key_words.save_all(list_of_service_keywords)

    # ambience keywords
    expander_ambience_keywords = st.expander('Ambience Keywords')
    # when the Reviews contains one of this words will be classified as 'Ambience' related.
    expander_ambience_keywords.write('This is the list of keywords that will be used to classify the reviews as **Ambience** related. You can add, delete and edit the keywords. You can also save the keywords to the database.')
    c1,c2,c3 = expander_ambience_keywords.columns(3)
    button_delete = c2.button('Delete all (ambience keywords)')
    if button_delete:
        db_ambience.delete_all()

    button_populate = c1.button('Default Ambience Keywords')
    if button_populate:
        db_ambience.delete_all()
        db_ambience.populate_db(key_words_related_to_ambience)


    menu_ambience_keywords = expander_ambience_keywords.data_editor(db_ambience.view(as_df=True), num_rows='dynamic', use_container_width=True)

    button_save = c3.button('Save (ambience keywords)')

    if button_save:
        list_of_ambience_keywords = menu_ambience_keywords['name'].tolist()
        # keep non empty values and not none
        list_of_ambience_keywords = [item for item in list_of_ambience_keywords if item is not None]
        list_of_ambience_keywords = [item for item in list_of_ambience_keywords if item != '']
        db_ambience.save_all(list_of_ambience_keywords)

