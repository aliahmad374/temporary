import scrapy
import requests
import random
import time
import mysql.connector
import pandas as pd


car_data = pd.read_csv('toyodiy_data.csv')
car_data_dictionaries = car_data.to_dict('records')
def create_database():
    my_db = mysql.connector.connect(
      host="127.0.0.1",
      user="root",
      password="root",
      database="toyodiy"
    )
    return my_db

MY_DB = create_database()
my_cursor = MY_DB.cursor()


all_urls = car_data.URL
try:
    urls_file = open("links_done.txt", "r")
    urls = urls_file.readlines()
    total_urls = [v.strip() for v in urls]
    urls_file.close()
except:
    total_urls = []
left_cars_urls = []
for loop_car_left in car_data_dictionaries:
    if loop_car_left['URL'] not in total_urls:
        left_cars_urls.append(loop_car_left)


car_data = pd.DataFrame(left_cars_urls)


urls_file = open("links_done.txt", "a")

count = 0

for group_name_make, df_group_make in car_data.groupby('make'):
    get_make_query = f""" SELECT id FROM  `toyodiy`.`manufacturer` where make ='{group_name_make}'"""
    my_cursor.execute(get_make_query)
    try:
        get_make_id = [my_cursor.fetchall()[0][0]]
    except:
        get_make_id = []


    if len(get_make_id) < 1:
        insert_into_manufacturer = """ INSERT INTO `toyodiy`.`manufacturer` (`make`) VALUES (%s)"""

        value = [group_name_make]

        my_cursor.execute(insert_into_manufacturer, value)
        MY_DB.commit()
        get_manufacturer_id = """ SELECT MAX(id) FROM  `toyodiy`.`manufacturer`"""

        my_cursor.execute(get_manufacturer_id)
        get_make_id = my_cursor.fetchall()[0][0]
    else:
        get_make_id = get_make_id[0]

    for group_name_model, df_group_model in df_group_make.groupby('model'):

        get_make_query = f""" SELECT id FROM  `toyodiy`.`model` where model ='{group_name_model}' and manufacturer_id ={get_make_id}"""
        my_cursor.execute(get_make_query)
        try:
            get_model_id = [my_cursor.fetchall()[0][0]]
        except:
            get_model_id = []

        if len(get_model_id) < 1:

            insert_into_model = """ INSERT INTO `toyodiy`.`model` (`manufacturer_id`,`model`) VALUES (%s,%s)"""

            value = [get_make_id, group_name_model]

            my_cursor.execute(insert_into_model, value)
            MY_DB.commit()
            my_cursor = MY_DB.cursor()

            get_model_id = """ SELECT MAX(id) FROM  `toyodiy`.`model`"""

            my_cursor.execute(get_model_id)
            get_model_id = my_cursor.fetchall()[0][0]

        else:
            get_model_id = get_model_id[0]

        for group_name_year, df_group_year in df_group_model.groupby('Year'):

            get_make_query = f""" SELECT id FROM  `toyodiy`.`type_year` where manufacturer_id ={get_make_id} and model_id ='{get_model_id}' and year ={group_name_year}"""
            my_cursor.execute(get_make_query)
            try:
                get_year_id = [my_cursor.fetchall()[0][0]]
            except:
                get_year_id = []


            if len(get_year_id)<1:

                insert_into_model = """ INSERT INTO `toyodiy`.`type_year` (`manufacturer_id`,`model_id`,`year`) VALUES (%s,%s,%s)"""

                value = [get_make_id,get_model_id, int(group_name_year)]

                my_cursor.execute(insert_into_model, value)
                MY_DB.commit()

                get_vehicle_query = """ SELECT MAX(id) FROM  `toyodiy`.`type_year`"""

                my_cursor.execute(get_vehicle_query)
                get_year_id = my_cursor.fetchall()[0][0]

            else:
                get_year_id = get_year_id[0]

            for group_name_vehicle, df_group_vehicle in df_group_model.groupby('engine'):

                get_make_query = f""" SELECT id FROM  `toyodiy`.`vehicle_engine` where manufacturer_id ={get_make_id} and model_id ='{get_model_id}' and type_year_id ={get_year_id} and engine_power ='{group_name_vehicle}'"""
                my_cursor.execute(get_make_query)
                try:
                    get_vehicle_id = [my_cursor.fetchall()[0][0]]
                except:
                    get_vehicle_id = []

                if len(get_vehicle_id)<1:
                    insert_into_model = """ INSERT INTO `toyodiy`.`vehicle_engine` (`manufacturer_id`,`model_id`,`type_year_id`,`engine_power`) VALUES (%s,%s,%s,%s)"""

                    value = [get_make_id,get_model_id,get_year_id, group_name_vehicle]

                    my_cursor.execute(insert_into_model, value)
                    MY_DB.commit()

                    get_vehicle_engine_query = """ SELECT MAX(id) FROM  `toyodiy`.`vehicle_engine`"""

                    my_cursor.execute(get_vehicle_engine_query)
                    get_vehicle_id = my_cursor.fetchall()[0][0]

                else:
                    get_vehicle_id = get_vehicle_id[0]


                for loop_parts in df_group_vehicle.to_dict('records'):
                    item = dict()
                    url = loop_parts['URL']
                    main_link = requests.get(url=url)
                    time.sleep(random.randint(1, 6))
                    total_data = scrapy.Selector(text=main_link.text)
                    try:
                        item["engine_code"] = "".join([t.css('::text').get() for t in total_data.css("#cap a[title]")
                                                       if "0CC" in t.get()])
                    except:
                        item["engine_code"] = ""
                    for j in total_data.css("div#page2 ol li a"):
                        item["category"] = j.css("::text").get()

                        try:
                            find_categoryid_query = f""" SELECT id FROM  `toyodiy`.`category` where manufacturer_id ={get_make_id} and model_id ='{get_model_id}' and type_year_id ={get_year_id} and vehicle_id={get_vehicle_id} and category_name ='{item["category"]}'"""
                            my_cursor.execute(find_categoryid_query)
                        except:
                            print('error')
                            pass
                        try:
                            get_category_id = [my_cursor.fetchall()[0][0]]
                        except:
                            get_category_id = []


                        if len(get_category_id) < 1:
                            insert_into_category = """ INSERT INTO `toyodiy`.`category` (`manufacturer_id`,`model_id`,`type_year_id`,`vehicle_id`,`category_name`) VALUES (%s,%s,%s,%s,%s)"""

                            value = [get_make_id,get_model_id,get_year_id,get_vehicle_id,item["category"]]

                            my_cursor.execute(insert_into_category, value)
                            MY_DB.commit()

                            get_category_query = """ SELECT MAX(id) FROM  `toyodiy`.`category`"""

                            my_cursor.execute(get_category_query)
                            get_category_id = my_cursor.fetchall()[0][0]

                        else:
                            get_category_id = get_category_id[0]
                        category_link = requests.get(url="https://www.toyodiy.com/parts/" + j.css("::attr(href)").get())
                        time.sleep(random.randint(1, 6))
                        category_resp = scrapy.Selector(text=category_link.text)
                        for k in category_resp.css(".diag-list a"):
                            item["sub_category"] = k.css("::text").get().split(":")[-1]

                            find_subcategoryid_query = f""" SELECT id FROM  `toyodiy`.`sub_category` where category_id={get_category_id} and sub_category_name ='{item["sub_category"]}'"""
                            my_cursor.execute(find_subcategoryid_query)
                            try:
                                get_subcategory_id = [my_cursor.fetchall()[0][0]]
                            except:
                                get_subcategory_id = []


                            if len(get_subcategory_id) < 1:
                                insert_into_category = """ INSERT INTO `toyodiy`.`sub_category` (`category_id`,`sub_category_name`) VALUES (%s,%s)"""

                                value = [get_category_id,item["sub_category"]]

                                my_cursor.execute(insert_into_category, value)
                                MY_DB.commit()

                                get_subcategory_query = """ SELECT MAX(id) FROM  `toyodiy`.`sub_category`"""
                                my_cursor.execute(get_subcategory_query)
                                get_subcategory_id = my_cursor.fetchall()[0][0]

                            else:
                                get_subcategory_id = get_subcategory_id[0]


                            subcategory_data = requests.get(
                                url="https://www.toyodiy.com/parts/" + k.css("::attr(href)").get())
                            time.sleep(random.randint(1, 6))
                            item["source"] = "https://www.toyodiy.com/parts/" + k.css("::attr(href)").get()
                            sub_category_resp = scrapy.Selector(text=subcategory_data.text)
                            for p in sub_category_resp.css("#d3 table tbody"):
                                subcategory_part_name = p.css("tr.h td[colspan]::text").get()
                                item["part_name"] = subcategory_part_name
                                for g in p.css("tr")[1:]:
                                    try:
                                        subcategory_part_id = g.css("tr td:nth-of_type(1)::text").get()
                                        item["part_number"] = subcategory_part_id
                                    except:
                                        item["part_number"] = ""
                                    try:
                                        subcategory_part_quantity = g.css("tr td:nth-of_type(3)::text").get()
                                        item["quantity_required"] = subcategory_part_quantity
                                    except:
                                        item["quantity_required"] = ""
                                    count = count + 1
                                    print(count)

                                    find_part_query = f""" SELECT id FROM  `toyodiy`.`parts` where sub_category_id={get_subcategory_id} and engine_code='{item['engine_code']}' and part_number='{item["part_number"]}'"""
                                    my_cursor.execute(find_part_query)
                                    try:
                                        get_part_id = [my_cursor.fetchall()[0][0]]
                                    except:
                                        get_part_id = []

                                    if len(get_part_id) <1:
                                        query2 = """ INSERT INTO `toyodiy`.`parts` (`sub_category_id`,`engine_code`,`market`,`part_number`,`part_name`,`quantity_required`,`part_source`,`price`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

                                        value2 = [get_subcategory_id,item['engine_code'],loop_parts['Market'] ,item["part_number"], item["part_name"],
                                                   item["quantity_required"], item["source"],0]

                                        try:
                                            my_cursor.execute(query2, value2)
                                            MY_DB.commit()
                                            print('Done')
                                        except:
                                            a=1
                                            pass
                    urls_file.write(url+'\n')
                    urls_file.flush()


