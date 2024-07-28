import math
import pymysql
import requests, os, gzip, hashlib, json
from lxml import html
from sql_queries import main_cat_query, sub_cat_query, products_query


def req_sender(url: str, method: str, query_dict: dict = None, cookies: dict = None, headers: dict = None) -> bytes or None:
    # Prepare headers for the HTTP request

    # Send HTTP request
    _response = requests.request(method=method, url=url, data=query_dict, cookies=cookies, headers=headers)
    # Check if response is successful
    if _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    return _response  # Return the response if successful


def ensure_dir_exists(dir_path: str):
    # Check if directory exists, if not, create it
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        print(f'Directory {dir_path} Created')  # Print confirmation of directory creation


def page_checker(url: str, method: str, directory_path: str, query_dict: dict = None):
    # Create a unique hash for the URL to use as the filename
    page_hash = hashlib.sha256(string=url.encode(encoding='UTF-8', errors='backslashreplace')).hexdigest()
    ensure_dir_exists(dir_path=directory_path)  # Ensure the directory exists
    file_path = os.path.join(directory_path, f"{page_hash}.html.gz")  # Define file path
    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Filename is {page_hash}")
        with gzip.open(filename=file_path, mode='rb') as file:
            file_text = file.read().decode(encoding='UTF-8', errors='backslashreplace')  # Read and decode file
        return file_text  # Return the content of the file
    else:
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        _response = req_sender(url=url, method=method, query_dict=query_dict)  # Send the HTTP request
        if _response is not None:
            print(f"Filename is {page_hash}")
            with gzip.open(filename=file_path, mode='wb') as file:
                if isinstance(_response, str):
                    file.write(_response.encode())  # Write response if it is a string
                    return _response
                file.write(_response.content)  # Write response content if it is bytes
            return _response.text  # Return the response text


def page_checker_json(url: str, method: str, directory_path: str, cookies: dict = None, headers: dict = None, query_dict: dict = None):
    # Create a unique hash for the URL and data to use as the filename
    hash_input = url + json.dumps(query_dict, sort_keys=True)  # Combine URL and data for hashing
    page_hash = hashlib.sha256(hash_input.encode('UTF-8')).hexdigest()
    ensure_dir_exists(dir_path=directory_path)  # Ensure the directory exists
    file_path = os.path.join(directory_path, f"{page_hash}.json")  # Define file path

    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Filename is {page_hash}")
        with open(file_path, 'r', encoding='UTF-8') as file:
            file_text = file.read()  # Read the file
        return json.loads(file_text)  # Return the content as a dictionary

    else:
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        _response = req_sender(url=url, method=method, query_dict=query_dict, cookies=cookies, headers=headers)  # Send the GET request
        if _response is not None:
            response_json = _response.json()  # Get the JSON response
            print(f"Filename is {page_hash}")
            with open(file_path, 'w', encoding='UTF-8') as file:
                json.dump(response_json, file, ensure_ascii=False, indent=4)  # Write JSON response to file
            return response_json  # Return the JSON response as a dictionary


class Scraper:
    def __init__(self):
        # Connecting to the Database
        connection = pymysql.connect(host='localhost', user='root', database='baslerweb_db', password='actowiz', charset='utf8mb4', autocommit=True)
        if connection.open:
            print('Database connection Successful!')
        else:
            print('Database connection Un-Successful.')
        self.cursor = connection.cursor()

        # Creating Table in Database If Not Exists
        try:
            self.cursor.execute(query=main_cat_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=sub_cat_query)
        except Exception as e:
            print(e)
        try:
            self.cursor.execute(query=products_query)
        except Exception as e:
            print(e)

        # Creating Saved Pages Directory if not Exists
        project_name = 'Basler_Web'

        self.project_files_dir = f'C:\\Project Files\\{project_name}_Project_Files'
        ensure_dir_exists(dir_path=self.project_files_dir)

        self.main_page_url = 'https://www.baslerweb.com/en-us/'

    def scrape(self):
        main_page_text = page_checker(url=self.main_page_url, method='GET', directory_path=os.path.join(self.project_files_dir, 'Main_Page'))
        parsed_html = html.fromstring(main_page_text)  # Parsing the main page response text

        xpath_main_categories = '//li[contains(@class, "nav-main__item nav-main__item--level-2")]/a[@class="nav-main__item-link"]'
        main_categories_links_relative = parsed_html.xpath(xpath_main_categories)[1:8]
        main_page_link_concat = self.main_page_url.replace('/en-us/', '')
        # Iterating on each main categories Elements for retrieving their data
        slug_counter = 0
        for main_cat_elem in main_categories_links_relative:
            main_category_name = ' '.join(main_cat_elem.xpath('.//text()'))
            if main_category_name not in ['Kits & Bundles', 'Software']:
                print('Main Category Name: ', main_category_name)
                main_cat_link = main_page_link_concat + ' '.join(main_cat_elem.xpath('./@href'))
                print('Main Category Link: ', main_cat_link)
                # Iterating on each Sub-Categories Elements for retrieving their data
                sub_category_elements = main_cat_elem.xpath('./following-sibling::ul/li/a[not(contains(text(), "All"))][span]')
                # sub_category_elements = main_cat_elem.xpath('./following-sibling::ul/li/a/span[not(contains(text(), "All"))]')
                print('+' * 10)
                print(sub_category_elements)
                # Iterating on each Sub Categories Elements for retrieving their data
                for sub_cat_elm in sub_category_elements:
                    sub_cat_name = ' '.join(sub_cat_elm.xpath('./span/text()'))
                    sub_cat_link = main_page_link_concat + ' '.join(sub_cat_elm.xpath('./@href[not(contains(@href, "all"))]'))
                    if 'all' not in sub_cat_name.lower():
                        print(sub_cat_elm)
                        print('Sub-Category Name: ', sub_cat_name)
                        print('Sub-Category Link: ', sub_cat_link)
                        prod_cat_links_related = sub_cat_elm.xpath('./following-sibling::ul/li/a/@href')
                        prod_cat_links_list = [main_page_link_concat + prod_cat_link for prod_cat_link in prod_cat_links_related]
                        # Iterating on each Product Category Elements for retrieving their data
                        for prod_cat_link in prod_cat_links_list:
                            if '/shop/' in prod_cat_link:  # If the Product category is the direct link to a particular product of the sub-category, Storing Their link in Database table
                                print('Prod Shop Link: ', prod_cat_link)

                                # Inserting into Database
                                data_dict = dict()
                                metadata_list = list()
                                metadata_list.append({'main category link': main_cat_link})
                                metadata_list.append({'Sub category link': main_cat_link})
                                metadata_list.append({'Prod category link': 'N/A'})

                                metadata = json.dumps(metadata_list)
                                data_dict.update({'product_link': prod_cat_link})
                                data_dict.update({'metadata': metadata})

                                print(data_dict)
                                print('Storing into Database')
                                try:
                                    cols = data_dict.keys()
                                    rows = data_dict.values()
                                    insert_query = f'''INSERT INTO `products_links` ({', '.join(tuple(cols))}) VALUES ({('%s, ' * len(data_dict)).rstrip(", ")});'''
                                    print(insert_query)
                                    self.cursor.execute(query=insert_query, args=tuple(rows))
                                except Exception as e:
                                    print(e)

                            else:  # If the Product Category links are links to 1st page of them
                                product_cat_link_template = 'https://www.baslerweb.com/api/magento/products?store=amer_en&locale=en-us&filters=%7B%22sort_dir%22:%5B%22asc%22%5D,%22page%22:%5B%22-PAGE_NO-%22%5D,%22category_id%22:%5B%22-C_I_D-%22%5D%7D'
                                print('Prod Cat Link: ', prod_cat_link)
                                slug = prod_cat_link.split('/en-us/')[1]
                                print('Slug: ', slug)
                                if not slug.endswith('/'):
                                    if '#products' in slug:
                                        slug = slug.replace('/?', '&').replace('/#products', '').replace('#products', '')
                                        cat_id_link = f'https://www.baslerweb.com/api/contentful?slug={slug}&locale=en-us'
                                        print('Category Id Link: ', cat_id_link)
                                        print('=' * 25)
                                        cat_id_response = page_checker_json(url=cat_id_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Category_id_jsons'))
                                        checker_list = cat_id_response.get('entry').get('linkedEntries').keys()
                                        for key_check in checker_list:
                                            key_check_list = cat_id_response.get('entry').get('linkedEntries').get(key_check).get('fields').keys()
                                            if 'staticFilters' in key_check_list:
                                                category_id = cat_id_response.get('entry').get('linkedEntries').get(key_check).get('fields').get('staticFilters').get('category_id')
                                                print('Category Id: ', category_id)
                                                product_cat_link_template = product_cat_link_template.replace("-PAGE_NO-", f'{1}').replace('-C_I_D-', category_id[0])
                                                response_dict = page_checker_json(url=product_cat_link_template, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages'))
                                                products_count = response_dict.get('total_count')
                                                print('Products Count: ', products_count)
                                                page_count = math.ceil(products_count/21)
                                                print('Page Count: ', page_count)
                                                for page_no in range(1, page_count+1):
                                                    product_cat_link_template = 'https://www.baslerweb.com/api/magento/products?store=amer_en&locale=en-us&filters=%7B%22sort_dir%22:%5B%22asc%22%5D,%22page%22:%5B%22-PAGE_NO-%22%5D,%22category_id%22:%5B%22-C_I_D-%22%5D%7D'
                                                    product_cat_link_template = product_cat_link_template.replace("-PAGE_NO-", f'{page_no}').replace('-C_I_D-', category_id[0])
                                                    response_dict = page_checker_json(url=product_cat_link_template, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages'))
                                                    prod_items = response_dict.get('items')
                                                    for prod_dict in prod_items:
                                                        product_url_key = prod_dict.get('url_key')
                                                        print(product_url_key)
                                                        product_link = f'https://www.baslerweb.com/en-us/shop/{product_url_key}'
                                                        # Inserting into Database
                                                        data_dict = dict()
                                                        metadata_list = list()
                                                        metadata_list.append({'main category link': main_cat_link})
                                                        metadata_list.append({'Sub category link': sub_cat_link})
                                                        metadata_list.append({'Prod category link': prod_cat_link})

                                                        metadata = json.dumps(metadata_list)
                                                        data_dict.update({'product_link': product_link})
                                                        data_dict.update({'metadata': metadata})

                                                        print(data_dict)
                                                        print('Storing into Database')
                                                        try:
                                                            cols = data_dict.keys()
                                                            rows = data_dict.values()
                                                            insert_query = f'''INSERT INTO `products_links` ({', '.join(tuple(cols))}) VALUES ({('%s, ' * len(data_dict)).rstrip(", ")});'''
                                                            print(insert_query)
                                                            self.cursor.execute(query=insert_query, args=tuple(rows))
                                                        except Exception as e:
                                                            print(e)
                                                print('----')
                                        slug_counter += 1
                                        print(slug_counter)
                                    else:
                                        cat_id_link = f'https://www.baslerweb.com/api/contentful?slug={slug}&locale=en-us'
                                        print('Category Id Link: ', cat_id_link)
                                        print('=' * 25)
                                        cat_id_response = page_checker_json(url=cat_id_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Category_id_jsons'))
                                        checker_list = cat_id_response.get('entry').get('linkedEntries').keys()
                                        for key_check in checker_list:
                                            key_check_list = cat_id_response.get('entry').get('linkedEntries').get(key_check).keys()
                                            for key in key_check_list:
                                                if key is not None:
                                                    c_id = cat_id_response.get('entry').get('linkedEntries').get(key_check).get('fields').get('staticFilters')
                                                    if c_id is not None:
                                                        category_id = c_id.get('category_id')
                                                        print('Category Id: ', category_id)
                                                        product_cat_link_template = product_cat_link_template.replace("-PAGE_NO-", f'{1}').replace('-C_I_D-', category_id[0])
                                                        response_dict = page_checker_json(url=product_cat_link_template, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages'))
                                                        products_count = response_dict.get('total_count')
                                                        print('Products Count: ', products_count)
                                                        page_count = math.ceil(products_count/21)
                                                        print('Page Count: ', page_count)
                                                        for page_no in range(1, page_count + 1):
                                                            product_cat_link_template = 'https://www.baslerweb.com/api/magento/products?store=amer_en&locale=en-us&filters=%7B%22sort_dir%22:%5B%22asc%22%5D,%22page%22:%5B%22-PAGE_NO-%22%5D,%22category_id%22:%5B%22-C_I_D-%22%5D%7D'
                                                            product_cat_link_template = product_cat_link_template.replace("-PAGE_NO-", f'{page_no}').replace('-C_I_D-', category_id[0])
                                                            response_dict = page_checker_json(url=product_cat_link_template, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages'))
                                                            prod_items = response_dict.get('items')
                                                            for prod_dict in prod_items:
                                                                product_url_key = prod_dict.get('url_key')
                                                                print(product_url_key)
                                                                product_link = f'https://www.baslerweb.com/en-us/shop/{product_url_key}'
                                                                # Inserting into Database
                                                                data_dict = dict()
                                                                metadata_list = list()
                                                                metadata_list.append({'main category link': main_cat_link})
                                                                metadata_list.append({'Sub category link': sub_cat_link})
                                                                metadata_list.append({'Prod category link': prod_cat_link})

                                                                metadata = json.dumps(metadata_list)
                                                                data_dict.update({'product_link': product_link})
                                                                data_dict.update({'metadata': metadata})

                                                                print(data_dict)
                                                                print('Storing into Database')
                                                                try:
                                                                    cols = data_dict.keys()
                                                                    rows = data_dict.values()
                                                                    insert_query = f'''INSERT INTO `products_links` ({', '.join(tuple(cols))}) VALUES ({('%s, ' * len(data_dict)).rstrip(", ")});'''
                                                                    print(insert_query)
                                                                    self.cursor.execute(query=insert_query, args=tuple(rows))
                                                                except Exception as e:
                                                                    print(e)
                                        slug_counter += 1
                                        print(slug_counter)
                                else:
                                    print('p' * 80)
                                    slug = slug[:-1]
                                    cat_id_link = f'https://www.baslerweb.com/api/contentful?slug={slug}&locale=en-us'
                                    print('Category Id Link: ', cat_id_link)
                                    print('=' * 25)
                                    # continue
                                    cat_id_response = page_checker_json(url=cat_id_link, method='GET', directory_path=os.path.join(self.project_files_dir, 'Category_id_jsons'))
                                    checker_list = cat_id_response.get('entry').get('linkedEntries').keys()
                                    for key_check in checker_list:
                                        key_check_list = cat_id_response.get('entry').get('linkedEntries').get(key_check).keys()
                                        if 'fields' in key_check_list:
                                            c_id = cat_id_response.get('entry').get('linkedEntries').get(key_check).get('fields').get('staticFilters')
                                            if c_id is not None:
                                                category_id = c_id.get('category_id')
                                                print('Category Id: ', category_id)
                                                product_cat_link_template = product_cat_link_template.replace("-PAGE_NO-", f'{1}').replace('-C_I_D-', category_id[0])
                                                response_dict = page_checker_json(url=product_cat_link_template, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages'))
                                                products_count = response_dict.get('total_count')
                                                print('Products Count: ', products_count)
                                                page_count = math.ceil(products_count/21)
                                                print('Page Count: ', page_count)
                                                for page_no in range(1, page_count+1):
                                                    product_cat_link_template = 'https://www.baslerweb.com/api/magento/products?store=amer_en&locale=en-us&filters=%7B%22sort_dir%22:%5B%22asc%22%5D,%22page%22:%5B%22-PAGE_NO-%22%5D,%22category_id%22:%5B%22-C_I_D-%22%5D%7D'
                                                    product_cat_link_template = product_cat_link_template.replace("-PAGE_NO-", f'{page_no}').replace('-C_I_D-', category_id[0])
                                                    response_dict = page_checker_json(url=product_cat_link_template, method='GET', directory_path=os.path.join(self.project_files_dir, 'Product_Pages'))
                                                    prod_items = response_dict.get('items')
                                                    for prod_dict in prod_items:
                                                        product_url_key = prod_dict.get('url_key')
                                                        print(product_url_key)
                                                        product_link = f'https://www.baslerweb.com/en-us/shop/{product_url_key}'
                                                        # Inserting into Database
                                                        data_dict = dict()
                                                        metadata_list = list()
                                                        metadata_list.append({'main category link': main_cat_link})
                                                        metadata_list.append({'Sub category link': sub_cat_link})
                                                        metadata_list.append({'Prod category link': prod_cat_link})

                                                        metadata = json.dumps(metadata_list)
                                                        data_dict.update({'product_link': product_link})
                                                        data_dict.update({'metadata': metadata})

                                                        print(data_dict)
                                                        print('Storing into Database')
                                                        try:
                                                            cols = data_dict.keys()
                                                            rows = data_dict.values()
                                                            insert_query = f'''INSERT INTO `products_links` ({', '.join(tuple(cols))}) VALUES ({('%s, ' * len(data_dict)).rstrip(", ")});'''
                                                            print(insert_query)
                                                            self.cursor.execute(query=insert_query, args=tuple(rows))
                                                        except Exception as e:
                                                            print(e)
                                    slug_counter += 1
                                    print(slug_counter)
                                print('Done', prod_cat_link)
                            print('*' * 5)
                        print('+' * 20) 
                print('-' * 30)


Scraper().scrape()
