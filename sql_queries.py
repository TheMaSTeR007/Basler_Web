main_cat_query = '''CREATE TABLE IF NOT EXISTS main_categories_links (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    main_category_name VARCHAR(255),
                    main_category_link VARCHAR(255),
                    link_status VARCHAR(255) DEFAULT 'pending'
                    );'''

sub_cat_query = '''CREATE TABLE IF NOT EXISTS sub_categories_links (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sub_category_name VARCHAR(255),
                    sub_category_link VARCHAR(255),
                    link_status VARCHAR(255) DEFAULT 'pending'
                    );'''

products_query = '''CREATE TABLE IF NOT EXISTS products_links (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_link VARCHAR(255) UNIQUE,
                    metadata JSON
                    );'''