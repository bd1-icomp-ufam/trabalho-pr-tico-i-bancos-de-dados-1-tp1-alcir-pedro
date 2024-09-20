import psycopg2
import psycopg2.extras
import re

host = "localhost"
database = "postgres" # Utilizar este banco, ou outro banco existente que possua as permissões necessárias
usuario = "postgres"  
senha = "12345"  

conector = psycopg2.connect(
    host=host,
    dbname=database,
    user=usuario,
    password=senha
)
conector.autocommit = True
cursor = conector.cursor()

# Criando o banco de dados
cursor.execute('''
    SELECT 1 FROM pg_database WHERE datname = 'tp1';
''')
exists = cursor.fetchone()
if not exists:
    cursor.execute('CREATE DATABASE tp1;')

cursor.close()
conector.close()

host = "localhost"
database = "tp1" # Agora acessando o banco de dados 'tp1'
usuario = "postgres"
senha = "12345"

conector = psycopg2.connect(
    host=host,
    dbname=database,
    user=usuario,
    password=senha
)
cursor = conector.cursor()

# Criando as tabelas
cursor.execute('''
CREATE TABLE IF NOT EXISTS Products (
    product_id INT PRIMARY KEY,
    ASIN VARCHAR(10) UNIQUE NOT NULL,
    title VARCHAR(500),
    product_group VARCHAR(100),
    salesrank INT,
    discontinued BOOLEAN,
    num_similar INT,
    num_categories INT
);

CREATE TABLE IF NOT EXISTS "Similar" (
    ASIN VARCHAR(10),
    product_asin VARCHAR(10),
    PRIMARY KEY (ASIN, product_asin),
    FOREIGN KEY (ASIN) REFERENCES Products(ASIN),
    FOREIGN KEY (product_asin) REFERENCES Products(ASIN)
);

CREATE TABLE IF NOT EXISTS Category (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100),
    parent_category_id INT,
    FOREIGN KEY (parent_category_id) REFERENCES Category(category_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS Product_Categories (
    ASIN VARCHAR(10),
    category_id INT,
    PRIMARY KEY (ASIN, category_id),
    FOREIGN KEY (ASIN) REFERENCES Products(ASIN),
    FOREIGN KEY (category_id) REFERENCES Category(category_id)
);

CREATE TABLE IF NOT EXISTS Review (
    ASIN VARCHAR(10) PRIMARY KEY,
    review_total INT,
    review_avg FLOAT,
    review_downloads INT,
    FOREIGN KEY (ASIN) REFERENCES Products(ASIN)
);

CREATE TABLE IF NOT EXISTS User_Review (
    ASIN VARCHAR(10),
    review_date DATE,
    review_customer VARCHAR(100),
    review_rating INT CHECK (review_rating BETWEEN 1 AND 5),
    review_votes FLOAT,
    review_helpful INT,
    PRIMARY KEY (ASIN, review_customer),
    FOREIGN KEY (ASIN) REFERENCES Products(ASIN)
);
''')
conector.commit()

# Parser 

# Patterns
patterns = {
    'id': re.compile(r'Id:\s*(\d+)'),
    'asin': re.compile(r'ASIN:\s*([A-Z0-9]+)'),
    'discontinued': re.compile(r'discontinued product'),
    'title': re.compile(r'title:\s*(.*)'),
    'group': re.compile(r'group:\s*(.*)'),
    'salesrank': re.compile(r'salesrank:\s*(\d+)'),
    'similar': re.compile(r'similar:\s*(\d+)\s*([\w\s]*)categories:'),
    'categories': re.compile(r'categories:\s*(\d+)(.*?)reviews:', re.DOTALL),
    'reviews': re.compile(r'reviews:\s*total:\s*(\d+)\s*downloaded:\s*(\d+)\s*avg rating:\s*([\d\.]+)(.*)', re.DOTALL),
    'review_data': re.compile(r'(\d{4}-\d{1,2}-\d{1,2})\s*cutomer:\s*([\w\d]+)\s*rating:\s*(\d+)\s*votes:\s*(\d+)\s*helpful:\s*(\d+)')
}

# Função para extrair categorias
def extract_categories(categories_text):
    categories = re.findall(r'\|([^[]+)\[(\d+)\]', categories_text)
    hierarchy = categories_text.split('|')  
    parent_id = None
    
    categories_list = []
    for cat in categories:
        category_name = cat[0].strip()
        category_id = cat[1]

        categories_list.append({
            'name': category_name,
            'id': category_id,
            'parent_id': parent_id
        })

        parent_id = category_id

    return categories_list

# Função para extrair reviews
def extract_reviews(reviews_text):
    return [{'date': m[0], 'customer': m[1], 'rating': m[2], 'votes': m[3], 'helpful': m[4]} 
            for m in patterns['review_data'].findall(reviews_text)]


# Função para limpar o texto
def clean_text(text):
    lines = text.splitlines()
    cleaned_lines = [line for line in lines if not line.startswith('#') and "Total items:" not in line]
    return '\n'.join(cleaned_lines)

# Função para parsear o produto
def parse_product(text):
    product = {}

    text = clean_text(text)

    id_match = patterns['id'].search(text)
    asin_match = patterns['asin'].search(text)

    if not id_match or not asin_match:
        return None  
    
    product['id'] = id_match.group(1)
    product['asin'] = asin_match.group(1)

    if patterns['discontinued'].search(text):
        product['discontinued'] = True
        return product

    product['title'] = patterns['title'].search(text).group(1).strip() if patterns['title'].search(text) else None
    product['group'] = patterns['group'].search(text).group(1).strip() if patterns['group'].search(text) else None
    product['salesrank'] = patterns['salesrank'].search(text).group(1) if patterns['salesrank'].search(text) else None

    similar_match = patterns['similar'].search(text)
    if similar_match:
        product['num_similar'] = similar_match.group(1)
        similars_text = similar_match.group(2).strip()
        product['similars'] = similars_text.split() if similars_text and product['num_similar'] != '0' else []

    categories_match = patterns['categories'].search(text)
    if categories_match:
        product['num_categories'] = categories_match.group(1)
        product['categories'] = extract_categories(categories_match.group(2))

    reviews_match = patterns['reviews'].search(text)
    if reviews_match:
        product['review_total'] = reviews_match.group(1)
        product['downloaded'] = reviews_match.group(2)
        product['avg_rating'] = reviews_match.group(3)
        reviews_text = reviews_match.group(4).strip()
        product['list_reviews'] = extract_reviews(reviews_text) if reviews_text and product['review_total'] != '0' else []

    return product

# Listas para inserção em batch
products_list = []
similars_list = []
categories_list = []
product_categories_list = []
reviews_list = []
user_reviews_list = []

def add_to_lists(product):
    # Adiciona na lista Products
    products_list.append((
        product['id'], 
        product['asin'], 
        product.get('title'), 
        product.get('group'), 
        product.get('salesrank'), 
        product.get('discontinued', False), 
        product.get('num_similar', 0), 
        product.get('num_categories', 0)
    ))

    # Adiciona relações de similares
    for similar_asin in product.get('similars', []):
        similars_list.append((product['asin'], similar_asin))

    # Adiciona categorias e relações de produto com categoria
    for category in product.get('categories', []):
        categories_list.append((category['id'], category['name'], category['parent_id']))
        product_categories_list.append((product['asin'], category['id']))

    # Adiciona reviews
    if 'review_total' in product:
        reviews_list.append((
            product['asin'], 
            product['review_total'], 
            product['avg_rating'], 
            product['downloaded']
        ))

    # Adiciona user reviews
    for review in product.get('list_reviews', []):
        user_reviews_list.append((
            product['asin'], 
            review['date'], 
            review['customer'], 
            review['rating'], 
            review['votes'], 
            review['helpful']
        ))

# Função para inserir dados em batch no banco de dados
def batch_insert():
    try:
        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO Products (product_id, ASIN, title, product_group, salesrank, discontinued, num_similar, num_categories)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ASIN) DO NOTHING;
        ''', products_list)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO Category (category_id, category_name, parent_category_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (category_id) DO NOTHING;
        ''', categories_list)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO Product_Categories (ASIN, category_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        ''', product_categories_list)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO Review (ASIN, review_total, review_avg, review_downloads)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ASIN) DO NOTHING;
        ''', reviews_list)

        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO User_Review (ASIN, review_date, review_customer, review_rating, review_votes, review_helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        ''', user_reviews_list)

        conector.commit()

    except Exception as e:
        print(f"Erro ao inserir em lote no banco de dados: {e}")
        conector.rollback()

def insert_similars():
    subListaSimilar = []
    for asin, similar_asin in similars_list:
        try:
            # Verificar se o similar_asin existe na tabela Products
            cursor.execute('''
                SELECT ASIN FROM Products WHERE ASIN = %s;
            ''', (similar_asin,))
            result = cursor.fetchone()
            if result:
                subListaSimilar.append((asin, similar_asin))
                # Inserir o similar apenas se existir na tabela Products

        except Exception as e:
            print(f"Erro ao inserir similar ({asin}, {similar_asin}): {e}")
            conector.rollback()
    
    try:
        psycopg2.extras.execute_batch(cursor, '''
            INSERT INTO "Similar" (ASIN, product_asin)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        ''', subListaSimilar)
        conector.commit()
    except Exception as e:
        print(f"Erro ao executar batch insert: {e}")
        conector.rollback()


# Alterar o caminho do arquivo para o caminho do arquivo amazon-meta.txt
path = 'amazon-meta.txt'

text = open(path, encoding='utf-8').read()
i=0
print("Inserindo itens...")
for product_text in text.split("\n\n"):  
    product = parse_product(product_text)
    if product:
        add_to_lists(product)
        if(i%10000==0 and i!=0):
            batch_insert()
            print(f'{i} itens inseridos...')
            products_list = []
            categories_list = []
            product_categories_list = []
            reviews_list = []
            user_reviews_list = []
        i+=1
print("Inserindo últimos itens...")
batch_insert()
insert_similars()
print("Inserção finalizada.")
cursor.close()
conector.close()