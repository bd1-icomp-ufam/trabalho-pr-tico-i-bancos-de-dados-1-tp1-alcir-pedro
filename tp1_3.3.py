from tabulate import tabulate
import psycopg2

class Query:
    consultas = {
        'A': 'Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação',
        'B': 'Dado um produto, listar os produtos similares com maiores vendas do que ele',
        'C': 'Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada',
        'D': 'Listar os 10 produtos líderes de venda em cada grupo de produtos',
        'E': 'Listar os 10 produtos com a maior média de avaliações úteis positivas por produto',
        'F': 'Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto',
        'G': 'Listar os 10 clientes que mais fizeram comentários por grupo de produto'
    }

    def __init__(self, query, cursor):
        self.query_methods = {
            'A': self.query_a,
            'B': self.query_b,
            'C': self.query_c,
            'D': self.query_d,
            'E': self.query_e,
            'F': self.query_f,
            'G': self.query_g
        }
        self.headers = {
            'A': ['ASIN', 'Date', 'Review Customer', 'Rating', 'Votes', 'Helpful'],
            'B': ['ASIN', 'Title', 'Product Group', 'Salesrank'],
            'C': ['Date', 'Mean'],
            'D': ['Product Group', 'ASIN', 'Salesrank'],
            'E': ['ASIN', 'Title', 'Average Helpful Reviews'],
            'F': ['Category', 'Average Helpful Reviews'],
            'G': ['Review Customer', 'Product Group', 'Total Reviews']
        }
        self.query_func = self.query_methods[query]
        self.cursor = cursor
        self.header = self.headers[query]
        self.query_code = query

    def query_a(self, asin):
        query = f"""
            (SELECT *
            FROM user_review
            WHERE ASIN = '{asin}'
            ORDER BY review_helpful DESC, review_rating DESC
            LIMIT 5)
            UNION ALL
            (SELECT *
            FROM user_review
            WHERE ASIN = 'B00005J9UN'
            ORDER BY review_helpful DESC, review_rating ASC
            LIMIT 5);
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_b(self, asin):
        query = f"""
            SELECT p.ASIN, p.title, p.product_group, p.salesrank
            FROM products p
            JOIN "Similar" s ON p.ASIN = s.product_asin
            WHERE s.ASIN = '{asin}' 
              AND p.salesrank < (SELECT salesrank FROM products WHERE ASIN = '{asin}')
            ORDER BY p.salesrank ASC;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_c(self, asin):
        query = f"""
            SELECT DISTINCT review_date, AVG(review_rating) OVER (ORDER BY review_date) AS media
            FROM user_review
            WHERE ASIN = '{asin}'
            ORDER BY review_date;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_d(self):
        query = """
            WITH RankedProducts AS (
                SELECT product_group, ASIN, salesrank,
                ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY salesrank ASC) AS rank
                FROM Products
                WHERE salesrank > 0
            )
            SELECT product_group, ASIN, salesrank
            FROM RankedProducts
            WHERE rank <= 10
            ORDER BY product_group, rank;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_e(self):
        query = """
            SELECT p.ASIN, p.title, AVG(ur.review_helpful) AS avg_helpful_reviews
            FROM products p
            JOIN user_review ur ON p.ASIN = ur.ASIN
            GROUP BY p.ASIN, p.title
            HAVING AVG(ur.review_helpful) IS NOT NULL
            ORDER BY avg_helpful_reviews DESC
            LIMIT 10;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_f(self):
        query = """
            SELECT c.category_name, AVG(ur.review_helpful) AS avg_helpful_reviews
            FROM Category c, Product_Categories pc, Products p, User_Review ur
            WHERE c.category_id = pc.category_id
            AND pc.ASIN = p.ASIN
            AND p.ASIN = ur.ASIN
            GROUP BY c.category_name
            ORDER BY avg_helpful_reviews DESC
            LIMIT 5;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_g(self):
        query = """
            SELECT ur.review_customer, p.product_group, COUNT(ur.ASIN) AS total_reviews
            FROM User_Review ur
            JOIN Products p ON ur.ASIN = p.ASIN
            GROUP BY ur.review_customer, p.product_group
            ORDER BY total_reviews DESC
            LIMIT 10;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def tabulate_print(self, data, output_file=None):
        table = tabulate(data, headers=self.header, tablefmt='simple')
        if output_file:
            with open(output_file, 'a') as file:
                file.write(f"\n\n{self.query_code}) {Query.consultas[self.query_code]}\n")
                file.write(table + "\n")
        else:
            print(table, '\n')

def dashboard(cursor):
    escolha = None
    while escolha != 'EXIT':
        for query in Query.consultas:
            print(f'\033[93m{query}) {Query.consultas[query]}\033[0m\n')

        print('\033[93mEXIT ou exit - Sair\033[0m\n')

        escolha = input('\nEscolha uma opção: ').upper()
        if escolha in Query.consultas:
            query_instance = Query(escolha, cursor)
            if escolha in ['A', 'B', 'C']:
                codigo = input('Digite o código do produto (ASIN): ')
                if len(codigo) != 10:
                    print('Código inválido.')
                else:
                    resultado = query_instance.query_func(codigo)
                    query_instance.tabulate_print(resultado)
            else:
                resultado = query_instance.query_func()
                query_instance.tabulate_print(resultado)
        elif escolha != 'EXIT':
            print('Opção inválida.')

host = "localhost"
database = "tp1"
usuario = "postgres"
senha = "12345"

conector = psycopg2.connect(f"host={host} dbname={database} user={usuario} password={senha}")
cursor = conector.cursor()

dashboard(cursor)
