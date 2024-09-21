[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/zixaop7v)

## Equipe

- **Alcir Heber Castro Figueiredo**
- **Pedro Lucas Bezerra Mendes**

## Instalação de Dependências

Para instalar as dependências necessárias para a execução dos scripts deste trabalho, siga os passos abaixo para executar o script `setup.sh`:

```bash
chmod 700 setup.sh
./setup.sh
```

## Executando o Arquivo `tp1_3.2.py`

Para rodar o script `tp1_3.2.py`, são necessárias algumas modificações no código.

### 1. Modificar as Variáveis de Acesso ao Banco de Dados

#### Primeiro Trecho:

No início, o banco que será acessado deve ser o `postgres` ou outro banco existente que possua as permissões necessárias para CRIAR bancos, pois o banco `tp1` será criado. Faça a modificação nas linhas indicadas INSERINDO AS **SUAS CREDENCIAIS**:

```bash
# Linhas 5-8
5  host = "localhost"
6  database = "postgres"  # Utilizar este banco, ou outro banco existente que possua as permissões necessárias
7  usuario = "postgres"  
8  senha = "12345"
```

#### Segundo Trecho:

Após o banco de dados `tp1` ser criado, configure as variáveis para que o script utilize o banco de dados `tp1`. Faça as modificações a partir da linha 30 INSERINDO AS **SUAS CREDENCIAIS**:

```bash
# Linhas 30-33
30 host = "localhost"
31 database = "tp1"  # Agora acessando o banco de dados 'tp1'
32 usuario = "postgres"
33 senha = "12345"
```

### 2. Modificar o Caminho do Arquivo de Dados

Você deve alterar o caminho para onde o arquivo `amazon-meta.txt` está localizado na sua máquina. Modifique a linha 312 conforme o exemplo:

```bash
# Linha 312
312 path = '/seu/caminho/para/amazon-meta.txt'
```

### 3. Executar o Script

Com as modificações feitas, basta rodar o script com o seguinte comando:

```bash
python3 tp1_3.2.py
```

---

## Executando o Arquivo `tp1_3.3.py`

### 1. Modificar as Variáveis de Acesso ao Banco de Dados

Neste arquivo, você deve apenas garantir que as credenciais estão corretas para acessar o banco de dados `tp1`. Faça a modificação nas seguintes linhas:

```bash
# Linhas 166-169
166 host = "localhost"
167 database = "tp1"
168 usuario = "postgres"
169 senha = "12345"
```

### 2. Executar o Script

Após as modificações, execute o script com o seguinte comando:

```bash
python3 tp1_3.3.py
```

