import psycopg2 as conector
import os
from psycopg2 import OperationalError


def criar_conexao(db_name, db_user, db_password, db_host, db_port):
    conexao = None
    try:
        conexao = conector.connect(
            database = db_name,
            user = db_user,
            password = db_password,
            host = db_host,
            port = db_port)
        print("\nA CONEXÃO COM O BANCO DE DADOS FOI BEM SUCEDIDA\n")
        op = ''
        cursor = conexao.cursor()
        while op != 0:
            op = int(input("""----Escolha uma Ação----\n
Para criar tabela Pessoa e Conta: 1
Para adicionar os arquivos: 2
Para consultar o registro: 3
Para alterar uma Linha: 4
Para sair: 0\n
Escolha: """))
            if op == 1:
                criarTabelaConta(conexao)
                criarTabelaPessoa(conexao)
            elif op == 2:
                adicionarArquivoConta(cursor, conexao)
                adicionarArquivoNomes(cursor, conexao)
            elif op == 3:
                consultarLinha(cursor)
            elif op == 4:
                alterarValor(cursor, conexao)
        cursor.close()
        conexao.close()
    except OperationalError as e:
        print(f"O erro '{e}' ocorreu")
    return conexao

def criarTabelaPessoa(conexao):
    conexao.autocommit = True
    cursor = conexao.cursor()
    try:
        query = """create table Pessoa(
        CPF varchar(20),
        Primeiro_Nome varchar(30),
        Nome_Do_Meio varchar(20),
        Sobrenome varchar(20),
        Idade Int,
        Conta int,
        FOREIGN KEY (Conta) REFERENCES conta (Titular)
        )"""
        cursor.execute(query)
        conexao.commit()
        print("Tabela Pessoa criada com sucesso!")
        cursor.close()
    except OperationalError as e:
        print(f"O erro '{e}' ocorreu")

def criarTabelaConta(conexao):
    conexao.autocommit = True
    cursor = conexao.cursor()
    try:
        query = """CREATE TABLE conta( 
        Agencia varchar(20),
        Numero varchar(20),
        Saldo float,
        Gerente int,
        Titular int PRIMARY KEY
        )"""
        cursor.execute(query)
        conexao.commit()
        print("Tabela Conta criada com sucesso!")
        cursor.close()
    except OperationalError as e:
        print(f"O erro '{e}' ocorreu")

def adicionarArquivoConta(cursor, conexao):
    arquivo = open("contas.txt", "r")
    for linha in arquivo.readlines():
        agencia = (linha.strip().split()[0])
        num = (linha.strip().split()[1])
        saldo = (linha.strip().split()[2])
        gerente = (linha.strip().split()[3])
        titular = (linha.strip().split()[4])
        query = f"""insert into conta
        values ('{agencia}', '{num}', '{saldo}', '{gerente}', '{titular}')"""
        cursor.execute(query)
        conexao.commit()

def adicionarArquivoNomes(cursor, conexao):
    arquivo = open("nomes.txt", "r")
    for linha in arquivo.readlines():
        cpf = (linha.strip().split()[0])
        nom = (linha.strip().split()[1])
        meio = (linha.strip().split()[2])
        sobre = (linha.strip().split()[3])
        idade = (linha.strip().split()[4])
        idconta = (linha.strip().split()[5])
        query = f"""insert into pessoa
        values ('{cpf}', '{nom}', '{meio}', '{sobre}', '{idade}', '{idconta}')"""
        cursor.execute(query)
        conexao.commit()

def consultarLinha(cursor):
    conta = str(input("Qual o número da Conta: "))
    query = f"""select * from public.pessoa where conta = '{conta}'"""
    cursor.execute(query)
    try:
        arquivocv = open("pessoa/nome.csv", "a+")
    except FileNotFoundError as e:
        print(e)
        print("Criando Diretorio")
        os.mkdir("pessoa")
        arquivocv = open("pessoa/nome.csv", "a+")
    linha = cursor.fetchone()
    arquivocv.writelines(f"""-------------------
CPF: {linha[0]}
Primeiro-Nome: {linha[1]}
Nome-do-Meio: {linha[2]}
Sobrenome: {linha[3]}
Idade: {linha[4]} anos
Id_Conta: {linha[5]}
-------------------""")
    query2 = f"""select * from public.conta where titular = '{conta}'"""
    cursor.execute(query2)
    try:
        arquivocv2 = open("conta/titular.csv", "a+")
    except FileNotFoundError as e:
        print(e)
        print("Criando Diretorio")
        os.mkdir("conta")
        arquivocv2 = open("conta/titular.csv", "a+")
    linha2 = cursor.fetchone()
    arquivocv2.writelines(f"""-------------------
Id_Titular: {linha2[4]}
Agencia: {linha2[0]}
Numero: {linha2[1]}
Saldo: {linha2[2]}
Gerente: {linha2[3]}
------------------""")

def alterarValor(cursor, conexao):
    tabela = str(input("Qual tabela(pessoa/conta): ")).lower()
    if tabela == 'pessoa':
        coluna = str(input("Qual coluna (Digite de forma exata): "))
        conta = int(input("Qual o id da Conta: "))
        valornov = input("Qual valor novo: ")
        query = f"""update {tabela}
        set {coluna} = '{valornov}'
        where Conta = '{conta}'"""
    else:
        coluna = str(input("Qual coluna (Digite de forma exata): "))
        titu = int(input("Qual o id do Titular: "))
        valornov = input("Qual valor novo: ")
        query = f"""update {tabela}
        set {coluna} = '{valornov}'
        where Titular = '{titu}'"""
    cursor.execute(query)
    conexao.commit()



criar_conexao("postgres", "postgres", "admin12345", "localhost", "5432")

