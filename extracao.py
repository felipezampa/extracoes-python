import os

projetos = ['projeto_b3', 'projeto_euro']

for projeto in projetos:
    if not os.path.exists(f"{projeto}/data") and not os.path.exists(f"{projeto}/script"):
        os.makedirs(f"{projeto}/data")
        os.makedirs(f"{projeto}/script")
        os.makedirs(f"{projeto}/Job")
        with open(f"{projeto}/script/main.py", 'w') as f:
            f.write('# aqui comeca seu codigo\n')
            f.write('#%% \n')
            f.write('import pandas as pd\n')
            f.write('import requests\n')
            f.write('from bs4 import BeautifulSoup\n')
            f.write(f'#%% \n')
            f.write(f'url = "https://www.google.com"\n')
            f.write(f'response = requests.get(url)\n')
            f.write(f'html = response.content\n')
            f.write(f'soup = BeautifulSoup(html, "html.parser")\n')

        print(f"Pasta do projeto {projeto} criada com sucesso!")
    else:
        print(f'Pasta {projeto}, ja existe !')

# SCRIPT PADRÃO PARA ACESSAR O DIRETORIO DATA DE QUALQUER PROJETO

