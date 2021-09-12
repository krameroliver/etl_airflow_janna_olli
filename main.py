import os
from dotenv import load_dotenv

load_dotenv()

MY_ENV_VAR = os.getenv('KEYPASS')
print(MY_ENV_VAR)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
