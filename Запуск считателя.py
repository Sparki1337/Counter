import os
import subprocess

target_directory = r"C:\Users\New\Desktop\Программы\Моё\сурсы\Счётчик для отчётов — копия" #(менять на свою директорию)

try:
    os.chdir(target_directory)
    print(f"Успешно перешли в директорию: {os.getcwd()}")
    
    print("Запускаем bot.py...")
    subprocess.run("python bot.py", shell=True)
    
except FileNotFoundError:
    print(f"Ошибка: Директория не найдена: {target_directory}")
except Exception as e:
    print(f"Произошла ошибка: {e}")

input("Нажмите Enter для продолжения...") 
