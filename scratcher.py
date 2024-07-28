import os

project_name = 'Basler_Web_Project_Files'
project_files_dir = f'C:\\Project Files\\{project_name}'
try:
    os.makedirs(project_files_dir)
    print('Saved Pages Directory created.')
except Exception as e:
    print(e)
