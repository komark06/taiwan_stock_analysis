import os
import secrets
import string


def generate_password(min_length, max_length):
    length = secrets.randbelow(max_length - min_length + 1) + min_length
    characters = string.ascii_letters + string.digits + string.punctuation
    password = "".join(secrets.choice(characters) for _ in range(length))
    return password


folder = "db"
if not (os.path.exists(folder) and os.path.isdir(folder)):
    os.mkdir(folder)
min_length = 8  # Minimum length of the password
max_length = 16  # Maximum length of the password
password_file = ["db-password", "user-password"]
for file in password_file:
    path = os.path.join(folder, file)
    with open(path, "w", encoding="ascii") as fw:
        fw.write(generate_password(min_length, max_length))
