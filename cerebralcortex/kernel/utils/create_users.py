# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import mysql.connector
import sys
from datetime import datetime
import uuid
import random
import string
import hashlib


class CreateUsers():
    def __init__(self):
        """
        Constructor
        :param configuration:
        """
        # python3 create_users.py 127.0.0.1 3306 root pass create 2
        if not sys.argv[1]:
            print("Missing MySQL Args: Host Port UserName Password users-action total-users")
            print("action argument accepts three values only: create, list, delete")

        self.hostIP = sys.argv[1]
        self.hostPort = sys.argv[2]
        self.database = "cerebralcortex"
        self.dbUser = sys.argv[3]
        self.dbPassword = sys.argv[4]
        self.userTable = "user"
        self.defaultUsers = [{"type": "admin", "username": "ali", "password": "abc"},
                             {"type": "admin", "username": "tim", "password": "abc"}]

        self.dbConnection = mysql.connector.connect(host=self.hostIP, port=self.hostPort, user=self.dbUser,
                                                    password=self.dbPassword, database=self.database)
        self.cursor = self.dbConnection.cursor(dictionary=True)

        action = sys.argv[5]
        if action == "create":
            try:
                total_users = int(sys.argv[6])
            except:
                raise ValueError("Total-users run time parameter cannot be null/empty.")

        if action == "create":
            self.create_random_users(total_users)
        elif action == "list":
            self.list_all_users()
        elif action == "delete":
            self.delete_all_users()
        else:
            raise ValueError("Unknown parameter. Only supported parameters are: create, list, delete")

    def __del__(self):
        if self.dbConnection:
            self.dbConnection.close()

    def create_random_users(self, total_users: int):
        """
        :param total_users:
        """
        user_creation_datetime = datetime.now()
        qry = "INSERT INTO " + self.userTable + " (identifier, username, password, token, user_role, user_metadata, active, confirmed_at) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

        # generate pre-defined users
        for user in self.defaultUsers:
            user_id = uuid.uuid4()
            vals = str(user_id), str(user["username"]), str(self.encrypt_user_password(user["password"])), str(
                user["password"]), str(user["type"]), "{}", "1", str(user_creation_datetime)
            self.cursor.execute(qry, vals)
            self.dbConnection.commit()
        print("Created " + str(len(self.defaultUsers)) + " default users.")

        # generate random users
        for rand_user in range(total_users):
            random_password = self.gen_random_pass("varchar")
            user_id = uuid.uuid4()
            user_name = self.gen_random_pass("char")
            user_password = self.encrypt_user_password(random_password)
            token = random_password
            type = "participant"

            vals = str(user_id), str(user_name), str(user_password), str(token), str(type), "{}", "1", str(
                user_creation_datetime)
            self.cursor.execute(qry, vals)
            self.dbConnection.commit()
        print("Created " + str(total_users) + " random users.")

    def list_all_users(self):
        """
        List all users as CSV output
        """
        qry = "select * from " + self.userTable
        self.cursor.execute(qry)
        results = self.cursor.fetchall()

        if results:
            print("Identifier, User-Name, Password")
            for row in results:
                print('{}, {}, {}'.format(row["identifier"], row["username"], row["token"]))

    def delete_all_users(self):
        """
        Empty user table
        """
        qry = "truncate " + self.userTable
        self.cursor.execute(qry)
        self.dbConnection.commit()
        print("Deleted all users")

    def gen_random_pass(self, string_type: str, size: int = 8) -> str:
        """
        :param string_type:
        :param size:
        :return:
        """
        if (string_type == "varchar"):
            chars = string.ascii_lowercase + string.digits
        elif (string_type == "char"):
            chars = string.ascii_lowercase
        else:
            chars = string.digits

        return ''.join(random.choice(chars) for _ in range(size))

    def encrypt_user_password(self, user_password: str) -> str:
        """
        :param user_password:
        :return:
        """
        hash_pwd = hashlib.sha256(user_password.encode('utf-8'))
        return hash_pwd.hexdigest()


CreateUsers()