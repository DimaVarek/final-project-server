import datetime
import sqlite3
DB_NAME = "position_db.db"
CREATE_POSITIONS = '''CREATE TABLE POSITIONS
                    (ID INT PRIMARY KEY     NOT NULL,
                    OWNERID         INT     NOT NULL,
                    POSITIONLINK    TEXT    NOT NULL,
                    STARTDATE       TEXT    NOT NULL,
                    CHANGEDATE      TEXT    NOT NULL,
                    COMPANYNAME     TEXT    NOT NULL,
                    POSITIONNAME    TEXT    NOT NULL,
                    INTERVIEWID     INT     NOT NULL,
                    DESCCRIPTIONID  INT     NOT NULL);'''

CREATE_INTERVIEWSTAGES = '''CREATE TABLE INTERVIEWSTAGES
                    (ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    POSITIONID      INT     NOT NULL,
                    NUMBERINORDER   INT     NOT NULL,
                    INTERVIEWTYPE   INT     NOT NULL,
                    INTERVIEWSTATUS INT     NOT NULL,
                    COMMENT         TEXT    NOT NULL,
                    DATE            TEXT);'''

CREATE_DESCRIPTIONS = '''CREATE TABLE DESCRIPTIONS
                    (ID INT PRIMARY KEY     NOT NULL,
                    DESCRIPTION     TEXT    NOT NULL);'''


class SqlServer:
    def __init__(self, db_name):
        self.db_name = db_name
        connection = self.connect()
        try:
            connection.execute('SELECT * FROM POSITIONS limit 5')
            print('table POSITIONS exist')
        except sqlite3.OperationalError:
            connection.execute(CREATE_POSITIONS)
            print('table POSITIONS created')

        try:
            connection.execute('SELECT * FROM INTERVIEWSTAGES limit 5')
            print('table INTERVIEWSTAGES exist')
        except sqlite3.OperationalError:
            connection.execute(CREATE_INTERVIEWSTAGES)
            print('table INTERVIEWSTAGES created')

        try:
            connection.execute('SELECT * FROM DESCRIPTIONS limit 5')
            print('table DESCRIPTIONS exist')
        except sqlite3.OperationalError:
            connection.execute(CREATE_DESCRIPTIONS)
            print('table DESCRIPTIONS created')
        connection.close()

    def connect(self):
        return sqlite3.connect(self.db_name)

    def _next_id(self, who_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT max({who_id}) from POSITIONS")
        rows = cur.fetchall()
        if rows[0][0] != None:
            r_id = rows[0][0] + 1
        else:
            r_id = 0
        return r_id

    def _parse_position(self, position, cur):
        curr_pos = {
            "id": position[0],
            "owner_id": position[1],
            "position_link": position[2],
            "start_date": position[3],
            "change_date": position[4],
            "company_name": position[5],
            "position_name": position[6],
            "description": cur.execute(f"SELECT DESCRIPTION FROM DESCRIPTIONS WHERE ID={position[8]}")
            .fetchall()[0][0]
        }
        stages = cur.execute(f"SELECT * FROM INTERVIEWSTAGES WHERE POSITIONID={position[7]}").fetchall()
        stages.sort(key=lambda x: x[2])
        stages = map(lambda x: {'type': x[3], "status": x[4], 'comment': x[5], "date": x[6]}, stages)
        curr_pos['interview_stages'] = list(stages)
        return curr_pos

    def _delete_description_by_description_id(self, description_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM DESCRIPTIONS WHERE ID={description_id}")
        conn.commit()
        conn.close()

    def _delete_stages_by_position_id(self, position_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM INTERVIEWSTAGES WHERE POSITIONID={position_id}")
        conn.commit()
        conn.close()

    def delete_position_by_id(self, position_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM POSITIONS WHERE ID={position_id}")
        position = cur.fetchall()[0]
        self._delete_description_by_description_id(position[8])
        self._delete_stages_by_position_id(position[7])
        cur.execute(f"DELETE FROM POSITIONS WHERE ID={position_id}")
        conn.commit()
        conn.close()

    def get_positions(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM POSITIONS")
        positions = cur.fetchall()
        result = []
        for position in positions:
            result.append(self._parse_position(position, cur))
        conn.close()
        return result

    def get_position_by_id(self, position_id):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM POSITIONS WHERE ID={position_id}")
        position = cur.fetchall()[0]
        position = self._parse_position(position, cur)
        conn.close()
        return position

    def change_position(self, position_id, **kwargs):
        result = -1
        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM POSITIONS WHERE ID={position_id}")
            position_unparsed = cur.fetchall()[0]
            position = self._parse_position(position_unparsed, cur)
            start_date = position['start_date']
            change_date = datetime.date.today()
            position_id = position['id']
            description_id = position_unparsed[8]
            interview_id = position_unparsed[7]
            description_text = kwargs["description"]
            interview_stages = kwargs["interview_stages"]
            position = (position_id, kwargs["owner_id"], kwargs["position_link"], start_date, change_date,
                        kwargs["company_name"], kwargs["position_name"], interview_id, description_id)

            self.delete_position_by_id(position_id)

            description = (description_id, description_text)

            insert_into_positions = f"INSERT INTO POSITIONS (ID, OWNERID, POSITIONLINK, STARTDATE, CHANGEDATE," \
                                    f" COMPANYNAME, POSITIONNAME, INTERVIEWID, DESCCRIPTIONID) " \
                                    f"VALUES (?,?,?,?,?,?,?,?,?);"
            cur.execute(insert_into_positions, position)
            insert_into_description = f"INSERT INTO DESCRIPTIONS (ID, DESCRIPTION) VALUES (?,?);"
            cur.execute(insert_into_description, description)
            insert_into_interview_stages = f"INSERT INTO INTERVIEWSTAGES (POSITIONID, NUMBERINORDER, INTERVIEWTYPE, " \
                                           f"INTERVIEWSTATUS, COMMENT, DATE) VALUES (?,?,?,?,?,?);"
            for i in range(len(interview_stages)):
                interview_stage = (interview_id, i, interview_stages[i]["type"], interview_stages[i]["status"],
                                   interview_stages[i]["comment"], interview_stages[i]["date"])
                cur.execute(insert_into_interview_stages, interview_stage)
            conn.commit()
            conn.close()
            result = position_id

        except Exception:
            result = -1

        finally:
            return result



    def add_position(self, **kwargs):
        result = -1
        try:
            start_date = datetime.date.today()
            change_date = datetime.date.today()
            position_id = self._next_id("ID")
            description_id = self._next_id("DESCCRIPTIONID")
            interview_id = self._next_id("INTERVIEWID")
            description_text = kwargs["description"]
            interview_stages = kwargs["interview_stages"]
            position = (position_id, kwargs["owner_id"], kwargs["position_link"], start_date, change_date,
                        kwargs["company_name"], kwargs["position_name"], interview_id, description_id)

            description = (description_id, description_text)

            conn = self.connect()
            cur = conn.cursor()

            insert_into_positions = f"INSERT INTO POSITIONS (ID, OWNERID, POSITIONLINK, STARTDATE, CHANGEDATE," \
                                    f" COMPANYNAME, POSITIONNAME, INTERVIEWID, DESCCRIPTIONID) " \
                                    f"VALUES (?,?,?,?,?,?,?,?,?);"
            cur.execute(insert_into_positions, position)
            insert_into_description = f"INSERT INTO DESCRIPTIONS (ID, DESCRIPTION) VALUES (?,?);"
            cur.execute(insert_into_description, description)
            insert_into_interview_stages = f"INSERT INTO INTERVIEWSTAGES (POSITIONID, NUMBERINORDER, INTERVIEWTYPE, " \
                                           f"INTERVIEWSTATUS, COMMENT, DATE) VALUES (?,?,?,?,?,?);"
            for i in range(len(interview_stages)):
                interview_stage = (interview_id, i, interview_stages[i]["type"], interview_stages[i]["status"],
                                   interview_stages[i]["comment"], interview_stages[i]["date"])
                cur.execute(insert_into_interview_stages, interview_stage)
            conn.commit()
            conn.close()
            result = position_id
        except Exception:
            result = -1
        finally:
            return result


# a = SqlServer(DB_NAME)
# interview_stages = [
#     {"type": 'phone',
#      "comment": "It will be exacted!",
#      "date": '2023-09-10'},
#     {"type": 'tech',
#      "comment": "It will be exacted!",
#      "date": '2023-09-15'},
#     {"type": 'hr',
#      "comment": "It will be exacted!",
#      "date": '2023-09-20'}
# ]
# position = {
#     'owner_id': 1,
#     'position_link': 'google.com',
#     'company_name': 'intel',
#     'position_name': 'programmer',
#     'description': 'it`s good position',
#     'interview_stages': interview_stages
#     }
# # print(a.add_position(**position))
# result = a.get_positions()
# print(result)

# conn = sqlite3.connect(DB_NAME)
# conn.execute("DROP TABLE POSITIONS")
# conn.execute("DROP TABLE DESCRIPTIONS")
# conn.execute("DROP TABLE INTERVIEWSTAGES")
# conn.close()